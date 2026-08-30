//! Whole-file local drafts for the vfs write path.
//!
//! Pure disk logic: no HTTP, no async, no knowledge of `cache.rs`/`tree.rs`/
//! `vfs.rs`. Task 7's facade wraps a single instance in a `Mutex`, so every
//! method here takes `&mut self` even where a shared reference would
//! technically do — same rationale as `BlockCache` in `cache.rs`.
//!
//! Layout on disk, under `<cache_root>/drafts`: one subdirectory per remote
//! file being edited, named with the first 16 hex chars of
//! `sha256(remote_path)` (same sharding idiom as `cache.rs`), containing:
//! - `data`: the full local file content.
//! - `draft.json`: `{remote_path, base_etag, size, state, last_write_unix}`.
//!
//! Drafts are deliberately NOT part of `BlockCache`: keeping them in a
//! separate root means a draft pending upload can never be evicted by the
//! block cache's LRU (spec §5, "pending drafts exempt from LRU" holds by
//! construction — see D1 in the phase-2 write-path plan).

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Idle time after the last write before the (Task 8) background flusher
/// moves a draft still in `Editing` to `Pending` and queues it for upload.
/// Lives here, not in the flusher, because the draft lifecycle it gates is
/// owned by this module.
pub const WRITEBACK_DEBOUNCE: Duration = Duration::from_secs(2);

/// Where a draft is in its life. `Editing`: locally modified, not yet
/// queued. `Pending`: queued for upload, not yet in flight. `Uploading`: an
/// upload attempt is currently in progress. See `DraftStore::open` for why
/// `Uploading` never survives a restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DraftState {
    Editing,
    Pending,
    Uploading,
}

/// How `begin` seeds a new draft's content.
pub enum DraftInit {
    /// A brand new (or O_TRUNC-truncated) file: no content to fetch.
    Empty,
    /// A pre-downloaded copy of the remote file's current content, already
    /// sitting somewhere on disk (materialization happens in the Task 7
    /// facade, via the phase-1 read path). `begin` MOVES it into the draft
    /// directory rather than copying: the caller's temp file is consumed.
    Materialized(PathBuf),
}

/// What `draft.json` holds, verbatim.
#[derive(Debug, Serialize, Deserialize)]
struct DraftMeta {
    remote_path: String,
    base_etag: String,
    size: u64,
    state: DraftState,
    last_write_unix: i64,
}

/// In-memory state for one draft. Mirrors `DraftMeta` plus nothing else —
/// unlike `BlockCache`'s `Entry`, a draft has no derived/recency state, so
/// this struct exists only to avoid re-parsing JSON on every accessor call.
struct Entry {
    remote_path: String,
    base_etag: String,
    size: u64,
    state: DraftState,
    last_write_unix: i64,
}

pub struct DraftStore {
    root: PathBuf,
    /// Keyed by the same hex hash used for the on-disk directory name (see
    /// module docs), not by `remote_path` directly — recomputing the hash
    /// from a path is cheap and keeps this in lockstep with `BlockCache`'s
    /// equivalent map in `cache.rs`.
    entries: HashMap<String, Entry>,
}

impl DraftStore {
    /// Rebuilds draft state by scanning every `draft.json` under `root`, so
    /// drafts survive an app restart. A directory whose `draft.json` is
    /// missing or unreadable is dropped entirely (its `data` file, if any,
    /// is unusable without the metadata describing it) rather than failing
    /// `open()` — same policy as `BlockCache::open`.
    ///
    /// A draft found in `Uploading` is demoted to `Pending` here, in memory
    /// AND on disk. `Uploading` only ever means "an upload attempt is in
    /// flight in this process"; if the process died mid-upload, nothing
    /// confirms whether the server actually received the bytes, so the
    /// draft must never be treated as "already handled" on the next
    /// launch — it must be retried, not silently lost. Demoting back to
    /// `Pending` puts it back in the queue Task 8's flusher drains.
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("failed to create drafts root {}", root.display()))?;

        let mut entries = HashMap::new();
        for dir_entry in fs::read_dir(root)
            .with_context(|| format!("failed to read drafts root {}", root.display()))?
        {
            let dir_entry = dir_entry?;
            if !dir_entry.file_type()?.is_dir() {
                continue;
            }
            let hash = dir_entry.file_name().to_string_lossy().into_owned();
            let meta_path = dir_entry.path().join("draft.json");
            let Ok(raw) = fs::read(&meta_path) else {
                tracing::warn!(hash = %hash, "drafts: dropping entry with no draft.json");
                delete_orphaned_dir(&hash, &dir_entry.path());
                continue;
            };
            let mut meta: DraftMeta = match serde_json::from_slice(&raw) {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::warn!(hash = %hash, %err, "drafts: dropping unreadable draft.json");
                    delete_orphaned_dir(&hash, &dir_entry.path());
                    continue;
                }
            };

            let demoted = meta.state == DraftState::Uploading;
            if demoted {
                meta.state = DraftState::Pending;
            }

            let entry = Entry {
                remote_path: meta.remote_path.clone(),
                base_etag: meta.base_etag.clone(),
                size: meta.size,
                state: meta.state,
                last_write_unix: meta.last_write_unix,
            };

            if demoted {
                // Persist the demotion immediately: every mutation of a
                // draft's state is written atomically, and being found
                // mid-upload after a crash is itself a state change.
                write_meta(root, &hash, &entry)?;
            }

            entries.insert(hash, entry);
        }

        Ok(Self { root: root.to_path_buf(), entries })
    }

    /// Starts (or restarts) a draft for `remote_path`. Overwrites any
    /// existing draft at the same path, since the caller (the facade)
    /// only calls `begin` for a fresh create/open-for-write.
    pub fn begin(&mut self, remote_path: &str, base_etag: &str, initial: DraftInit) -> Result<()> {
        let hash = hash16(remote_path);
        let dir = self.entry_dir(&hash);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create draft dir {}", dir.display()))?;
        let data_path = self.data_path_for_hash(&hash);

        let size = match initial {
            DraftInit::Empty => {
                // create+truncate: a re-`begin` on a path that already had
                // a draft must discard whatever content was there before.
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&data_path)
                    .with_context(|| format!("failed to create {}", data_path.display()))?;
                0
            }
            DraftInit::Materialized(src) => {
                move_into(&src, &data_path)?;
                fs::metadata(&data_path)
                    .with_context(|| format!("failed to stat {}", data_path.display()))?
                    .len()
            }
        };

        let entry = Entry {
            remote_path: remote_path.to_string(),
            base_etag: base_etag.to_string(),
            size,
            state: DraftState::Editing,
            last_write_unix: now_unix(),
        };
        write_meta(&self.root, &hash, &entry)?;
        self.entries.insert(hash, entry);
        Ok(())
    }

    /// Writes `data` at `offset`, extending the draft's logical size if the
    /// write reaches past its current end. Extension relies on ordinary
    /// sparse-file semantics: seeking past the current end of a regular
    /// file and writing there reads back as zeros in the gap, on every
    /// platform this crate targets — the same assumption `BlockCache`
    /// makes for its `data` file.
    pub fn write(&mut self, remote_path: &str, offset: u64, data: &[u8]) -> Result<()> {
        let hash = hash16(remote_path);
        anyhow::ensure!(self.entries.contains_key(&hash), "no draft open for {remote_path}");

        let data_path = self.data_path_for_hash(&hash);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        drop(file);

        let entry = self.entries.get_mut(&hash).expect("checked above");
        entry.size = entry.size.max(offset + data.len() as u64);
        entry.last_write_unix = now_unix();
        write_meta(&self.root, &hash, entry)?;
        Ok(())
    }

    /// Reads up to `len` bytes starting at `offset`. Like the vfs read
    /// path (and POSIX `read(2)`), a request that reaches past the
    /// current end of the file is silently truncated to whatever remains
    /// rather than erroring or zero-padding — a read entirely at or past
    /// EOF returns an empty result.
    pub fn read(&mut self, remote_path: &str, offset: u64, len: u32) -> Result<Bytes> {
        let hash = hash16(remote_path);
        let Some(entry) = self.entries.get(&hash) else {
            anyhow::bail!("no draft open for {remote_path}");
        };
        if offset >= entry.size {
            return Ok(Bytes::new());
        }
        let want = (len as u64).min(entry.size - offset);

        let data_path = self.data_path_for_hash(&hash);
        let mut file = fs::File::open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; want as usize];
        file.read_exact(&mut buf)
            .with_context(|| format!("failed to read {}", data_path.display()))?;
        Ok(Bytes::from(buf))
    }

    /// Resizes the draft to exactly `size`: extends with zero bytes (a
    /// bigger file) or discards the tail (a smaller one) — `File::set_len`
    /// gives both for free on a regular file.
    pub fn truncate(&mut self, remote_path: &str, size: u64) -> Result<()> {
        let hash = hash16(remote_path);
        anyhow::ensure!(self.entries.contains_key(&hash), "no draft open for {remote_path}");

        let data_path = self.data_path_for_hash(&hash);
        let file = OpenOptions::new()
            .write(true)
            .open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        file.set_len(size)
            .with_context(|| format!("failed to set length of {}", data_path.display()))?;
        drop(file);

        let entry = self.entries.get_mut(&hash).expect("checked above");
        entry.size = size;
        entry.last_write_unix = now_unix();
        write_meta(&self.root, &hash, entry)?;
        Ok(())
    }

    pub fn size(&self, remote_path: &str) -> Option<u64> {
        self.entries.get(&hash16(remote_path)).map(|e| e.size)
    }

    pub fn state(&self, remote_path: &str) -> Option<DraftState> {
        self.entries.get(&hash16(remote_path)).map(|e| e.state.clone())
    }

    /// The draft's last local write time, unix seconds. Used by the facade's
    /// `getattr`/`readdir`/`lookup` overlay (D3): a drafted file's mtime
    /// must reflect the local edit, not the server's last-known timestamp.
    pub fn mtime_unix(&self, remote_path: &str) -> Option<i64> {
        self.entries.get(&hash16(remote_path)).map(|e| e.last_write_unix)
    }

    pub fn set_state(&mut self, remote_path: &str, s: DraftState) -> Result<()> {
        let hash = hash16(remote_path);
        let entry = self
            .entries
            .get_mut(&hash)
            .ok_or_else(|| anyhow::anyhow!("no draft open for {remote_path}"))?;
        entry.state = s;
        write_meta(&self.root, &hash, entry)?;
        Ok(())
    }

    pub fn base_etag(&self, remote_path: &str) -> Option<String> {
        self.entries.get(&hash16(remote_path)).map(|e| e.base_etag.clone())
    }

    /// The path the Uploader reads the draft's bytes from directly.
    pub fn data_path(&self, remote_path: &str) -> Option<PathBuf> {
        let hash = hash16(remote_path);
        self.entries.contains_key(&hash).then(|| self.data_path_for_hash(&hash))
    }

    /// Deletes the draft's whole directory. Idempotent: removing a path
    /// with no draft is a no-op rather than an error, matching
    /// `BlockCache::remove_entry`'s style.
    pub fn remove(&mut self, remote_path: &str) -> Result<()> {
        let hash = hash16(remote_path);
        self.entries.remove(&hash);
        let dir = self.entry_dir(&hash);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .with_context(|| format!("failed to remove draft dir {}", dir.display()))?;
        }
        Ok(())
    }

    /// Moves a draft to a new remote path (Task 10: the file being edited
    /// gets renamed remotely while a draft is open on it).
    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let from_hash = hash16(from);
        let mut entry = self
            .entries
            .remove(&from_hash)
            .ok_or_else(|| anyhow::anyhow!("no draft open for {from}"))?;
        entry.remote_path = to.to_string();

        let to_hash = hash16(to);
        if to_hash == from_hash {
            // Same shard (only possible if `from == to`): nothing to move
            // on disk, just persist the updated `remote_path`.
            write_meta(&self.root, &to_hash, &entry)?;
            self.entries.insert(to_hash, entry);
            return Ok(());
        }

        let from_dir = self.entry_dir(&from_hash);
        let to_dir = self.entry_dir(&to_hash);
        if to_dir.exists() {
            // Renaming onto a path that already has its own draft: the
            // destination's old draft is being overwritten by this one,
            // same as `begin` overwriting an existing draft.
            fs::remove_dir_all(&to_dir)
                .with_context(|| format!("failed to clear {}", to_dir.display()))?;
        }
        fs::rename(&from_dir, &to_dir).with_context(|| {
            format!("failed to rename {} to {}", from_dir.display(), to_dir.display())
        })?;

        write_meta(&self.root, &to_hash, &entry)?;
        self.entries.insert(to_hash, entry);
        Ok(())
    }

    /// Remote paths of every draft waiting to be uploaded or currently
    /// being uploaded — what Task 8's flusher and startup resume loop
    /// iterate over.
    pub fn pending(&self) -> Vec<String> {
        self.entries
            .values()
            .filter(|e| matches!(e.state, DraftState::Pending | DraftState::Uploading))
            .map(|e| e.remote_path.clone())
            .collect()
    }

    fn entry_dir(&self, hash: &str) -> PathBuf {
        self.root.join(hash)
    }

    fn data_path_for_hash(&self, hash: &str) -> PathBuf {
        self.entry_dir(hash).join("data")
    }
}

/// Directory name for a remote path: first 16 hex chars (8 bytes) of
/// `sha256(remote_path)`. A filesystem shard key, not a security boundary —
/// same rationale, and same truncation, as `cache.rs`'s `hash_key`.
fn hash16(remote_path: &str) -> String {
    let digest = Sha256::digest(remote_path.as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

fn write_meta(root: &Path, hash: &str, entry: &Entry) -> Result<()> {
    let meta = DraftMeta {
        remote_path: entry.remote_path.clone(),
        base_etag: entry.base_etag.clone(),
        size: entry.size,
        state: entry.state.clone(),
        last_write_unix: entry.last_write_unix,
    };
    let json = serde_json::to_vec(&meta)?;
    let dir = root.join(hash);
    let path = dir.join("draft.json");
    // Write-temp-then-rename: same atomicity idiom as cache.rs's
    // `write_meta` — a crash between the write and the rename leaves
    // either the previous draft.json or a stray `.tmp` file, never a torn
    // one.
    let tmp_path = dir.join("draft.json.tmp");
    fs::write(&tmp_path, json)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, &path).with_context(|| {
        format!("failed to rename {} to {}", tmp_path.display(), path.display())
    })
}

/// Moves `from` into `to` by renaming; if that fails (e.g. `from` sits on a
/// different volume than `to` — not expected in practice, since the cache
/// root and drafts root share a volume, but this must not crash if it ever
/// happens), falls back to copy-then-delete.
fn move_into(from: &Path, to: &Path) -> Result<()> {
    if fs::rename(from, to).is_ok() {
        return Ok(());
    }
    fs::copy(from, to)
        .with_context(|| format!("failed to copy {} to {}", from.display(), to.display()))?;
    fs::remove_file(from)
        .with_context(|| format!("failed to remove {} after copying", from.display()))?;
    Ok(())
}

/// Deletes a draft directory found unusable during `open()`'s scan.
/// Best-effort: a failure to delete only logs, matching
/// `cache.rs`'s `delete_orphaned_entry_dir`.
fn delete_orphaned_dir(hash: &str, dir: &Path) {
    if let Err(err) = fs::remove_dir_all(dir) {
        tracing::warn!(hash = %hash, %err, "drafts: failed to delete an orphaned draft directory");
    }
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn a_write_reads_back_identically_at_its_offset() {
        let dir = TempDir::new().unwrap();
        let mut s = DraftStore::open(dir.path()).unwrap();
        s.begin("docs/report.txt", "etag-1", DraftInit::Empty).unwrap();

        let payload = b"hello draft world";
        s.write("docs/report.txt", 100, payload).unwrap();

        assert_eq!(s.size("docs/report.txt"), Some(100 + payload.len() as u64));
        let back = s.read("docs/report.txt", 100, payload.len() as u32).unwrap();
        assert_eq!(back.as_ref(), &payload[..]);

        // The gap before the write must read back as zeros, not garbage.
        let gap = s.read("docs/report.txt", 0, 100).unwrap();
        assert_eq!(gap.as_ref(), &vec![0u8; 100][..]);

        // A read reaching past the end is truncated to what's actually
        // there, not padded or errored.
        let tail = s.read("docs/report.txt", 100 + payload.len() as u64 - 3, 50).unwrap();
        assert_eq!(tail.as_ref(), &payload[payload.len() - 3..]);
    }

    #[test]
    fn truncate_extends_with_zeros_and_shrinks() {
        let dir = TempDir::new().unwrap();
        let mut s = DraftStore::open(dir.path()).unwrap();
        s.begin("notes.md", "etag-1", DraftInit::Empty).unwrap();
        s.write("notes.md", 0, b"abcdef").unwrap();
        assert_eq!(s.size("notes.md"), Some(6));

        s.truncate("notes.md", 20).unwrap();
        assert_eq!(s.size("notes.md"), Some(20));
        let extended = s.read("notes.md", 6, 14).unwrap();
        assert_eq!(extended.as_ref(), &vec![0u8; 14][..], "extension must be zero-filled");
        let head = s.read("notes.md", 0, 6).unwrap();
        assert_eq!(head.as_ref(), b"abcdef", "original bytes must survive the extension");

        s.truncate("notes.md", 3).unwrap();
        assert_eq!(s.size("notes.md"), Some(3));
        let shrunk = s.read("notes.md", 0, 100).unwrap();
        assert_eq!(shrunk.as_ref(), b"abc", "shrink must discard the tail, keep the head");
    }

    #[test]
    fn reopening_restores_sizes_and_states() {
        let dir = TempDir::new().unwrap();
        {
            let mut s = DraftStore::open(dir.path()).unwrap();
            s.begin("a.txt", "etag-a", DraftInit::Empty).unwrap();
            s.write("a.txt", 0, b"twelve bytes").unwrap();

            s.begin("b.txt", "etag-b", DraftInit::Empty).unwrap();
            s.write("b.txt", 0, b"seven!!").unwrap();
            s.set_state("b.txt", DraftState::Pending).unwrap();
        }

        // Simulates a process restart: a fresh `DraftStore` scanning the
        // same root must see exactly what was persisted, nothing rebuilt
        // from in-memory state.
        let s = DraftStore::open(dir.path()).unwrap();
        assert_eq!(s.size("a.txt"), Some(12));
        assert_eq!(s.state("a.txt"), Some(DraftState::Editing));
        assert_eq!(s.base_etag("a.txt"), Some("etag-a".to_string()));

        assert_eq!(s.size("b.txt"), Some(7));
        assert_eq!(s.state("b.txt"), Some(DraftState::Pending));
    }

    #[test]
    fn remove_deletes_the_draft_directory() {
        let dir = TempDir::new().unwrap();
        let mut s = DraftStore::open(dir.path()).unwrap();
        s.begin("gone.bin", "etag-1", DraftInit::Empty).unwrap();
        let data_path = s.data_path("gone.bin").unwrap();
        assert!(data_path.exists());
        let entry_dir = data_path.parent().unwrap().to_path_buf();

        s.remove("gone.bin").unwrap();

        assert!(!entry_dir.exists(), "the whole draft directory must be gone");
        assert_eq!(s.size("gone.bin"), None);
        assert_eq!(s.state("gone.bin"), None);
        assert_eq!(s.data_path("gone.bin"), None);
    }

    #[test]
    fn an_uploading_draft_found_on_open_is_demoted_to_pending() {
        let dir = TempDir::new().unwrap();
        {
            let mut s = DraftStore::open(dir.path()).unwrap();
            s.begin("crash-me.txt", "etag-1", DraftInit::Empty).unwrap();
            s.write("crash-me.txt", 0, b"in flight").unwrap();
            s.set_state("crash-me.txt", DraftState::Uploading).unwrap();
            // No `remove` / success path reached: simulates the process
            // dying mid-upload, with `draft.json` still saying `Uploading`.
        }

        // A crash mid-upload never confirms whether the server actually
        // received the bytes, so the draft must be retried rather than
        // trusted as already handled: `open()` must demote it back to
        // `Pending`, putting it back in the upload queue.
        let s = DraftStore::open(dir.path()).unwrap();
        assert_eq!(
            s.state("crash-me.txt"),
            Some(DraftState::Pending),
            "a draft found Uploading after a restart must be demoted, never left/lost"
        );
        assert_eq!(s.pending(), vec!["crash-me.txt".to_string()]);
    }
}
