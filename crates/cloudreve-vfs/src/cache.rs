//! On-disk block cache.
//!
//! Pure disk logic: no HTTP, no async, no knowledge of `tree.rs`/`vfs.rs`.
//! Task 6's facade wraps a single instance in a `Mutex`, so every method
//! here takes `&mut self` even where a shared reference would technically
//! do — that keeps the locking story at the facade simple (one lock, one
//! writer at a time) instead of splitting it between interior mutability
//! here and a mutex there.
//!
//! Layout on disk, under the cache root: one subdirectory per (remote
//! path, etag) PAIR, named with the first 16 hex chars of
//! `sha256(remote_path || '\0' || etag)`, containing:
//! - `data`: a sparse file, block `i` written at offset `i * BLOCK_SIZE`.
//! - `meta.json`: `{"remote_path", "etag", "blocks": [u64...],
//!   "last_used_unix": i64}`.
//!
//! ## Phase 4 (this task): the etag ping-pong fix
//!
//! Before this task, the directory name depended on `remote_path` ALONE,
//! never the etag: a file that got a new etag reused the SAME directory
//! (after the stale contents were dropped by `read_block`/`write_block`'s
//! own etag-mismatch check), specifically to avoid leaking an orphaned
//! directory under the old etag's hash. That single-directory-per-path
//! design has a fatal flaw once TWO live handles disagree about which
//! etag is current — exactly what happens when one handle opened before a
//! remote rewrite and another opens after it, both still live: every read
//! under either handle's (now-differing) etag found the OTHER handle's
//! etag sitting in the shared directory, purged it, and re-fetched — and
//! the NEXT read from the other handle did the same thing right back. An
//! unbounded ping-pong of evict-then-refetch, one download per read,
//! alternating forever between the two handles.
//!
//! The fix: the directory identity now includes the etag, so two different
//! (path, etag) pairs simply never collide — each handle's cache entry
//! lives in its own directory and neither can evict the other's blocks by
//! reading under a different etag. `read_block`/`write_block` no longer
//! need (or have) an etag-mismatch-triggers-purge branch at all: a
//! mismatched etag is now just an ordinary cache MISS (a different
//! directory that doesn't exist yet), not something to actively tear down.
//!
//! This reintroduces the exact orphan-leak the old design deliberately
//! avoided — an old etag's directory now genuinely outlives its content
//! becoming stale, until something reclaims it. Two things do:
//! - `purge` (called by the write-back queue the moment ITS OWN upload
//!   replaces a file's content) now removes every entry for a given
//!   `remote_path`, across every etag it happens to be cached under, not
//!   just the one at a single, pre-computed directory — see its doc.
//! - Absent an explicit purge (e.g. the rewrite came from somewhere else
//!   entirely, not this app's own write-back queue), an orphaned old-etag
//!   entry is reclaimed the same way any other cold entry is: ordinary LRU
//!   eviction once the cache is over `max_bytes`. This is a real, disclosed
//!   trade-off — a drive that never gets read again might keep an
//!   old-etag entry around indefinitely if the cache never fills up — but
//!   is bounded and self-healing, unlike the ping-pong it replaces.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Every block, including the final one of a file, is capped at this size.
/// 1 MiB balances HTTP range-request overhead (too small = too many
/// requests) against wasted re-download on a partial-block cache miss (too
/// large = re-fetching a whole block for one changed byte).
pub const BLOCK_SIZE: u64 = 1_048_576;

/// Identifies one version of one remote file. Phase 4 (this task): the
/// on-disk directory identity is now `(remote_path, etag)` TOGETHER (see
/// module docs) — a `FileKey` is that identity, not just a lookup
/// convenience over a path-only directory any more.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileKey {
    pub remote_path: String,
    pub etag: String,
}

/// What `meta.json` holds. `remote_path` (phase 4, this task: newly added)
/// is what lets `purge` find every entry for a path without already
/// knowing which etags it's cached under — the directory name alone no
/// longer reveals `remote_path` now that it also folds in the etag (see
/// module docs). `#[serde(default)]`: an on-disk `meta.json` from before
/// this task (or, in principle, a directory this parser can't otherwise
/// account for) has no such field; deserializing it to an empty string
/// rather than failing outright degrades to "this entry never matches any
/// `purge` call" — a lingering-until-evicted entry, never data loss, never
/// a hard `open()` failure over one old file.
#[derive(Debug, Serialize, Deserialize)]
struct MetaFile {
    #[serde(default)]
    remote_path: String,
    etag: String,
    blocks: Vec<u64>,
    last_used_unix: i64,
}

/// In-memory state for one cached file.
struct Entry {
    remote_path: String,
    etag: String,
    /// Which block indices are actually present in `data`. Absence is not
    /// an error case: a file's blocks are typically filled in one at a
    /// time as they're read on demand, so most indices below the highest
    /// one ever written may legitimately be missing.
    blocks: BTreeSet<u64>,
    /// Persisted so recency order survives a restart (`open()` re-derives
    /// `seq` from this). Second resolution, so two accesses in the same
    /// wall-clock second are indistinguishable on disk — `seq` is what
    /// actually orders eviction within a process; this field exists only
    /// to seed that order from a fresh `open()`.
    last_used_unix: i64,
    /// Monotonic recency counter, process-local. Wall-clock seconds are
    /// too coarse to order two accesses that land in the same second (a
    /// realistic case in tests and under load), so eviction picks the
    /// LRU victim by comparing `seq`, not `last_used_unix`.
    seq: u64,
}

pub struct BlockCache {
    root: PathBuf,
    max_bytes: u64,
    /// Keyed by the same hex hash used for the on-disk directory name, not
    /// by `remote_path` — the two are equivalent as lookup keys (the hash
    /// is a pure function of the path), and using the hash means `open()`
    /// never needs to recover `remote_path` from disk.
    entries: HashMap<String, Entry>,
    /// Live-handle refcounts, keyed by the same hash as `entries` but kept
    /// as a wholly separate map: a `retain()` for a file with nothing
    /// cached yet (a fresh open, before its first block ever lands) must
    /// still be visible the instant `write_block` creates the entry, and
    /// an `Entry`-embedded flag can't record that ahead of the `Entry`
    /// existing. A hash present in this map always has a count > 0;
    /// `release` removes the key rather than leaving a `0` around, so
    /// `evict_if_over_budget` only ever has to ask "is this hash retained
    /// at all", never compare a count. In memory only, on purpose: retains
    /// model live handles for the current process, which restart always
    /// closes, so unlike `blocks`/`etag` they are never written to
    /// meta.json and never restored by `open()`.
    retains: HashMap<String, u32>,
    next_seq: u64,
}

impl BlockCache {
    /// Rebuilds cache state by scanning every `meta.json` under `root`, so
    /// an app restart doesn't discard blocks fetched in a previous run. A
    /// directory whose `meta.json` is missing or unreadable is treated as
    /// not part of the cache (skipped, left alone on disk) rather than
    /// failing `open()` outright: a corrupt entry should degrade to a
    /// cache miss, not take down the whole cache.
    pub fn open(root: &Path, max_bytes: u64) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("failed to create cache root {}", root.display()))?;

        let mut loaded: Vec<(String, Entry)> = Vec::new();
        for dir_entry in fs::read_dir(root)
            .with_context(|| format!("failed to read cache root {}", root.display()))?
        {
            let dir_entry = dir_entry?;
            if !dir_entry.file_type()?.is_dir() {
                continue;
            }
            let hash = dir_entry.file_name().to_string_lossy().into_owned();
            let meta_path = dir_entry.path().join("meta.json");
            let Ok(raw) = fs::read(&meta_path) else {
                // No meta.json (fresh dir, or a crash before write_meta's
                // rename ever landed one): the data directory, if any, is
                // unusable without its etag/block index and would
                // otherwise sit on disk forever, invisible to
                // `used_bytes`/eviction. Delete the whole entry rather than
                // just skipping it.
                delete_orphaned_entry_dir(&hash, &dir_entry.path());
                continue;
            };
            let meta: MetaFile = match serde_json::from_slice(&raw) {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::warn!(hash = %hash, %err, "cache: dropping unreadable meta.json");
                    // Same reasoning as the missing-meta.json case above: a
                    // meta.json that fails to parse (torn by a crash, or
                    // otherwise corrupt) leaves its data unusable, so the
                    // whole entry directory must go, not just be skipped.
                    delete_orphaned_entry_dir(&hash, &dir_entry.path());
                    continue;
                }
            };
            loaded.push((
                hash,
                Entry {
                    remote_path: meta.remote_path,
                    etag: meta.etag,
                    blocks: meta.blocks.into_iter().collect(),
                    last_used_unix: meta.last_used_unix,
                    seq: 0, // assigned below, in last-used order
                },
            ));
        }

        // Hand out sequence numbers in persisted-recency order so eviction
        // right after a restart still picks the true LRU entry, even
        // though same-second timestamps can't be told apart by value alone.
        loaded.sort_by_key(|(_, entry)| entry.last_used_unix);
        let mut entries = HashMap::with_capacity(loaded.len());
        for (seq, (hash, mut entry)) in loaded.into_iter().enumerate() {
            entry.seq = seq as u64;
            entries.insert(hash, entry);
        }
        let next_seq = entries.len() as u64;

        // Retains are handle liveness, not persisted state: a fresh
        // `open()` always starts with nothing retained, even if the
        // process that crashed had files open (see the field doc above).
        Ok(Self { root: root.to_path_buf(), max_bytes, entries, retains: HashMap::new(), next_seq })
    }

    /// `None` if the block is absent for this EXACT `(remote_path, etag)`
    /// pair. Phase 4 (this task): no longer actively purges anything on a
    /// mismatch — the directory identity now folds in the etag (see module
    /// docs), so a different etag is simply a different, possibly-not-yet-
    /// existing directory, an ordinary cache miss like any other.
    pub fn read_block(&mut self, key: &FileKey, block_idx: u64) -> Result<Option<Bytes>> {
        let hash = hash_key(&key.remote_path, &key.etag);

        let Some(entry) = self.entries.get(&hash) else {
            return Ok(None);
        };
        if !entry.blocks.contains(&block_idx) {
            return Ok(None);
        }

        let max_idx = *entry.blocks.iter().max().expect("checked non-empty above");
        let data_path = self.data_path(&hash);
        let mut file = File::open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        let file_len = file.metadata()?.len();
        let offset = block_idx * BLOCK_SIZE;
        // Only the highest-indexed block of a file can be short (the
        // file's final, partial block); every other block was always
        // written with a full BLOCK_SIZE payload, so its length never
        // needs to be measured off the file.
        let len = if block_idx == max_idx {
            file_len.saturating_sub(offset).min(BLOCK_SIZE)
        } else {
            BLOCK_SIZE
        };
        let mut buf = vec![0u8; len as usize];
        file.seek(SeekFrom::Start(offset))?;
        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                // A crash left meta.json claiming this block is present
                // (at its full expected length) while the data file on
                // disk was never fully flushed to match. Per spec, cache
                // corruption is a miss, not an error: self-heal by
                // dropping the lying index so nothing keeps claiming a
                // block that isn't really there, and let the caller
                // re-download and rewrite it like any other miss.
                self.drop_block(&hash, block_idx)?;
                return Ok(None);
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to read {}", data_path.display()));
            }
        }

        // Recency-only update: bump `seq`/`last_used_unix` in memory but do
        // NOT persist meta.json here. The block SET didn't change, so there
        // is nothing on disk that needs to change either — persisting would
        // mean one meta.json rewrite per cached read, which is pure SSD wear
        // for a hot file. meta.json is written only when the block set
        // itself changes (write_block/drop_block/eviction); see `touch`'s
        // doc for the resulting reopen trade-off.
        self.touch(&hash);

        Ok(Some(Bytes::from(buf)))
    }

    /// Writes one block, creating the entry if needed. Whether that entry
    /// is evictable is decided purely by `self.retains` (see its field
    /// doc), never by an argument here — so a key retained *before* its
    /// first block ever lands is already safe the moment this call creates
    /// the entry, with no separate "pin the entry I just created" step
    /// that could race the eviction sweep at the end of this same call.
    /// That sweep runs inside this call, against a brand new entry, so the
    /// atomicity is what makes self-eviction on a retained file's very
    /// first write impossible even when `max_bytes` is already over budget
    /// with nothing else to evict — `an_open_files_first_write_never_evicts_itself`
    /// pins this down.
    pub fn write_block(&mut self, key: &FileKey, block_idx: u64, data: &[u8]) -> Result<()> {
        let hash = hash_key(&key.remote_path, &key.etag);

        let dir = self.entry_dir(&hash);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create cache entry dir {}", dir.display()))?;
        let data_path = self.data_path(&hash);
        // No `truncate`: writing block N must never erase blocks the file
        // already has cached. Seeking past the current end (a fresh entry,
        // or a block index beyond what has been written so far) is exactly
        // what makes `data` a sparse file.
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        file.seek(SeekFrom::Start(block_idx * BLOCK_SIZE))?;
        file.write_all(data)?;
        drop(file);

        let seq = self.next_seq;
        self.next_seq += 1;
        let entry = self.entries.entry(hash.clone()).or_insert_with(|| Entry {
            remote_path: key.remote_path.clone(),
            etag: key.etag.clone(),
            blocks: BTreeSet::new(),
            last_used_unix: 0,
            seq: 0,
        });
        entry.etag = key.etag.clone();
        entry.blocks.insert(block_idx);
        entry.last_used_unix = now_unix();
        entry.seq = seq;

        let entry = self.entries.get(&hash).expect("just inserted/updated above");
        self.write_meta(&hash, entry)?;

        self.evict_if_over_budget()?;
        Ok(())
    }

    /// Registers a live file handle: while retained, this key's entry (or
    /// the entry `write_block` creates for it later, if none exists yet)
    /// is never evicted. Two handles on the same file at the SAME etag
    /// (e.g. Finder holding it open while Quick Look previews it) each get
    /// their own call, and the entry survives until both release — see
    /// `release`. Keyed on the same `(remote_path, etag)` hash as `entries`
    /// (phase 4, this task — previously `remote_path` alone): each open
    /// handle's `FileKey` is fixed at open time for its whole lifetime
    /// (`OpenFile` in the facade never mutates it), and now that a
    /// different etag is a genuinely different directory rather than
    /// in-place churn of a shared one (see module docs), there is no
    /// longer any "entry churn under a live handle" for this to need to
    /// survive independently of the entry it protects.
    pub fn retain(&mut self, key: &FileKey) {
        let hash = hash_key(&key.remote_path, &key.etag);
        *self.retains.entry(hash).or_insert(0) += 1;
    }

    /// Drops one handle registered by `retain`. Only once the count reaches
    /// zero (every handle closed) does the key stop being protected from
    /// eviction — a still-live second handle must never see the entry
    /// evicted out from under it just because a different handle closed
    /// first.
    pub fn release(&mut self, key: &FileKey) {
        let hash = hash_key(&key.remote_path, &key.etag);
        if let std::collections::hash_map::Entry::Occupied(mut occ) = self.retains.entry(hash) {
            *occ.get_mut() -= 1;
            if *occ.get() == 0 {
                occ.remove();
            }
        }
    }

    /// Sum of actual stored block bytes across every entry — NOT
    /// `blocks.len() * BLOCK_SIZE`. A file's last block is very often
    /// shorter than BLOCK_SIZE (any file whose size isn't an exact
    /// multiple of it), and counting it as a full block would both lie
    /// about disk usage and make `max_bytes` stricter than the bytes it
    /// actually names. Cost: one `stat()` per entry, since the short
    /// length is derived from the real file size rather than tracked
    /// separately — cheap at the entry counts this cache deals with, and
    /// it avoids a second piece of state that could drift from disk.
    pub fn used_bytes(&self) -> u64 {
        self.entries.iter().map(|(hash, entry)| self.entry_bytes_on_disk(hash, entry)).sum()
    }

    /// Bumps an entry's recency (`seq` and `last_used_unix`) in memory only
    /// — deliberately never writes meta.json. `seq` is what eviction
    /// actually orders by, and it never needs to survive a restart exactly:
    /// `last_used_unix` is only persisted (and only accurate as of) the
    /// last time the block SET changed (`write_block`/`drop_block`/
    /// eviction), so a restart right after a long run of cache hits seeds
    /// `seq` from a stale-ish timestamp. The result is early eviction being
    /// slightly less accurate immediately after a restart — never data
    /// loss or a wrong cache hit — which is a fine trade for not rewriting
    /// meta.json on every cached read.
    fn touch(&mut self, hash: &str) {
        let seq = self.next_seq;
        self.next_seq += 1;
        if let Some(entry) = self.entries.get_mut(hash) {
            entry.last_used_unix = now_unix();
            entry.seq = seq;
        }
    }

    /// Deletes unretained entries, oldest-`seq` (least recently used)
    /// first, until total usage fits `max_bytes` again. Stops rather than
    /// errors if everything left is retained and still over budget:
    /// a live handle is a hard guarantee to the caller, so this cache
    /// trades staying over budget for never evicting a file someone has
    /// open.
    fn evict_if_over_budget(&mut self) -> Result<()> {
        while self.used_bytes() > self.max_bytes {
            let victim = self
                .entries
                .iter()
                .filter(|(hash, _)| !self.retains.contains_key(*hash))
                .min_by_key(|(_, entry)| entry.seq)
                .map(|(hash, _)| hash.clone());
            match victim {
                Some(hash) => self.remove_entry(&hash)?,
                None => break,
            }
        }
        Ok(())
    }

    fn entry_bytes_on_disk(&self, hash: &str, entry: &Entry) -> u64 {
        let Some(&max_idx) = entry.blocks.iter().max() else {
            return 0;
        };
        // Every block below the highest index is always a full BLOCK_SIZE
        // (see the comment in `read_block`); only the highest one can be
        // short, and its real length is read off the file on disk.
        let full_blocks = entry.blocks.len() as u64 - 1;
        let file_len = fs::metadata(self.data_path(hash)).map(|m| m.len()).unwrap_or(0);
        let last_len = file_len.saturating_sub(max_idx * BLOCK_SIZE).min(BLOCK_SIZE);
        full_blocks * BLOCK_SIZE + last_len
    }

    fn write_meta(&self, hash: &str, entry: &Entry) -> Result<()> {
        let meta = MetaFile {
            remote_path: entry.remote_path.clone(),
            etag: entry.etag.clone(),
            blocks: entry.blocks.iter().copied().collect(),
            last_used_unix: entry.last_used_unix,
        };
        let json = serde_json::to_vec(&meta)?; // machine-only file, no need for pretty-printing
        let path = self.meta_path(hash);
        // Write-temp-then-rename: a crash between the write and the rename
        // leaves either the previous meta.json (untouched) or a stray
        // `.tmp` file behind, but never a torn/partially-written
        // `meta.json` — `fs::rename` within the same directory is atomic,
        // unlike a direct `fs::write`, which a crash mid-syscall can leave
        // truncated and unparseable.
        let tmp_path = self.entry_dir(hash).join("meta.json.tmp");
        fs::write(&tmp_path, json)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &path).with_context(|| {
            format!("failed to rename {} to {}", tmp_path.display(), path.display())
        })
    }

    /// Drops one block index from an entry's in-memory set AND from its
    /// persisted meta.json, so a corrupt/truncated block never keeps being
    /// claimed as present across reads or after a restart. Leaves the rest
    /// of the entry (its other blocks, pin state) untouched — this is a
    /// per-block self-heal, not a whole-entry eviction.
    fn drop_block(&mut self, hash: &str, block_idx: u64) -> Result<()> {
        if let Some(entry) = self.entries.get_mut(hash) {
            entry.blocks.remove(&block_idx);
        }
        if let Some(entry) = self.entries.get(hash) {
            self.write_meta(hash, entry)?;
        }
        Ok(())
    }

    /// Drops every cached block of `remote_path`, whatever etag they were
    /// stored under, deleting each entry directory on disk too. Idempotent: a
    /// path with nothing cached is a no-op. Deliberately ignores `retains` —
    /// unlike eviction, a purge means the stored bytes are known stale (the
    /// write-back queue calls this the moment an upload replaces the file's
    /// remote content), and a live handle must never shield bytes that no
    /// longer match anything on the server. The retain count itself is left
    /// untouched, so the handle's eventual `release` still balances.
    ///
    /// Phase 4 (this task): iterates every entry looking for one matching
    /// `remote_path` — a single `hash_key(remote_path, etag)` lookup is no
    /// longer possible without already knowing which etag(s) to ask for,
    /// now that the directory identity folds in the etag (see module docs).
    /// A caller with only `remote_path` (this method's only caller,
    /// `WriteBackQueue`, doesn't track every etag a path was ever cached
    /// under) genuinely needs to purge ALL of them, which this now does —
    /// strictly more thorough than the old single-directory purge, and
    /// exactly what reclaims an old-etag entry's disk space once this
    /// process's own upload is known to have made it stale.
    pub fn purge(&mut self, remote_path: &str) -> Result<()> {
        let hashes: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.remote_path == remote_path)
            .map(|(hash, _)| hash.clone())
            .collect();
        for hash in hashes {
            self.remove_entry(&hash)?;
        }
        Ok(())
    }

    fn remove_entry(&mut self, hash: &str) -> Result<()> {
        self.entries.remove(hash);
        let dir = self.entry_dir(hash);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .with_context(|| format!("failed to remove cache entry {}", dir.display()))?;
        }
        Ok(())
    }

    fn entry_dir(&self, hash: &str) -> PathBuf {
        self.root.join(hash)
    }

    fn data_path(&self, hash: &str) -> PathBuf {
        self.entry_dir(hash).join("data")
    }

    fn meta_path(&self, hash: &str) -> PathBuf {
        self.entry_dir(hash).join("meta.json")
    }
}

/// Directory name for a `(remote_path, etag)` pair (phase 4, this task —
/// previously `remote_path` alone, see module docs): first 16 hex chars (8
/// bytes) of `sha256(remote_path || '\0' || etag)`. The NUL separator
/// prevents an ambiguous concatenation (`"ab" + "c"` vs `"a" + "bc"|`
/// colliding without one) from ever mapping two genuinely different pairs
/// onto the same directory. Truncated because this is a filesystem shard
/// key, not a security boundary — 64 bits of collision resistance is far
/// more than the number of (path, etag) pairs any one drive will ever
/// cache.
fn hash_key(remote_path: &str, etag: &str) -> String {
    let mut input = Vec::with_capacity(remote_path.len() + 1 + etag.len());
    input.extend_from_slice(remote_path.as_bytes());
    input.push(0);
    input.extend_from_slice(etag.as_bytes());
    let digest = Sha256::digest(&input);
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Deletes an entry directory found unusable during `open()`'s scan (its
/// meta.json is missing or unparseable). Best-effort: a failure to delete
/// only logs, it must never fail `open()` itself — the entry is already
/// being treated as not part of the cache either way.
fn delete_orphaned_entry_dir(hash: &str, dir: &Path) {
    if let Err(err) = fs::remove_dir_all(dir) {
        tracing::warn!(hash = %hash, %err, "cache: failed to delete an orphaned entry directory");
    }
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn key(path: &str, etag: &str) -> FileKey {
        FileKey { remote_path: path.into(), etag: etag.into() }
    }

    #[test]
    fn a_written_block_reads_back_identically() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        let payload = vec![7u8; BLOCK_SIZE as usize];
        c.write_block(&key("a", "e1"), 3, &payload).unwrap();
        assert_eq!(c.read_block(&key("a", "e1"), 3).unwrap().unwrap().as_ref(), &payload[..]);
        assert!(c.read_block(&key("a", "e1"), 2).unwrap().is_none(), "unwritten block");
    }

    #[test]
    fn a_new_etag_invalidates_every_cached_block_of_the_file() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        c.write_block(&key("a", "e1"), 0, &[1u8; 1024]).unwrap();
        assert!(
            c.read_block(&key("a", "e2"), 0).unwrap().is_none(),
            "the server rewrote the file: old bytes must not be served"
        );
    }

    #[test]
    fn cache_state_survives_reopening() {
        let dir = TempDir::new().unwrap();
        {
            let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
            c.write_block(&key("a", "e1"), 0, &[9u8; 512]).unwrap();
        }
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        assert_eq!(c.read_block(&key("a", "e1"), 0).unwrap().unwrap().as_ref(), &[9u8; 512][..]);
    }

    #[test]
    fn a_block_truncated_by_a_crash_is_a_miss_not_an_error() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        let full_block = vec![5u8; BLOCK_SIZE as usize];
        c.write_block(&key("a", "e1"), 0, &full_block).unwrap();
        c.write_block(&key("a", "e1"), 1, &full_block).unwrap();

        // Simulate a crash mid-write: meta.json still lists block 0 as a
        // full BLOCK_SIZE block, but the data file on disk got truncated
        // before that block's bytes were ever fully flushed. Block 0 is
        // not the file's highest index (block 1 is), so it must always be
        // a full block — this truncation can only be corruption, not a
        // legitimate short final block.
        let hash = hash_key("a", "e1");
        let data_path = dir.path().join(&hash).join("data");
        let file = std::fs::OpenOptions::new().write(true).open(&data_path).unwrap();
        file.set_len(BLOCK_SIZE / 2).unwrap();
        drop(file);

        assert!(
            c.read_block(&key("a", "e1"), 0).unwrap().is_none(),
            "truncated block must be treated as a cache miss, not an I/O error"
        );

        // Self-healed: the lying block index must be dropped from
        // meta.json itself, not just papered over in memory, so nothing
        // keeps claiming a block that isn't really there.
        let meta_path = dir.path().join(&hash).join("meta.json");
        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        let blocks = meta["blocks"].as_array().unwrap();
        assert!(
            !blocks.iter().any(|b| b.as_u64() == Some(0)),
            "stale block index must be dropped from meta.json, got {blocks:?}"
        );

        // And the cache is usable again: re-downloading and rewriting the
        // block must round-trip normally.
        let fresh = vec![6u8; BLOCK_SIZE as usize];
        c.write_block(&key("a", "e1"), 0, &fresh).unwrap();
        assert_eq!(c.read_block(&key("a", "e1"), 0).unwrap().unwrap().as_ref(), &fresh[..]);
    }

    #[test]
    fn an_entry_with_a_torn_meta_is_deleted_on_open_not_leaked() {
        let dir = TempDir::new().unwrap();
        {
            let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
            c.write_block(&key("a", "e1"), 0, &[1u8; 1024]).unwrap();
        }
        let hash = hash_key("a", "e1");
        let entry_dir = dir.path().join(&hash);
        // Simulate a crash that left meta.json torn: garbage bytes rather
        // than the JSON `write_meta` would have produced.
        std::fs::write(entry_dir.join("meta.json"), b"not valid json {{{").unwrap();

        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        assert!(
            c.read_block(&key("a", "e1"), 0).unwrap().is_none(),
            "a torn meta.json must never serve the data next to it"
        );
        assert!(
            !entry_dir.exists(),
            "an entry whose meta.json couldn't be read must be deleted on open, \
             not left on disk unaccounted for by used_bytes/eviction"
        );
    }

    #[test]
    fn an_open_files_first_write_never_evicts_itself() {
        let dir = TempDir::new().unwrap();
        // Smaller than a single block, so a fresh entry is already over
        // budget the instant its first block lands — with nothing else in
        // the cache to evict instead. A retained overrun like this is
        // accepted (a live handle is a hard guarantee; eviction just stops
        // when nothing unretained is left, see `evict_if_over_budget`), so
        // both blocks below must survive despite the cache staying over
        // budget.
        let mut c = BlockCache::open(dir.path(), BLOCK_SIZE / 2).unwrap();

        // Retained BEFORE the entry exists at all — the pending-retain
        // case `write_block`'s doc comment describes: the entry gets
        // created already unevictable the instant this call inserts it,
        // not via a separate call after `write_block` returns.
        c.retain(&key("open-file", "e1"));
        c.write_block(&key("open-file", "e1"), 0, &[1u8; BLOCK_SIZE as usize]).unwrap();
        c.write_block(&key("open-file", "e1"), 1, &[2u8; BLOCK_SIZE as usize]).unwrap();

        assert!(
            c.read_block(&key("open-file", "e1"), 0).unwrap().is_some(),
            "the file's own first write self-evicted its entry"
        );
        assert!(
            c.read_block(&key("open-file", "e1"), 1).unwrap().is_some(),
            "the file's own second write self-evicted its entry"
        );
    }

    #[test]
    // `1 * BLOCK_SIZE` is a no-op multiplication (clippy::identity_op), kept
    // verbatim from the brief for the "budget expressed as N blocks" idiom
    // shared with the `3 * BLOCK_SIZE` case elsewhere in this file.
    #[allow(clippy::identity_op)]
    fn two_handles_on_the_same_file_survive_the_first_close() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 1 * BLOCK_SIZE).unwrap();
        let k = key("shared", "e");
        c.retain(&k); c.retain(&k);            // Finder + Quick Look
        c.write_block(&k, 0, &vec![5u8; BLOCK_SIZE as usize]).unwrap();
        c.release(&k);                          // first close
        // Over-budget write of another file must NOT evict the still-open one.
        c.write_block(&key("other", "e"), 0, &vec![6u8; BLOCK_SIZE as usize]).unwrap();
        assert!(c.read_block(&k, 0).unwrap().is_some(), "evicted while a handle was still open");
        c.release(&k);
        c.write_block(&key("third", "e"), 0, &vec![7u8; BLOCK_SIZE as usize]).unwrap();
        assert!(c.read_block(&k, 0).unwrap().is_none(), "still unevictable after the last close");
    }

    #[test]
    #[allow(clippy::identity_op)] // see the comment on the test above
    fn a_write_landing_after_the_last_release_is_evictable() {
        // The phase-1 readahead-after-close leak, pinned dead.
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 1 * BLOCK_SIZE).unwrap();
        let k = key("closed", "e");
        c.retain(&k); c.release(&k);            // opened and closed
        c.write_block(&k, 0, &vec![8u8; BLOCK_SIZE as usize]).unwrap(); // late readahead
        c.write_block(&key("next", "e"), 0, &vec![9u8; BLOCK_SIZE as usize]).unwrap();
        assert!(c.read_block(&k, 0).unwrap().is_none(), "late readahead write re-pinned a closed file");
    }

    #[test]
    fn cached_reads_do_not_rewrite_the_meta_file() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 10 * BLOCK_SIZE).unwrap();
        let k = key("hot", "e");
        c.write_block(&k, 0, &[1u8; 1024]).unwrap();
        let hash = hash_key("hot", "e");
        let meta = dir.path().join(&hash).join("meta.json");
        let before = std::fs::metadata(&meta).unwrap().modified().unwrap();
        for _ in 0..50 {
            c.read_block(&k, 0).unwrap();
        }
        let after = std::fs::metadata(&meta).unwrap().modified().unwrap();
        assert_eq!(before, after, "50 cached reads rewrote meta.json 50 times — SSD wear for nothing");
    }

    #[test]
    fn purge_drops_every_block_of_a_path_even_while_retained() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        c.write_block(&key("edited", "e1"), 0, &[1u8; 1024]).unwrap();
        c.write_block(&key("edited", "e1"), 1, &[2u8; 1024]).unwrap();
        c.write_block(&key("other", "e1"), 0, &[3u8; 1024]).unwrap();
        // A still-open handle must not shield known-stale bytes: purge is
        // invalidation ("these bytes no longer match the server"), not
        // eviction, so it ignores retains where eviction honors them.
        c.retain(&key("edited", "e1"));

        c.purge("edited").unwrap();

        assert!(
            c.read_block(&key("edited", "e1"), 0).unwrap().is_none(),
            "purged blocks must be gone even under the exact etag they were stored with"
        );
        assert!(c.read_block(&key("edited", "e1"), 1).unwrap().is_none());
        assert!(
            !dir.path().join(hash_key("edited", "e1")).exists(),
            "the entry directory itself must be deleted, not just forgotten in memory"
        );
        assert!(
            c.read_block(&key("other", "e1"), 0).unwrap().is_some(),
            "purging one path must not touch another path's blocks"
        );
        c.purge("never-cached").unwrap(); // idempotent: purging nothing is fine
    }

    #[test]
    fn eviction_drops_the_least_recently_used_unpinned_file_first() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 3 * BLOCK_SIZE).unwrap();
        let one_block = vec![1u8; BLOCK_SIZE as usize];
        c.write_block(&key("old", "e"), 0, &one_block).unwrap();
        c.write_block(&key("pinned", "e"), 0, &one_block).unwrap();
        c.retain(&key("pinned", "e"));
        c.write_block(&key("recent", "e"), 0, &one_block).unwrap();
        c.read_block(&key("old", "e"), 0).unwrap(); // old is now MRU
        c.write_block(&key("new", "e"), 0, &one_block).unwrap(); // must evict someone
        assert!(c.read_block(&key("old", "e"), 0).unwrap().is_some(), "recently used, kept");
        assert!(c.read_block(&key("pinned", "e"), 0).unwrap().is_some(), "pinned, kept");
        assert!(c.read_block(&key("recent", "e"), 0).unwrap().is_none(), "LRU victim");
    }
}
