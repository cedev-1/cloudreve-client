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

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use cloudreve_uploader::{NoSessionStore, UploadParams, Uploader, UploaderConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Mutex as TokioMutex};

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

    /// Overwrites the draft's state, unconditionally — this method itself
    /// enforces no transition rules; the write-back queue is what owns the
    /// state machine's discipline:
    /// - `Editing -> Pending`: only `Vfs::close`, when the closing handle
    ///   leaves a dirty draft behind (arms the debounce timer).
    /// - `Pending -> Editing`: only `Vfs::open`, and only after
    ///   `WriteBackQueue::cancel` reports it actually stopped the armed
    ///   timer — never on a draft already `Uploading` or one that already
    ///   exhausted its retries (see `open`'s doc for why).
    /// - `Pending -> Uploading`: only `WriteBackQueue::process`, right
    ///   before it hands the draft's bytes to the uploader.
    /// - `Uploading -> Pending`: `WriteBackQueue::process` on a failed
    ///   attempt (after exhausting `UPLOAD_RETRIES`), or `DraftStore::open`
    ///   demoting a draft found `Uploading` after a crash (Task 6).
    /// - `Uploading -> `(removed)`: `WriteBackQueue::process` on success —
    ///   the draft is deleted outright, not moved to a fourth state.
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
    ///
    /// Ordering is UNSPECIFIED and MUST NOT be relied on: this iterates
    /// `entries`, a `HashMap`, so the order is arbitrary and can change
    /// between calls even with no mutation in between. That's safe here
    /// because the write-back queue treats its pending drafts as an
    /// unordered set, not a FIFO — `WriteBackQueue::retry_pending` fires
    /// one independent upload attempt per path (each drains through the
    /// same serializing `upload_gate`, but which one goes first has no
    /// observable effect: every draft's own `base_etag`/conflict check/
    /// retry outcome is computed purely from that draft's own state, never
    /// from another draft's). If a future caller ever needs a stable or
    /// prioritized drain order, that ordering has to be added explicitly
    /// here (e.g. sorting by `last_write_unix`) — do not assume `pending()`
    /// already provides one.
    ///
    /// Includes `Uploading` entries deliberately: at startup they've always
    /// already been demoted to `Pending` by `DraftStore::open` before this
    /// is ever called, but `WriteBackQueue::retry_pending` also calls this
    /// mid-process, where a `Uploading` entry can be a cycle stranded by a
    /// rename (see `migrate_armed_timer`'s doc) rather than one truly still
    /// in flight — `retry_pending` is what tells the two apart and recovers
    /// the former.
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

// ---------------------------------------------------------------------
// The queue half (Task 8): debounced write-back, real uploads, conflict
// copies, retry. Everything above this point is pure disk logic; from here
// on this module does HTTP (through `cloudreve_uploader::Uploader`) and
// owns background tokio tasks.
// ---------------------------------------------------------------------

/// Total attempts made for one draft's upload before parking it back
/// `Pending` and giving up until the next `retry_pending_uploads` (or app
/// restart). Pinned for the whole phase-2 write path.
pub const UPLOAD_RETRIES: u32 = 3;

/// Backoff slept between upload attempts, indexed by attempt number (after
/// the first failure sleeps `UPLOAD_RETRY_BACKOFF[0]`, after the second
/// `UPLOAD_RETRY_BACKOFF[1]`, then the third and final attempt runs with no
/// further wait).
pub const UPLOAD_RETRY_BACKOFF: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(5)];

/// A no-op progress sink: the write-back queue doesn't report per-chunk
/// progress anywhere (unlike `cloudreve-sync`'s foreground upload task,
/// nothing in this crate is watching it yet).
struct NoOpProgress;

impl cloudreve_uploader::ProgressCallback for NoOpProgress {
    fn on_progress(&self, _update: cloudreve_uploader::ProgressUpdate) {}
}

/// The write-back queue: debounces a closed draft, then drains it through
/// the real chunked uploader. All fields are cheaply `Clone`-able (every one
/// is an `Arc` or a plain sender) so a background task can own a copy that
/// outlives the `&Vfs` call that spawned it — required since `tokio::spawn`
/// demands `'static`.
///
/// Cheaply `Clone` on purpose: `arm`/`retry_pending` each hand a clone to a
/// spawned task rather than threading `Arc<Self>` through, which would force
/// every call site to already hold one.
#[derive(Clone)]
pub struct WriteBackQueue {
    client: Arc<cloudreve_api::Client>,
    tree: Arc<crate::tree::VfsTree>,
    drafts: Arc<TokioMutex<DraftStore>>,
    events: mpsc::UnboundedSender<crate::vfs::VfsEvent>,
    /// Serializes actual upload attempts: the plan is explicit that the
    /// queue drains sequentially, one upload at a time (YAGNI on
    /// parallelism) — held for a whole draft's processing (conflict check
    /// through final state), not just the upload call itself.
    upload_gate: Arc<TokioMutex<()>>,
    /// Debounce timers currently armed, keyed by remote path, so a reopen
    /// within the window can cancel exactly the right one. A timer removes
    /// its own entry the instant it fires (before doing any real work), so
    /// a `cancel` racing in after that point correctly finds nothing and
    /// reports it did not stop anything — see `cancel`'s doc.
    timers: Arc<StdMutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    /// Outstanding work items: an armed-but-not-yet-fired timer, a
    /// just-fired timer waiting for the `upload_gate`, and an upload
    /// actually in flight all count as exactly one unit each, decremented
    /// only when that item's whole cycle concludes (success, conflict, or
    /// retries exhausted) or when a still-armed timer is cancelled by a
    /// reopen. `wait_for_writeback_idle` polls this to zero.
    busy: Arc<AtomicUsize>,
    /// Test-overridable so the suite doesn't have to sit through the real
    /// 2s debounce (or the 1s/5s retry backoff) on every run. Not
    /// `#[cfg(test)]`: integration tests under `tests/` link this crate
    /// compiled *without* `cfg(test)`, so a cfg-gated method would be
    /// invisible to them. Kept honest instead by naming and by living only
    /// here, never read by any non-test-only call site.
    debounce: Arc<StdMutex<Duration>>,
    retry_backoff: Arc<StdMutex<[Duration; 2]>>,
    /// Remote paths for which `run`/`process` is CURRENTLY executing — the
    /// queue's own ground truth for "is anything genuinely in flight for
    /// this path right now". Since draining is sequential (`upload_gate`)
    /// this holds at most one path in practice today, but is a set (not a
    /// single slot) so it stays correct if that ever changes.
    ///
    /// Exists to tell a draft that merely LOOKS `Uploading` in `DraftStore`
    /// (state left behind by a cycle that got cut short — see
    /// `migrate_armed_timer`'s doc for how a rename can cause exactly that)
    /// apart from one an active `run` call still genuinely owns — see
    /// `retry_pending`.
    in_flight: Arc<StdMutex<HashSet<String>>>,
}

impl WriteBackQueue {
    pub fn new(
        client: Arc<cloudreve_api::Client>,
        tree: Arc<crate::tree::VfsTree>,
        drafts: Arc<TokioMutex<DraftStore>>,
        events: mpsc::UnboundedSender<crate::vfs::VfsEvent>,
    ) -> Self {
        Self {
            client,
            tree,
            drafts,
            events,
            upload_gate: Arc::new(TokioMutex::new(())),
            timers: Arc::new(StdMutex::new(HashMap::new())),
            busy: Arc::new(AtomicUsize::new(0)),
            debounce: Arc::new(StdMutex::new(WRITEBACK_DEBOUNCE)),
            retry_backoff: Arc::new(StdMutex::new(UPLOAD_RETRY_BACKOFF)),
            in_flight: Arc::new(StdMutex::new(HashSet::new())),
        }
    }

    /// Test-only override for the debounce delay. See the `debounce`
    /// field's doc for why this isn't `#[cfg(test)]`.
    pub fn set_debounce_for_tests(&self, d: Duration) {
        *self.debounce.lock().unwrap() = d;
    }

    /// Test-only override for the retry backoff. See the `debounce` field's
    /// doc — same reasoning applies here.
    pub fn set_retry_backoff_for_tests(&self, backoff: [Duration; 2]) {
        *self.retry_backoff.lock().unwrap() = backoff;
    }

    /// Arms the debounce timer for a draft just parked `Pending` by
    /// `Vfs::close`. Emits `UploadQueued` immediately — the draft IS queued
    /// from this point on, even though the actual upload attempt waits out
    /// the debounce first.
    pub fn arm(&self, remote_path: String) {
        self.busy.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .events
            .send(crate::vfs::VfsEvent::UploadQueued { remote_path: remote_path.clone() });

        let debounce = *self.debounce.lock().unwrap();
        let timers = self.timers.clone();
        let this = self.clone();
        let path_for_timer = remote_path.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(debounce).await;
            // Remove ourselves before doing any real work: once this has
            // run, a racing `cancel` for the same path must find nothing
            // and correctly report that it stopped nothing (see `cancel`).
            timers.lock().unwrap().remove(&path_for_timer);
            this.run(path_for_timer).await;
        });
        self.timers.lock().unwrap().insert(remote_path, handle);
    }

    /// Cancels a still-armed debounce timer for `remote_path` (a reopen
    /// within the window). Returns whether a timer was actually stopped:
    /// `false` means it had already fired (or was never armed) — the
    /// caller (`Vfs::open`) must only flip the draft back to `Editing` when
    /// this returns `true`, or it would silently un-park a draft whose
    /// upload is already underway (or already exhausted its retries and is
    /// legitimately parked `Pending` awaiting a manual retry).
    pub fn cancel(&self, remote_path: &str) -> bool {
        let handle = self.timers.lock().unwrap().remove(remote_path);
        match handle {
            Some(handle) => {
                handle.abort();
                self.busy.fetch_sub(1, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    /// Task 10: moves a still-armed debounce timer from `old_path` to
    /// `new_path` (the draft itself was already relocated in `DraftStore` by
    /// the caller — see `Vfs::rename`). Without this, a rename of a `Pending`
    /// draft would leave its timer armed under the OLD path; once it fired,
    /// `process` would find nothing at that path any more (the draft
    /// genuinely lives elsewhere now) and silently give up — the queued
    /// upload would vanish under EITHER name, not just move to the new one.
    ///
    /// A fresh debounce window starting over is an acceptable, harmless
    /// side effect (the same "just keep editing" coalescing `arm`/`cancel`
    /// already provide for a reopen) — restarting the clock never loses
    /// data, it only delays the eventual upload a little. A no-op if
    /// nothing was actually armed (draft still `Editing`, or already
    /// `Uploading`): there is nothing to migrate in either case.
    ///
    /// This only ever handles a timer caught BEFORE it fires — `cancel`
    /// (which this is built on) is exactly as honest about a timer that
    /// already fired as it always has been, and that honesty is exactly
    /// what leaves two known gaps here, both a genuine TOCTOU between
    /// `Vfs::rename` and a timer firing at the same moment, not merely a
    /// theoretical concern:
    ///
    /// (a) The timer fires and `run`/`process` reads `base_etag(old_path)`
    ///     BEFORE `DraftStore::rename` lands: `cancel` above already found
    ///     nothing (the timer removed itself from `timers` the instant it
    ///     fired, before doing any work — see `cancel`'s doc), so this is a
    ///     no-op and no new timer is armed for `new_path`. `process` itself
    ///     then finds the draft gone from `old_path` and gives up
    ///     ("draft vanished — nothing to do"). The entry re-appears at
    ///     `new_path` still `Pending` (rename never touches the state
    ///     field) with no timer watching it — recoverable through
    ///     `retry_pending` exactly like any other `Pending` draft, since
    ///     that path never depends on a timer at all.
    /// (b) The timer fires and `process` gets far enough to flip the draft
    ///     to `Uploading` (still under `old_path`) before `DraftStore::rename`
    ///     runs: the rename then relocates the (now `Uploading`) entry to
    ///     `new_path` out from under `process`, whose own `data_path`
    ///     lookup at `old_path` fails ("draft vanished mid-flight") and
    ///     returns WITHOUT ever resetting the state — the entry is left
    ///     sitting at `new_path` marked `Uploading` forever, in this
    ///     process, with nothing left running for it. `retry_pending`'s
    ///     stranded-`Uploading` recovery (see its own doc) is exactly the
    ///     fix for this case: it is what makes (b) recoverable without a
    ///     full app restart.
    pub fn migrate_armed_timer(&self, old_path: &str, new_path: String) {
        if self.cancel(old_path) {
            self.arm(new_path);
        }
    }

    /// Re-arms every draft still `Pending` for immediate upload, bypassing
    /// the debounce entirely. Returns how many were queued.
    ///
    /// Also recovers a draft found `Uploading` that `in_flight` proves
    /// nothing is genuinely processing any more — a STRANDED cycle, not one
    /// actually in progress (see `migrate_armed_timer`'s doc, case (b), for
    /// how a rename racing a firing debounce timer produces exactly this).
    /// Before this recovery existed, a stranded `Uploading` draft was
    /// invisible to this method (it only ever looked at `Pending`) and sat
    /// stuck until the next full app restart, since only `DraftStore::open`
    /// demotes `Uploading` back to `Pending`. Demoting it here first makes
    /// the SAME hook phase 4 wires to reconnect (this method) able to
    /// recover it too, without waiting for a restart. A draft genuinely
    /// still being processed by THIS queue (present in `in_flight`) is left
    /// alone — re-enqueueing it would race the upload already in flight for
    /// it.
    pub async fn retry_pending(&self) -> usize {
        let in_flight = self.in_flight.lock().unwrap().clone();
        let candidates: Vec<String> = {
            let mut drafts = self.drafts.lock().await;
            let mut candidates = Vec::new();
            for path in drafts.pending() {
                match drafts.state(&path) {
                    Some(DraftState::Pending) => candidates.push(path),
                    Some(DraftState::Uploading) if !in_flight.contains(&path) => {
                        match drafts.set_state(&path, DraftState::Pending) {
                            Ok(()) => candidates.push(path),
                            Err(err) => tracing::warn!(
                                remote_path = %path,
                                %err,
                                "writeback: failed to demote a stranded Uploading draft"
                            ),
                        }
                    }
                    _ => {}
                }
            }
            candidates
        };
        self.enqueue_immediate(candidates)
    }

    /// Queues each of `paths` for immediate upload, bypassing the debounce
    /// entirely — the shared tail of `retry_pending` (phase 4's reconnect
    /// hook) and `Vfs::new`'s startup re-enqueue (Task 9: a draft still
    /// `Pending` when the app last quit must not wait for anyone to notice
    /// and call `retry_pending_uploads` by hand). Takes plain paths, not a
    /// `DraftStore` lock, so `Vfs::new` can call it with paths read off the
    /// just-opened store BEFORE that store is even wrapped in its `Arc<Mutex>`
    /// — no lock, no await, needed at that call site.
    pub(crate) fn enqueue_immediate(&self, paths: Vec<String>) -> usize {
        for path in &paths {
            self.busy.fetch_add(1, Ordering::SeqCst);
            let _ = self
                .events
                .send(crate::vfs::VfsEvent::UploadQueued { remote_path: path.clone() });
            let this = self.clone();
            let path = path.clone();
            tokio::spawn(async move { this.run(path).await });
        }
        paths.len()
    }

    /// Resolves once nothing is armed, queued, or uploading. Polling rather
    /// than a `Notify` on purpose: the busy count changes from several
    /// independent places (arm, cancel, run's completion), and a short poll
    /// is far simpler to get race-free than threading a condvar-style wake
    /// through all of them for a method only tests and shutdown call.
    pub async fn wait_idle(&self) {
        while self.busy.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Runs one draft's whole processing cycle behind `upload_gate`
    /// (sequential draining) and accounts for it in `busy` regardless of
    /// outcome.
    async fn run(&self, remote_path: String) {
        let _gate = self.upload_gate.lock().await;
        self.in_flight.lock().unwrap().insert(remote_path.clone());
        self.process(&remote_path).await;
        self.in_flight.lock().unwrap().remove(&remote_path);
        self.busy.fetch_sub(1, Ordering::SeqCst);
    }

    /// D5 (conflict check) + upload + D6 (success promotion) / retry, for
    /// one draft. Never propagates an error: every failure mode here ends
    /// in a well-defined draft state and an event, not a panic or a
    /// silently dropped future.
    async fn process(&self, remote_path: &str) {
        let Some(base_etag) = self.drafts.lock().await.base_etag(remote_path) else {
            return; // draft vanished (e.g. removed concurrently) — nothing to do.
        };

        // D5: only a draft with a remote counterpart can conflict with
        // anything. A brand-new file's `base_etag` is empty by construction
        // (see `Vfs::create`) and must never run this check.
        let mut conflict_copy: Option<String> = None;
        if !base_etag.is_empty() {
            match self.tree.refresh_etag(remote_path).await {
                Ok(Some(current)) if current != base_etag => {
                    conflict_copy = Some(conflict_copy_path(remote_path));
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        remote_path,
                        %err,
                        "writeback: conflict check failed, uploading in place anyway"
                    );
                }
            }
        }

        let (upload_uri, overwrite, previous_version) = match &conflict_copy {
            Some(conflict_path) => (conflict_path.clone(), false, String::new()),
            None => (remote_path.to_string(), !base_etag.is_empty(), base_etag.clone()),
        };

        if let Err(err) =
            self.drafts.lock().await.set_state(remote_path, DraftState::Uploading)
        {
            tracing::warn!(remote_path, %err, "writeback: failed to mark draft Uploading");
        }

        let (data_path, size, mtime) = {
            let drafts = self.drafts.lock().await;
            let Some(data_path) = drafts.data_path(remote_path) else {
                return; // draft vanished mid-flight.
            };
            (data_path, drafts.size(remote_path).unwrap_or(0), drafts.mtime_unix(remote_path))
        };

        let params = UploadParams {
            local_path: data_path,
            remote_uri: upload_uri.clone(),
            file_size: size,
            mime_type: None,
            last_modified: mtime.map(|secs| secs.saturating_mul(1000)),
            overwrite,
            previous_version,
            task_id: format!("vfs-writeback-{remote_path}"),
            drive_id: "vfs".to_string(),
        };

        let backoff = *self.retry_backoff.lock().unwrap();
        let uploader =
            Uploader::new(self.client.clone(), Arc::new(NoSessionStore), UploaderConfig::default());

        let mut last_err = None;
        for attempt in 0..UPLOAD_RETRIES {
            match uploader.upload(params.clone(), NoOpProgress).await {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(err) => {
                    last_err = Some(err);
                    if attempt + 1 < UPLOAD_RETRIES {
                        tokio::time::sleep(backoff[attempt as usize]).await;
                    }
                }
            }
        }

        match last_err {
            None => match conflict_copy {
                Some(conflict_path) => {
                    // The original is untouched by design (D5): only
                    // invalidate both paths so a subsequent lookup sees the
                    // new copy and refetches the original's now-known-
                    // divergent state, and drop the draft — its content is
                    // safe under the copy.
                    self.tree.invalidate_path(&conflict_path).await;
                    self.tree.invalidate_path(remote_path).await;
                    let _ = self.drafts.lock().await.remove(remote_path);
                    let _ = self.events.send(crate::vfs::VfsEvent::ConflictSaved {
                        original: remote_path.to_string(),
                        conflict_copy: conflict_path,
                    });
                }
                None => {
                    // D6: record the new etag (best-effort — a listing that
                    // doesn't yet reflect the upload just yields an empty
                    // string), delete the draft, and leave the block cache
                    // untouched/empty for this file so the next read
                    // refetches rather than serving stale or converted
                    // content.
                    let new_etag =
                        self.tree.refresh_etag(remote_path).await.ok().flatten().unwrap_or_default();
                    let _ = self.drafts.lock().await.remove(remote_path);
                    let _ = self.events.send(crate::vfs::VfsEvent::UploadSucceeded {
                        remote_path: remote_path.to_string(),
                        new_etag,
                    });
                }
            },
            Some(err) => {
                if let Err(e) =
                    self.drafts.lock().await.set_state(remote_path, DraftState::Pending)
                {
                    tracing::warn!(remote_path, %e, "writeback: failed to park draft back to Pending");
                }
                let _ = self.events.send(crate::vfs::VfsEvent::UploadFailed {
                    remote_path: remote_path.to_string(),
                    error: err.to_string(),
                    will_retry: true,
                });
            }
        }
    }
}

/// D5/D-const conflict-copy name: `"{stem} (conflict {YYYY-MM-DD}){.ext}"`,
/// applied to the leaf of `remote_path` only — the directory is unchanged.
fn conflict_copy_path(remote_path: &str) -> String {
    let (dir, filename) = remote_path.rsplit_once('/').unwrap_or(("", remote_path));
    let date = chrono::Utc::now().format("%Y-%m-%d");
    let (stem, ext) = match filename.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s, format!(".{e}")),
        _ => (filename, String::new()),
    };
    let conflict_name = format!("{stem} (conflict {date}){ext}");
    if dir.is_empty() {
        conflict_name
    } else {
        format!("{dir}/{conflict_name}")
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

    /// Task 10 carried obligation: `rename` is a destructive directory move
    /// (the shard directory name is `sha256(remote_path)`, so a path change
    /// always means a different shard) — data, metadata, and every accessor
    /// must all agree on the NEW path afterwards, and the OLD directory must
    /// be entirely gone, not just unreferenced.
    #[test]
    fn rename_moves_data_and_meta_to_the_new_shard_dir() {
        let dir = TempDir::new().unwrap();
        let mut s = DraftStore::open(dir.path()).unwrap();
        s.begin("old/report.txt", "etag-1", DraftInit::Empty).unwrap();
        s.write("old/report.txt", 0, b"payload bytes").unwrap();

        let old_dir = s.data_path("old/report.txt").unwrap().parent().unwrap().to_path_buf();
        assert!(old_dir.exists());

        s.rename("old/report.txt", "new/renamed.txt").unwrap();

        // This is a MOVE, not a copy: the old shard directory must not
        // survive it at all.
        assert!(!old_dir.exists(), "the old draft directory must not survive a rename");

        // The store's own accessors must agree the old path resolves to
        // nothing and the new one has everything.
        assert_eq!(s.state("old/report.txt"), None, "the old path must no longer resolve");
        assert_eq!(s.size("new/renamed.txt"), Some(b"payload bytes".len() as u64));
        assert_eq!(s.base_etag("new/renamed.txt"), Some("etag-1".to_string()));
        let back = s.read("new/renamed.txt", 0, 100).unwrap();
        assert_eq!(back.as_ref(), b"payload bytes", "content must survive the rename intact");

        // Different remote_path must land in a genuinely different shard —
        // the whole reason this is a directory move rather than an in-place
        // metadata edit.
        let new_dir = s.data_path("new/renamed.txt").unwrap().parent().unwrap().to_path_buf();
        assert_ne!(old_dir, new_dir, "renaming to a different path must move to a different shard");

        // The persisted draft.json itself (not just the in-memory
        // accessors) must carry the NEW remote_path — this is exactly what
        // a mutation skipping the meta rewrite gets wrong: the directory
        // moves, but the JSON payload inside it still says the OLD path.
        let raw = std::fs::read(new_dir.join("draft.json")).unwrap();
        let meta: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            meta["remote_path"], "new/renamed.txt",
            "draft.json on disk must be rewritten with the new remote_path, not just moved verbatim"
        );
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
