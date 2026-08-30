//! Read facade: on-demand ranged downloads through [`BlockCache`], with
//! deduplicated readahead.
//!
//! This is the single choke point every read of an open file passes
//! through — the NFS (macOS) and FUSE (Linux) frontends added in phase 3
//! are thin adapters over [`Vfs`]; neither touches [`BlockCache`] or HTTP
//! directly. `open`/`read`/`close` mirror the POSIX calls of the same name
//! closely enough that a frontend can forward them almost verbatim.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use cloudreve_api::api::ExplorerApi;
use cloudreve_api::models::explorer::FileURLService;
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::cache::{BlockCache, FileKey, BLOCK_SIZE};
use crate::tree::{NodeAttr, NodeId, VfsTree};
use crate::writeback::{DraftInit, DraftState, DraftStore};

/// How many blocks past the end of a satisfied read are proactively
/// fetched in the background. Sized for smooth sequential access (video
/// playback, large document scrolling) without turning every small read
/// into a large one: readahead never sits on the read's own critical path
/// (it is `tokio::spawn`ed) and is capped so one read never schedules an
/// unbounded fetch.
pub const READAHEAD_BLOCKS: u64 = 4;

/// Default cap on total on-disk cache size, used when nothing else
/// overrides it. 10 GiB is generous for a laptop's spare disk without
/// being effectively unbounded.
pub const DEFAULT_CACHE_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Total attempts made for one ranged GET before giving up, per spec §7:
/// "retry with backoff, then EIO" on a transient download failure
/// (transport error or unexpected/5xx status). A 403 (expired signed URL)
/// is handled orthogonally by `fetch_range_with_retry`'s own one-time URL
/// refresh and never consumes a retry from this budget.
pub const FETCH_RETRIES: u32 = 3;

/// Backoff slept before each retry of a ranged GET, indexed by retry
/// number (the first retry sleeps `FETCH_RETRY_BACKOFF[0]`, and so on).
/// Per spec §7.
pub const FETCH_RETRY_BACKOFF: [Duration; 2] =
    [Duration::from_millis(100), Duration::from_millis(500)];

/// User-Agent presented on every request the vfs's own `reqwest::Client`
/// makes. Field-verified against the real Cloudreve instance (Task 0's
/// Range probe): its WAF 403s any request with no User-Agent header, and
/// `reqwest` sends none by default. Mirrors the format of
/// `cloudreve_sync::USER_AGENT`, which `cloudreve-api`'s own client is
/// configured with elsewhere in the app.
const USER_AGENT: &str = concat!("cloudreve-desktop/", env!("CARGO_PKG_VERSION"));

/// Opaque handle returned by [`Vfs::open`], threaded through [`Vfs::read`]
/// and [`Vfs::close`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileHandle(pub u64);

/// Everything a `read`/`close` needs for one open file, besides the handle
/// number itself. Held behind an `Arc` (not just the `open_files` map's
/// `RwLock`) so a background readahead task spawned from `read` can own a
/// clone that outlives the borrow of `&self`.
struct OpenFile {
    key: FileKey,
    /// File size as of `open()`. Phase 1 has no live invalidation of an
    /// already-open file, so this is fixed for the handle's lifetime —
    /// exactly like a POSIX fd's size doesn't change under an `flock`ed
    /// reader either.
    size: u64,
    /// Signed download URL, fetched once per handle and reused across
    /// every read. Replaced in place (not just on the next `open`) the one
    /// time a request comes back 403: the URL can expire mid-session, and
    /// re-opening the file just to keep reading would be a surprising
    /// frontend-visible failure for something recoverable in one retry.
    download_url: RwLock<String>,
}

/// Outcome of one ranged GET, distinguishing the two response shapes this
/// facade must recover from automatically rather than surfacing as an
/// error: an expired signed URL, and a range past the server's (possibly
/// since-drifted) idea of the file's end.
enum FetchOutcome {
    Data(Bytes),
    /// HTTP 416 — the server's own answer to a range past EOF. Field-
    /// verified as the real server's actual behavior (Task 0): a well-
    /// behaved response, not an error condition.
    RangeNotSatisfiable,
    /// HTTP 403 — the signed URL most likely expired.
    Forbidden,
}

/// Events the write-back path (Task 8) reports to whatever owns the
/// receiver half of `Vfs::new`'s channel — a dashboard row, a toast, or (in
/// tests) nothing at all. Nothing in this crate emits one yet; the channel
/// exists from Task 7 on so the constructor's shape never has to change
/// again once Task 8 starts sending.
#[derive(Debug, Clone, PartialEq)]
pub enum VfsEvent {
    UploadQueued { remote_path: String },
    UploadSucceeded { remote_path: String, new_etag: String },
    UploadFailed { remote_path: String, error: String, will_retry: bool },
    ConflictSaved { original: String, conflict_copy: String },
}

pub struct Vfs {
    tree: VfsTree,
    cache: Arc<Mutex<BlockCache>>,
    /// Whole-file local drafts absorbing every write (D1/D2/D3). A plain
    /// `Mutex`, not the cache's own lock: `DraftStore`'s methods are all
    /// synchronous `&mut self`, so this is held only for the duration of one
    /// disk op at a time, same discipline as `cache` — never across a
    /// network await.
    drafts: Mutex<DraftStore>,
    client: Arc<cloudreve_api::Client>,
    http: reqwest::Client,
    /// Root every temp file created while materializing a draft (D2) is
    /// placed under, so the eventual move into the drafts store (see
    /// `DraftStore::begin`) is a same-volume rename, never a cross-device
    /// copy.
    cache_dir: PathBuf,
    open_files: RwLock<HashMap<u64, Arc<OpenFile>>>,
    next_handle: AtomicU64,
    /// Uniqueness source for materialization temp file names — a separate
    /// counter from `next_handle` on purpose: the two count unrelated
    /// things and mixing them would make either one's value misleading.
    next_tmp: AtomicU64,
    /// Remote paths with a readahead task currently in flight. Consulted
    /// (and updated) only while holding this `std::sync::Mutex` for the
    /// instant it takes to check-and-insert/remove — never across an
    /// await — so a `tokio::sync::Mutex` would be needless overhead here.
    readahead_inflight: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Per-path async locks serializing "does a draft exist yet, and if
    /// not, begin one" for concurrent writers/truncators on the same file
    /// (phase 3's NFS frontend can dispatch concurrent WRITEs against the
    /// same handle). Without this, two callers can both observe "no draft
    /// yet", both materialize, and the second `DraftStore::begin` — which
    /// unconditionally overwrites — silently discards whatever the first
    /// caller already wrote and had acknowledged. Guarded by a
    /// `std::sync::Mutex` (never held across an await, only for the
    /// instant it takes to get-or-insert one entry); entries are never
    /// removed, bounded by the number of distinct files ever drafted in
    /// this process — far too small to matter for a desktop sync client.
    draft_begin_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Sender half of the `VfsEvent` channel returned by `new`. Nothing
    /// sends through it yet (Task 8 does) — held here so the field exists
    /// once emission lands rather than requiring the constructor to change
    /// shape again.
    #[allow(dead_code)]
    events: mpsc::UnboundedSender<VfsEvent>,
}

impl Vfs {
    pub fn new(
        client: Arc<cloudreve_api::Client>,
        remote_base: String,
        cache_dir: &Path,
        cache_max_bytes: u64,
    ) -> Result<(Self, mpsc::UnboundedReceiver<VfsEvent>)> {
        let tree = VfsTree::new(client.clone(), remote_base);
        // D1 TRAP: `BlockCache::open` deletes any directory under its root
        // that lacks a `meta.json` — pointing it at a root that also holds
        // `drafts/` would destroy every draft at startup the first time a
        // draft directory (which has no `meta.json`, only `draft.json`) is
        // scanned. Segregate the two roots before either is ever opened.
        let cache = BlockCache::open(&cache_dir.join("blocks"), cache_max_bytes)
            .context("failed to open block cache")?;
        let drafts = DraftStore::open(&cache_dir.join("drafts"))
            .context("failed to open draft store")?;
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .context("failed to build the vfs http client")?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        Ok((
            Self {
                tree,
                cache: Arc::new(Mutex::new(cache)),
                drafts: Mutex::new(drafts),
                client,
                http,
                cache_dir: cache_dir.to_path_buf(),
                open_files: RwLock::new(HashMap::new()),
                next_handle: AtomicU64::new(1),
                next_tmp: AtomicU64::new(1),
                readahead_inflight: Arc::new(std::sync::Mutex::new(HashSet::new())),
                draft_begin_locks: std::sync::Mutex::new(HashMap::new()),
                events: events_tx,
            },
            events_rx,
        ))
    }

    pub fn tree(&self) -> &VfsTree {
        &self.tree
    }

    /// Opens a node for reading: resolves its signed download URL once and
    /// retains its cache entry so it can never be evicted while the handle
    /// is live — even if this is the file's very first open and nothing
    /// has been downloaded for it yet (see `BlockCache::retain`). `node`
    /// must already be known to the tree (from an earlier `readdir`/
    /// `lookup`) and must not be a directory.
    pub async fn open(&self, node: NodeId) -> Result<FileHandle> {
        let attr = self
            .tree
            .getattr(node)
            .await?
            .context("open: unknown node (readdir/lookup it first)")?;
        if attr.is_dir {
            bail!("cannot open a directory as a file");
        }
        let key = FileKey { remote_path: attr.remote_path.clone(), etag: attr.etag.clone() };
        self.cache.lock().await.retain(&key);

        let download_url = fetch_download_url(&self.client, &key).await?;
        let open_file =
            Arc::new(OpenFile { key, size: attr.size, download_url: RwLock::new(download_url) });

        let handle_id = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.open_files.write().await.insert(handle_id, open_file);
        Ok(FileHandle(handle_id))
    }

    /// Reads `[offset, offset+len)`, serving whatever is already cached and
    /// fetching the rest with one ranged GET per contiguous missing run.
    /// Reads that cross or start past EOF return the truncated (possibly
    /// empty) tail — POSIX semantics frontends rely on, not an error.
    pub async fn read(&self, h: FileHandle, offset: u64, len: u32) -> Result<Bytes> {
        let of = self.open_file(h).await?;

        // D3: a file with an active draft is read from the draft, never
        // from the block cache — the cache may hold stale (or no) content
        // for exactly the bytes the draft has since overwritten. Checked
        // first, before any cache/network logic below even runs.
        {
            let mut drafts = self.drafts.lock().await;
            if drafts.state(&of.key.remote_path).is_some() {
                return drafts.read(&of.key.remote_path, offset, len);
            }
        }

        self.read_from_cache(&of, offset, len).await
    }

    /// The read path phase 1 shipped: serves whatever is already cached and
    /// fetches the rest with one ranged GET per contiguous missing run.
    /// Factored out of `read` so `materialize` (Task 7 — D2) can reuse it
    /// verbatim to pull a whole file's current content into a draft, one
    /// chunk at a time, through the exact same cache-filling logic a normal
    /// read uses.
    async fn read_from_cache(&self, of: &Arc<OpenFile>, offset: u64, len: u32) -> Result<Bytes> {
        // A zero-length read is a legal POSIX call (NFS3/FUSE frontends in
        // phase 3 forward it verbatim), and `offset` alone can legitimately
        // sit anywhere up to EOF for one. Must be handled before the EOF
        // clamp below even touches block math: with `len == 0`,
        // `offset + len - 1` underflows (`u64`), which panics rather than
        // just computing a wrong index.
        if offset >= of.size || len == 0 {
            return Ok(Bytes::new());
        }
        let len = (len as u64).min(of.size - offset);

        let first_block = offset / BLOCK_SIZE;
        let last_block = (offset + len - 1) / BLOCK_SIZE;

        // First pass: pull whatever is already cached. No network wait is
        // ever incurred here — only local disk reads under the cache lock.
        let mut blocks: Vec<Option<Bytes>> = Vec::with_capacity((last_block - first_block + 1) as usize);
        {
            let mut cache = self.cache.lock().await;
            for b in first_block..=last_block {
                blocks.push(cache.read_block(&of.key, b)?);
            }
        }

        // Fetch each contiguous missing run with exactly one ranged GET,
        // then re-read just those blocks back out of the cache.
        let present: Vec<bool> = blocks.iter().map(Option::is_some).collect();
        for (run_first, run_last) in missing_runs(first_block, &present) {
            fetch_and_cache_run(&self.client, &self.http, &self.cache, of, run_first, run_last)
                .await?;
            let mut cache = self.cache.lock().await;
            for b in run_first..=run_last {
                blocks[(b - first_block) as usize] = cache.read_block(&of.key, b)?;
            }
        }

        // Assemble exactly the requested slice. A block still `None` here
        // means the server had fewer bytes than the tracked size promised
        // (drift): treat it as empty, same as any other EOF tail.
        let mut out = BytesMut::with_capacity(len as usize);
        for (idx, block) in blocks.into_iter().enumerate() {
            let b = first_block + idx as u64;
            let block = block.unwrap_or_default();
            let block_start = b * BLOCK_SIZE;
            let want_start = offset.max(block_start) - block_start;
            let want_end = (offset + len).min(block_start + BLOCK_SIZE) - block_start;
            let want_start = want_start.min(block.len() as u64) as usize;
            let want_end = want_end.min(block.len() as u64) as usize;
            if want_start < want_end {
                out.extend_from_slice(&block[want_start..want_end]);
            }
        }

        self.spawn_readahead(of.clone(), last_block);
        Ok(out.freeze())
    }

    /// Closes a handle opened by `open`, releasing its retain on the cache
    /// entry. Only once every handle on this file has closed does the
    /// entry become eligible for eviction again like any other cached
    /// file — a second handle still open (e.g. another app previewing the
    /// same file) keeps it pinned regardless of this one closing.
    ///
    /// If the handle leaves behind a dirty draft (`Editing`: written to but
    /// not yet queued), it is parked `Pending` here. That's all Task 7
    /// does — Task 8's write-back queue is what actually notices `Pending`
    /// drafts and drains them.
    pub async fn close(&self, h: FileHandle) -> Result<()> {
        let of = self
            .open_files
            .write()
            .await
            .remove(&h.0)
            .context("close: file handle is not open")?;
        {
            let mut drafts = self.drafts.lock().await;
            if let Some(DraftState::Editing) = drafts.state(&of.key.remote_path) {
                drafts.set_state(&of.key.remote_path, DraftState::Pending)?;
            }
        }
        self.cache.lock().await.release(&of.key);
        Ok(())
    }

    /// Begins a brand new file: a local-only tree entry (visible to
    /// `readdir`/`lookup` immediately, before any upload) plus an `Empty`
    /// draft, and a handle already open on it ready for `write`.
    ///
    /// Refuses (EEXIST) a name that already resolves to something — a real
    /// remote file/dir, or another draft already in progress under this
    /// parent. Without this guard, `insert_local_entry` would reuse the
    /// existing name's `NodeId`, clobbering its cached attrs with a size-0
    /// placeholder and shadowing its real content with an empty draft whose
    /// blank `base_etag` would later bypass the conflict check (D5)
    /// entirely — a silent overwrite of a file this call never touched.
    pub async fn create(&self, parent: NodeId, name: &str) -> Result<(NodeId, FileHandle)> {
        if self.lookup(parent, name).await?.is_some() {
            anyhow::bail!("EEXIST: an entry named {name:?} already exists in this directory");
        }
        let node = self.tree.insert_local_entry(parent, name).await?;
        let attr = self
            .tree
            .getattr(node)
            .await?
            .context("create: the just-inserted node vanished")?;

        // Empty base_etag: this path has no remote counterpart yet, so the
        // eventual upload (Task 8, D5) must never run the conflict check —
        // there is nothing on the server to conflict with.
        self.drafts.lock().await.begin(&attr.remote_path, "", DraftInit::Empty)?;

        let key = FileKey { remote_path: attr.remote_path.clone(), etag: String::new() };
        self.cache.lock().await.retain(&key);
        let open_file = Arc::new(OpenFile { key, size: 0, download_url: RwLock::new(String::new()) });
        let handle_id = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.open_files.write().await.insert(handle_id, open_file);
        Ok((node, FileHandle(handle_id)))
    }

    /// Writes `data` at `offset`. The first write on a handle with no draft
    /// yet materializes the file first (D2: downloads the whole current
    /// content through `read_from_cache` into a fresh draft) — unless a
    /// draft already exists, e.g. because `create` or `truncate(h, 0)` ran
    /// on this handle first, in which case this just writes straight into
    /// it.
    pub async fn write(&self, h: FileHandle, offset: u64, data: &[u8]) -> Result<u32> {
        let of = self.open_file(h).await?;
        self.ensure_drafted(&of).await?;
        self.drafts.lock().await.write(&of.key.remote_path, offset, data)?;
        Ok(data.len() as u32)
    }

    /// Resizes the file. `size == 0` on a handle with no draft yet is the
    /// `O_TRUNC` fast path real editors' save pattern relies on: the draft
    /// starts `Empty`, and nothing is downloaded. Any other size on an
    /// undrafted handle still has to materialize first (D2) — there is no
    /// way to know what the resized file should contain otherwise.
    pub async fn truncate(&self, h: FileHandle, size: u64) -> Result<()> {
        let of = self.open_file(h).await?;
        if !self.has_draft(&of).await {
            // Serialized per-path (see `draft_begin_locks`'s doc): a
            // concurrent `write`/`truncate` on the same undrafted file must
            // never race this into materializing (or beginning `Empty`)
            // twice, which would let the second `DraftStore::begin` — an
            // unconditional overwrite — silently discard the first one's
            // already-applied content.
            let path_lock = self.draft_begin_lock_for(&of.key.remote_path);
            let _guard = path_lock.lock().await;
            if !self.has_draft(&of).await {
                if size == 0 {
                    self.drafts.lock().await.begin(&of.key.remote_path, &of.key.etag, DraftInit::Empty)?;
                    return Ok(()); // an `Empty` draft already IS size 0.
                }
                self.materialize(&of).await?;
            }
        }
        self.drafts.lock().await.truncate(&of.key.remote_path, size)?;
        Ok(())
    }

    /// Lists a directory the way frontends must from phase 3 on: the real
    /// server-backed listing (`VfsTree::readdir`), overlaid with (a) any
    /// locally-created entries not yet confirmed by the server
    /// (`insert_local_entry` via `create`) and (b) draft size/mtime for
    /// every entry that has one (D3).
    pub async fn readdir(&self, dir: NodeId) -> Result<Vec<(NodeId, NodeAttr)>> {
        let mut listing = self.tree.readdir(dir).await?;
        let known_ids: HashSet<NodeId> = listing.iter().map(|(id, _)| *id).collect();
        for (id, attr) in self.tree.known_children_of(dir).await {
            if !known_ids.contains(&id) {
                listing.push((id, attr));
            }
        }
        for (_, attr) in listing.iter_mut() {
            self.overlay_draft(attr).await;
        }
        Ok(listing)
    }

    /// Resolves one child by name, falling back to a locally-created entry
    /// (not yet part of any server listing) the same way `readdir` does.
    pub async fn lookup(&self, parent: NodeId, name: &str) -> Result<Option<(NodeId, NodeAttr)>> {
        if let Some((id, mut attr)) = self.tree.lookup(parent, name).await? {
            self.overlay_draft(&mut attr).await;
            return Ok(Some((id, attr)));
        }
        for (id, mut attr) in self.tree.known_children_of(parent).await {
            if attr.name == name {
                self.overlay_draft(&mut attr).await;
                return Ok(Some((id, attr)));
            }
        }
        Ok(None)
    }

    /// Reads one node's attributes, overlaid with its draft's size/mtime if
    /// it has one (D3). Never triggers a listing, same as `VfsTree::getattr`.
    pub async fn getattr(&self, node: NodeId) -> Result<Option<NodeAttr>> {
        let Some(mut attr) = self.tree.getattr(node).await? else { return Ok(None) };
        self.overlay_draft(&mut attr).await;
        Ok(Some(attr))
    }

    /// Applies D3's overlay in place: if `attr.remote_path` has an active
    /// draft, its size and last-write time replace the tree's (possibly
    /// stale, possibly nonexistent-yet) attributes.
    async fn overlay_draft(&self, attr: &mut NodeAttr) {
        let drafts = self.drafts.lock().await;
        if let Some(size) = drafts.size(&attr.remote_path) {
            attr.size = size;
            if let Some(mtime) = drafts.mtime_unix(&attr.remote_path) {
                attr.mtime_secs = mtime;
            }
        }
    }

    /// Begins a draft for `of`'s path if none exists yet: downloads the
    /// whole current content through the ordinary read/cache path first
    /// (D2) so a partial in-place write never loses the untouched bytes. A
    /// no-op if a draft is already open (started by `create`, a prior
    /// `truncate(h, 0)`, or an earlier write on the same handle).
    ///
    /// Serialized per-path via `draft_begin_locks`: two writers racing this
    /// on the same undrafted file (phase 3's NFS frontend dispatches
    /// concurrent WRITEs routinely) must not both observe "no draft yet"
    /// and both materialize — the second `DraftStore::begin` unconditionally
    /// overwrites, which would silently discard the first writer's
    /// already-applied, already-acknowledged bytes. The state check is
    /// repeated AFTER acquiring the lock (not just before): a writer that
    /// arrives while another is materializing must wait, then discover the
    /// draft already exists, and skip straight to writing into it.
    async fn ensure_drafted(&self, of: &Arc<OpenFile>) -> Result<()> {
        if self.has_draft(of).await {
            return Ok(());
        }
        let path_lock = self.draft_begin_lock_for(&of.key.remote_path);
        let _guard = path_lock.lock().await;
        if self.has_draft(of).await {
            return Ok(());
        }
        self.materialize(of).await
    }

    async fn has_draft(&self, of: &Arc<OpenFile>) -> bool {
        self.drafts.lock().await.state(&of.key.remote_path).is_some()
    }

    /// Gets (creating if needed) the per-path async lock that serializes
    /// "begin a draft for this path" between concurrent callers. See
    /// `draft_begin_locks`'s field doc for why entries are never removed.
    fn draft_begin_lock_for(&self, remote_path: &str) -> Arc<Mutex<()>> {
        let mut locks = self.draft_begin_locks.lock().unwrap();
        locks
            .entry(remote_path.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// D2's materialization: pulls the whole file's current content through
    /// `read_from_cache` (the exact same block/network path a normal read
    /// uses — no separate download logic to keep in sync) into a temp file
    /// under the cache root, then hands it to `DraftStore::begin` as
    /// `Materialized`, which moves (same-volume rename) it into the draft
    /// directory.
    async fn materialize(&self, of: &Arc<OpenFile>) -> Result<()> {
        let tmp_dir = self.cache_dir.join("tmp");
        std::fs::create_dir_all(&tmp_dir)
            .with_context(|| format!("failed to create {}", tmp_dir.display()))?;
        let tmp_id = self.next_tmp.fetch_add(1, Ordering::SeqCst);
        let tmp_path = tmp_dir.join(format!("materialize-{tmp_id}"));

        {
            let mut file = File::create(&tmp_path)
                .with_context(|| format!("failed to create {}", tmp_path.display()))?;
            // Chunked, not one giant read: `read_from_cache`'s `len` is a
            // `u32`, so a single call could never even ask for more than
            // ~4 GiB — chunking keeps materialization correct for files of
            // any size instead of silently truncating huge ones.
            const CHUNK: u32 = 8 * 1024 * 1024;
            let mut offset = 0u64;
            while offset < of.size {
                let want = ((of.size - offset).min(CHUNK as u64)) as u32;
                let data = self.read_from_cache(of, offset, want).await?;
                if data.is_empty() {
                    break; // server has fewer bytes than the tracked size promised
                }
                file.write_all(&data)
                    .with_context(|| format!("failed to write {}", tmp_path.display()))?;
                offset += data.len() as u64;
            }
        }

        self.drafts.lock().await.begin(&of.key.remote_path, &of.key.etag, DraftInit::Materialized(tmp_path))?;
        Ok(())
    }

    async fn open_file(&self, h: FileHandle) -> Result<Arc<OpenFile>> {
        self.open_files
            .read()
            .await
            .get(&h.0)
            .cloned()
            .context("read: file handle is not open")
    }

    /// Schedules a background top-up of the next `READAHEAD_BLOCKS` blocks
    /// after `last_block`, deduplicated per remote file: if a readahead
    /// task is already running for this file (e.g. Finder issuing several
    /// parallel reads against the same handle), this call is a no-op
    /// rather than a second overlapping fetch. Fire-and-forget: a failure
    /// only costs a future cache miss, never surfaces to the caller of
    /// `read`.
    fn spawn_readahead(&self, of: Arc<OpenFile>, last_block: u64) {
        let last_file_block = (of.size - 1) / BLOCK_SIZE;
        if last_block >= last_file_block {
            return; // the read already reached EOF: nothing left to prefetch
        }
        let start = last_block + 1;
        let end = (last_block + READAHEAD_BLOCKS).min(last_file_block);

        {
            let mut inflight = self.readahead_inflight.lock().unwrap();
            if !inflight.insert(of.key.remote_path.clone()) {
                return; // already readahead-ing this file: avoid a stampede
            }
        }

        let client = self.client.clone();
        let http = self.http.clone();
        let cache = self.cache.clone();
        let readahead_inflight = self.readahead_inflight.clone();
        let remote_path = of.key.remote_path.clone();
        tokio::spawn(async move {
            if let Err(err) = readahead_fill(&client, &http, &cache, &of, start, end).await {
                tracing::warn!(remote_path = %remote_path, %err, "vfs: readahead fetch failed");
            }
            readahead_inflight.lock().unwrap().remove(&remote_path);
        });
    }
}

/// Fills `[first_block, last_block]` in the cache for a background
/// readahead task: same missing-run logic as the foreground path in
/// `Vfs::read`, minus assembling a result — nobody is waiting on these
/// bytes yet.
async fn readahead_fill(
    client: &cloudreve_api::Client,
    http: &reqwest::Client,
    cache: &Arc<Mutex<BlockCache>>,
    of: &OpenFile,
    first_block: u64,
    last_block: u64,
) -> Result<()> {
    let present: Vec<bool> = {
        let mut cache = cache.lock().await;
        (first_block..=last_block)
            .map(|b| cache.read_block(&of.key, b).map(|v| v.is_some()))
            .collect::<Result<_>>()?
    };
    for (run_first, run_last) in missing_runs(first_block, &present) {
        fetch_and_cache_run(client, http, cache, of, run_first, run_last).await?;
    }
    Ok(())
}

/// Downloads one contiguous run of missing blocks with a single ranged GET
/// and writes each block into the cache. The byte range is clamped to
/// `of.size - 1` so a well-formed request is never even sent past the
/// tracked EOF; `fetch_range_with_retry` still treats a 416 arriving
/// anyway (server-side size drift) as an empty result rather than an
/// error, per the field-verified server behavior.
async fn fetch_and_cache_run(
    client: &cloudreve_api::Client,
    http: &reqwest::Client,
    cache: &Arc<Mutex<BlockCache>>,
    of: &OpenFile,
    run_first_block: u64,
    run_last_block: u64,
) -> Result<()> {
    let start_byte = run_first_block * BLOCK_SIZE;
    let end_byte = (run_last_block * BLOCK_SIZE + BLOCK_SIZE - 1).min(of.size.saturating_sub(1));
    if start_byte > end_byte {
        return Ok(()); // the run starts at/past EOF: nothing to fetch
    }

    let data = fetch_range_with_retry(client, http, of, start_byte, end_byte).await?;

    let mut cache = cache.lock().await;
    for b in run_first_block..=run_last_block {
        let rel_start = ((b - run_first_block) * BLOCK_SIZE) as usize;
        if rel_start >= data.len() {
            break; // the server returned fewer bytes than asked: real EOF
        }
        let rel_end = (rel_start + BLOCK_SIZE as usize).min(data.len());
        // No pin argument here anymore: `write_block` consults the cache's
        // own retain count, which was already set by this handle's
        // `Vfs::open`. That also fixes the phase-1 readahead-after-close
        // leak — if `close`/`release` already ran by the time this
        // (`tokio::spawn`ed) readahead write lands, the key is no longer
        // retained and the block is correctly evictable rather than
        // re-pinning a file nobody has open anymore.
        cache.write_block(&of.key, b, &data[rel_start..rel_end])?;
    }
    Ok(())
}

/// Performs one ranged GET, refreshing the handle's cached download URL
/// and retrying exactly once if the server answers 403 (the signed URL
/// most likely expired mid-session — mirrors the recovery
/// `cloudreve-sync`'s download task relies on for the same URLs). A 416,
/// on either attempt, resolves as an empty tail rather than an error.
async fn fetch_range_with_retry(
    client: &cloudreve_api::Client,
    http: &reqwest::Client,
    of: &OpenFile,
    start: u64,
    end: u64,
) -> Result<Bytes> {
    let url = of.download_url.read().await.clone();
    match fetch_range_with_backoff(http, &url, start, end).await? {
        FetchOutcome::Data(bytes) => return Ok(bytes),
        FetchOutcome::RangeNotSatisfiable => return Ok(Bytes::new()),
        FetchOutcome::Forbidden => {}
    }

    let fresh = fetch_download_url(client, &of.key)
        .await
        .context("failed to refresh an expired download URL after a 403")?;
    *of.download_url.write().await = fresh.clone();
    match fetch_range_with_backoff(http, &fresh, start, end).await? {
        FetchOutcome::Data(bytes) => Ok(bytes),
        FetchOutcome::RangeNotSatisfiable => Ok(Bytes::new()),
        FetchOutcome::Forbidden => bail!("download url still forbidden after one refresh"),
    }
}

/// Performs one ranged GET with up to [`FETCH_RETRIES`] attempts total,
/// sleeping [`FETCH_RETRY_BACKOFF`] between them. Only a transport-level
/// failure or an unexpected/5xx status (an `Err` from `fetch_range`) is
/// retried; a well-formed 403/416/200/206 outcome returns immediately and
/// never consumes an attempt from this budget — a 403 is handled by the
/// caller's own one-time URL refresh, orthogonal to this backoff. Once
/// attempts are exhausted, the last error propagates up through
/// `Vfs::read`, which phase 3's NFS/FUSE frontends map to `EIO` per spec
/// §7 ("retry with backoff, then EIO").
async fn fetch_range_with_backoff(
    http: &reqwest::Client,
    url: &str,
    start: u64,
    end: u64,
) -> Result<FetchOutcome> {
    let mut attempt = 0u32;
    loop {
        match fetch_range(http, url, start, end).await {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                attempt += 1;
                if attempt >= FETCH_RETRIES {
                    return Err(err);
                }
                let backoff = FETCH_RETRY_BACKOFF[(attempt - 1) as usize];
                tracing::warn!(%err, attempt, ?backoff, "vfs: ranged GET failed, retrying");
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

async fn fetch_range(http: &reqwest::Client, url: &str, start: u64, end: u64) -> Result<FetchOutcome> {
    let resp = http
        .get(url)
        .header("Range", format!("bytes={start}-{end}"))
        .send()
        .await
        .with_context(|| format!("range GET failed for {url}"))?;
    match resp.status().as_u16() {
        200 | 206 => {
            let bytes = resp.bytes().await.context("failed to read range response body")?;
            Ok(FetchOutcome::Data(bytes))
        }
        416 => Ok(FetchOutcome::RangeNotSatisfiable),
        403 => Ok(FetchOutcome::Forbidden),
        status => bail!("unexpected status {status} fetching bytes {start}-{end} from {url}"),
    }
}

/// Resolves a fresh signed download URL exactly the way
/// `cloudreve-sync/src/tasks/download.rs` does for the same server:
/// request the file's uri (scoped to its current entity/etag when known),
/// take the first url, then rewrite its origin to this client's configured
/// base — the server may answer with its own internal `SiteURL` rather
/// than the host the client actually talked to.
async fn fetch_download_url(client: &cloudreve_api::Client, key: &FileKey) -> Result<String> {
    let mut request = FileURLService { uris: vec![key.remote_path.clone()], ..Default::default() };
    if !key.etag.is_empty() {
        request.entity = Some(key.etag.clone());
    }
    let res = client.get_file_url(&request).await.context("failed to fetch a download URL")?;
    let raw = res.urls.first().context("no download URL in response")?.url.clone();
    Ok(client.rewrite_url_origin(&raw))
}

/// Groups a sequence of per-block presence flags (`present[i]` describes
/// block `first_block + i`) into contiguous absent runs, each reported as
/// an inclusive `(first_missing_block, last_missing_block)` pair. Pure and
/// allocation-light so both the foreground read path and the background
/// readahead path can share it without either one owning the other.
fn missing_runs(first_block: u64, present: &[bool]) -> Vec<(u64, u64)> {
    let mut runs = Vec::new();
    let mut run_start: Option<u64> = None;
    for (i, &is_present) in present.iter().enumerate() {
        let b = first_block + i as u64;
        if is_present {
            if let Some(s) = run_start.take() {
                runs.push((s, b - 1));
            }
        } else if run_start.is_none() {
            run_start = Some(b);
        }
    }
    if let Some(s) = run_start {
        runs.push((s, first_block + present.len() as u64 - 1));
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_present_yields_no_runs() {
        assert_eq!(missing_runs(0, &[true, true, true]), vec![]);
    }

    #[test]
    fn a_single_gap_is_one_run() {
        assert_eq!(missing_runs(10, &[true, false, false, true]), vec![(11, 12)]);
    }

    #[test]
    fn multiple_gaps_are_separate_runs() {
        assert_eq!(
            missing_runs(0, &[false, true, false, false, true, false]),
            vec![(0, 0), (2, 3), (5, 5)]
        );
    }

    #[test]
    fn a_trailing_gap_runs_to_the_end() {
        assert_eq!(missing_runs(5, &[true, false, false]), vec![(6, 7)]);
    }
}
