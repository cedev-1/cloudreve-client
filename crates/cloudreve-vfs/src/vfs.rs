//! Read facade: on-demand ranged downloads through [`BlockCache`], with
//! deduplicated readahead.
//!
//! This is the single choke point every read of an open file passes
//! through — the NFS (macOS) and FUSE (Linux) frontends added in phase 3
//! are thin adapters over [`Vfs`]; neither touches [`BlockCache`] or HTTP
//! directly. `open`/`read`/`close` mirror the POSIX calls of the same name
//! closely enough that a frontend can forward them almost verbatim.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use cloudreve_api::api::ExplorerApi;
use cloudreve_api::models::explorer::FileURLService;
use tokio::sync::{Mutex, RwLock};

use crate::cache::{BlockCache, FileKey, BLOCK_SIZE};
use crate::tree::{NodeId, VfsTree};

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

pub struct Vfs {
    tree: VfsTree,
    cache: Arc<Mutex<BlockCache>>,
    client: Arc<cloudreve_api::Client>,
    http: reqwest::Client,
    open_files: RwLock<HashMap<u64, Arc<OpenFile>>>,
    next_handle: AtomicU64,
    /// Remote paths with a readahead task currently in flight. Consulted
    /// (and updated) only while holding this `std::sync::Mutex` for the
    /// instant it takes to check-and-insert/remove — never across an
    /// await — so a `tokio::sync::Mutex` would be needless overhead here.
    readahead_inflight: Arc<std::sync::Mutex<HashSet<String>>>,
}

impl Vfs {
    pub fn new(
        client: Arc<cloudreve_api::Client>,
        remote_base: String,
        cache_dir: &Path,
        cache_max_bytes: u64,
    ) -> Result<Self> {
        let tree = VfsTree::new(client.clone(), remote_base);
        let cache =
            BlockCache::open(cache_dir, cache_max_bytes).context("failed to open block cache")?;
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .context("failed to build the vfs http client")?;
        Ok(Self {
            tree,
            cache: Arc::new(Mutex::new(cache)),
            client,
            http,
            open_files: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            readahead_inflight: Arc::new(std::sync::Mutex::new(HashSet::new())),
        })
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
            fetch_and_cache_run(&self.client, &self.http, &self.cache, &of, run_first, run_last)
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

        self.spawn_readahead(of, last_block);
        Ok(out.freeze())
    }

    /// Closes a handle opened by `open`, releasing its retain on the cache
    /// entry. Only once every handle on this file has closed does the
    /// entry become eligible for eviction again like any other cached
    /// file — a second handle still open (e.g. another app previewing the
    /// same file) keeps it pinned regardless of this one closing.
    pub async fn close(&self, h: FileHandle) -> Result<()> {
        let of = self
            .open_files
            .write()
            .await
            .remove(&h.0)
            .context("close: file handle is not open")?;
        self.cache.lock().await.release(&of.key);
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
