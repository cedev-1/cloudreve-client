# VFS Phase 1 — Read Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new `cloudreve-vfs` crate whose facade lists a Cloudreve drive
lazily and serves `open/read/close` with block-level on-demand downloads and
a bounded LRU disk cache — fully tested against wiremock, nothing mounted yet.

**Architecture:** `tree.rs` (lazy virtual tree fed by `cloudreve-api`),
`cache.rs` (1 MiB block store on disk, keyed by etag), `vfs.rs` (facade the
future NFS/FUSE frontends will call). See the spec for the whole picture.

**Tech Stack:** Rust, tokio, `cloudreve-api` (workspace crate), reqwest,
wiremock + tempfile for tests.

**Spec:** `docs/superpowers/specs/2026-08-29-vfs-on-demand-design.md`

## Global Constraints

- **NEVER run `cargo fmt`** — the repo is not rustfmt-formatted. Match the
  surrounding style by hand.
- **Claude never commits.** Every "Commit" step means: STOP, show the user
  the suggested message, and let THEM run git. Do not run `git commit`.
- All work on branch `feat/vfs-on-demand` (the user creates and owns it).
- TDD strictly: write the test, RUN it and watch it fail, implement, watch
  it pass. After each task, mutation-verify: break the implementation, prove
  the new tests go red, revert, prove green. A task is not done otherwise.
- Tests are behavioral: assert on bytes, HTTP requests received by wiremock
  (including exact `Range` headers) and files on disk — never on the
  implementation's own constants. Test through the facade, the same entry
  point the frontends will call.
- Code and comments in English. Commit messages: `feat:`/`fix:`/`test:`,
  lowercase, no scope.
- Block size is **1 MiB** (`1_048_576`); readahead is **4 blocks**; listing
  TTL is **5 seconds**; default cache cap **10 GiB** (all `pub const` in one
  place, `vfs.rs`).

---

### Task 0: Range spike — decision gate

**Files:** none (throwaway commands; result recorded in the spec)

This is the spec's "plan step 0". It needs the USER's real Cloudreve
instance; run it together with them before anything else.

- [ ] **Step 1: Get a download URL from the real instance**

Ask the user for their instance URL + a valid access token + the URI of any
file ≥ 5 MB, then:

```bash
curl -s -X POST "https://<instance>/api/v4/file/url" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"uris":["cloudreve://my/path/to/file.bin"]}' | python3 -m json.tool
# → copy .data.urls[0].url
```

- [ ] **Step 2: Probe Range support**

```bash
curl -s -D- -o /dev/null -H "Range: bytes=0-1023" "<download-url>"
```

Expected for PASS: `HTTP/1.1 206 Partial Content` + a `Content-Range:
bytes 0-1023/<size>` header. A `200` with full body = that backend ignores
Range.

- [ ] **Step 3: Record the decision**

Edit the spec's "RISK TO VERIFY FIRST" bullet: replace it with the observed
result ("Range verified OK on <backend> — no fallback built" or "Range NOT
honored on <backend> — per-file whole-download fallback added to Phase 1
scope as Task 9"). If NOT honored, stop and design Task 9 with the user
before proceeding.

---

### Task 1: Crate skeleton

**Files:**
- Create: `crates/cloudreve-vfs/Cargo.toml`
- Create: `crates/cloudreve-vfs/src/lib.rs`
- Modify: `Cargo.toml` (workspace root — add member)

**Interfaces:**
- Produces: an empty `cloudreve-vfs` lib crate that compiles inside the
  workspace, with modules `tree`, `cache`, `vfs` declared.

- [ ] **Step 1: Write `crates/cloudreve-vfs/Cargo.toml`**

Copy the dependency VERSIONS from `crates/cloudreve-sync/Cargo.toml` (same
workspace, keep them identical — check that file first):

```toml
[package]
name = "cloudreve-vfs"
version = "0.1.0"
edition = "2021"

[dependencies]
cloudreve-api = { path = "../cloudreve-api" }
tokio = { version = "<same as cloudreve-sync>", features = ["full"] }
anyhow = "<same>"
tracing = "<same>"
reqwest = { version = "<same>", features = ["stream"] }
bytes = "<same or 1>"
serde = { version = "<same>", features = ["derive"] }
serde_json = "<same>"

[dev-dependencies]
wiremock = "<same as cloudreve-sync dev-deps>"
tempfile = "<same>"
uuid = { version = "<same>", features = ["v4"] }
```

- [ ] **Step 2: Write `src/lib.rs`**

```rust
//! On-demand virtual filesystem for Cloudreve drives.
//!
//! One brain, two plugs: `vfs` is the facade holding all logic; the NFS
//! (macOS) and FUSE (Linux) frontends added in phase 3 are thin adapters
//! over it. Nothing in this crate mounts anything.

pub mod cache;
pub mod tree;
pub mod vfs;
```

Create `src/cache.rs`, `src/tree.rs`, `src/vfs.rs` each containing only a
placeholder doc comment (`//! Block cache.` etc.) so the crate compiles.

- [ ] **Step 3: Add `"crates/cloudreve-vfs"` to the workspace `members` list** in the root `Cargo.toml`.

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p cloudreve-vfs`
Expected: success, no warnings.

- [ ] **Step 5: Commit (BY THE USER)**

Suggested: `feat: add empty cloudreve-vfs crate`

---

### Task 2: Test harness

**Files:**
- Create: `crates/cloudreve-vfs/tests/common/mod.rs`
- Create: `crates/cloudreve-vfs/tests/read_on_demand.rs` (harness smoke test only)

**Interfaces:**
- Produces (used by every later test):
  - `VfsTestEnv::new().await` → wiremock server + authenticated
    `Arc<cloudreve_api::Client>` + `tempfile::TempDir` cache dir
  - `env.set_remote_files(vec![...])` — register the listing endpoint
    (adapt the JSON shapes from
    `crates/cloudreve-sync/tests/common/mod.rs::set_remote_files`; same
    server API, keep the two harnesses independent — do NOT try to share
    the module across crates)
  - `remote_file(name: &str, size: i64, etag: &str) -> serde_json::Value`
  - `remote_dir(name: &str) -> serde_json::Value`
  - `env.serve_file_content(name: &str, content: &[u8])` — registers BOTH
    the `POST /api/v4/file/url` endpoint (returning a download URL pointing
    back at the mock server) AND the download endpoint itself, which MUST
    honor `Range: bytes=a-b` (reply `206` with the slice) and record every
    request so tests can assert exact ranges.
  - `env.download_requests(name) -> Vec<Option<String>>` — the `Range`
    header of each download hit, in order.
  - `env.client() -> Arc<Client>`, `env.cache_dir() -> &Path`
  - `common::REMOTE_BASE: &str` (the mocked drive root uri) and
    `common::uri_of(name: &str) -> String` (full `cloudreve://` uri of a
    root-level file)
  - `common::fetch_download_url(&env, name) -> String` (see Step 1 note)

Build the client and auth mocks by copying the patterns from
`crates/cloudreve-sync/tests/common/mod.rs` (Credentials struct, base URL =
`server.uri()`). Read that file first; it is the reference for every mock
shape.

- [ ] **Step 1: Write the harness + a smoke test proving Range slicing works**

```rust
// tests/read_on_demand.rs
mod common;
use common::{remote_file, VfsTestEnv};

/// The harness itself must slice: every later test trusts these mocks.
#[tokio::test]
async fn the_mock_server_honors_range_requests() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(3 * 1024 * 1024).collect();
    env.set_remote_files(vec![remote_file("big.bin", body.len() as i64, "etag-1")]).await;
    env.serve_file_content("big.bin", &body).await;

    let url = common::fetch_download_url(&env, "big.bin").await;
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Range", "bytes=1048576-2097151")
        .send().await.unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &body[1_048_576..2_097_152]);
}
```

(`fetch_download_url` is a harness helper that calls
`client.get_file_url(&FileURLService { uris: vec![uri], .. })` and applies
`rewrite_url_origin`, exactly like
`crates/cloudreve-sync/src/tasks/download.rs:329-342` does.)

- [ ] **Step 2: Run it — expect compile failures, then implement the harness until it passes**

Run: `cargo test -p cloudreve-vfs --test read_on_demand`
Expected first: FAIL (missing harness). Then: PASS.

- [ ] **Step 3: Commit (BY THE USER)**

Suggested: `test: wiremock harness for the vfs crate with range-aware downloads`

---

### Task 3: Lazy virtual tree

**Files:**
- Modify: `crates/cloudreve-vfs/src/tree.rs`
- Test: `crates/cloudreve-vfs/tests/tree_listing.rs`

**Interfaces:**
- Consumes: harness from Task 2.
- Produces (used by `vfs.rs` in Task 6 and the frontends in phase 3):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);                    // root is always NodeId(1)

#[derive(Debug, Clone)]
pub struct NodeAttr {
    pub name: String,
    pub remote_path: String,                   // "cloudreve://…" uri string
    pub size: u64,
    pub mtime_secs: i64,
    pub is_dir: bool,
    pub etag: String,                          // empty for dirs
}

pub struct VfsTree { /* Arc<Client>, remote base uri, interior mutability via tokio::sync::RwLock */ }

impl VfsTree {
    pub fn new(client: Arc<cloudreve_api::Client>, remote_base: String) -> Self;
    pub fn root(&self) -> NodeId;
    pub async fn readdir(&self, dir: NodeId) -> anyhow::Result<Vec<(NodeId, NodeAttr)>>;
    pub async fn lookup(&self, parent: NodeId, name: &str) -> anyhow::Result<Option<(NodeId, NodeAttr)>>;
    pub async fn getattr(&self, node: NodeId) -> anyhow::Result<Option<NodeAttr>>;
}
```

Listing goes through `ExplorerApiExt::list_files_all` (handles both offset
and cursor pagination — see the note in the repo memory / its call sites in
`cloudreve-sync/src/drive/sync.rs`). NodeIds are allocated from a `u64`
counter and stay stable for the lifetime of the tree; a `HashMap<NodeId,
Node>` plus per-directory child map is enough. No TTL yet (Task 4).

- [ ] **Step 1: Write failing tests**

```rust
// tests/tree_listing.rs
mod common;
use common::{remote_dir, remote_file, VfsTestEnv};
use cloudreve_vfs::tree::VfsTree;

/// Mounting must be instant on huge drives: only the directory being read
/// is listed, never the whole tree.
#[tokio::test]
async fn subdirectories_are_listed_only_when_first_read() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("photos"), remote_file("readme.txt", 12, "e1")]).await;

    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    let root = tree.readdir(tree.root()).await.unwrap();

    let names: Vec<_> = root.iter().map(|(_, a)| a.name.as_str()).collect();
    assert_eq!(names, vec!["photos", "readme.txt"]);
    assert_eq!(env.list_request_count(), 1, "only the root may have been listed");

    // Reading the subdirectory triggers exactly one more listing.
    let (photos_id, attr) = tree.lookup(tree.root(), "photos").await.unwrap().unwrap();
    assert!(attr.is_dir);
    env.set_remote_files_at("photos", vec![remote_file("cat.jpg", 4096, "e2")]).await;
    let photos = tree.readdir(photos_id).await.unwrap();
    assert_eq!(photos[0].1.name, "cat.jpg");
    assert_eq!(photos[0].1.size, 4096);
    assert_eq!(env.list_request_count(), 2);
}

/// Same node asked twice gets the same id — frontends cache by NodeId.
#[tokio::test]
async fn node_ids_are_stable_across_lookups() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    tree.readdir(tree.root()).await.unwrap();
    let first = tree.lookup(tree.root(), "a.txt").await.unwrap().unwrap().0;
    let second = tree.lookup(tree.root(), "a.txt").await.unwrap().unwrap().0;
    assert_eq!(first, second);
}
```

(This needs two small harness additions: `list_request_count()` and
`set_remote_files_at(subdir, files)` — add them to `tests/common/mod.rs` as
part of this task.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p cloudreve-vfs --test tree_listing` → FAIL (nothing implemented).

- [ ] **Step 3: Implement `tree.rs`** per the interface block above. Listing a dir: call `list_files_all` on `<remote_base>/<dir path>`, map each `FileResponse` to a `NodeAttr` (uri from `f.path`, `mtime` parsed from `updated_at` the way `sync.rs` does with `chrono::DateTime::parse_from_rfc3339(...).timestamp()`), allocate ids for unseen names, keep ids for known ones.

- [ ] **Step 4: Run to verify pass** — same command, PASS, and the whole crate: `cargo test -p cloudreve-vfs`.

- [ ] **Step 5: Mutation check** — make `readdir` list eagerly (recurse into child dirs immediately); `subdirectories_are_listed_only_when_first_read` must fail on the request count. Revert, re-run, green.

- [ ] **Step 6: Commit (BY THE USER)** — `feat: lazy virtual tree for the vfs`

---

### Task 4: Listing TTL and invalidation

**Files:**
- Modify: `crates/cloudreve-vfs/src/tree.rs`
- Test: `crates/cloudreve-vfs/tests/tree_listing.rs` (extend)

**Interfaces:**
- Produces:

```rust
impl VfsTree {
    /// Forget the cached listing containing this remote path (and the
    /// entry's attributes), so the next readdir/lookup refetches. Called by
    /// the SSE hookup in phase 4 and after writes in phase 2.
    pub async fn invalidate_path(&self, remote_path: &str);
}
pub const LISTING_TTL: std::time::Duration = std::time::Duration::from_secs(5);
```

Each directory node stores `listed_at: Option<Instant>`; `readdir` refetches
when older than `LISTING_TTL`. Inject time the same way the repo tests do it
elsewhere — if no pattern exists, gate TTL through
`tokio::time::Instant::now()` and use `tokio::time::pause()` in tests
(`#[tokio::test(start_paused = true)]` breaks wiremock's real sockets, so
instead expose `#[cfg(test)] fn force_expire(&self, dir: NodeId)` — simpler
and honest about what is under test: the refetch behavior, not the clock).

- [ ] **Step 1: Write failing tests**

```rust
/// Finder calls readdir in bursts; each burst must cost one HTTP call.
#[tokio::test]
async fn a_fresh_listing_is_not_refetched() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    tree.readdir(tree.root()).await.unwrap();
    tree.readdir(tree.root()).await.unwrap();
    tree.readdir(tree.root()).await.unwrap();
    assert_eq!(env.list_request_count(), 1, "a fresh listing must be served from memory");
}

/// A server-side change (SSE, phase 4) must become visible immediately.
#[tokio::test]
async fn invalidating_a_path_forces_a_refetch() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    tree.readdir(tree.root()).await.unwrap();

    env.set_remote_files(vec![remote_file("a.txt", 99, "e2")]).await;
    tree.invalidate_path(&common::uri_of("a.txt")).await;

    let listing = tree.readdir(tree.root()).await.unwrap();
    assert_eq!(listing[0].1.size, 99, "stale attributes served after invalidation");
    assert_eq!(env.list_request_count(), 2);
}
```

- [ ] **Step 2: Run → FAIL.** — `cargo test -p cloudreve-vfs --test tree_listing`
- [ ] **Step 3: Implement** (`listed_at` + `invalidate_path` walking to the parent dir of the uri).
- [ ] **Step 4: Run → PASS** (whole crate).
- [ ] **Step 5: Mutation check** — remove the `listed_at` freshness test (always refetch): first test fails. Make `invalidate_path` a no-op: second test fails. Revert both, green.
- [ ] **Step 6: Commit (BY THE USER)** — `feat: listing ttl and path invalidation in the vfs tree`

---

### Task 5: Block cache on disk

**Files:**
- Modify: `crates/cloudreve-vfs/src/cache.rs`
- Test: unit tests inside `cache.rs` (`#[cfg(test)] mod tests` — pure fs
  logic, repo convention for unit tests)

**Interfaces:**
- Consumes: nothing (pure disk module; no HTTP).
- Produces (used by `vfs.rs` in Task 6/7):

```rust
pub const BLOCK_SIZE: u64 = 1_048_576;

pub struct BlockCache { /* root dir, per-file state, LRU order, max_bytes */ }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileKey { pub remote_path: String, pub etag: String }

impl BlockCache {
    pub fn open(root: &Path, max_bytes: u64) -> anyhow::Result<Self>;
    /// None if the block is absent OR the stored etag differs (stale).
    pub fn read_block(&mut self, key: &FileKey, block_idx: u64) -> anyhow::Result<Option<bytes::Bytes>>;
    pub fn write_block(&mut self, key: &FileKey, block_idx: u64, data: &[u8]) -> anyhow::Result<()>;
    /// Files currently open must never be evicted; phase 2 adds drafts here.
    pub fn pin(&mut self, key: &FileKey);
    pub fn unpin(&mut self, key: &FileKey);
    pub fn used_bytes(&self) -> u64;
}
```

Layout on disk, under `root`: one subdir per file
(`<sha256(remote_path)[..16]>/`) containing `data` (sparse file, blocks
written at `block_idx * BLOCK_SIZE`) and `meta.json`
(`{"etag": "...", "blocks": [u64...], "last_used_unix": i64}`). `open()`
scans existing `meta.json` files to rebuild state (survives app restarts).
An etag mismatch in `read_block`/`write_block` deletes the stale entry
first. Eviction: when `used_bytes` would exceed `max_bytes` after a write,
delete unpinned entries oldest-`last_used` first until it fits.

- [ ] **Step 1: Write failing unit tests** (concrete, no absurd values):

```rust
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
        assert!(c.read_block(&key("a", "e2"), 0).unwrap().is_none(),
            "the server rewrote the file: old bytes must not be served");
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
    fn eviction_drops_the_least_recently_used_unpinned_file_first() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 3 * BLOCK_SIZE).unwrap();
        let one_block = vec![1u8; BLOCK_SIZE as usize];
        c.write_block(&key("old", "e"), 0, &one_block).unwrap();
        c.write_block(&key("pinned", "e"), 0, &one_block).unwrap();
        c.pin(&key("pinned", "e"));
        c.write_block(&key("recent", "e"), 0, &one_block).unwrap();
        c.read_block(&key("old", "e"), 0).unwrap();      // old is now MRU
        c.write_block(&key("new", "e"), 0, &one_block).unwrap(); // must evict someone
        assert!(c.read_block(&key("old", "e"), 0).unwrap().is_some(), "recently used, kept");
        assert!(c.read_block(&key("pinned", "e"), 0).unwrap().is_some(), "pinned, kept");
        assert!(c.read_block(&key("recent", "e"), 0).unwrap().is_none(), "LRU victim");
    }
}
```

- [ ] **Step 2: Run → FAIL.** `cargo test -p cloudreve-vfs --lib cache`
- [ ] **Step 3: Implement `cache.rs`** per the layout above.
- [ ] **Step 4: Run → PASS** (whole crate).
- [ ] **Step 5: Mutation checks** — (a) skip the etag comparison in `read_block`: invalidation test fails. (b) evict newest-first: LRU test fails. (c) ignore `pin`: LRU test fails. Revert each, green.
- [ ] **Step 6: Commit (BY THE USER)** — `feat: on-disk block cache with etag invalidation and pinned lru`

---

### Task 6: Read facade with ranged downloads and readahead

**Files:**
- Modify: `crates/cloudreve-vfs/src/vfs.rs`
- Test: `crates/cloudreve-vfs/tests/read_on_demand.rs` (extend)

**Interfaces:**
- Consumes: `VfsTree` (Task 3/4), `BlockCache` (Task 5), harness Range mocks (Task 2).
- Produces (the API phase-2 extends and phase-3 frontends call):

```rust
pub const READAHEAD_BLOCKS: u64 = 4;
pub const DEFAULT_CACHE_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;

pub struct Vfs { /* tree, Mutex<BlockCache>, Arc<Client>, http: reqwest::Client */ }
pub struct FileHandle(pub u64);

impl Vfs {
    pub fn new(client: Arc<cloudreve_api::Client>, remote_base: String,
               cache_dir: &Path, cache_max_bytes: u64) -> anyhow::Result<Self>;
    pub fn tree(&self) -> &VfsTree;
    pub async fn open(&self, node: NodeId) -> anyhow::Result<FileHandle>;   // pins the FileKey
    pub async fn read(&self, h: FileHandle, offset: u64, len: u32) -> anyhow::Result<bytes::Bytes>;
    pub async fn close(&self, h: FileHandle) -> anyhow::Result<()>;         // unpins
}
```

`read` computes the block span for `[offset, offset+len)`, serves cached
blocks, fetches missing ones with ONE ranged GET per contiguous missing run
(`Range: bytes=<first_missing_byte>-<last_byte_of_run>`), writes them to the
cache, then spawns (tokio::spawn, fire-and-forget, deduplicated per file) a
readahead fetch of the next `READAHEAD_BLOCKS` blocks. The download URL is
obtained once per open handle via `get_file_url` +
`rewrite_url_origin` (mirror the exact call in
`cloudreve-sync/src/tasks/download.rs:329-342`) and cached on the handle;
a 403/expired URL response triggers one refetch of the URL then a retry.
Reads past EOF return the truncated (possibly empty) tail — POSIX semantics,
frontends rely on it.

- [ ] **Step 1: Write failing tests**

```rust
/// The whole point of the feature: bytes travel only for what is read.
#[tokio::test]
async fn reading_a_slice_downloads_only_that_slice_plus_readahead() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(20 * 1024 * 1024).collect();
    env.set_remote_files(vec![remote_file("video.mp4", body.len() as i64, "e1")]).await;
    env.serve_file_content("video.mp4", &body).await;

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "video.mp4").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    let bytes = vfs.read(h, 0, 65536).await.unwrap();
    assert_eq!(bytes.as_ref(), &body[..65536]);

    env.wait_for_downloads_to_settle().await; // harness helper: readahead is async
    let ranged: u64 = env.total_bytes_served("video.mp4");
    assert!(ranged <= 5 * 1024 * 1024,
        "read 64 KiB but downloaded {ranged} bytes — more than block + readahead");
    vfs.close(h).await.unwrap();
}

/// Second read of the same data must not touch the network.
#[tokio::test]
async fn a_cached_block_is_served_without_any_http_request() {
    let env = VfsTestEnv::new().await;
    let body = vec![42u8; 2 * 1024 * 1024];
    env.set_remote_files(vec![remote_file("doc.pdf", body.len() as i64, "e1")]).await;
    env.serve_file_content("doc.pdf", &body).await;

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "doc.pdf").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.read(h, 0, 1024).await.unwrap();
    env.wait_for_downloads_to_settle().await;
    let before = env.download_requests("doc.pdf").len();

    let again = vfs.read(h, 0, 1024).await.unwrap();
    assert_eq!(again.as_ref(), &body[..1024]);
    assert_eq!(env.download_requests("doc.pdf").len(), before,
        "a cached read went to the network");
    vfs.close(h).await.unwrap();
}

/// Reads crossing EOF return the honest tail, like a local file.
#[tokio::test]
async fn a_read_past_the_end_returns_the_truncated_tail() {
    let env = VfsTestEnv::new().await;
    let body = b"short file".to_vec();
    env.set_remote_files(vec![remote_file("s.txt", body.len() as i64, "e1")]).await;
    env.serve_file_content("s.txt", &body).await;
    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "s.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    assert_eq!(vfs.read(h, 6, 100).await.unwrap().as_ref(), b"file");
    assert_eq!(vfs.read(h, 500, 100).await.unwrap().len(), 0);
}
```

(Harness helpers to add in this task: `wait_for_downloads_to_settle` —
poll until no new download request lands for 200 ms, with the phase-1
`wait_until`-style deadline pattern from
`cloudreve-sync/tests/ignored_files.rs`; `total_bytes_served(name)`.)

- [ ] **Step 2: Run → FAIL.** `cargo test -p cloudreve-vfs --test read_on_demand`
- [ ] **Step 3: Implement `vfs.rs`** per the interface block.
- [ ] **Step 4: Run → PASS**, then the whole crate.
- [ ] **Step 5: Mutation checks** — (a) download the WHOLE file on first read: test 1 fails on `total_bytes_served`. (b) skip the cache lookup (always refetch): test 2 fails. (c) clamp nothing at EOF: test 3 fails. Revert each, green.
- [ ] **Step 6: Commit (BY THE USER)** — `feat: on-demand ranged reads with readahead through the vfs facade`

---

### Task 7: Cache cap wiring

(The spec's `disk_space.rs` guard — cap additionally bounded by free disk —
lands in phase 4: that module lives in `cloudreve-sync`, which will depend on
this crate, not the other way around. The effective cap passed to `Vfs::new`
is where phase 4 plugs it in.)

**Files:**
- Modify: `crates/cloudreve-vfs/src/vfs.rs`
- Test: `crates/cloudreve-vfs/tests/read_on_demand.rs` (extend)

**Interfaces:**
- Consumes: `BlockCache` pin/evict (Task 5).
- Produces: nothing new — proves the facade honors the cap end-to-end
  (eviction was unit-tested in isolation; this is the behavioral pass
  through the real entry point, per repo discipline).

- [ ] **Step 1: Write the failing test**

```rust
/// The cap is enforced through the facade, not just inside cache.rs:
/// reading a 4th file under a 3-file cap evicts the coldest CLOSED file.
#[tokio::test]
async fn the_cache_cap_holds_through_real_reads() {
    let env = VfsTestEnv::new().await;
    let mb = 1024 * 1024;
    for (name, byte) in [("a.bin", 1u8), ("b.bin", 2), ("c.bin", 3), ("d.bin", 4)] {
        env.add_remote_file(name, vec![byte; mb], &format!("e-{name}")).await;
    }
    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       3 * mb as u64).unwrap();
    for name in ["a.bin", "b.bin", "c.bin", "d.bin"] {
        let node = vfs.tree().lookup(vfs.tree().root(), name).await.unwrap().unwrap().0;
        let h = vfs.open(node).await.unwrap();
        vfs.read(h, 0, mb as u32).await.unwrap();
        vfs.close(h).await.unwrap();
    }
    env.wait_for_downloads_to_settle().await;
    let disk: u64 = common::dir_size(env.cache_dir());
    assert!(disk <= 3 * mb as u64 + 64 * 1024,
        "cache dir holds {disk} bytes, cap was {}", 3 * mb);
    // a.bin was evicted: reading it again must hit the network once more.
    let before = env.download_requests("a.bin").len();
    let node = vfs.tree().lookup(vfs.tree().root(), "a.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.read(h, 0, 1024).await.unwrap();
    assert!(env.download_requests("a.bin").len() > before);
}
```

(Harness helpers: `add_remote_file` = `set_remote_files` accumulate +
`serve_file_content`; `dir_size` walks the dir. Readahead makes exact
byte-accounting fuzzy — the 64 KiB slack covers `meta.json` files only;
disable readahead in this test via `Vfs::new` if flakiness appears, and if
so add `readahead_blocks: u64` to the constructor instead of the const.)

- [ ] **Step 2: Run → FAIL** (facade doesn't wire `max_bytes` yet or eviction never triggers through it).
- [ ] **Step 3: Implement** — plumb `cache_max_bytes` into `BlockCache::open`, evict inside `write_block` (already done in Task 5 — this task usually only fixes what the behavioral test flushes out, e.g. pins not released on close).
- [ ] **Step 4: Run → PASS**, whole crate, then `cargo test --workspace` (nothing outside the crate may break).
- [ ] **Step 5: Mutation check** — pass `u64::MAX` instead of the configured cap when constructing `BlockCache`: test fails. Revert, green.
- [ ] **Step 6: Commit (BY THE USER)** — `feat: enforce the vfs cache cap through the read facade`

---

### Task 8: Phase gate

**Files:**
- Modify: `docs/superpowers/plans/2026-08-29-vfs-master-index.md` (mark phase 1 done)

- [ ] **Step 1: Full verification** — `cargo test --workspace` all green; `cargo clippy -p cloudreve-vfs --all-targets` introduces ZERO new warnings (compare against a `git stash` baseline count, the technique used on 2026-08-29).
- [ ] **Step 2: Coverage audit** — re-read the spec §4; every bullet must map to a passing test. Confirm each task's mutation checks were actually run (they are in the transcript, not just claimed).
- [ ] **Step 3: Update the master index** — phase 1 row → done, with the test counts.
- [ ] **Step 4: Commit (BY THE USER)** — `docs: mark vfs phase 1 complete`
- [ ] **Step 5: Write the phase-2 plan** (write path) with the writing-plans skill, folding in every learning from phase 1.
