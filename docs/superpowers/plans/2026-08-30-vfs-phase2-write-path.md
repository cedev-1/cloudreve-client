# VFS Phase 2 — Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Files created or edited through the VFS facade upload themselves back
to Cloudreve — drafts on write, debounced write-back on close, conflict copies,
offline retry, restart survival — plus the three cache/tree debts phase 1
carried.

**Architecture:** A `DraftStore` (whole-file local drafts, outside the block
cache, never evicted) absorbs all writes; a write-back queue drains closed
drafts through the real chunked `Uploader`, extracted from `cloudreve-sync`
into a shared `cloudreve-uploader` crate. The facade grows
`create/write/truncate/mkdir/unlink/rename` and overlays drafts onto the
server-backed tree so new files appear instantly.

**Tech Stack:** Rust, tokio, `cloudreve-api`, `cloudreve-uploader` (new,
extracted), wiremock.

**Spec:** `docs/superpowers/specs/2026-08-29-vfs-on-demand-design.md` (§5 is
this phase's scope; §7's upload-side error bullets too). Debts binding this
plan: "Debts carried out of phase 1" in
`docs/superpowers/plans/2026-08-29-vfs-master-index.md`.

## Global Constraints

- **NEVER run `cargo fmt`.** Match surrounding style by hand.
- **Claude never commits.** Every "Commit" step = stage with `git add`, STOP,
  the user commits personally.
- Branch `feat/vfs-on-demand`; work in `crates/cloudreve-vfs` (+ the Task-1
  extraction).
- TDD strictly (test → RED → implement → GREEN) and mutation-verify each
  task; a task is not done otherwise. Tests are behavioral, fixtures never
  derived from the implementation's constants, always through the facade.
- Never touch or read the stray `desktop/` directory.
- Existing phase-1 tests are the regression net: they stay green untouched
  (except signatures the plan itself changes, called out per task).
- Constants pinned for the whole phase (all `pub const` in `vfs.rs` /
  `writeback.rs`): `WRITEBACK_DEBOUNCE = 2s`; upload retry = 3 attempts,
  backoff `[1s, 5s]`; conflict copy name `"{stem} (conflict {YYYY-MM-DD}){.ext}"`.

## Design decisions (read before any task — tasks reference these)

- **D1 — Draft storage:** `<cache_root>/drafts/<hash16(remote_path)>/` with
  `data` (full local file) + `draft.json`
  (`{remote_path, base_etag, size, state, last_write_unix}`,
  `state ∈ Editing|Pending|Uploading`). Drafts are NOT in `BlockCache`:
  "pending drafts exempt from LRU" (spec §5) holds by construction.
  **TRAP (Task 7 must handle it first):** phase 1's `BlockCache::open`
  DELETES any entry dir lacking a `meta.json` — pointing it at a root that
  also contains `drafts/` would destroy every draft at startup. `Vfs::new`
  therefore segregates: `BlockCache::open(cache_dir.join("blocks"), …)` and
  `DraftStore::open(cache_dir.join("drafts"))`. No migration concern —
  phase 1 never shipped. The phase-1 cap test's `dir_size` moves to the
  `blocks/` subdir so draft bytes never count against the block-cache cap.
- **D2 — Materialization:** first write to an existing remote file downloads
  the WHOLE file into the draft first (via the phase-1 read path), then
  applies the write — EXCEPT when the draft began with `truncate(size=0)`
  (the O_TRUNC path frontends send) or the file is newly created: then the
  draft starts empty and nothing is downloaded. Rationale in a comment:
  partial in-place rewrites of huge files are rare; correctness beats the
  optimization, and the truncate fast-path covers how editors actually save.
- **D3 — Reads of a drafted file** are served from the draft file, never
  from the block cache; `getattr/readdir/lookup` report the draft's size and
  mtime (facade overlay, Task 7).
- **D4 — Events:** `Vfs::new` now returns
  `Result<(Self, tokio::sync::mpsc::UnboundedReceiver<VfsEvent>)>`, with
  `pub enum VfsEvent { UploadQueued{remote_path: String}, UploadSucceeded{remote_path: String, new_etag: String}, UploadFailed{remote_path: String, error: String, will_retry: bool}, ConflictSaved{original: String, conflict_copy: String} }`.
  Phase 4 turns these into toasts/dashboard rows. Phase-1 tests update the
  destructuring at the call sites and nothing else.
- **D5 — Conflict rule:** before uploading a draft whose `base_etag` is
  non-empty, fetch the file's current remote etag (tree `getattr` after an
  `invalidate_path`, or a direct one-file listing); if it differs from
  `base_etag`, upload to the conflict-copy name instead (D-const above),
  leave the original untouched, emit `ConflictSaved`. Never overwrite.
- **D6 — Upload success promotes the draft:** record the new etag, delete
  the draft, and (cheaply) leave the block cache empty for that file — the
  next read refetches; do NOT try to convert the draft into cache blocks.
  `invalidate_path` the file so the tree shows the server's new etag.
- **D7 — After Task 2, `BlockCache` pins are a refcount** driven by
  `retain/release`; `write_block` loses its `pinned` parameter and consults
  the refcount internally. Readahead landing after the last `release` writes
  an unpinned entry — the phase-1 pin-leak dies by construction.

---

### Task 1: Extract the uploader into a `cloudreve-uploader` crate

**Files:**
- Create: `crates/cloudreve-uploader/` (moved from
  `crates/cloudreve-sync/src/uploader/` — `mod.rs` becomes `src/lib.rs`,
  submodules `chunk.rs`, `encrypt.rs`, `error.rs`, `progress.rs`,
  `session.rs`, `providers/` move as-is)
- Modify: root `Cargo.toml` (workspace member), `crates/cloudreve-sync/Cargo.toml`
  (+ dependency), `crates/cloudreve-sync/src/lib.rs` (re-export), the single
  `use crate::inventory::InventoryDb` coupling
- Test: existing workspace suites (this is a mechanical move; green-stays-green
  is the acceptance bar) + one new unit test for the trait seam

**Interfaces:**
- Consumes: current `crates/cloudreve-sync/src/uploader/*` (verified: its ONLY
  `cloudreve-sync` coupling is `InventoryDb`, used once — `mod.rs:247`,
  `inventory.delete_upload_session(&session.id)`).
- Produces: crate `cloudreve-uploader` exposing everything `uploader` exposes
  today (`Uploader`, `UploadParams`, `UploaderConfig`, `ProgressCallback`, …)
  plus:

```rust
/// The one persistence hook the uploader needs. cloudreve-sync backs it
/// with InventoryDb; cloudreve-vfs uses `NoSessionStore` (drafts are the
/// persistence there).
pub trait SessionStore: Send + Sync {
    fn delete_upload_session(&self, id: &str) -> anyhow::Result<()>;
}
pub struct NoSessionStore;
impl SessionStore for NoSessionStore { fn delete_upload_session(&self, _: &str) -> anyhow::Result<()> { Ok(()) } }
```

- [ ] **Step 1:** `git mv crates/cloudreve-sync/src/uploader crates/cloudreve-uploader/src` scaffolding: create `crates/cloudreve-uploader/Cargo.toml` (deps copied from what the module actually uses — grep its `use` lines; versions identical to cloudreve-sync's), rename `mod.rs` → `lib.rs`, fix `crate::uploader::` paths to `crate::`.
- [ ] **Step 2:** Replace the `InventoryDb` field/param with `Arc<dyn SessionStore>`; add the trait + `NoSessionStore` (code above). In `cloudreve-sync`, `impl SessionStore for InventoryDb` (delegating to the existing method) next to the inventory code, and pass the Arc at the `Uploader::new` call sites (grep `Uploader::new` — upload.rs task + any other).
- [ ] **Step 3:** `crates/cloudreve-sync/src/lib.rs`: `pub use cloudreve_uploader as uploader;` so every existing `cloudreve_sync::uploader::...` path (internal and in tests) keeps compiling.
- [ ] **Step 4:** New unit test in `cloudreve-uploader` (`lib.rs` tests mod): `the_null_session_store_is_a_no_op` — trivial, but the REAL acceptance is Step 5.
- [ ] **Step 5:** `cargo test --workspace` — every pre-existing suite green, count ≥ 200. `cargo clippy --workspace --all-targets` — warning count vs baseline 86 (only moved-code warnings may move file, none may appear).
- [ ] **Step 6:** Mutation: make `InventoryDb`'s `SessionStore` impl a no-op → whichever existing uploader-session test covers deletion must fail (find it with `grep -rn "delete_upload_session" crates/cloudreve-sync/tests src`); if NO existing test fails, say so honestly in the report and add one at the seam (insert a session row via InventoryDb, call the trait method, assert the row is gone). Revert, green.
- [ ] **Step 7:** Stage (`git add -A crates/cloudreve-uploader crates/cloudreve-sync Cargo.toml Cargo.lock`). Commit (BY THE USER): `refactor: extract the chunked uploader into a shared crate`

---

### Task 2: Pin refcount (kills the boolean-pin and readahead-after-close debts)

**Files:**
- Modify: `crates/cloudreve-vfs/src/cache.rs` (pin model), `crates/cloudreve-vfs/src/vfs.rs` (call sites)
- Test: unit tests in `cache.rs` + one facade test in `tests/read_on_demand.rs`

**Interfaces:**
- Consumes: phase-1 `BlockCache` (`write_block(key, idx, data, pinned)`, boolean `pin/unpin`).
- Produces (every later task uses this shape):

```rust
impl BlockCache {
    /// A live file handle. Entries with retain_count > 0 are never evicted.
    pub fn retain(&mut self, key: &FileKey);
    /// Drops one handle; at zero the entry becomes evictable.
    pub fn release(&mut self, key: &FileKey);
    /// write_block consults the retain count internally — no `pinned` param.
    pub fn write_block(&mut self, key: &FileKey, block_idx: u64, data: &[u8]) -> Result<()>;
}
```

`retain` counts live in memory only (restart clears them — pins were never
persisted in phase 1 either; keep it that way and say so in a comment).
The self-eviction guarantee from phase 1 must survive: an entry being
written for a retained key is created non-evictable atomically.

- [ ] **Step 1: failing unit tests** (cache.rs tests mod; keep every phase-1 test, adapting only the removed `pinned` argument):

```rust
#[test]
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
```

- [ ] **Step 2:** RED (`cargo test -p cloudreve-vfs --lib cache`) — the API doesn't exist.
- [ ] **Step 3:** Implement (`retains: HashMap<FileKey, u32>` or a count on the entry + a pending-retain set for not-yet-created entries — the phase-1 atomicity property must hold for retained keys). Update `vfs.rs`: `open` → `retain`, `close` → `release`, readahead + read-path `write_block` calls drop the boolean. Adapt phase-1 cache tests (`an_open_files_first_write_never_evicts_itself` becomes retain-based, same scenario and budget).
- [ ] **Step 4:** GREEN, whole crate.
- [ ] **Step 5:** Mutations: (a) `release` decrements to zero on first call regardless of count → two-handles test fails; (b) `write_block` ignores the retain count at entry creation → phase-1's self-eviction test (adapted) fails. Revert each, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `fix: refcounted cache pins tied to handle liveness`

---

### Task 3: LRU stamping without write amplification

**Files:**
- Modify: `crates/cloudreve-vfs/src/cache.rs`
- Test: unit tests in `cache.rs`

**Interfaces:** none new — `read_block` simply stops rewriting `meta.json`.

Recency (`seq`/`last_used`) moves to memory only, refreshed on reads and
writes; `meta.json` is written only when the block SET changes (write_block,
drop_block, eviction). On reopen, recency falls back to `meta.json`'s
`last_used_unix` — a restart loses fine-grained recency, which only makes
early eviction slightly less accurate. Document that trade in a comment.

- [ ] **Step 1: failing test**

```rust
#[test]
fn cached_reads_do_not_rewrite_the_meta_file() {
    let dir = TempDir::new().unwrap();
    let mut c = BlockCache::open(dir.path(), 10 * BLOCK_SIZE).unwrap();
    let k = key("hot", "e");
    c.write_block(&k, 0, &[1u8; 1024]).unwrap();
    let meta = /* the entry's meta.json path — compute the shard dir like the torn-meta test does */;
    let before = std::fs::metadata(&meta).unwrap().modified().unwrap();
    for _ in 0..50 { c.read_block(&k, 0).unwrap(); }
    let after = std::fs::metadata(&meta).unwrap().modified().unwrap();
    assert_eq!(before, after, "50 cached reads rewrote meta.json 50 times — SSD wear for nothing");
}
```

- [ ] **Step 2:** RED. **Step 3:** implement. **Step 4:** GREEN + whole crate (the LRU test must still pass — recency now in memory). **Step 5:** Mutation: stamp recency ONLY at write time (ignore reads) → the phase-1 LRU test's "old is now MRU thanks to a read" step fails. Revert, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `fix: keep lru recency in memory instead of rewriting meta on every read`

---

### Task 4: Tree prunes ghosts on re-list

**Files:**
- Modify: `crates/cloudreve-vfs/src/tree.rs`
- Test: `crates/cloudreve-vfs/tests/tree_listing.rs`

**Interfaces:** none new — `ensure_listed`'s refresh path prunes.

- [ ] **Step 1: failing test**

```rust
/// A file deleted on the server must vanish — and not just from readdir:
/// its attrs must be forgotten too, or the maps grow forever under churn.
#[tokio::test]
async fn a_file_deleted_remotely_disappears_after_invalidation() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1"), remote_file("gone.txt", 1, "e2")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    let (gone_id, _) = tree.lookup(tree.root(), "gone.txt").await.unwrap().unwrap();

    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1")]).await;
    tree.invalidate_path(&common::uri_of("gone.txt")).await;

    assert!(tree.lookup(tree.root(), "gone.txt").await.unwrap().is_none());
    assert!(tree.getattr(gone_id).await.unwrap().is_none(),
        "the ghost's attrs survived the re-list");
}
```

- [ ] **Step 2:** RED. **Step 3:** implement (on re-list, remove attrs — and known_children id-mappings — of names absent from the fresh listing; keep ids of names still present: the stability test guards that). **Step 4:** GREEN + whole crate. **Step 5:** Mutation: skip the prune → test fails on the getattr assert. Revert, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `fix: prune ghost entries from the vfs tree on re-list`

---

### Task 5: Upload mocks in the test harness

**Files:**
- Modify: `crates/cloudreve-vfs/tests/common/mod.rs`
- Test: one smoke test in a NEW file `crates/cloudreve-vfs/tests/write_back.rs`

**Interfaces:**
- Produces (all later write tests depend on these):
  - `env.expect_uploads()` — mounts the upload endpoints: `PUT /api/v4/file/upload`-style session creation (mirror the REAL routes: read `crates/cloudreve-uploader/src/{lib,session}.rs` and `providers/local.rs` for the exact paths — session create, chunk `POST {upload_url}?chunk={i}`, and whatever completion/callback the local policy does; also mirror how `crates/cloudreve-sync` mocks uploads if any of its tests do — grep `upload` in its tests/ first).
  - `env.uploaded_content(remote_name: &str) -> Option<Vec<u8>>` — reassembled bytes the mock received, in chunk order.
  - `env.upload_session_count() -> usize`
  - `env.fail_next_upload_sessions(n: usize)` — the next n session creations 500.
  - `env.set_remote_etag(name, etag)` — mutate the listing the conflict check will see.
- The smoke test drives `cloudreve_uploader::Uploader` DIRECTLY (not the facade — it doesn't write yet) with a small temp file and asserts `uploaded_content` matches, proving the mocks speak the real protocol.

- [ ] **Step 1:** smoke test + RED (helpers missing). **Step 2:** implement helpers until GREEN. **Step 3:** whole crate green. **Step 4:** No mutation step (harness); instead assert the smoke test fails if chunks are reassembled out of order (flip the mock's ordering once, observe red, restore). **Step 5:** Stage. Commit (BY THE USER): `test: upload-capable wiremock harness for the vfs crate`

---

### Task 6: DraftStore

**Files:**
- Modify: `crates/cloudreve-vfs/src/writeback.rs` (currently a placeholder)
- Test: unit tests in `writeback.rs` (pure fs, like cache.rs)

**Interfaces:**
- Produces (per D1/D2; Vfs wires it in Task 7):

```rust
pub const WRITEBACK_DEBOUNCE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq)]
pub enum DraftState { Editing, Pending, Uploading }

pub struct DraftStore { /* root = <cache_root>/drafts */ }

impl DraftStore {
    pub fn open(root: &Path) -> Result<Self>;               // scans existing draft.json files
    pub fn begin(&mut self, remote_path: &str, base_etag: &str, initial: DraftInit) -> Result<()>;
    pub fn write(&mut self, remote_path: &str, offset: u64, data: &[u8]) -> Result<()>;
    pub fn read(&mut self, remote_path: &str, offset: u64, len: u32) -> Result<Bytes>;
    pub fn truncate(&mut self, remote_path: &str, size: u64) -> Result<()>;
    pub fn size(&self, remote_path: &str) -> Option<u64>;
    pub fn state(&self, remote_path: &str) -> Option<DraftState>;
    pub fn set_state(&mut self, remote_path: &str, s: DraftState) -> Result<()>;
    pub fn base_etag(&self, remote_path: &str) -> Option<String>;
    pub fn data_path(&self, remote_path: &str) -> Option<PathBuf>; // the Uploader reads this file
    pub fn remove(&mut self, remote_path: &str) -> Result<()>;
    pub fn rename(&mut self, from: &str, to: &str) -> Result<()>;  // Task 10 uses it
    pub fn pending(&self) -> Vec<String>;                          // remote_paths in Pending|Uploading
}

pub enum DraftInit { Empty, Materialized(PathBuf) } // Materialized: pre-downloaded content moved in
```

Every mutation persists `draft.json` atomically (temp+rename — same idiom as
cache.rs's write_meta). Materialization itself (the download) is Task 7's
job in the facade; DraftStore only receives the file.

- [ ] **Step 1: failing unit tests** — concrete behaviors, one test each: write-then-read round-trip at an offset; truncate extends with zeros / shrinks; `open()` rescan restores state and sizes after "restart" (drop + reopen); `remove` deletes the dir; a draft in `Uploading` state found by `open()` is demoted to `Pending` (crash mid-upload must re-try, never be lost — comment why).
- [ ] **Step 2:** RED. **Step 3:** implement. **Step 4:** GREEN + whole crate. **Step 5:** Mutations: (a) skip the demote-on-scan → restart test fails; (b) make truncate a no-op → truncate test fails. Revert, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `feat: whole-file draft store for the vfs write path`

---

### Task 7: The facade writes (create/write/truncate + draft overlay)

**Files:**
- Modify: `crates/cloudreve-vfs/src/vfs.rs`
- Test: `crates/cloudreve-vfs/tests/write_back.rs`

**Interfaces:**
- Consumes: `DraftStore` (Task 6), refcount cache (Task 2).
- Produces (phase-3 frontends call exactly this):

```rust
impl Vfs {
    // NEW constructor shape (D4): the receiver carries VfsEvents.
    pub fn new(client, remote_base, cache_dir, cache_max_bytes)
        -> Result<(Self, mpsc::UnboundedReceiver<VfsEvent>)>;
    pub async fn create(&self, parent: NodeId, name: &str) -> Result<(NodeId, FileHandle)>;
    pub async fn write(&self, h: FileHandle, offset: u64, data: &[u8]) -> Result<u32>;
    pub async fn truncate(&self, h: FileHandle, size: u64) -> Result<()>;
    // Facade-level metadata that overlays drafts (D3) — frontends must use
    // these, not tree() directly, from phase 3 on:
    pub async fn readdir(&self, dir: NodeId) -> Result<Vec<(NodeId, NodeAttr)>>;
    pub async fn lookup(&self, parent: NodeId, name: &str) -> Result<Option<(NodeId, NodeAttr)>>;
    pub async fn getattr(&self, node: NodeId) -> Result<Option<NodeAttr>>;
}
```

Semantics: first `write` on a clean handle begins a draft (D2 — materialize
by downloading the whole file through the existing read path unless the
draft began via `create` or `truncate(0)`); once drafted, `read` serves from
the draft; `create` inserts a local-only tree entry (new tree helper
`insert_local_entry(parent, name) -> NodeId`, `#[doc(hidden)]` ok) so the
file lists immediately; `close` on a dirty draft sets `Pending` and pokes
the write-back queue (Task 8 — until then close just leaves `Pending`).

- [ ] **Step 1: failing tests** (write_back.rs; the essential four):

```rust
/// Editors' real save pattern: truncate + rewrite. No download may occur.
#[tokio::test]
async fn a_truncate_then_rewrite_downloads_nothing() { /* open existing 2MiB file,
    truncate(h,0), write new bytes, read back == new bytes,
    env.download_requests(name) is EMPTY */ }

/// A partial in-place write first materializes the original (D2).
#[tokio::test]
async fn a_partial_write_keeps_the_untouched_bytes() { /* 3MiB file, write 10 bytes
    at offset 1_500_000, read whole file back: prefix+suffix intact, middle new;
    downloads happened (materialization) */ }

/// A created file exists immediately for the frontends.
#[tokio::test]
async fn a_created_file_is_visible_before_any_upload() { /* create(root,"new.txt"),
    write, vfs.readdir(root) contains it with the draft size; server listing mock
    unchanged */ }

/// Drafted reads bypass the block cache.
#[tokio::test]
async fn reads_of_a_drafted_file_see_the_draft_not_the_cache() { /* warm the cache
    with a read, then truncate+write; read returns draft bytes, not cached */ }
```

- [ ] **Step 2:** RED. **Step 3:** implement (+ migrate the `Vfs::new` call sites in phase-1 tests to the tuple return). **Step 4:** GREEN + whole crate. **Step 5:** Mutations: (a) skip materialization (D2) → partial-write test fails on the prefix; (b) serve drafted reads from the block cache → bypass test fails. Revert, green.
- [ ] **Step 6 (ledger debt from phase 1):** add the missing read-path test `a_transient_error_then_an_expired_url_still_serves_the_read` — wiremock: first download attempt 500, second 403, URL refetch, success; asserts the bytes and that the 403 did not consume a backoff attempt (4 download hits total: 500, 403, then success on the fresh URL — count them). RED only if the behavior is broken; otherwise state it passed first try and mutate (`FETCH_RETRIES=1`) to prove teeth.
- [ ] **Step 7:** Stage. Commit (BY THE USER): `feat: create, write and truncate through the vfs facade with draft overlay`

---

### Task 8: Write-back queue (debounce, upload, conflicts, retry)

**Files:**
- Modify: `crates/cloudreve-vfs/src/writeback.rs` (queue half), `crates/cloudreve-vfs/src/vfs.rs` (close hook, events)
- Test: `crates/cloudreve-vfs/tests/write_back.rs`

**Interfaces:**
- Consumes: `DraftStore`, `cloudreve_uploader::{Uploader, UploadParams, UploaderConfig, NoSessionStore}` (Task 1), harness upload mocks (Task 5).
- Produces:

```rust
pub const UPLOAD_RETRIES: u32 = 3;
pub const UPLOAD_RETRY_BACKOFF: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(5)];

impl Vfs {
    /// Re-arms every Pending draft (offline recovery; phase 4 calls this on
    /// reconnect). Returns how many were queued.
    pub async fn retry_pending_uploads(&self) -> usize;
    /// Test/shutdown hook: resolves when no upload is queued or in flight.
    pub async fn wait_for_writeback_idle(&self);
}
```

Mechanics: `close` on a dirty draft → `Pending` + debounce timer
(`WRITEBACK_DEBOUNCE`); a re-open of the same path within the window cancels
the timer (draft goes back to `Editing`). The queue drains sequentially
(one upload at a time — YAGNI on parallelism). Per drain: conflict check
(D5) → `Uploading` → `Uploader` on `data_path` with
`previous_version = base_etag`, `overwrite = !base_etag.is_empty()` →
on success D6 + `UploadSucceeded`; on failure retry per the consts, then
back to `Pending` + `UploadFailed{will_retry: true}` (a later
`retry_pending_uploads` re-arms). Conflict path: upload to the conflict
name, D5, `ConflictSaved`, draft removed (its content is safe under the
copy), original invalidated.

- [ ] **Step 1: failing tests** (the essential five):

```rust
#[tokio::test] async fn a_closed_draft_uploads_and_the_server_receives_the_exact_bytes()
    { /* create+write+close, wait_for_writeback_idle, uploaded_content == bytes,
       event UploadSucceeded received, draft gone, subsequent read refetches (D6) */ }

#[tokio::test] async fn save_close_reopen_save_uploads_once()
    { /* write+close, immediately reopen+write+close, idle → upload_session_count()==1,
       final uploaded bytes = second save */ }

#[tokio::test] async fn a_remote_change_since_the_draft_began_becomes_a_conflict_copy()
    { /* file etag e1, draft begun, env.set_remote_etag(name,"e2"), close, idle →
       uploaded name contains "(conflict ", original untouched, ConflictSaved event */ }

#[tokio::test] async fn a_failed_upload_keeps_the_draft_and_retries_on_demand()
    { /* fail_next_upload_sessions(all attempts), close, idle → UploadFailed{will_retry:true},
       draft still Pending with the bytes intact; heal the mock; retry_pending_uploads()==1;
       idle → uploaded_content matches */ }

#[tokio::test] async fn nothing_is_uploaded_for_a_file_only_read()
    { /* phase-1 style read-only session, idle → upload_session_count()==0 */ }
```

- [ ] **Step 2:** RED. **Step 3:** implement. **Step 4:** GREEN + whole crate (debounce tests: drive time with real short waits or a test-visible debounce override — if 2s makes the suite crawl, add `#[cfg(test)] fn set_debounce_for_tests`, honest and localized). **Step 5:** Mutations: (a) skip the conflict check → conflict test fails; (b) debounce ignores the reopen → uploads-once test fails (2 sessions); (c) drop the draft on upload failure → retry test fails. Revert each, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `feat: debounced write-back uploads with conflict copies and retry`

---

### Task 9: Drafts survive a restart

**Files:**
- Modify: `crates/cloudreve-vfs/src/vfs.rs` (startup scan → re-enqueue)
- Test: `crates/cloudreve-vfs/tests/write_back.rs`

**Interfaces:** none new — `Vfs::new` calls `DraftStore::open` (already
demotes `Uploading`→`Pending`, Task 6) and enqueues every `Pending` draft
into the write-back queue at construction.

- [ ] **Step 1: failing test**

```rust
/// Quit mid-upload, relaunch: the edit still reaches the server.
#[tokio::test]
async fn pending_drafts_upload_after_a_restart() {
    /* build a Vfs with uploads FAILING (fail_next_upload_sessions), write+close,
       idle (draft parked Pending). DROP the Vfs entirely. Heal the mock.
       Construct a NEW Vfs over the same cache_dir → wait_for_writeback_idle →
       uploaded_content matches, draft gone. */
}
```

- [ ] **Step 2:** RED. **Step 3:** implement. **Step 4:** GREEN + whole crate. **Step 5:** Mutation: skip the startup enqueue → test hangs at idle with nothing uploaded (deadline-bounded assert). Revert, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `feat: pending drafts re-upload at the next launch`

---

### Task 10: mkdir, unlink, rename

**Files:**
- Modify: `crates/cloudreve-vfs/src/vfs.rs`, `crates/cloudreve-vfs/tests/common/mod.rs` (mocks for create_file/delete/rename/move)
- Test: `crates/cloudreve-vfs/tests/namespace_ops.rs` (new)

**Interfaces:**
- Produces:

```rust
impl Vfs {
    pub async fn mkdir(&self, parent: NodeId, name: &str) -> Result<NodeId>;
    pub async fn unlink(&self, parent: NodeId, name: &str) -> Result<()>;
    pub async fn rename(&self, parent: NodeId, name: &str,
                        new_parent: NodeId, new_name: &str) -> Result<()>;
}
```

Synchronous API calls (spec §5): `mkdir` → `create_file` (folder type —
mirror how the sync engine creates folders: grep `create_empty_file_or_folder`
in `crates/cloudreve-sync/src/tasks/upload.rs`); `unlink` → `delete_files`;
`rename` → same-dir `rename_file`, cross-dir `move_files` (+`rename_file`
when the leaf name changes too). Each op invalidates the affected tree
path(s). Draft interactions: unlink of a drafted file drops the draft (and
skips the API call if the file never existed remotely); rename of a drafted
file calls `DraftStore::rename` so the eventual upload targets the new path.

- [ ] **Step 1: failing tests** — five, one per behavior: mkdir lists immediately; unlink disappears + API hit recorded; rename same-dir; rename of a *drafted-new* file uploads to the NEW name only (no API rename call — nothing exists remotely yet); unlink of a drafted-new file makes no delete API call.
- [ ] **Step 2:** RED. **Step 3:** implement. **Step 4:** GREEN + whole crate. **Step 5:** Mutations: (a) skip tree invalidation after rename → the lists-immediately assertions fail; (b) always call the rename API even for drafted-new files → the no-API-call test fails. Revert, green.
- [ ] **Step 6:** Stage. Commit (BY THE USER): `feat: mkdir, unlink and rename through the vfs facade`

---

### Task 11: Phase gate

- [ ] **Step 1:** `cargo test --workspace` all green; `cargo clippy -p cloudreve-vfs -p cloudreve-uploader --all-targets` — zero warnings outside the pre-existing cloudreve-api set; workspace clippy count vs 86 baseline.
- [ ] **Step 2:** Coverage audit — every spec §5 bullet and every "Phase 2 MUST include" debt line in the master index maps to a passing test; mutation checks all ran (evidence in reports, not claims).
- [ ] **Step 3:** Update the master index: phase 2 row → done with test counts; strike the phase-2 debt lines, carry anything discovered-but-deferred into the phase-3/4 debt list.
- [ ] **Step 4:** Stage docs. Commit (BY THE USER): `docs: mark vfs phase 2 complete`
- [ ] **Step 5:** Final whole-phase review (most capable model) over the phase-2 commit range + ONE fix wave + scoped re-review, exactly like phase 1. Then write the phase-3 plan.
