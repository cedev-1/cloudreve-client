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
use cloudreve_api::models::explorer::{
    CreateFileService, DeleteFileService, FileURLService, MoveFileService, RenameFileService,
};
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::cache::{BlockCache, FileKey, BLOCK_SIZE};
use crate::tree::{NodeAttr, NodeId, VfsTree};
use crate::writeback::{DraftInit, DraftState, DraftStore, WriteBackQueue};

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
    ///
    /// `None` — an honest absence, not a dummy empty string — for a handle
    /// that never needs one at all: a brand-new local-only file (`create`)
    /// with no remote counterpart yet, or any handle opened on a path that
    /// already has an active draft (Task 9's promoted fix). Both read
    /// exclusively from the draft (see `Vfs::read`) and so never reach
    /// `fetch_range_with_retry`, the only consumer of this field. Should
    /// such a handle OUTLIVE its draft (the upload landed and removed it),
    /// reads fail loudly with [`StaleHandleError`] rather than falling
    /// through to the (purged) block cache — see `Vfs::read`.
    download_url: RwLock<Option<String>>,
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
/// tests) nothing at all. Emitted by `writeback::WriteBackQueue` (see
/// `Vfs::close`/`open`'s hooks into it) and, for `UploadCancelled`/
/// `UploadRenamed`, directly by `Vfs::open`/`Vfs::unlink` and
/// `WriteBackQueue::migrate_armed_timer` — see each variant's doc for its
/// exact emission site(s).
///
/// RECONSTRUCTION-COMPLETE STATE MACHINE (phase-4 obligation carried from
/// phase 2): every [`VfsEvent::UploadQueued`] for one draft's upload cycle
/// is eventually followed by EXACTLY ONE of the following, closing that
/// cycle out:
/// - [`VfsEvent::UploadSucceeded`] — the upload landed in place.
/// - [`VfsEvent::UploadFailed`] — every attempt in this cycle was
///   exhausted; the draft is parked `Pending` again, picked back up only by
///   an explicit `retry_pending_uploads` (or app restart) — which starts a
///   brand new `UploadQueued`/terminal cycle of its own, not a continuation
///   of this one. (No separate "this one will never retry" variant exists:
///   this event itself is always this cycle's terminal outcome, regardless
///   of its `will_retry` field — that field describes whether the DRAFT
///   overall will be retried later, not whether THIS event has a
///   successor.)
/// - [`VfsEvent::UploadCancelled`] — the debounce timer armed for this
///   cycle was stopped before it ever fired: a reopen-for-write within the
///   window (`Vfs::open`), or `Vfs::unlink` dropping the queued draft
///   outright. Nothing was ever uploaded for this cycle.
/// - [`VfsEvent::UploadRenamed`] — `Vfs::rename` migrated this cycle's
///   still-armed timer to a new path
///   (`WriteBackQueue::migrate_armed_timer`) before it fired. The lifecycle
///   continues uninterrupted under the NEW path: a fresh
///   `UploadQueued { remote_path: to }` follows immediately, itself
///   guaranteed one of these same terminal outcomes.
/// - [`VfsEvent::ConflictSaved`] — the upload detected a remote change
///   since the draft began and landed as a conflict copy instead; the
///   original draft is settled the same way `UploadSucceeded` settles one
///   (removed, or kept and re-armed — a fresh `UploadQueued` — if written
///   again mid-flight; see `WriteBackQueue::settle_uploaded_draft`).
///
/// KNOWN GAP (pre-existing, narrower than the three sites above): a draft
/// queued via `retry_pending_uploads`/the startup resume
/// (`WriteBackQueue::enqueue_immediate`, which spawns its upload
/// immediately with no debounce timer for `cancel`/`migrate_armed_timer` to
/// find) that gets unlinked or renamed in the brief scheduling window
/// between being spawned and `process` actually running gets NO terminal
/// event for that cycle: `cancel` finds nothing armed to stop, and
/// `process` finds its draft already gone and returns silently (no event).
/// Never observed to matter in practice (the window is a few microseconds
/// of tokio scheduling, not a debounce-length one), and closing it fully
/// would need synchronizing `enqueue_immediate`'s spawn against
/// `unlink`/`rename`'s own per-path locks — out of scope for this task.
#[derive(Debug, Clone, PartialEq)]
pub enum VfsEvent {
    UploadQueued { remote_path: String },
    UploadSucceeded { remote_path: String, new_etag: String },
    UploadFailed { remote_path: String, error: String, will_retry: bool },
    /// A queued upload's debounce timer was stopped before it fired:
    /// `Vfs::open` cancelling it on a reopen-for-write within the window,
    /// or `Vfs::unlink` dropping the queued draft outright. See the enum's
    /// own doc for the full terminal-state contract.
    UploadCancelled { remote_path: String },
    /// A queued upload's still-armed debounce timer was migrated to a new
    /// path by `Vfs::rename` (via `WriteBackQueue::migrate_armed_timer`)
    /// before it fired. The lifecycle continues under `to`: expect a fresh
    /// `UploadQueued { remote_path: to }` immediately after this event.
    UploadRenamed { from: String, to: String },
    ConflictSaved { original: String, conflict_copy: String },
}

/// Distinct error returned by [`Vfs::read`] for a handle that outlived its
/// draft: the handle was opened while a draft existed for its path (so it
/// carries no download URL and a frozen pre-upload size), and that draft has
/// since been uploaded and removed. The handle's frozen view of the file can
/// no longer be served honestly — the block cache was purged of the file's
/// pre-upload blocks on upload success, and the handle has no URL (nor the
/// file's new etag/size) to refetch with. Frontends translate this to
/// `EIO`/`ESTALE`; reopening the file yields a fresh handle with the
/// current key, size, and URL. Detectable via
/// `anyhow::Error::downcast_ref::<StaleHandleError>()`.
#[derive(Debug, Clone)]
pub struct StaleHandleError {
    pub remote_path: String,
}

impl std::fmt::Display for StaleHandleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "stale file handle for {}: its draft was uploaded and removed while the handle \
             stayed open — reopen the file",
            self.remote_path
        )
    }
}

impl std::error::Error for StaleHandleError {}

/// Returned by [`Vfs::rename`] when a handle is currently open (or a live
/// draft sits) on the entry being renamed — its SOURCE path, before the
/// change, or (phase 4, deliverable C) its DESTINATION path: the
/// atomic-save idiom every real editor uses is "write a tmp file, then
/// rename it OVER the target" — if the target is the file the user still
/// has open, that rename must not silently clobber the handle's view of the
/// world any more than renaming the source out from under an open handle
/// may. Phase 2 shipped the source-side hazard as documented UB instead of
/// a guard — an `OpenFile`'s `key.remote_path` is fixed at `open`/`create`
/// time and `rename` never touches it, so a handle that outlived a rename
/// kept reading/writing under the OLD path while the tree believed the
/// entry lived at the new one. This error replaces that hazard (both
/// sides) with an explicit EBUSY-class refusal: frontends (NFS/FUSE) map it
/// to `EBUSY`, exactly like renaming a file still open under POSIX on a
/// filesystem that enforces the same restriction. Closing every handle on
/// the entry (or letting the frontend serialize instead of deny, if it
/// prefers) and retrying succeeds. Detectable via
/// `anyhow::Error::downcast_ref::<RenameBusyError>()`.
#[derive(Debug, Clone)]
pub struct RenameBusyError {
    pub remote_path: String,
}

impl std::fmt::Display for RenameBusyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot rename {}: a handle is still open on it (or a live draft sits on it) — \
             close it first",
            self.remote_path
        )
    }
}

impl std::error::Error for RenameBusyError {}

/// Returned by [`Vfs::unlink`] when a handle is currently open on the FILE
/// being removed (phase 4, deliverable B). Without this guard, deleting a
/// file out from under an open handle risks silent resurrection: the
/// handle's `download_url`/key stay valid (unlike a rename, `unlink` never
/// touches `OpenFile` at all), so a subsequent `write` on it happily calls
/// `ensure_drafted` -> `materialize`, re-fetching the (deleted, but perhaps
/// not yet gone from the block cache or a soft-delete-lagging server) old
/// content into a BRAND NEW draft and eventually re-uploading it — silently
/// undoing the delete the user just performed elsewhere. A directory is
/// never "open" in this sense at all (see `Vfs::open`'s doc, it bails on
/// one) — see [`DirNotEmptyError`] for the guard that applies to a
/// directory's removal instead.
///
/// A SEPARATE type from [`RenameBusyError`] rather than a rename of that
/// type (least-churn judgment call, phase 4 task 2): `RenameBusyError` is a
/// public type multiple call sites downcast on by name
/// (`frontend_util::classify_error`, both adapters' tests) — renaming it to
/// something generic would touch every one of those for no behavioral gain,
/// whereas adding this sibling only requires one new downcast arm in
/// `classify_error`. Both types classify to the same
/// [`crate::frontend_util::FrontendErrno::Busy`] and thus the same wire/errno
/// on both frontends — the split is about naming which OPERATION was
/// refused, not about a different frontend-visible outcome. Detectable via
/// `anyhow::Error::downcast_ref::<UnlinkBusyError>()`.
#[derive(Debug, Clone)]
pub struct UnlinkBusyError {
    pub remote_path: String,
}

impl std::fmt::Display for UnlinkBusyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot delete {}: a handle is still open on it — close it first", self.remote_path)
    }
}

impl std::error::Error for UnlinkBusyError {}

/// Returned by [`Vfs::unlink`] when asked to remove a DIRECTORY that is not
/// empty (phase 4, deliverable D). This facade never does a recursive
/// delete — POSIX `rmdir` itself refuses a non-empty directory
/// (`ENOTDIR`/`ENOTEMPTY`), and silently cascading a delete through an
/// entire subtree (potentially discarding unsaved drafts several levels
/// down) is a far more destructive default than this crate should pick on
/// a caller's behalf. "Empty" is judged the same way `Vfs::readdir` builds
/// a listing: the real server listing PLUS the local overlay of any
/// not-yet-confirmed `create()`d child — so a brand-new, never-uploaded
/// file sitting in the directory counts as an occupant exactly like a real
/// one, even though the server itself doesn't know it exists yet. Detectable
/// via `anyhow::Error::downcast_ref::<DirNotEmptyError>()`.
#[derive(Debug, Clone)]
pub struct DirNotEmptyError {
    pub remote_path: String,
}

impl std::fmt::Display for DirNotEmptyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot remove {}: the directory is not empty", self.remote_path)
    }
}

impl std::error::Error for DirNotEmptyError {}

/// Returned by [`Vfs::rename`] when a drafted (never-uploaded, empty
/// `base_etag`) FILE source is renamed onto a destination name that
/// currently resolves to an existing DIRECTORY (phase 4 task 3, R1 —
/// routed from the task 2 re-review). Deleting a directory to "make room"
/// for a file is far more destructive than this call should ever do on a
/// caller's behalf (same reasoning `rename`'s own doc gives for never
/// bridging a directory destination at all), so this refuses loudly instead
/// of the alternative this fix replaces: silently letting `rename()` return
/// `Ok(())` with the draft migrated in name only, its base etag still
/// empty, so its eventual upload ran `overwrite=false` against the
/// directory's own uri and retried an identical, doomed `ObjectExisted`
/// (40004) forever — a different door onto the exact class of bug R2's
/// bridge closes for the ordinary rename-onto-existing-FILE case. A REMOTE
/// (already-uploaded) source hitting the same collision is NOT covered by
/// this type: it still falls through to the server's own native refusal on
/// the `rename_file`/`move_files` call (surfaces as a generic `Io`/`EIO`
/// via `classify_error`'s fallback) — see `rename`'s own doc for why only
/// the drafted-source path needed a facade-level guard at all: a remote
/// source's collision is always caught by a real API call, a drafted
/// source's never is, since nothing ever reaches the server until the
/// eventual, decoupled upload. Detectable via
/// `anyhow::Error::downcast_ref::<RenameOntoDirectoryError>()`.
#[derive(Debug, Clone)]
pub struct RenameOntoDirectoryError {
    pub remote_path: String,
}

impl std::fmt::Display for RenameOntoDirectoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot rename onto {}: it is a directory", self.remote_path)
    }
}

impl std::error::Error for RenameOntoDirectoryError {}

/// Test-only rendezvous for pausing `create()` mid-body — see
/// `Vfs::pause_create_before_registration_for_tests`. Exists because the
/// window this is meant to test (`create` making a name visible before it
/// finishes registering its own handle) has no `.await` inside it in
/// production, so it is sub-microsecond and cannot be reliably raced by a
/// plain concurrent-tasks test; this widens it to something deterministic.
pub struct CreatePauseHook {
    /// Notified once by `create` the instant it reaches the pause point —
    /// lets a test know it is now safe to run whatever it wants to race
    /// against the still-parked `create` call.
    pub parked: tokio::sync::Notify,
    /// Notified once by the test to let the parked `create` call continue.
    pub resume: tokio::sync::Notify,
}

impl CreatePauseHook {
    pub fn new() -> Self {
        Self { parked: tokio::sync::Notify::new(), resume: tokio::sync::Notify::new() }
    }
}

impl Default for CreatePauseHook {
    fn default() -> Self {
        Self::new()
    }
}

/// Test-only rendezvous for pausing `open()` mid-body — see
/// `Vfs::pause_open_before_registration_for_tests`. A separate type from
/// [`CreatePauseHook`] rather than a shared one (same least-churn reasoning
/// as [`UnlinkBusyError`] vs [`RenameBusyError`]): each hook name maps 1:1
/// to the single method it pauses, and the two are never armed at once by
/// any real test, so there is nothing to gain from unifying them beyond a
/// cosmetic rename that would touch `namespace_ops.rs`'s existing import for
/// no behavioral change. Same reasoning as `CreatePauseHook` for why this
/// exists at all: the window `open()` needs to pause in (attrs resolved,
/// locks held, but the handle not yet registered in `open_files`) has no
/// `.await` of its own in production, so it's sub-microsecond and cannot be
/// reliably raced by a plain concurrent-tasks test.
pub struct OpenPauseHook {
    /// Notified once by `open` the instant it reaches the pause point.
    pub parked: tokio::sync::Notify,
    /// Notified once by the test to let the parked `open` call continue.
    pub resume: tokio::sync::Notify,
}

impl OpenPauseHook {
    pub fn new() -> Self {
        Self { parked: tokio::sync::Notify::new(), resume: tokio::sync::Notify::new() }
    }
}

impl Default for OpenPauseHook {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Vfs {
    /// D10 (phase 4): the hierarchical namespace guard closing the
    /// descendant check-then-act residual the phase-3 final review disclosed
    /// (`is_subtree_open`'s doc, and `rename`'s: "closing it fully needs
    /// hierarchical locking; tracked as a phase-4 obligation"). Without this,
    /// a directory `rename`'s subtree busy-check
    /// (`is_subtree_open`/`has_draft_strictly_under`, both keyed on
    /// `old_path`'s exact string) and a BRAND-NEW descendant `open`/`create`
    /// racing it (keyed on the DESCENDANT's own, different, path) use
    /// unrelated lock keys and can interleave: the rename's check can run in
    /// the instant between the descendant call resolving its attrs and it
    /// registering its handle, see nothing, and let the rename through —
    /// exactly the resurrection bug this whole guard family exists to close,
    /// just one level up.
    ///
    /// THE NEW OUTERMOST LOCK LEVEL IN THIS CRATE — CRITICAL ORDERING RULE:
    /// acquired BEFORE any `open_lock_for`/`open_locks` use, in EVERY method
    /// that touches it, with NO exceptions. `open`/`create` take `read()`
    /// across their full lookup-to-registration window (the same window
    /// `open_locks` already serializes per-EXACT-path — this adds the
    /// per-SUBTREE serialization `open_locks` structurally cannot); `rename`
    /// takes `write()` across check+server-call+migration, but ONLY when the
    /// entry being renamed is a DIRECTORY (a file has no descendants, so its
    /// existing exact-path `open_locks` guard is already fully race-free —
    /// see `rename`'s own doc for why it deliberately takes no namespace
    /// lock at all in the file case); `unlink` of a DIRECTORY takes `write()`
    /// the same way, across its emptiness check and the actual delete call
    /// (a FILE `unlink` likewise takes none, same file-has-no-descendants
    /// reasoning). Never acquired by `readdir`/`lookup`/`getattr`/`write`/
    /// `truncate`/`close`/`mkdir` — none of them can create the
    /// registration-window race this exists to close, and `unlink`'s
    /// directory branch calls `Vfs::readdir` internally WHILE HOLDING
    /// `write()`, which would deadlock instantly against a `read()`
    /// acquisition inside `readdir` (a `tokio::sync::RwLock` is not
    /// reentrant) — this is exactly why `readdir` must never take it.
    ///
    /// Read-mostly by construction: only a DIRECTORY rename/unlink ever
    /// takes `write()`, an uncommon operation next to the steady stream of
    /// `open`/`create` calls a mounted drive produces, so contention in
    /// practice is rare and brief (one subtree op at a time, never blocking
    /// other subtree ops from PROCEEDING once the current writer commits).
    /// Coarse-grained on purpose (a single lock for the whole `Vfs`, not
    /// keyed per-directory): D10 accepts this in exchange for having no new
    /// lock-ordering pairs to reason about — see each acquiring method's own
    /// doc for its exact position in the sequence, and the phase-4 task
    /// report for the full per-method acquisition table.
    namespace_lock: tokio::sync::RwLock<()>,

    /// `Arc`-wrapped (not owned outright) because the write-back queue's
    /// background tasks (Task 8) need a `'static` handle that outlives any
    /// particular `&Vfs` call — `tokio::spawn` requires it.
    tree: Arc<VfsTree>,
    cache: Arc<Mutex<BlockCache>>,
    /// Whole-file local drafts absorbing every write (D1/D2/D3). A plain
    /// `Mutex`, not the cache's own lock: `DraftStore`'s methods are all
    /// synchronous `&mut self`, so this is held only for the duration of one
    /// disk op at a time, same discipline as `cache` — never across a
    /// network await. `Arc`-wrapped for the same reason as `tree` above:
    /// the write-back queue's background tasks need to lock it too, well
    /// after the `&Vfs` call (`close`) that armed them has returned.
    drafts: Arc<Mutex<DraftStore>>,
    /// Debounces a closed dirty draft, then drains it through the real
    /// uploader (Task 8) — conflict check, retry, and the events below are
    /// all its responsibility. See `writeback::WriteBackQueue`.
    write_queue: WriteBackQueue,
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
    ///
    /// LOCK ORDERING RULE (with `open_locks` below): `namespace_lock` before
    /// `open_lock` before `draft_begin_lock`, ALWAYS — see `namespace_lock`'s
    /// own field doc for why it is the new outermost level (phase 4, D10).
    /// `create` is the only call site that ever
    /// holds both for the same path at once (see its body), and it
    /// acquires `open_lock` first, `draft_begin_lock` nested inside it.
    /// `open`/`rename` only ever touch `open_locks`; `write`/`truncate`
    /// (via `ensure_drafted`) only ever touch `draft_begin_locks` — neither
    /// pair nests the two locks in the opposite order, so this rule has
    /// nothing to conflict with today. Keep it that way: introducing a
    /// second call site that acquires `draft_begin_lock` first and
    /// `open_lock` second would deadlock against `create`.
    draft_begin_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Per-path async locks making `open`'s handle registration,
    /// `rename`'s open-handle guard, and `create`'s own handle registration
    /// race-free with respect to each other (cycle A of the phase-2 debt
    /// burn-down, plus its coordinator-review follow-up for `create`).
    /// `open` takes the lock for its target path before doing anything else
    /// and holds it through registering the new handle in `open_files`;
    /// `rename` takes the SAME lock for its source path before checking
    /// `open_files` and holds it through the whole rename; `create` takes
    /// it for its prospective path before even checking EEXIST and holds it
    /// through registering ITS new handle. That makes all three operations
    /// strictly ordered for a given path: an `open`/`create` already in
    /// flight always finishes (and is correctly seen as busy) before a
    /// racing `rename`'s check runs, and an `open`/`create` arriving after
    /// `rename` has taken the lock blocks until the rename fully commits —
    /// there is no window in which a handle can slip into existence unseen
    /// between the check and the actual rename, whether that handle belongs
    /// to a pre-existing file (`open`) or a brand-new one still being
    /// created (`create`). See `draft_begin_locks`'s doc above for the
    /// ordering rule the two lock maps must keep with respect to each
    /// other. Same non-removal/std-Mutex-only-for-the-instant-of-lookup
    /// discipline as `draft_begin_locks`.
    ///
    /// EXTENDED SCOPE (phase 4, deliverable C): `rename` now also takes the
    /// lock keyed to its DESTINATION path (in addition to its source path,
    /// unchanged from before), sorted lexicographically with the source
    /// lock so the two are always acquired in the SAME global order
    /// regardless of which one is the source and which the destination —
    /// otherwise two concurrent renames that swap two paths (A onto B, and B
    /// onto A, at once) could each hold one lock while waiting on the other.
    /// `unlink` (phase 4, deliverable B) also takes this lock for its own
    /// target path, unconditionally (file or directory) — see its own doc
    /// for why a directory branch still takes it (to serialize against a
    /// `rename` of that same directory) despite never checking
    /// `is_subtree_open` itself. See `namespace_lock`'s doc above for the
    /// new, stricter rule this map must now also respect:
    /// `namespace_lock` before any use of this map, everywhere, no
    /// exceptions.
    open_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Test-only pause point for `create` — see `CreatePauseHook`'s doc and
    /// `pause_create_before_registration_for_tests`. `None` (the default)
    /// means `create` never pauses, which is every non-test call.
    create_pause_hook: std::sync::Mutex<Option<Arc<CreatePauseHook>>>,
    /// Test-only pause point for `open` — see `OpenPauseHook`'s doc and
    /// `pause_open_before_registration_for_tests`. `None` (the default)
    /// means `open` never pauses, which is every non-test call.
    open_pause_hook: std::sync::Mutex<Option<Arc<OpenPauseHook>>>,
    /// Sender half of the `VfsEvent` channel returned by `new`. A clone of
    /// this lives inside `write_queue`, which emits most events; this
    /// facade-level copy is what `open`/`unlink` use for the two terminal
    /// events they emit directly (`UploadCancelled` on a reopen/unlink
    /// racing a still-armed debounce timer — see `VfsEvent`'s doc).
    events: mpsc::UnboundedSender<VfsEvent>,
}

impl Vfs {
    pub fn new(
        client: Arc<cloudreve_api::Client>,
        remote_base: String,
        cache_dir: &Path,
        cache_max_bytes: u64,
    ) -> Result<(Self, mpsc::UnboundedReceiver<VfsEvent>)> {
        let tree = Arc::new(VfsTree::new(client.clone(), remote_base));
        // D1 TRAP: `BlockCache::open` deletes any directory under its root
        // that lacks a `meta.json` — pointing it at a root that also holds
        // `drafts/` would destroy every draft at startup the first time a
        // draft directory (which has no `meta.json`, only `draft.json`) is
        // scanned. Segregate the two roots before either is ever opened.
        let cache = Arc::new(Mutex::new(
            BlockCache::open(&cache_dir.join("blocks"), cache_max_bytes)
                .context("failed to open block cache")?,
        ));
        // Cycle C (phase-2 debt burn-down): sweep any leftover contents of
        // `materialize`'s temp directory. A temp file there is referenced
        // ONLY within the single `materialize` call that created it — it's
        // either moved into the drafts store before that call returns
        // (`DraftStore::begin`'s `Materialized` case) or never referenced
        // again at all. A file still sitting here at startup can therefore
        // only be an orphan from an earlier, unclean shutdown (the process
        // died mid-download, before the move), and — unlike a draft — it
        // has no metadata identifying which write it belonged to, so
        // there's nothing to resume; it's pure leftover. Swept unconditionally,
        // before anything else opens a fallible root, so a leftover from a
        // previous crash can never accumulate across restarts. Sweeps
        // CONTENTS only, not the directory itself: `materialize` always
        // `create_dir_all`s it again anyway, but a missing directory would
        // make `tmp_dir.exists()` a surprising false right after `Vfs::new`
        // for any caller inspecting the cache layout. Best-effort: a failure
        // to remove one stray entry only leaves that entry behind — it must
        // never fail the whole constructor over a leftover file this crate
        // was already free to ignore.
        if let Ok(entries) = std::fs::read_dir(cache_dir.join("tmp")) {
            for entry in entries.flatten() {
                let path = entry.path();
                let result = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_file(&path)
                };
                if let Err(err) = result {
                    tracing::warn!(
                        path = %path.display(),
                        %err,
                        "vfs: failed to sweep a leftover materialization temp"
                    );
                }
            }
        }

        let drafts_store =
            DraftStore::open(&cache_dir.join("drafts")).context("failed to open draft store")?;
        // Task 9: every draft still `Pending` here survived an unclean
        // shutdown — either a debounce timer that never got the chance to
        // fire, or an upload that was in flight when the process died
        // (already demoted back to `Pending` by `open` above, Task 6).
        // Read the list off the plain store now, before it is wrapped in
        // its `Arc<Mutex>` below: nothing else has a handle to it yet, so
        // this needs no lock/await, unlike `WriteBackQueue::retry_pending`'s
        // own (otherwise identical) read of the same list.
        let resume_paths = drafts_store.pending();
        let drafts = Arc::new(Mutex::new(drafts_store));
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .context("failed to build the vfs http client")?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let write_queue = WriteBackQueue::new(
            client.clone(),
            tree.clone(),
            drafts.clone(),
            cache.clone(),
            events_tx.clone(),
        );
        // Re-enqueue them for an immediate upload attempt now, before this
        // constructor returns — synchronously bumping the queue's `busy`
        // counter here (not from a spawned task) closes the race a test
        // calling `wait_for_writeback_idle` right after `Vfs::new` would
        // otherwise hit: it must never see a falsely idle queue with the
        // re-enqueue still pending.
        write_queue.enqueue_immediate(resume_paths);
        Ok((
            Self {
                namespace_lock: tokio::sync::RwLock::new(()),
                tree,
                cache,
                drafts,
                write_queue,
                client,
                http,
                cache_dir: cache_dir.to_path_buf(),
                open_files: RwLock::new(HashMap::new()),
                next_handle: AtomicU64::new(1),
                next_tmp: AtomicU64::new(1),
                readahead_inflight: Arc::new(std::sync::Mutex::new(HashSet::new())),
                draft_begin_locks: std::sync::Mutex::new(HashMap::new()),
                open_locks: std::sync::Mutex::new(HashMap::new()),
                create_pause_hook: std::sync::Mutex::new(None),
                open_pause_hook: std::sync::Mutex::new(None),
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
        // D10 (phase 4, outermost lock — see `namespace_lock`'s field doc):
        // held `read()` across this ENTIRE call, including registration at
        // the very end. Taken FIRST, before even resolving `attr` below —
        // this is what makes a racing DIRECTORY `rename`/`unlink` (which
        // takes `write()`) unable to even START its subtree busy-check while
        // this open is still in flight, closing the phase-3-disclosed
        // "check-then-act across different lock keys" residual: without
        // this, the rename's check and this open's registration used
        // unrelated per-path lock keys and could interleave.
        let _ns_guard = self.namespace_lock.read().await;

        let attr = self
            .tree
            .getattr(node)
            .await?
            .context("open: unknown node (readdir/lookup it first)")?;
        if attr.is_dir {
            bail!("cannot open a directory as a file");
        }

        // Held through the whole rest of this call, including registering
        // the new handle at the very end: this is the other half of
        // `rename`'s EBUSY guard (see `open_locks`'s field doc) — an open
        // already in progress for this path always finishes (and becomes
        // visible in `open_files`) before a racing `rename`'s busy-check can
        // run.
        //
        // The reverse ordering is NOT fully closed, and an earlier version
        // of this comment overclaimed that it was. `attr` above is resolved
        // (via `tree.getattr`) BEFORE this lock is taken, so an open racing
        // a `rename` that commits first still waits here on the SAME lock
        // key (both resolve to the entry's pre-rename path) but then
        // resumes holding a now-STALE `attr.remote_path` — waiting does NOT
        // re-resolve it. In practice this fails LOUDLY rather than leaving a
        // surviving stale handle: by the time the rename has committed, its
        // draft (if any) has already migrated to the new path, so
        // `has_draft` below is false, and the `fetch_download_url` call
        // further down 404s against the server for a path that no longer
        // exists there — this whole function returns that error before ever
        // reaching the final `open_files.write().await.insert(..)`, so
        // nothing stale is ever registered. Reachable only via NFS's
        // per-RPC concurrency (FUSE's single dispatch thread can't race its
        // own calls this way); `create` has the same shape, narrower
        // (its parent attr is likewise resolved before its own
        // `open_lock_for` call).
        let open_lock = self.open_lock_for(&attr.remote_path);
        let _open_guard = open_lock.lock().await;

        let key = FileKey { remote_path: attr.remote_path.clone(), etag: attr.etag.clone() };
        self.cache.lock().await.retain(&key);

        // Task 9 (promoted fix): a path with an active draft is always read
        // straight from that draft (see `read`), never through
        // `fetch_range_with_retry` — so a handle opened on one never needs
        // a download URL at all. Fetching one unconditionally used to fail
        // outright for a LOCAL-ONLY file (`create`, empty `etag`/
        // `base_etag`): the server has never heard of a path that has never
        // been uploaded, so there was nothing for the fetch to resolve.
        // Skipping it here also removes the only fallible step that used to
        // stand between "a draft exists" and the `Pending` -> `Editing`
        // flip just below — see that block's doc for why the ordering
        // concern from review finding 1 (Task 8) no longer applies.
        let has_draft = self.drafts.lock().await.state(&attr.remote_path).is_some();
        let download_url = if has_draft {
            None
        } else {
            Some(fetch_download_url(&self.client, &key).await?)
        };

        // Task 8: a reopen within the write-back debounce window cancels
        // it and puts the draft back into `Editing` — the whole point of
        // debouncing at all is to coalesce exactly this "save, then
        // immediately keep editing" pattern into one eventual upload.
        // `cancel` only reports success (and only then do we flip the
        // state) if the timer had not already fired: a draft whose upload
        // is already underway, or one that already exhausted its retries
        // and is legitimately parked `Pending` for a manual retry, must be
        // left alone — reopening it to look at its bytes (D3) must not
        // silently un-park it.
        //
        // Review finding 1 (Task 8) originally required this block to run
        // only AFTER a fallible `fetch_download_url` immediately above, so
        // a failed reopen (e.g. offline) could never strand the draft in
        // `Editing` with no timer and no handle to ever close it again —
        // `retry_pending_uploads` only re-arms `Pending` drafts, so an
        // acknowledged save would otherwise never reach the server even
        // after reconnecting. Task 9's fix above removes that hazard by
        // construction rather than by position: this block only ever runs
        // when `has_draft` is true (`state(..)` must be `Some` to match
        // `Pending` below), and `has_draft` true is exactly the branch that
        // skips the fetch entirely. There is no fallible network step left
        // anywhere between "a draft exists" and this flip, so "an
        // acknowledged save always eventually uploads" now holds
        // unconditionally, not merely because of where this code sits.
        {
            let mut drafts = self.drafts.lock().await;
            if let Some(DraftState::Pending) = drafts.state(&attr.remote_path) {
                if self.write_queue.cancel(&attr.remote_path) {
                    // ORDERING CONTRACT (reviewer-caught, fix round 1): send
                    // the terminal event BEFORE the fallible `set_state`
                    // call below, never after. `cancel()` returning `true`
                    // just above already `abort()`ed the debounce timer —
                    // that is the commitment point, and it is irreversible:
                    // nothing will ever fire for this cycle through the
                    // normal timer path again. If the event send instead
                    // sat downstream of `set_state`'s disk write and that
                    // write failed, `open()` would return early via `?` and
                    // this cycle's `UploadCancelled` would be silently
                    // swallowed forever — a permanent hole in the
                    // reconstruction-complete invariant documented on
                    // `VfsEvent`, for no benefit (the in-memory draft state
                    // flips to `Editing` regardless of whether the persist
                    // below succeeds — see `DraftStore::set_state`'s doc).
                    // Mirrors `Vfs::unlink`'s already-correct ordering.
                    // Pinned by
                    // `a_reopen_cancel_still_emits_cancelled_even_if_persisting_editing_fails`
                    // via `fail_next_draft_persist_for_tests`.
                    let _ = self
                        .events
                        .send(VfsEvent::UploadCancelled { remote_path: attr.remote_path.clone() });
                    drafts.set_state(&attr.remote_path, DraftState::Editing)?;
                }
            }
        }

        // Test-only pause point — see `OpenPauseHook`'s doc. A no-op
        // (`open_pause_hook` is `None`) on every real call. Still holding
        // `namespace_lock.read()` and `open_lock` across this pause, exactly
        // as a real in-flight `open` would while awaiting the network calls
        // above — the point is to widen this specific window
        // (resolved-but-not-yet-registered) to something a test can
        // deterministically land a racing directory rename in.
        let pause_hook = self.open_pause_hook.lock().unwrap().clone();
        if let Some(hook) = pause_hook {
            hook.parked.notify_one();
            hook.resume.notified().await;
        }

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

        // No draft + no download URL: this handle was opened while a draft
        // existed (see `download_url`'s field doc) and that draft has since
        // uploaded and been removed. Its frozen key/size describe a version
        // of the file that no longer exists anywhere reachable — the
        // upload's success purged the pre-upload blocks, and the handle has
        // no URL to refetch with. Fail loudly with the distinct error
        // rather than falling through to `read_from_cache`, which would
        // silently serve whatever stale bytes (or emptiness, for a created
        // file whose frozen size is 0) the caches happen to hold. Checked
        // BEFORE `read_from_cache`'s EOF clamp on purpose: that clamp would
        // otherwise answer a created-file handle's reads with clean empty
        // results forever.
        if of.download_url.read().await.is_none() {
            return Err(anyhow::Error::new(StaleHandleError {
                remote_path: of.key.remote_path.clone(),
            }));
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
    /// not yet queued), it is parked `Pending` and the write-back queue's
    /// debounce timer is armed for it (Task 8) — a reopen of the same path
    /// within the window cancels it again (see `open`).
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
                drop(drafts);
                self.write_queue.arm(of.key.remote_path.clone());
            }
        }
        self.cache.lock().await.release(&of.key);
        Ok(())
    }

    /// Test-only: makes every subsequent `open()` call pause mid-body —
    /// after resolving attrs and running the reopen-cancel dance, before
    /// this call registers its own handle in `open_files` — until the test
    /// notifies `hook.resume`. See `OpenPauseHook`'s doc for why this exists
    /// instead of a plain racing-tasks test (deliverable A, phase 4: races a
    /// directory `rename` against this exact window). Same non-`#[cfg(test)]`
    /// reasoning as `pause_create_before_registration_for_tests`.
    pub fn pause_open_before_registration_for_tests(&self, hook: Arc<OpenPauseHook>) {
        *self.open_pause_hook.lock().unwrap() = Some(hook);
    }

    /// Re-arms every draft still `Pending` for immediate upload, bypassing
    /// the debounce entirely — offline recovery: phase 4 calls this on
    /// reconnect. Returns how many were queued.
    pub async fn retry_pending_uploads(&self) -> usize {
        self.write_queue.retry_pending().await
    }

    /// Resolves once the write-back queue has nothing armed, queued, or
    /// uploading. Test and shutdown hook.
    pub async fn wait_for_writeback_idle(&self) {
        self.write_queue.wait_idle().await;
    }

    /// Test-only: overrides the write-back debounce delay so the suite
    /// doesn't have to sit through the real `WRITEBACK_DEBOUNCE`. See
    /// `WriteBackQueue`'s `debounce` field doc for why this isn't gated by
    /// `#[cfg(test)]`.
    pub fn set_debounce_for_tests(&self, d: Duration) {
        self.write_queue.set_debounce_for_tests(d);
    }

    /// Test-only: overrides the upload retry backoff. Same reasoning as
    /// `set_debounce_for_tests`.
    pub fn set_retry_backoff_for_tests(&self, backoff: [Duration; 2]) {
        self.write_queue.set_retry_backoff_for_tests(backoff);
    }

    /// Test-only: forces a draft's state directly, bypassing every normal
    /// transition path (`close`'s arm, `open`'s cancel, `process`'s own
    /// transitions). Same non-`#[cfg(test)]` reasoning as the two methods
    /// above. Exists specifically to reproduce, without depending on the
    /// actual microsecond scheduling race, the END STATE a cycle stranded
    /// by a rename racing a firing debounce timer leaves behind — see
    /// `WriteBackQueue::migrate_armed_timer`'s doc, case (b) — so the
    /// recovery path (`retry_pending_uploads`) can be pinned by a
    /// deterministic test.
    pub async fn set_draft_state_for_tests(&self, remote_path: &str, state: DraftState) -> Result<()> {
        self.drafts.lock().await.set_state(remote_path, state)
    }

    /// Test-only: reads a draft's current state directly, without going
    /// through any facade-level overlay. The read-only counterpart of
    /// `set_draft_state_for_tests` above — same non-`#[cfg(test)]`
    /// reasoning. Used to observe a draft settle into `Uploading` (proving
    /// a debounced upload attempt actually started) without depending on
    /// `wait_for_writeback_idle`, which a still-in-flight or panicked
    /// attempt may not resolve promptly (phase 4, this task's panic test).
    pub async fn draft_state_for_tests(&self, remote_path: &str) -> Option<DraftState> {
        self.drafts.lock().await.state(remote_path)
    }

    /// Test-only: makes the very next write-back `process` call panic right
    /// before it would hand the draft's bytes to the uploader — see
    /// `WriteBackQueue::panic_next_upload_for_tests`'s doc. Phase 4, this
    /// task: pins the `in_flight` scope-guard fix.
    pub fn panic_next_upload_for_tests(&self) {
        self.write_queue.panic_next_upload_for_tests();
    }

    /// Test-only: makes the very next `DraftStore::set_state` call anywhere
    /// (e.g. inside `open`'s reopen-cancel, or `close`'s arm) fail as if its
    /// disk persist step failed, without touching disk. Pins the ordering
    /// contract a reviewer caught: a caller must send any terminal
    /// `VfsEvent` it already owes — because it made some OTHER commitment
    /// irreversible first, like `WriteBackQueue::cancel` aborting a debounce
    /// timer — BEFORE this kind of fallible call, never downstream of it.
    /// See `DraftStore::fail_next_set_state_for_tests`'s doc.
    pub async fn fail_next_draft_persist_for_tests(&self) {
        self.drafts.lock().await.fail_next_set_state_for_tests();
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
    ///
    /// The EEXIST check and the eventual `DraftStore::begin` are guarded by
    /// the SAME per-path lock `ensure_drafted` uses (`draft_begin_locks`):
    /// without it, two concurrent `create`s of the same name can both pass
    /// the check before either has inserted anything, and the second
    /// `begin` — an unconditional overwrite — silently discards the first
    /// caller's already-returned, already-acknowledged draft. The lock is
    /// keyed by the prospective remote path, computed up front from the
    /// parent's own attrs (cheap and synchronous — no listing involved) so
    /// it can be taken before any of the check-then-act section runs.
    ///
    /// Coordinator review (cycle A follow-up): the whole call is ALSO
    /// wrapped in `open_lock_for(remote_path)` — the same lock `open` and
    /// `rename` use for their own EBUSY guard (see `open_locks`'s field
    /// doc). Without it, `insert_local_entry` below makes the new name
    /// visible in the tree several `.await`s before this call finishes
    /// registering its own handle in `open_files`; a `rename` of that SAME
    /// not-yet-fully-created name landing in that window would see
    /// `is_subtree_open == false` and complete, reproducing cycle A's exact
    /// stale-handle bug for a name that didn't exist a moment ago instead
    /// of one that already did. Taken FIRST, before `draft_begin_lock` — see
    /// `draft_begin_locks`'s doc for the ordering rule the two lock maps
    /// must keep.
    ///
    /// D10 (phase 4, outermost lock): `namespace_lock.read()` is held across
    /// this entire call too, ahead of even `open_lock` — see
    /// `namespace_lock`'s field doc. It plays the identical role here that it
    /// plays in `open`: without it, a DIRECTORY rename's subtree check and
    /// this call's own registration window use unrelated lock keys and can
    /// interleave.
    pub async fn create(&self, parent: NodeId, name: &str) -> Result<(NodeId, FileHandle)> {
        let _ns_guard = self.namespace_lock.read().await;

        let parent_attr = self
            .tree
            .getattr(parent)
            .await?
            .context("create: unknown parent (readdir/lookup it first)")?;
        let remote_path = format!("{}/{name}", parent_attr.remote_path);

        let open_lock = self.open_lock_for(&remote_path);
        let _open_guard = open_lock.lock().await;
        let path_lock = self.draft_begin_lock_for(&remote_path);
        let _guard = path_lock.lock().await;

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

        // Test-only pause point — see `CreatePauseHook`'s doc. A no-op
        // (`create_pause_hook` is `None`) on every real call.
        let pause_hook = self.create_pause_hook.lock().unwrap().clone();
        if let Some(hook) = pause_hook {
            hook.parked.notify_one();
            hook.resume.notified().await;
        }

        let key = FileKey { remote_path: attr.remote_path.clone(), etag: String::new() };
        self.cache.lock().await.retain(&key);
        // No remote counterpart exists yet, so there is nothing to fetch a
        // download URL for — see `download_url`'s field doc.
        let open_file = Arc::new(OpenFile { key, size: 0, download_url: RwLock::new(None) });
        let handle_id = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.open_files.write().await.insert(handle_id, open_file);
        Ok((node, FileHandle(handle_id)))
    }

    /// Test-only: makes every subsequent `create()` call pause mid-body —
    /// after the new draft begins, before this call registers its own
    /// handle in `open_files` — until the test notifies `hook.resume`. See
    /// `CreatePauseHook`'s doc for why this exists instead of a plain
    /// racing-tasks test. Same non-`#[cfg(test)]` reasoning as
    /// `set_debounce_for_tests`.
    pub fn pause_create_before_registration_for_tests(&self, hook: Arc<CreatePauseHook>) {
        *self.create_pause_hook.lock().unwrap() = Some(hook);
    }

    /// Creates a folder, synchronously — unlike `create`'s files, a folder
    /// has no draft/upload phase at all: `create_file` either succeeds (the
    /// folder now genuinely exists on the server) or fails outright.
    ///
    /// Because the server round-trip already happened, this deliberately
    /// does NOT use `insert_local_entry`'s client-side-only overlay the way
    /// `create` does for files: `invalidate_path` forces the very next
    /// listing to hit the network and pick up the real, now-authoritative
    /// state (including the folder's real id/attrs, not a size-0
    /// placeholder this facade would have to keep in sync by hand). The
    /// `lookup` right after intentionally reuses that same fresh listing
    /// (the tree's `LISTING_TTL` keeps it cached) to hand back a `NodeId`
    /// without a second network round-trip.
    pub async fn mkdir(&self, parent: NodeId, name: &str) -> Result<NodeId> {
        let parent_attr = self
            .tree
            .getattr(parent)
            .await?
            .context("mkdir: unknown parent (readdir/lookup it first)")?;
        anyhow::ensure!(parent_attr.is_dir, "mkdir: parent is not a directory");
        let uri = format!("{}/{name}", parent_attr.remote_path);

        self.client
            .create_file(&CreateFileService {
                uri: uri.clone(),
                file_type: "folder".to_string(),
                // Deliberate deviation from `cloudreve-sync`'s
                // `create_empty_file_or_folder`, which sends `Some(false)`
                // for folders (a sync pass re-creating an already-existing
                // remote folder is expected and must be a harmless no-op).
                // `mkdir` is a direct, single-shot user action instead: an
                // existing entry of the same name is a real EEXIST the
                // caller should see as an error, not something to silently
                // succeed over — hence `Some(true)` here, not a mirror of
                // the sync engine's shape.
                err_on_conflict: Some(true),
                metadata: None,
            })
            .await
            .context("failed to create folder")?;

        self.tree.invalidate_path(&uri).await;
        let (id, _attr) = self
            .tree
            .lookup(parent, name)
            .await?
            .context("mkdir: created folder not found in the fresh listing")?;
        Ok(id)
    }

    /// Removes a file or folder. A drafted file's local edits (and any
    /// armed/queued upload) are discarded outright — deleting the file
    /// makes any pending write moot. If the draft's `base_etag` is empty
    /// (`create`'s brand-new-file case), the file never existed remotely and
    /// `delete_files` is skipped entirely: there is nothing on the server to
    /// remove.
    ///
    /// Two guards, phase 4:
    /// - **EBUSY** ([`UnlinkBusyError`], deliverable B): a FILE with a
    ///   currently open handle refuses deletion outright — see that error's
    ///   own doc for the resurrection hazard this closes. A directory is
    ///   never "open" (`open` bails on one), so this never applies to one.
    /// - **NotEmpty** ([`DirNotEmptyError`], deliverable D): this facade
    ///   never does a recursive delete — a non-empty DIRECTORY refuses
    ///   removal outright, exactly like POSIX `rmdir`. "Empty" is judged by
    ///   `readdir`'s own listing (real server listing + the local overlay of
    ///   any not-yet-confirmed `create()`d child), so a drafted-but-unlisted
    ///   child counts as an occupant too — see that error's own doc.
    ///
    /// D10 (outermost lock, see `namespace_lock`'s field doc): a DIRECTORY
    /// removal takes `write()` across its emptiness check and the actual
    /// delete call, closing the same check-then-act shape `rename` closes
    /// for a directory rename (a concurrent `open`/`create` of a new child
    /// landing between this call's check and its delete would otherwise be
    /// silently destroyed, or the delete would wrongly proceed against a
    /// directory that stopped being empty a moment before). A FILE removal
    /// takes no namespace lock at all — it has no descendants to protect,
    /// so its own `open_lock_for` guard below is already sufficient, exactly
    /// like a file `rename`.
    ///
    /// `open_lock_for(remote_path)` (see `open_locks`'s field doc) is taken
    /// unconditionally, file or directory, AFTER `namespace_lock` — for a
    /// file this IS the EBUSY guard; for a directory it has no busy check of
    /// its own to gate (an empty directory's emptiness check already implies
    /// no descendant handle exists — see the NotEmpty check's own comment
    /// below) but still serializes this call against a `rename` of the SAME
    /// directory, which takes the identical lock key.
    pub async fn unlink(&self, parent: NodeId, name: &str) -> Result<()> {
        let (id, attr) = self
            .lookup(parent, name)
            .await?
            .with_context(|| format!("unlink: no such entry {name:?}"))?;
        let remote_path = attr.remote_path;

        // D10: see this method's own doc above for why only a DIRECTORY
        // takes this, and why it must be acquired before `open_lock_for`
        // next, no exceptions.
        let _ns_write_guard =
            if attr.is_dir { Some(self.namespace_lock.write().await) } else { None };

        let open_lock = self.open_lock_for(&remote_path);
        let _open_guard = open_lock.lock().await;

        if attr.is_dir {
            // M1 (review fold-in): force a fresh listing before judging
            // emptiness rather than trusting whatever `readdir` last cached
            // (up to `LISTING_TTL` = 5s stale — tree.rs). Already holding
            // `namespace_lock.write()` here (this whole branch only runs for
            // a directory) and already tolerating a network round-trip in
            // this call (the eventual `delete_files` below), so this one
            // extra listing shrinks the advisory window from "up to 5s
            // stale" to "a genuine in-flight race" for free.
            self.tree.invalidate_path(&remote_path).await;
            // NotEmpty guard (deliverable D): `readdir` merges the real
            // server listing with any locally-created, not-yet-confirmed
            // child (`create()`'s overlay), so a drafted-but-unlisted child
            // counts as non-empty exactly like a real one. An open handle on
            // a descendant implies that descendant is listed/known as a
            // child too (it can only have been opened via a prior lookup/
            // readdir that found it) — so this check alone also subsumes
            // "does any descendant have an open handle", no separate
            // `is_subtree_open` call is needed here. (Narrow documented
            // residual: a descendant deleted REMOTELY, out from under an
            // still-open local handle, in the instant between the
            // invalidate above and this check, would not be counted — the
            // same class of narrow, accepted external-change race this
            // crate already lives with elsewhere, not something this guard
            // newly introduces.)
            if !self.readdir(id).await?.is_empty() {
                return Err(anyhow::Error::new(DirNotEmptyError { remote_path }));
            }
        } else if self.is_subtree_open(&remote_path).await {
            // EBUSY guard (deliverable B): exact-match only ever fires for a
            // file (no descendants possible), same discipline `rename`'s
            // guard already relies on for its own exact-path case.
            return Err(anyhow::Error::new(UnlinkBusyError { remote_path }));
        }

        // `cancel` is a harmless no-op if nothing is currently armed for
        // this path (draft still `Editing`, or an upload already
        // `Uploading` — the latter simply finds its draft gone mid-flight,
        // a case `WriteBackQueue::process` already handles gracefully).
        let dropped_base_etag = {
            let mut drafts = self.drafts.lock().await;
            match drafts.base_etag(&remote_path) {
                Some(base_etag) => {
                    // Closes out this cycle's `UploadQueued` (see
                    // `VfsEvent`'s state-machine doc) whenever there really
                    // was a still-armed debounce timer to stop — a draft
                    // still `Editing` (never closed, never queued) or one
                    // already `Uploading` correctly emits nothing here: the
                    // former was never queued at all, the latter's own
                    // in-flight cycle settles on its own terms (see
                    // `WriteBackQueue::process`'s "draft vanished mid-flight"
                    // handling).
                    if self.write_queue.cancel(&remote_path) {
                        let _ = self
                            .events
                            .send(VfsEvent::UploadCancelled { remote_path: remote_path.clone() });
                    }
                    drafts.remove(&remote_path)?;
                    Some(base_etag)
                }
                None => None,
            }
        };

        if dropped_base_etag.as_deref() == Some("") {
            self.tree.invalidate_path(&remote_path).await;
            return Ok(());
        }

        self.client
            .delete_files(&DeleteFileService {
                uris: vec![remote_path.clone()],
                unlink: None,
                skip_soft_delete: None,
            })
            .await
            .context("failed to delete")?;

        self.tree.invalidate_path(&remote_path).await;
        Ok(())
    }

    /// Renames/moves an entry. Same-directory name changes call
    /// `rename_file`; a directory change calls `move_files`; a directory
    /// change THAT ALSO changes the leaf name calls both, in that order —
    /// `move_files` first (relocating the entry while it still has its old
    /// name), then `rename_file` targeting the entry's now-current uri
    /// (parent changed, name hasn't yet). Operating on the entry's actual
    /// current uri at each step, rather than computing both target paths up
    /// front, is what makes the two-call sequence correct regardless of
    /// which the server processes first.
    ///
    /// A draft with an empty `base_etag` (`create`'s brand-new-file case)
    /// has no remote counterpart at all: both API calls are skipped
    /// entirely (there's nothing on the server to move/rename), and instead
    /// a fresh local-only entry is inserted at the destination — the same
    /// client-side overlay `create` itself uses — so the renamed draft
    /// stays visible before its eventual upload lands under the new path.
    ///
    /// Any active draft on the entry is retargeted via `DraftStore::rename`
    /// so its eventual upload (successful or not) always lands under the
    /// NEW path, never the old one.
    ///
    /// EBUSY CONTRACT (cycle A of the phase-2 debt burn-down, replacing the
    /// phase-2 "known blocker" this used to document): a handle open on the
    /// entry being renamed makes this call fail with [`RenameBusyError`]
    /// rather than silently proceeding. Phase 2 let the rename go through
    /// while an `OpenFile`'s `key.remote_path` — fixed at `open`/`create`
    /// time and never touched here — kept pointing at the OLD path, so a
    /// subsequent `write`/`read` on that handle would silently diverge from
    /// what the caller just renamed (or fail confusingly, depending on pure
    /// cache-state luck). The guard below closes that off at the source
    /// instead: no rename ever completes while a stale handle could result.
    /// Frontends (NFS/FUSE) map `RenameBusyError` to `EBUSY`, the same
    /// answer POSIX gives for an analogous conflict; closing every handle
    /// on the entry and retrying succeeds (see the guard's own doc, and
    /// `open_locks`'s field doc, for why this is race-free rather than a
    /// mere check-then-act).
    ///
    /// For a DIRECTORY rename the same guard extends to descendants (any
    /// open handle or live draft strictly under the old path also answers
    /// EBUSY). Phase 3 left one honestly-narrower gap here: `open_locks` is
    /// keyed per exact path, so a brand-new open/create of a *descendant*
    /// racing this call was not serialized against it — a check-then-act
    /// window of about one HTTP round-trip. D10's `namespace_lock` (phase 4)
    /// closes exactly this gap: see its own field doc, and the paragraph
    /// below.
    ///
    /// D10 (phase 4, outermost lock): when the entry being renamed is a
    /// DIRECTORY, this call also takes `namespace_lock.write()` — acquired
    /// BEFORE the `open_lock_for` guard below, held across the whole rest of
    /// this call. While held, NO `open`/`create` ANYWHERE in this `Vfs` can
    /// even start past their own first line (both take `namespace_lock.
    /// read()` first) — that is what finally makes this call's subtree
    /// check race-free against a descendant open/create in flight, not just
    /// against one already registered. A FILE rename takes NO namespace
    /// lock at all: a file has no descendants, so the exact-path
    /// `open_locks` guard above is already fully race-free for it (see
    /// `open`'s own doc for the one remaining, narrower, non-D10 caveat —
    /// an open that resolved its attrs before this rename committed).
    ///
    /// DESTINATION busy check (phase 4, deliverable C): the SAME two checks
    /// (`is_subtree_open`/`has_draft_strictly_under`) also run against
    /// `new_path`, refusing with [`RenameBusyError`] if the destination (or,
    /// for a directory destination, anything under it) is open or drafted —
    /// the atomic-save idiom every real editor uses (write a tmp file, then
    /// rename it OVER the target) must not silently clobber a handle's view
    /// of the file it still has open. Both the source's and the
    /// destination's `open_lock_for` are taken together, in a fixed
    /// (lexicographic) order regardless of which is source/destination, so
    /// two concurrent renames that swap two paths can never deadlock each
    /// holding one lock while waiting on the other.
    ///
    /// Callers should also expect this call to sometimes BLOCK rather than
    /// answer immediately, even outside the EBUSY path: `open_locks` (the
    /// per-path lock this call and `open`/`create` all share) can be held
    /// across a network hop, not just an in-memory check. `open` holds its
    /// lock across one `fetch_download_url` HTTP round-trip; `create`'s
    /// EEXIST `lookup` can itself trigger a fresh directory listing if the
    /// parent's cached one has expired. A `rename` arriving while either is
    /// in flight for the same path simply waits for it to finish before its
    /// own busy-check even runs — by design (see `open_locks`'s field doc),
    /// but a frontend mapping this call directly onto a synchronous syscall
    /// must not assume EBUSY is the only way this call takes noticeable
    /// time.
    pub async fn rename(
        &self,
        parent: NodeId,
        name: &str,
        new_parent: NodeId,
        new_name: &str,
    ) -> Result<()> {
        let (_id, attr) = self
            .lookup(parent, name)
            .await?
            .with_context(|| format!("rename: no such entry {name:?}"))?;
        let old_path = attr.remote_path;
        let source_is_dir = attr.is_dir;
        let new_parent_attr = self
            .tree
            .getattr(new_parent)
            .await?
            .context("rename: unknown new_parent (readdir/lookup it first)")?;
        let new_path = format!("{}/{new_name}", new_parent_attr.remote_path);

        if old_path == new_path {
            return Ok(()); // renaming onto itself: nothing to do.
        }

        // D10 (phase 4, outermost lock — see `namespace_lock`'s and this
        // method's own doc above): write() only when the SOURCE is a
        // directory, acquired BEFORE `open_lock_for` next, no exceptions.
        let _ns_write_guard =
            if source_is_dir { Some(self.namespace_lock.write().await) } else { None };

        // EBUSY guard: see `RenameBusyError`'s and `open_locks`'s docs for
        // why taking these locks, and holding them for the whole rest of
        // this call, is what makes the checks race-free rather than a
        // check-then-act TOCTOU — dropped automatically at the end of this
        // function (early return on either busy path, or falling off the
        // end on success). Both `old_path`'s and `new_path`'s locks are
        // taken together, in a fixed lexicographic order — see this
        // method's own doc for why.
        let (first_path, second_path) = if old_path <= new_path {
            (old_path.clone(), new_path.clone())
        } else {
            (new_path.clone(), old_path.clone())
        };
        let first_lock = self.open_lock_for(&first_path);
        let _first_guard = first_lock.lock().await;
        let second_lock = self.open_lock_for(&second_path);
        let _second_guard = second_lock.lock().await;

        // SOURCE side. Covers the whole SUBTREE, not just `old_path` itself:
        // renaming a DIRECTORY must be refused if any descendant currently
        // has an open handle OR a live draft — otherwise `DraftStore::rename`
        // below (which only ever migrates the one exact path handed to it)
        // would leave that descendant's draft targeting the OLD,
        // just-renamed-away path, and its eventual upload would resurrect
        // the renamed directory there with the user's edit inside (the
        // master-index BLOCKER this guard exists to close). The draft check
        // deliberately uses `has_draft_strictly_under`, NOT a plain
        // exact-or-prefix match: a draft sitting AT `old_path` itself (the
        // ordinary drafted-FILE-rename case, no descendants possible) must
        // stay allowed through unmigrated-until-below, exactly as before
        // this fix — see that method's doc.
        if self.is_subtree_open(&old_path).await
            || self.drafts.lock().await.has_draft_strictly_under(&old_path)
        {
            return Err(anyhow::Error::new(RenameBusyError { remote_path: old_path }));
        }

        // DESTINATION side (phase 4, deliverable C): the atomic-save idiom
        // — write a tmp file, then rename it OVER the target — must not
        // silently clobber a handle's (or a live draft's) view of whatever
        // currently sits at `new_path`. Same subtree logic as the source
        // check above (so a directory destination's open descendants are
        // caught too); deliberately reuses the same "strictly under"
        // draft check, so an existing EXACT-path draft at the destination
        // (not open) is not itself treated as busy — this call has no
        // narrower "would this rename orphan the destination's own draft"
        // concern to add beyond what already existed for a plain overwrite.
        if self.is_subtree_open(&new_path).await
            || self.drafts.lock().await.has_draft_strictly_under(&new_path)
        {
            return Err(anyhow::Error::new(RenameBusyError { remote_path: new_path }));
        }

        let existed_remotely = match self.drafts.lock().await.base_etag(&old_path) {
            Some(base_etag) => !base_etag.is_empty(),
            None => true, // no draft at all: an ordinary remote file/dir.
        };

        // Fix round 1 (C1, protocol fidelity): resolved BEFORE either branch
        // below decides what to do, since both need it. `None` covers both
        // "nothing at all sits at `new_path`" and "only a brand-new,
        // never-uploaded local draft sits there" (nothing on the server to
        // collide with either way) — see the helper's own doc. Never a
        // DIRECTORY destination: deleting one to "make room" for a file is
        // far more destructive than this call should ever do on a caller's
        // behalf, so `dest_remote` itself always filters a directory out
        // below, whatever the source turns out to be.
        let dest_lookup = self.remote_destination_if_exists(new_parent, new_name).await?;

        // R1 (phase 4 task 3, routed from the task 2 re-review): a
        // DRAFTED-source FILE (`!existed_remotely` — a directory can never
        // be `!existed_remotely`, it never has a draft at all, so
        // `!source_is_dir` is technically implied but checked explicitly
        // for clarity) hitting an existing DIRECTORY name must be refused
        // loudly — see `RenameOntoDirectoryError`'s doc for the
        // doomed-40004-retry-loop this closes. A REMOTE source colliding
        // the same way is deliberately left to fall through to the server's
        // own native refusal on the `rename_file`/`move_files` call just
        // below (surfaces as a generic `Io`/`EIO`) — that path always
        // reaches a real API call, unlike a drafted source's, which never
        // does until its eventual, decoupled upload.
        if !existed_remotely && !source_is_dir {
            if let Some(dest_attr) = &dest_lookup {
                if dest_attr.is_dir {
                    return Err(anyhow::Error::new(RenameOntoDirectoryError {
                        remote_path: new_path,
                    }));
                }
            }
        }

        let dest_remote = dest_lookup.filter(|a| !a.is_dir);

        if existed_remotely {
            // REMOTE-SOURCE bridge: the real Cloudreve server refuses
            // `rename_file`/`move_files` onto an existing sibling name
            // outright (`ErrFileExisted`/`ObjectExisted`, 40004 — see
            // `tests/common/mod.rs`'s mock, fixed to match) whereas POSIX
            // `rename(2)` and NFSv3 `RENAME` both require silent replace.
            // Bridged here, under the SAME locks already held for the whole
            // rest of this call (`open_lock_for` on both paths, and
            // `namespace_lock.write()` if the source is a directory): delete
            // the existing destination FIRST, then perform the ordinary
            // rename/move exactly as before this fix.
            //
            // CRASH-WINDOW DISCLOSURE: this facade has no cross-call
            // transaction. If the process dies between the delete below and
            // the rename/move call actually landing, `new_path` briefly (or
            // permanently, absent a retry) resolves to NOTHING on the
            // server — neither the destination's old content nor the
            // source's new content. No byte loss for either side: the
            // destination's old bytes were the ones this whole operation
            // was asked to discard (ordinary overwrite-on-rename semantics,
            // not data the caller wanted kept), and the source's bytes are
            // still fully intact and readable under `old_path` (untouched
            // by the delete call, which only ever names `new_path`) — a
            // retried rename after restart completes it. The failure mode
            // is a lagging NAME, never lost DATA.
            if let Some(dest_attr) = &dest_remote {
                self.client
                    .delete_files(&DeleteFileService {
                        uris: vec![dest_attr.remote_path.clone()],
                        unlink: None,
                        skip_soft_delete: None,
                    })
                    .await
                    .context("failed to delete the rename destination")?;
            }

            let same_dir = parent == new_parent;
            if same_dir {
                self.client
                    .rename_file(&RenameFileService {
                        uri: old_path.clone(),
                        new_name: new_name.to_string(),
                    })
                    .await
                    .context("failed to rename")?;
            } else {
                self.client
                    .move_files(&MoveFileService {
                        uris: vec![old_path.clone()],
                        dst: new_parent_attr.remote_path.clone(),
                        copy: None,
                    })
                    .await
                    .context("failed to move")?;
                if name != new_name {
                    // The entry now lives under the new parent, still under
                    // its OLD name — operate on its current uri, not the
                    // pre-move one.
                    let moved_path = format!("{}/{name}", new_parent_attr.remote_path);
                    self.client
                        .rename_file(&RenameFileService {
                            uri: moved_path,
                            new_name: new_name.to_string(),
                        })
                        .await
                        .context("failed to rename after move")?;
                }
            }
        }

        // DRAFTED-SOURCE bridge (R2 fix, phase 4 task 3): when the source
        // never existed on the server (empty `base_etag`, so no `delete`/
        // `rename` call was ever made above — there is nothing on the
        // server to move) AND a real object sits at the destination, the
        // migrated draft must adopt the destination's remote identity —
        // rebasing onto its current etag — so its eventual upload runs
        // `overwrite=true, previous_version=dest_attr.etag`: an ordinary
        // rewrite-in-place, arbitrated by the SAME D5 conflict machinery an
        // edit of an already-existing file always goes through (a remote
        // change since adoption correctly becomes a conflict copy, not a
        // 40004 loop). Without this, the migrated draft would keep its
        // EMPTY `base_etag` and its eventual upload would run
        // `overwrite=false` against a uri that ALREADY exists remotely —
        // refused with 40004, treated as a plain transient failure (not a
        // conflict — see `WriteBackQueue::process`'s conflict-copy gate,
        // which only fires for a non-empty `base_etag`), parking the draft
        // to retry the IDENTICAL doomed request forever.
        //
        // Computed BEFORE the `rename` call below (not as a separate
        // `set_base_etag` call afterward) and passed into it directly:
        // `DraftStore::rename`'s single `write_meta` persists the migrated
        // path and the adopted etag TOGETHER, so there is no window in
        // which a crash (or a cancelled future) could land one without the
        // other — see that method's own doc.
        let adopted_etag =
            if !existed_remotely { dest_remote.as_ref().map(|attr| attr.etag.clone()) } else { None };

        let had_draft = {
            let mut drafts = self.drafts.lock().await;
            if drafts.state(&old_path).is_some() {
                drafts.rename(&old_path, &new_path, adopted_etag.as_deref())?;
                true
            } else {
                false
            }
        };
        if had_draft {
            // A no-op unless a debounce timer was actually armed for the
            // old path (see `migrate_armed_timer`'s doc) — a draft still
            // `Editing` or already `Uploading` has nothing to migrate here.
            self.write_queue.migrate_armed_timer(&old_path, new_path.clone());
        }

        self.tree.invalidate_path(&old_path).await;
        if existed_remotely || dest_remote.is_some() {
            // The destination side changed remotely — either the ordinary
            // server-side rename/move landed there, or (drafted-source
            // bridge) it stays the same already-real entry now carrying an
            // adopted-identity draft overlay; either way the tree's cached
            // view of `new_path` must be refetched, not treated as a
            // client-side-only insert.
            self.tree.invalidate_path(&new_path).await;
        } else if had_draft {
            self.tree.insert_local_entry(new_parent, new_name).await?;
        }
        Ok(())
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

    /// Gets (creating if needed) the per-path async lock `open` and `rename`
    /// share — see `open_locks`'s field doc for the race it closes.
    fn open_lock_for(&self, remote_path: &str) -> Arc<Mutex<()>> {
        let mut locks = self.open_locks.lock().unwrap();
        locks
            .entry(remote_path.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Whether any live handle currently points at `remote_path` itself, OR
    /// at anything nested under it (`remote_path` + "/" + anything).
    /// The `+ "/"` boundary is deliberate: a sibling whose name merely
    /// shares `remote_path` as a string prefix (`/docs` vs `/docs2`) must
    /// NOT be treated as a descendant — only a real path separator makes
    /// one an ancestor of the other.
    ///
    /// The exact-match branch is what a plain FILE rename has always
    /// relied on (a file can't have descendants, so only that branch ever
    /// fires for one); the prefix branch is what a DIRECTORY rename needs
    /// — a directory itself is never opened as a file (`open` bails on
    /// one), so without it this check could never see a descendant's open
    /// handle at all. Callers needing a race-free answer must hold
    /// `open_lock_for(remote_path)` across both this check and whatever it
    /// gates — see `rename`'s and `unlink`'s guards. That lock alone is
    /// keyed to `remote_path` only, not to every descendant's own path, so
    /// by itself it does NOT close the race of a *new* open/create arriving
    /// on a descendant while this check is already running for an ANCESTOR
    /// path — closing THAT (phase-3-disclosed) gap is exactly what
    /// `namespace_lock` (D10, phase 4) is for: see its field doc, and
    /// `rename`'s own doc for how the two locks compose.
    async fn is_subtree_open(&self, remote_path: &str) -> bool {
        let nested_prefix = format!("{remote_path}/");
        self.open_files
            .read()
            .await
            .values()
            .any(|of| of.key.remote_path == remote_path || of.key.remote_path.starts_with(&nested_prefix))
    }

    /// Resolves whether a REAL remote object currently sits at
    /// `parent`/`name` — as opposed to nothing at all, or only a local-only,
    /// never-uploaded `create()` draft with no remote counterpart yet (the
    /// same "empty `base_etag`" test `existed_remotely` uses for a rename's
    /// SOURCE, applied here to its DESTINATION). Used exclusively by
    /// `rename`'s protocol-fidelity bridge (deliverable C, fix round 1): the
    /// server refuses `rename_file`/`move_files`/an `overwrite=false` upload
    /// session onto an existing sibling name outright
    /// (`ErrFileExisted`/`ObjectExisted`, 40004), so the facade must know
    /// BEFORE attempting either path whether the destination is real enough
    /// to require bridging around that refusal.
    ///
    /// `Some(attr)` when a real remote object sits there — either an
    /// ordinary, never-drafted entry, or one currently being edited
    /// in-place (drafted with a non-empty `base_etag`, i.e. a real rewrite
    /// in progress, not a brand-new file). `None` when nothing sits there at
    /// all, or only a drafted-but-never-uploaded `create()`'d file — the
    /// server has never heard of either, so there is nothing to collide
    /// with and no bridge is needed.
    async fn remote_destination_if_exists(
        &self,
        parent: NodeId,
        name: &str,
    ) -> Result<Option<NodeAttr>> {
        let Some((_id, attr)) = self.lookup(parent, name).await? else {
            return Ok(None);
        };
        let only_a_local_draft = matches!(
            self.drafts.lock().await.base_etag(&attr.remote_path),
            Some(base_etag) if base_etag.is_empty()
        );
        Ok((!only_a_local_draft).then_some(attr))
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
    let url = of.download_url.read().await.clone().context(
        "attempted a ranged fetch on a handle with no download url — drafted/local-only reads \
         must never reach this path (see `download_url`'s field doc)",
    )?;
    match fetch_range_with_backoff(http, &url, start, end).await? {
        FetchOutcome::Data(bytes) => return Ok(bytes),
        FetchOutcome::RangeNotSatisfiable => return Ok(Bytes::new()),
        FetchOutcome::Forbidden => {}
    }

    let fresh = fetch_download_url(client, &of.key)
        .await
        .context("failed to refresh an expired download URL after a 403")?;
    *of.download_url.write().await = Some(fresh.clone());
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
