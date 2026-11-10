//! Linux FUSE adapter over the [`Vfs`] facade (phase 3, D6/D7).
//!
//! **THIS FILE HAS NEVER COMPILED OR RUN ON THE MACHINE THAT WROTE IT.** It
//! is declared behind `#[cfg(target_os = "linux")]` at `lib.rs`'s module
//! list, so on this (macOS) development machine it is never even parsed by
//! rustc — every claim below about its behavior follows from reading the
//! vendored `fuser = "0.15.1"` source under
//! `~/.cargo/registry/src/.../fuser-0.15.1/` (cited by file/line where the
//! API choice is non-obvious), not from having run it. It is validated only
//! by Task 5's Linux CI job.
//!
//! `VfsFuse` implements `fuser::Filesystem`, the sync, callback-style
//! counterpart to `nfs.rs`'s async `NfsReadFileSystem`/`NfsFileSystem` — see
//! that module's doc first for the handle-lifecycle background this one
//! extends rather than repeats.
//!
//! ## D6: sync trait over an async facade
//!
//! `fuser::Filesystem`'s methods take `&mut self` and are plain sync
//! functions — there is no `async fn` variant of this trait. `VfsFuse` holds
//! a `tokio::runtime::Handle` (captured once, at construction, by
//! `mount.rs`'s Linux branch — see its doc) and calls
//! [`VfsFuse::block_on`] to run each facade `.await` to completion inline.
//! This does NOT deadlock the executor: `fuser::Session::run()` — the
//! request-dispatch loop that calls into every method below — runs on a
//! plain `std::thread::spawn`'d OS thread
//! (`session.rs::BackgroundSession::new`, not a `tokio::spawn`), so it is
//! never itself a tokio worker thread. `Handle::block_on` only panics when
//! called from *inside* the runtime it belongs to (a worker driving async
//! code); called from an unrelated OS thread — which is exactly what every
//! FUSE callback below runs on — it just blocks that one thread until the
//! future resolves, same as any other blocking call. The important
//! constraint this places on `mount.rs`: the `Handle` passed to
//! `VfsFuse::new` must belong to a runtime that keeps running for the whole
//! life of the mount (the caller's already-running runtime, in practice —
//! see `mount::mount`'s Linux branch).
//!
//! Concurrency: `Session::run()`'s own doc (`session.rs`) says its
//! read-dispatch loop is "non-concurrent... but the filesystem methods may
//! run concurrent by spawning threads". Requests that can wait on the
//! network or on another request — `lookup`, `getattr`, `readdir`, `open`,
//! `read`, `unlink`/`rmdir`, `rename` — run as tasks on the runtime
//! ([`VfsFuse::spawn`]) and reply when done, so one slow download never
//! freezes the mount, and an `unlink`/`rename` waiting out
//! [`crate::vfs::BUSY_GRACE`] for a late `release` (the kernel sends it
//! asynchronously, possibly after the app's next request) leaves the
//! dispatch thread free to deliver that `release`. `write` and `setattr`
//! too (the first write or resize of a file not yet drafted downloads it
//! whole). `create`, `flush`, `release` and `mkdir` stay inline. A handle opened
//! by one `open()` call persists in `Vfs::open_files` until its `release`,
//! exactly like a real file descriptor, so `rename`/`unlink` of a file
//! some app still holds open answers EBUSY once the grace runs out. See
//! `tests/mounted_linux.rs`'s EBUSY test.
//!
//! ## Handle model (D6)
//!
//! Unlike NFSv3 (stateless, no OPEN/CLOSE), FUSE's `open`/`create` return an
//! opaque `fh: u64` the kernel hands back unchanged on every subsequent
//! `read`/`write`/`setattr`/`flush`/`release` for that file — precisely
//! [`FileHandle`]'s own shape. This adapter does not maintain any separate
//! fh-to-handle table: `FileHandle(fh)` reconstructs the facade handle
//! directly from the raw `u64` fuser threads through, per D6's "real fh
//! values from the facade's FileHandle" instruction.
//!
//! ## Inode model
//!
//! `NodeId.0` is used directly as the FUSE `ino` (root = 1), mirroring
//! `nfs.rs`'s `fileid3` choice — the facade already promises node ids never
//! change for the tree's lifetime (`tree.rs`'s doc), which is exactly FUSE's
//! own inode-stability contract. `forget`/`lookup`'s `generation` counter
//! (distinguishing a REUSED inode number across a filesystem's lifetime,
//! `reply.rs:178`'s `ReplyEntry::entry` doc) is always `0`: this facade
//! never reuses a `NodeId`, so there is nothing to distinguish.
//!
//! ## Errno mapping (D2)
//!
//! Every fallible call routes through [`classify_error`]/[`errno_for`] into
//! a plain `libc` errno `c_int`, the exact shape `Reply*::error` wants —
//! no protocol-specific enum layer the way `nfs.rs` has `nfsstat3`. This is
//! the frontend where `RenameBusyError` finally reaches the OS as a REAL
//! `EBUSY` (not NFS3's `JUKEBOX` stand-in) and `StaleHandleError` as a real
//! `ESTALE`, because — per the module doc above — FUSE's persistent handles
//! make both races genuinely, deterministically reachable here.
//!
//! ## Attribute TTLs
//!
//! `reply.entry`/`reply.attr` both take a `ttl: &Duration` telling the
//! KERNEL how long it may serve a cached copy of the returned attributes
//! without asking again (`reply.rs:178` `ReplyEntry::entry`, `reply.rs:212`
//! `ReplyAttr::attr`). [`ATTR_TTL`] is 1 s, shorter than the tree's own
//! `LISTING_TTL` (5 s): a remote change reaches the kernel only when it asks
//! again (FUSE's `notify_inval_*` would push it, but needs fuser's
//! `abi-7-12` feature and still emits no inotify event for a file manager —
//! see `crate::nudge`), so the TTL is the visibility bound for a name
//! deleted or renamed remotely and for a file's new size. The extra
//! LOOKUP/GETATTR calls are answered from memory; HTTP stays bounded by
//! `LISTING_TTL`. A failed lookup is never cached (`lookup` replies an
//! error, not a zero-inode entry), so a name created remotely resolves at
//! once. Same 1 s as the macOS mount's `actimeo=1` (`mount.rs`).
//!
//! ## Disclosed simplifications (mirroring `nfs.rs`'s own disclosed list)
//!
//! - `setattr`'s `mode`/`uid`/`gid`/`atime`/`mtime` arguments are accepted
//!   and ignored (only `size`, i.e. truncate, has a server-side counterpart
//!   to act on) — identical to `nfs.rs`'s `setattr` and for the same reason:
//!   D7 fixes every entry's reported mode/owner/times regardless, so
//!   honoring a client-requested change here would be a lie the very next
//!   `getattr` contradicts.
//! - `rename`'s `flags` (Linux's `RENAME_NOREPLACE`/`RENAME_EXCHANGE` via
//!   `renameat2`) are accepted and ignored — the facade has no equivalent
//!   primitive, same category of simplification as `nfs.rs`'s
//!   `create_exclusive` accepting-and-ignoring its verifier. `std::fs::rename`
//!   (what every test in this crate uses) never sets them.
//! - `flush` is a no-op success, matching `nfs.rs`'s `commit`: every write
//!   already lands synchronously in the draft store before `write` returns
//!   (see `Vfs::write`'s own doc), so there is nothing left to flush by the
//!   time either method is called.
//! - `.`/`..` are NOT synthesized in `readdir`, matching `nfs.rs`'s
//!   `readdirplus` (`VfsReadDirIter` iterates the facade's listing only).
//!   The facade tracks no "parent NodeId" for an arbitrary directory (only
//!   its own `remote_path`), so synthesizing a real `..` entry would need
//!   new facade API this task's scope doesn't call for; omitting them only
//!   affects `ls -a`-style enumeration, not `cd ..`/lookup navigation (the
//!   kernel's own dentry cache already knows a directory's parent from the
//!   `lookup` call that reached it).
//! - `open`'s `flags` (e.g. `O_TRUNC`) are ignored: fuser's `INIT_FLAGS`
//!   (`lib.rs`) never sets `FUSE_ATOMIC_O_TRUNC`, so the kernel truncates by
//!   issuing a SEPARATE `setattr(size=0)` call rather than requiring `open`
//!   to inspect flags itself — the crate's own `examples/simple.rs` doesn't
//!   implement O_TRUNC-in-open either, for the same reason.

use std::ffi::OsStr;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};

use crate::frontend_util::{classify_error, to_frontend_attr, FrontendAttr, FrontendErrno};
use crate::tree::NodeId;
use crate::vfs::{FileHandle, Vfs};

/// Kernel-side cache TTL for every `getattr`/`lookup`/`create`/`mkdir`
/// reply's attributes — see the module doc's "Attribute TTLs" section.
const ATTR_TTL: Duration = Duration::from_secs(1);

/// FUSE adapter over [`Vfs`]. See the module doc for the sync/async bridge,
/// handle model, and inode scheme.
pub struct VfsFuse {
    vfs: Arc<Vfs>,
    /// Captured once at construction — see the module doc's D6 section for
    /// why this is sound and what it requires of the caller (`mount.rs`).
    rt: tokio::runtime::Handle,
    /// Reports the session ending on its own — see [`SessionEndNotifier`].
    end_notifier: Option<SessionEndNotifier>,
}

/// Dropped with the [`VfsFuse`] it sits in, which fuser drops when its
/// session loop ends — on purpose (`MountedVfs::unmount`/drop, which set
/// `deliberate` first) or not (the kernel side went away, an error in the
/// loop). Only the latter is reported, as `Some(reason)` on the channel
/// `MountedVfs::server_stopped` hands out.
pub(crate) struct SessionEndNotifier {
    pub(crate) tx: tokio::sync::watch::Sender<Option<String>>,
    pub(crate) deliberate: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) drive: String,
}

impl Drop for SessionEndNotifier {
    fn drop(&mut self) {
        if self.deliberate.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let reason = "the FUSE session ended".to_string();
        tracing::error!(drive = %self.drive, %reason, "vfs: the FUSE session behind a mounted drive ended; the drive is unreachable until remounted");
        self.tx.send_replace(Some(reason));
    }
}

impl VfsFuse {
    pub fn new(vfs: Arc<Vfs>, rt: tokio::runtime::Handle) -> Self {
        Self { vfs, rt, end_notifier: None }
    }

    pub(crate) fn with_end_notifier(mut self, notifier: SessionEndNotifier) -> Self {
        self.end_notifier = Some(notifier);
        self
    }

    /// Runs one facade `.await` to completion on this adapter's captured
    /// runtime handle. See the module doc's D6 section for why this never
    /// deadlocks from a FUSE callback thread.
    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.rt.block_on(fut)
    }

    /// Runs a call on the runtime instead of blocking fuser's single
    /// dispatch thread, replying from the task when the facade answers —
    /// see the module doc's concurrency paragraph. Without this, ONE read
    /// waiting on the network (opening a large video, a slow server) froze
    /// every other request on the mount: the file manager hung on a plain
    /// folder listing until that download finished. A spawned read never
    /// outlives its handle: the kernel sends `release` only once every
    /// request holding the file has been answered.
    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.rt.spawn(fut);
    }
}

impl Filesystem for VfsFuse {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let vfs = self.vfs.clone();
        let name = name.to_string();
        self.spawn(async move {
            match vfs.lookup(NodeId(parent), &name).await {
                Ok(Some((id, attr))) => reply.entry(&ATTR_TTL, &to_file_attr(&to_frontend_attr(id.0, &attr)), 0),
                // Lookup miss: the facade's `Ok(None)`, not an error — same
                // "constructed at the call site" pattern as `nfs.rs`'s
                // `FrontendErrno::NotFound` (see that module's doc).
                Ok(None) => reply.error(libc::ENOENT),
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.getattr(NodeId(ino)).await {
                Ok(Some(attr)) => reply.attr(&ATTR_TTL, &to_file_attr(&to_frontend_attr(ino, &attr))),
                Ok(None) => reply.error(libc::ENOENT),
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, reply: ReplyDirectory) {
        let vfs = self.vfs.clone();
        self.spawn(async move { readdir_reply(vfs, ino, offset, reply).await });
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.open(NodeId(ino)).await {
                Ok(handle) => reply.opened(handle.0, 0),
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let vfs = self.vfs.clone();
        // The kernel never sends a negative offset in practice; clamped
        // rather than asserted so a malformed request degrades to "read
        // from the start" instead of panicking.
        let offset = offset.max(0) as u64;
        self.spawn(async move {
            match vfs.read(FileHandle(fh), offset, size).await {
                Ok(data) => reply.data(&data),
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }

    /// Without this, fuser answers 0 blocks: `df` shows a 0-byte drive and
    /// file managers see no free space (Nautilus refuses to copy into a
    /// volume it thinks is full). The facade has no quota to report, so
    /// this mirrors the macOS mount, whose NFS server reports 1 TiB total
    /// and free.
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        const BLOCK: u32 = 4096;
        const BLOCKS: u64 = (1u64 << 40) / BLOCK as u64; // 1 TiB
        const FILES: u64 = 1_000_000;
        reply.statfs(BLOCKS, BLOCKS, BLOCKS, FILES, FILES, BLOCK, 255, BLOCK);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let vfs = self.vfs.clone();
        let offset = offset.max(0) as u64;
        let data = data.to_vec();
        // On the runtime: the first write to a file not yet drafted
        // downloads it whole (D2), and blocking the dispatch thread on that
        // froze every other request on the mount meanwhile. The kernel only
        // sends `release` once this reply is in.
        self.spawn(async move {
            match vfs.write(FileHandle(fh), offset, &data).await {
                Ok(written) => reply.written(written),
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }

    fn flush(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        // See the module doc's disclosed-simplifications list: every write
        // already landed synchronously in the draft store, mirroring
        // `nfs.rs`'s `commit`.
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let vfs = self.vfs.clone();
        match self.block_on(vfs.close(FileHandle(fh))) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_for(&err)),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let vfs = self.vfs.clone();
        let (node, handle) = match self.block_on(vfs.create(NodeId(parent), name)) {
            Ok(created) => created,
            Err(err) => {
                reply.error(errno_for(&err));
                return;
            }
        };
        match self.block_on(vfs.getattr(node)) {
            Ok(Some(attr)) => {
                reply.created(&ATTR_TTL, &to_file_attr(&to_frontend_attr(node.0, &attr)), 0, handle.0, 0)
            }
            // The just-created node vanishing before this getattr would be
            // a genuine bug elsewhere in the facade — reported as ENOENT
            // rather than panicking, same defensive posture as `nfs.rs`'s
            // equivalent `.context(...)` sites.
            Ok(None) => reply.error(libc::ENOENT),
            Err(err) => reply.error(errno_for(&err)),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let vfs = self.vfs.clone();
        let node = match self.block_on(vfs.mkdir(NodeId(parent), name)) {
            Ok(node) => node,
            Err(err) => {
                reply.error(errno_for(&err));
                return;
            }
        };
        match self.block_on(vfs.getattr(node)) {
            Ok(Some(attr)) => reply.entry(&ATTR_TTL, &to_file_attr(&to_frontend_attr(node.0, &attr)), 0),
            Ok(None) => reply.error(libc::ENOENT),
            Err(err) => reply.error(errno_for(&err)),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_entry(parent, name, reply);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        // `Vfs::unlink` already handles files AND folders uniformly (see its
        // own doc) — FUSE's separate `unlink`/`rmdir` calls both map onto
        // it, exactly like `nfs.rs`'s single `remove`.
        self.remove_entry(parent, name, reply);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            reply.error(libc::EINVAL);
            return;
        };
        let vfs = self.vfs.clone();
        let (name, newname) = (name.to_string(), newname.to_string());
        // Spawned, like `unlink`: the facade may wait (`BUSY_GRACE`) for a
        // `release` the kernel has not delivered yet, and that `release`
        // must be able to reach this adapter in the meantime.
        self.spawn(async move {
            match vfs.rename(NodeId(parent), &name, NodeId(newparent), &newname).await {
                Ok(()) => reply.ok(),
                // `RenameBusyError` -> `FrontendErrno::Busy` -> `EBUSY` (a
                // handle still open past `BUSY_GRACE`) — see
                // `tests/mounted_linux.rs`'s EBUSY test.
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let vfs = self.vfs.clone();
        // On the runtime, like `write`: a resize of a file not yet drafted
        // downloads it first.
        self.spawn(setattr_reply(vfs, ino, size, fh, reply));
    }
}

/// `setattr`'s body, run on the runtime (see `VfsFuse::spawn`).
async fn setattr_reply(vfs: Arc<Vfs>, ino: u64, size: Option<u64>, fh: Option<u64>, reply: ReplyAttr) {
    {
        if let Some(size) = size {
            let result = match fh {
                // The kernel already gave us the open handle this file is
                // being resized under (e.g. `open(O_TRUNC)` — see the
                // module doc's disclosed-simplifications list) — reuse it
                // directly instead of opening a second one. Unlike
                // `nfs.rs`'s `setattr` (which MUST open+close a fresh
                // handle per call — NFSv3 has no persistent handle to
                // reuse), this is a genuine advantage of FUSE's handle
                // model.
                Some(fh) => vfs.truncate(FileHandle(fh), size).await,
                // `truncate(2)` on a path with no currently-open fd is a
                // real POSIX case the kernel can still send here with
                // `fh: None` — fall back to `nfs.rs`'s open+truncate+close
                // pattern.
                None => async {
                    let handle = vfs.open(NodeId(ino)).await?;
                    let result = vfs.truncate(handle, size).await;
                    crate::frontend_util::close_logged(&vfs, handle).await;
                    result
                }
                .await,
            };
            if let Err(err) = result {
                reply.error(errno_for(&err));
                return;
            }
        }
        match vfs.getattr(NodeId(ino)).await {
            Ok(Some(attr)) => reply.attr(&ATTR_TTL, &to_file_attr(&to_frontend_attr(ino, &attr))),
            Ok(None) => reply.error(libc::ENOENT),
            Err(err) => reply.error(errno_for(&err)),
        }
    }
}

/// `readdir`'s body, run on the runtime (see `VfsFuse::spawn`).
async fn readdir_reply(vfs: Arc<Vfs>, ino: u64, offset: i64, mut reply: ReplyDirectory) {
    let listing = match vfs.readdir(NodeId(ino)).await {
        Ok(listing) => listing,
        Err(err) => {
            reply.error(errno_for(&err));
            return;
        }
    };
    // Same cookie scheme as `nfs.rs`'s `VfsReadDirIter`: the offset
    // handed back with each entry is `1 + <its index in this
    // snapshot>`, so resuming at that offset starts right after it. A
    // client-supplied `offset` past the end of a fresh snapshot (the
    // directory shrank between two paged calls) just yields an empty
    // `.skip()` — an empty reply is FUSE's own end-of-stream signal
    // (`lib.rs`'s `readdir` doc: "Send an empty buffer on end of
    // stream"), unlike NFSv3's explicit `BAD_COOKIE` error code.
    // Clamped the same way `read`/`write` clamp their own `offset`:
    // the kernel never sends a negative one in practice, but a bare
    // `as usize` cast on one would wrap to a huge value instead of the
    // intended "start from 0".
    for (index, (id, attr)) in listing.iter().enumerate().skip(offset.max(0) as usize) {
        let kind = if attr.is_dir { FileType::Directory } else { FileType::RegularFile };
        let buffer_full = reply.add(id.0, (index + 1) as i64, kind, &attr.name);
        if buffer_full {
            break;
        }
    }
    reply.ok();
}

impl VfsFuse {
    /// Shared body of `unlink`/`rmdir` — see `rmdir`'s doc for why both
    /// FUSE calls map onto the same facade method.
    fn remove_entry(&mut self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let vfs = self.vfs.clone();
        let name = name.to_string();
        // Spawned: the facade may wait (`BUSY_GRACE`) for this file's
        // `release`, which the kernel sends asynchronously and possibly
        // after this request — the dispatch thread must stay free to
        // deliver it.
        self.spawn(async move {
            match vfs.unlink(NodeId(parent), &name).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(errno_for(&err)),
            }
        });
    }
}

/// D2's mapping, this frontend's own final step (see the module doc): turns
/// the portable [`FrontendErrno`] into a plain `libc` errno `c_int`.
fn to_errno(errno: FrontendErrno) -> i32 {
    match errno {
        FrontendErrno::NotFound => libc::ENOENT,
        FrontendErrno::Stale => libc::ESTALE,
        FrontendErrno::Exist => libc::EEXIST,
        FrontendErrno::Busy => libc::EBUSY,
        // Phase 4, deliverable D: unlike `nfs.rs`'s `JUKEBOX` stand-in for
        // `Busy`, libc has a real, purpose-built errno for this exact
        // condition.
        FrontendErrno::NotEmpty => libc::ENOTEMPTY,
        // Phase 4 task 3, R1: a real, purpose-built errno for this exact
        // condition (`man 2 rename`'s "newpath is a non-empty directory").
        FrontendErrno::IsDir => libc::EISDIR,
        FrontendErrno::Access => libc::EACCES,
        FrontendErrno::CrossDevice => libc::EXDEV,
        FrontendErrno::NoSpace => libc::ENOSPC,
        FrontendErrno::FileTooBig => libc::EFBIG,
        FrontendErrno::Io => libc::EIO,
    }
}

/// Classifies then converts an `anyhow::Error` from any `Vfs` call in one
/// step — same PURPOSE as `nfs.rs`'s free function of this name (`nfs.rs`
/// also calls its version `to_errno`, converting `&anyhow::Error` into
/// that module's `nfsstat3`); named `errno_for` here instead to avoid
/// colliding with THIS module's own `to_errno(FrontendErrno) -> i32` right
/// above, which plays the role `nfs.rs` calls `to_nfsstat3`.
fn errno_for(err: &anyhow::Error) -> i32 {
    to_errno(classify_error(err))
}

/// Converts the portable [`FrontendAttr`] into fuser's `FileAttr`.
fn to_file_attr(attr: &FrontendAttr) -> FileAttr {
    let mtime = to_system_time(attr.mtime_secs, attr.time_nsecs);
    FileAttr {
        ino: attr.fileid,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: mtime,
        mtime,
        ctime: mtime,
        // No real creation time tracked anywhere in this facade; `mtime` is
        // as honest a stand-in as D7's own atime-equals-mtime choice.
        crtime: mtime,
        kind: if attr.is_dir { FileType::Directory } else { FileType::RegularFile },
        perm: attr.mode as u16,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

/// `NodeAttr::mtime_secs` is a signed unix timestamp; a negative one (which
/// a well-behaved server should never send) clamps to the epoch rather than
/// underflowing `SystemTime`'s unsigned duration-since-epoch representation
/// — same clamp-not-wrap philosophy as `nfs.rs`'s `to_nfstime3`.
fn to_system_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nsecs.min(999_999_999))
    } else {
        SystemTime::UNIX_EPOCH
    }
}

// Errno-table mapping tests only (phase 4, deliverable D — mirrors
// `nfs.rs`'s equivalent test module): this whole file is
// `#[cfg(target_os = "linux")]`-gated (see the module doc), so these never
// run or even compile on this (macOS) development machine — validated only
// by the Linux CI job, same platform-honesty caveat as every other claim in
// this file.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::{DirNotEmptyError, RenameBusyError, StaleHandleError, UnlinkBusyError};

    #[test]
    fn not_found_maps_to_enoent() {
        assert_eq!(to_errno(FrontendErrno::NotFound), libc::ENOENT);
    }

    #[test]
    fn junk_refusals_map_to_eacces_and_exdev() {
        assert_eq!(to_errno(FrontendErrno::Access), libc::EACCES);
        assert_eq!(to_errno(FrontendErrno::CrossDevice), libc::EXDEV);
    }

    #[test]
    fn stale_handle_error_maps_to_estale() {
        let err = anyhow::Error::new(StaleHandleError { remote_path: "x".into() });
        assert_eq!(errno_for(&err), libc::ESTALE);
    }

    #[test]
    fn rename_busy_error_maps_to_ebusy() {
        let err = anyhow::Error::new(RenameBusyError { remote_path: "x".into() });
        assert_eq!(errno_for(&err), libc::EBUSY);
    }

    #[test]
    fn unlink_busy_error_maps_to_ebusy() {
        let err = anyhow::Error::new(UnlinkBusyError { remote_path: "x".into() });
        assert_eq!(errno_for(&err), libc::EBUSY);
    }

    #[test]
    fn dir_not_empty_error_maps_to_enotempty() {
        let err = anyhow::Error::new(DirNotEmptyError { remote_path: "x".into() });
        assert_eq!(errno_for(&err), libc::ENOTEMPTY);
    }

    #[test]
    fn eexist_marker_maps_to_eexist() {
        let err = anyhow::anyhow!("EEXIST: an entry named \"dup.txt\" already exists");
        assert_eq!(errno_for(&err), libc::EEXIST);
    }

    #[test]
    fn an_unrecognized_error_maps_to_eio() {
        let err = anyhow::anyhow!("some transient failure");
        assert_eq!(errno_for(&err), libc::EIO);
    }
}
