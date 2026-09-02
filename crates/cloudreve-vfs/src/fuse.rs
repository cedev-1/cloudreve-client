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
//! One consequence worth stating plainly: `Session::run()`'s own doc
//! (`session.rs`) says its read-dispatch loop is "non-concurrent... but the
//! filesystem methods may run concurrent by spawning threads" — this
//! adapter does NOT spawn extra threads per call, so at most one FUSE
//! operation is ever in flight against `VfsFuse` at a time. That is
//! actually what makes the EBUSY contract below deterministic rather than
//! scheduler-luck-dependent (contrast with `nfs.rs`'s module doc, which
//! could only pin `RenameBusyError`'s mapping with a direct unit test): a
//! `rename` FUSE call and an already-open file's `read`/`write` calls can
//! never race each other inside this process — but a handle opened by one
//! `open()` call still very much persists in `Vfs::open_files` across the
//! *separate*, later FUSE call that eventually `release`s it, exactly like
//! a real file descriptor. So `open` a file, keep holding it (its `fh`
//! lives in the KERNEL now, not on this thread's stack), and a `rename` of
//! that same path arriving as its own later, separate FUSE call reliably
//! finds `Vfs::rename`'s `is_path_open` guard tripped — no race, no
//! scheduler dependency. See `tests/mounted_linux.rs`'s EBUSY test.
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
//! `ReplyAttr::attr`). [`ATTR_TTL`] reuses `tree::LISTING_TTL` (5s)
//! verbatim rather than inventing a second constant: the facade's own
//! directory listings are only trusted for that long before a fresh network
//! round-trip, so letting the kernel cache metadata LONGER than that would
//! let it serve attributes staler than the facade itself would ever serve.
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
use crate::tree::{NodeId, LISTING_TTL};
use crate::vfs::{FileHandle, Vfs};

/// Kernel-side cache TTL for every `getattr`/`lookup`/`create`/`mkdir`
/// reply's attributes — see the module doc's "Attribute TTLs" section.
const ATTR_TTL: Duration = LISTING_TTL;

/// FUSE adapter over [`Vfs`]. See the module doc for the sync/async bridge,
/// handle model, and inode scheme.
pub struct VfsFuse {
    vfs: Arc<Vfs>,
    /// Captured once at construction — see the module doc's D6 section for
    /// why this is sound and what it requires of the caller (`mount.rs`).
    rt: tokio::runtime::Handle,
}

impl VfsFuse {
    pub fn new(vfs: Arc<Vfs>, rt: tokio::runtime::Handle) -> Self {
        Self { vfs, rt }
    }

    /// Runs one facade `.await` to completion on this adapter's captured
    /// runtime handle. See the module doc's D6 section for why this never
    /// deadlocks from a FUSE callback thread.
    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.rt.block_on(fut)
    }
}

impl Filesystem for VfsFuse {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let vfs = self.vfs.clone();
        match self.block_on(vfs.lookup(NodeId(parent), name)) {
            Ok(Some((id, attr))) => reply.entry(&ATTR_TTL, &to_file_attr(&to_frontend_attr(id.0, &attr)), 0),
            // Lookup miss: the facade's `Ok(None)`, not an error — same
            // "constructed at the call site" pattern as `nfs.rs`'s
            // `FrontendErrno::NotFound` (see that module's doc).
            Ok(None) => reply.error(libc::ENOENT),
            Err(err) => reply.error(errno_for(&err)),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let vfs = self.vfs.clone();
        match self.block_on(vfs.getattr(NodeId(ino))) {
            Ok(Some(attr)) => reply.attr(&ATTR_TTL, &to_file_attr(&to_frontend_attr(ino, &attr))),
            Ok(None) => reply.error(libc::ENOENT),
            Err(err) => reply.error(errno_for(&err)),
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let vfs = self.vfs.clone();
        let listing = match self.block_on(vfs.readdir(NodeId(ino))) {
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

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let vfs = self.vfs.clone();
        match self.block_on(vfs.open(NodeId(ino))) {
            Ok(handle) => reply.opened(handle.0, 0),
            Err(err) => reply.error(errno_for(&err)),
        }
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
        // from the start" instead of panicking a shared FUSE thread.
        let offset = offset.max(0) as u64;
        match self.block_on(vfs.read(FileHandle(fh), offset, size)) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(errno_for(&err)),
        }
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
        match self.block_on(vfs.write(FileHandle(fh), offset, data)) {
            Ok(written) => reply.written(written),
            Err(err) => reply.error(errno_for(&err)),
        }
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
        match self.block_on(vfs.rename(NodeId(parent), name, NodeId(newparent), newname)) {
            Ok(()) => reply.ok(),
            // This is the mapping the whole phase-3 FUSE task exists to make
            // real: `RenameBusyError` -> `FrontendErrno::Busy` -> `EBUSY`,
            // reachable deterministically (not just unit-tested) because
            // FUSE handles persist across calls — see the module doc and
            // `tests/mounted_linux.rs`'s EBUSY test.
            Err(err) => reply.error(errno_for(&err)),
        }
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
                Some(fh) => self.block_on(vfs.truncate(FileHandle(fh), size)),
                // `truncate(2)` on a path with no currently-open fd is a
                // real POSIX case the kernel can still send here with
                // `fh: None` — fall back to `nfs.rs`'s open+truncate+close
                // pattern.
                None => self.block_on(async {
                    let handle = vfs.open(NodeId(ino)).await?;
                    let result = vfs.truncate(handle, size).await;
                    let _ = vfs.close(handle).await;
                    result
                }),
            };
            if let Err(err) = result {
                reply.error(errno_for(&err));
                return;
            }
        }
        match self.block_on(vfs.getattr(NodeId(ino))) {
            Ok(Some(attr)) => reply.attr(&ATTR_TTL, &to_file_attr(&to_frontend_attr(ino, &attr))),
            Ok(None) => reply.error(libc::ENOENT),
            Err(err) => reply.error(errno_for(&err)),
        }
    }
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
        match self.block_on(vfs.unlink(NodeId(parent), name)) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_for(&err)),
        }
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
    let mtime = to_system_time(attr.mtime_secs);
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
fn to_system_time(secs: i64) -> SystemTime {
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        SystemTime::UNIX_EPOCH
    }
}
