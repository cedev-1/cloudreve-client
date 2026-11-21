//! NFSv3 adapter over the [`Vfs`] facade (phase 3, D4).
//!
//! `VfsNfs` implements `nfs3_server`'s [`NfsReadFileSystem`]/[`NfsFileSystem`]
//! traits (crate version pinned in `Cargo.toml`: `nfs3_server = "0.11"`,
//! resolved to `0.11.0` — see the phase-3 task report for how that was
//! verified). This module owns no socket and mounts nothing: `mount.rs`
//! (Task 3) is the only place that ever binds a port or shells out to
//! `mount_nfs`. Every test in `tests/nfs_adapter.rs` drives this trait
//! object directly.
//!
//! ## Handle model (D4)
//!
//! NFSv3 has no OPEN/CLOSE RPC at all — every trait method below (`read`,
//! `write`, `setattr`, …) receives just a `fileid`-shaped handle and must do
//! whatever it needs to do in that one call. That happens to be EXACTLY the
//! "open-per-operation" lifecycle D4 asks to start with, so there is no
//! separate lifecycle decision to make here: each method that touches file
//! content opens a fresh facade [`FileHandle`](crate::vfs::FileHandle),
//! does its work, and closes it again before returning — the draft/cache
//! layers underneath make that cheap (see `Vfs::open`'s doc: a path with an
//! active draft needs no download URL at all).
//!
//! One thing worth stating plainly: no facade handle here ever survives
//! BETWEEN two RPCs — each is opened and closed within the one call that
//! created it. That does NOT make the handle-lifecycle error paths
//! unreachable, though: `nfs3_server` spawns an independent tokio task per
//! incoming RPC (`rpcwire.rs`), so two RPCs on the same file genuinely CAN
//! run concurrently — e.g. a slow WRITE (its facade handle open for the
//! duration of that one call) overlapping a RENAME on the same path is
//! enough to legitimately trip `Vfs::rename`'s `is_subtree_open` guard and
//! return [`RenameBusyError`](crate::vfs::RenameBusyError)/`JUKEBOX`; the
//! narrower [`StaleHandleError`](crate::vfs::StaleHandleError) race (a
//! read overlapping the exact instant its draft's upload removes it) is
//! possible too, just tighter. What's actually true is narrower than
//! "unreachable": there is no DETERMINISTIC way to drive that overlap
//! through the trait object in a test (it depends on the tokio scheduler
//! racing two concurrently-dispatched RPCs), so both mappings are pinned by
//! direct unit tests (constructing the typed error and asserting the mapped
//! `nfsstat3`, see the `tests` module below) rather than an end-to-end
//! repro. Task 4's FUSE adapter, whose handles persist across calls by
//! design (not just by scheduler luck), makes both paths reachable and
//! testable end-to-end.
//!
//! ## Fileid / handle type
//!
//! `nfs3_server`'s handle type is a trait (`vfs::FileHandle`), not a bare
//! `fileid3` — the crate ships an 8-byte implementation,
//! [`FileHandleU64`], which this adapter
//! uses directly: `FileHandleU64::new(NodeId.0)`. `nfs3_server` wraps it in
//! its own opaque `nfs_fh3` (an 8-byte server generation number + our 8
//! bytes) before it ever reaches the wire, which is where the protocol's
//! OWN stale-handle detection (a handle from before a server restart) lives
//! — orthogonal to, and layered on top of, this facade's `StaleHandleError`.
//!
//! ## readdir cookies (D4)
//!
//! `readdirplus` takes one snapshot per call (`Vfs::readdir`) and hands out
//! a cookie equal to `1 + <the entry's position in that snapshot>`; cookie
//! `0` means "start of directory". Resuming with cookie `C` on the NEXT call
//! starts a FRESH snapshot at index `C` — correct as long as nothing
//! reordered the directory between the two calls. A concurrent
//! create/delete/rename elsewhere in the tree can reshuffle positions
//! between two paged calls (accepted per D4: a client that notices a
//! gap/duplicate just re-reads the directory, and NFSv3 gives no stronger
//! guarantee here without a server-side directory-generation cookie
//! verifier, which this server does not implement — see
//! `FileHandleConverter`'s doc in `nfs3_server` for the analogous handle
//! generation number, which is a different mechanism).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use nfs3_server::nfs3_types::nfs3::{
    createverf3, fattr3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, post_op_attr, sattr3,
    specdata3, stable_how, Nfs3Option, FSF3_CANSETTIME, FSF3_HOMOGENEOUS, FSF3_SYMLINK,
    FSINFO3resok,
};
use nfs3_server::vfs::{
    DirEntryPlus, FileHandleU64, NextResult, NfsFileSystem, NfsReadFileSystem, ReadDirPlusIterator,
};

use crate::frontend_util::{
    classify_error, process_gid, process_uid, to_frontend_attr, FrontendAttr, FrontendErrno,
};
use crate::appledouble::ROOT_SIDECAR_NAME;
use crate::cache::BLOCK_SIZE;
use crate::tree::{NodeAttr, NodeId};
use crate::vfs::Vfs;

/// How long a READ may hold its RPC before the server answers
/// `NFS3ERR_JUKEBOX` instead (the fetch keeps running in the background;
/// see [`VfsNfs::read`]).
///
/// Each JUKEBOX costs the app a client-side sleep of at least 4s before
/// the resend (`kext/nfs_socket.c`, `nfs_request_finish`: `r_delay` 4, 8,
/// 16, then 30s, at which point a soft mount gives up — a READ still
/// unanswered after ~28s of sleeps fails), so this must be as long as the
/// mount allows: a cold 1 MiB block from a remote server routinely takes
/// a second or two, and must be served directly.
///
/// The ceiling is the client's soft timeout. In `nfs_request_timer` an
/// RPC with no reply times out after `timeo` (with `dumbtimer`, see
/// `mount.rs`'s `MOUNT_OPTS`; without it, an RTO learned from past
/// replies), the timeout doubles per consecutive timeout of the mount, and
/// a soft mount fails the RPC at its `retrans + 1`-th timeout: 3s + 3s + 3s
/// = 9s at the earliest for `timeo=30,retrans=2` (when other replies keep
/// resetting the mount's timeout count), 3 + 6 + 10s otherwise. 2s stays
/// under even the FIRST timeout, so no READ of a live server ever bumps the
/// mount-wide timeout count — which also halves the "too long" cap for
/// every other request (`maxtime`), down to 3.3s once eight RPCs timed out.
///
/// Measured on the macos-latest CI runner, JUKEBOX disabled, after warm-up
/// traffic (`measure_soft_timeout_budget_for_held_rpcs`):
///
/// | held for | read(2) | mmap (cmp) | LOOKUP, RTO timer | LOOKUP, dumbtimer |
/// |----------|---------|------------|-------------------|-------------------|
/// | 0.25-1s  | ok      | ok         | ok                | ok                |
/// | 2s       | ok      | ok         | ETIMEDOUT @1.8s   | ok                |
/// | 4s       | ok      | ok         | ETIMEDOUT @0.7s   | ok                |
/// | 8s       | ok      | ok         | ETIMEDOUT @1.4s   | ok                |
///
/// read(2) never times out because the client issues it as an async
/// readahead buffer, whose RPC is `R_NOINTR` and exempt from the soft
/// timeout (`nfs_bio.c`, `nfs_buf_readahead`); the mmap probe most likely
/// survived because the READ RTO had not converged yet (initial srtt is 1s and
/// drifts down 1/8 per reply), but a synchronous READ times out like the
/// LOOKUP column once it has. JUKEBOX is what keeps those reads alive
/// past 9s.
pub const READ_JUKEBOX_AFTER: Duration = Duration::from_secs(2);

/// A READ answered with JUKEBOX whose client never came back (it gave up,
/// or the app went away) is dropped after this long, so its result does
/// not sit in [`VfsNfs`]'s in-flight map forever.
const ABANDONED_READ_TTL: Duration = Duration::from_secs(120);

/// The exact arguments of a READ RPC. The client resends a JUKEBOX'd READ
/// with the same handle, offset and count (only the xid changes), so this
/// is what lets a retry find the fetch its first attempt started.
type ReadKey = (u64, u64, u32);
type ReadResult = Result<(Vec<u8>, bool), nfsstat3>;

/// Which slow mutating call a parked task belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SlowOp {
    Write,
    Resize,
}

/// A resent WRITE carries the same handle, offset and length; a resent
/// resize the same handle and size.
type SlowKey = (SlowOp, u64, u64, u64);
/// A parked slow WRITE/resize: when it was parked, and its task.
type ParkedOp = (Instant, JoinHandle<Result<(), nfsstat3>>);
/// An exclusive CREATE's verifier and the fileid it created.
type RecentCreate = ([u8; 8], u64);

/// Spotlight opt-out: an empty, read-only file of this name at the volume
/// root is how Apple DTS tells `mds` to leave a volume alone (Apple
/// Developer Forums thread 820931, 2026). Served by this adapter alone —
/// Spotlight is a macOS concern, so the FUSE frontend has none: never
/// listed, never uploaded, never fetched. `crate::junk` reserves the name,
/// so a server-side file called that stays invisible and the facade
/// refuses to create one.
pub const NEVER_INDEX_NAME: &str = ".metadata_never_index";

/// The fileid of [`NEVER_INDEX_NAME`]: the tree allocates `NodeId`s upward
/// from 2, so it can never hand this one out.
const NEVER_INDEX_FILEID: u64 = u64::MAX;

/// NFSv3 adapter over [`Vfs`]. See the module doc for the handle lifecycle
/// and cookie scheme.
pub struct VfsNfs {
    vfs: Arc<Vfs>,
    /// Test-only: the `count` of every READ received, see
    /// [`VfsNfs::with_read_log`].
    read_log: Option<Arc<Mutex<Vec<u32>>>>,
    /// Test-only: per-procedure RPC counts, see [`VfsNfs::with_rpc_counts`].
    rpc_counts: Option<Arc<RpcCounts>>,
    /// [`READ_JUKEBOX_AFTER`] in production; see [`VfsNfs::with_read_jukebox_after`].
    read_jukebox_after: Duration,
    /// READs that outlived [`READ_JUKEBOX_AFTER`] and were answered with
    /// JUKEBOX, still running, waiting for the client's resend.
    slow_reads: Mutex<HashMap<ReadKey, (Instant, JoinHandle<ReadResult>)>>,
    /// WRITEs and resizes that outlived the JUKEBOX threshold, still running,
    /// waiting for the client's resend — see `run_or_jukebox`.
    slow_ops: Mutex<HashMap<SlowKey, ParkedOp>>,
    /// Recent exclusive CREATEs: (parent fileid, name) -> (verifier, fileid),
    /// so a resent one is recognized — see `create_exclusive`.
    exclusive_creates: Mutex<HashMap<(u64, String), RecentCreate>>,
}

impl VfsNfs {
    pub fn new(vfs: Arc<Vfs>) -> Self {
        Self::with_read_jukebox_after(vfs, READ_JUKEBOX_AFTER)
    }

    /// Test-only: overrides [`READ_JUKEBOX_AFTER`]. A huge value disables
    /// JUKEBOX entirely, which is how `tests/mounted_macos.rs` measures the
    /// client's own soft-timeout budget for a held READ.
    #[doc(hidden)]
    pub fn with_read_jukebox_after(vfs: Arc<Vfs>, read_jukebox_after: Duration) -> Self {
        Self {
            vfs,
            read_jukebox_after,
            read_log: None,
            rpc_counts: None,
            slow_reads: Mutex::new(HashMap::new()),
            exclusive_creates: Mutex::new(HashMap::new()),
            slow_ops: Mutex::new(HashMap::new()),
        }
    }

    /// Test-only: records the `count` of every READ RPC into `log`, so a
    /// mounted test can see the read size the client actually negotiated.
    #[doc(hidden)]
    pub fn with_read_log(mut self, log: Arc<Mutex<Vec<u32>>>) -> Self {
        self.read_log = Some(log);
        self
    }

    /// Test-only: counts every GETATTR/LOOKUP/READDIRPLUS/READ/WRITE this
    /// adapter answers into `counts`, so a mounted test can measure what a
    /// workload costs at the server under a given set of mount options.
    #[doc(hidden)]
    pub fn with_rpc_counts(mut self, counts: Arc<RpcCounts>) -> Self {
        self.rpc_counts = Some(counts);
        self
    }

    fn count(&self, pick: impl Fn(&RpcCounts) -> &AtomicU64) {
        if let Some(counts) = &self.rpc_counts {
            pick(counts).fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The facade this adapter serves.
    pub(crate) fn vfs(&self) -> Arc<Vfs> {
        self.vfs.clone()
    }

    /// Runs `op` (a WRITE or a resize) as a task; if it outlives
    /// `read_jukebox_after`, answers JUKEBOX and parks it under `key` for
    /// the client's resend — same scheme and reasons as READ's (see
    /// [`READ_JUKEBOX_AFTER`]). The first write to a file not yet drafted
    /// downloads the whole file (`Vfs::write`, D2): for a big remote file
    /// that outlived the client's soft timeout, and the app got EIO.
    async fn run_or_jukebox(
        &self,
        key: SlowKey,
        op: impl std::future::Future<Output = Result<(), nfsstat3>> + Send + 'static,
    ) -> Result<(), nfsstat3> {
        let mut task = {
            let mut slow = self.slow_ops.lock().unwrap();
            slow.retain(|_, (since, _)| since.elapsed() < ABANDONED_READ_TTL);
            match slow.remove(&key) {
                Some((_, task)) => task,
                None => tokio::spawn(op),
            }
        };
        match tokio::time::timeout(self.read_jukebox_after, &mut task).await {
            Ok(joined) => joined.unwrap_or(Err(nfsstat3::NFS3ERR_IO)),
            Err(_) => {
                self.slow_ops.lock().unwrap().insert(key, (Instant::now(), task));
                Err(nfsstat3::NFS3ERR_JUKEBOX)
            }
        }
    }

    fn is_never_index(&self, dir: &FileHandleU64, name: &str) -> bool {
        dir.as_u64() == self.vfs.tree().root().0 && name == NEVER_INDEX_NAME
    }
}

/// The in-process NFS server behind one mount, listening on `127.0.0.1`,
/// with no OS mount attached yet — see [`start_server`].
#[doc(hidden)]
pub struct NfsServer {
    pub port: u16,
    task: tokio::task::AbortHandle,
    stopped: tokio::sync::watch::Receiver<Option<String>>,
}

impl NfsServer {
    /// Stops the server deliberately: [`NfsServer::stopped`] never reports
    /// it (its channel just closes).
    pub fn abort(&self) {
        self.task.abort();
    }

    /// `Some(reason)` once the server stopped ON ITS OWN — it can no longer
    /// answer the kernel, and every app touching the mount gets errors
    /// until it is remounted. A deliberate [`NfsServer::abort`] (unmount)
    /// leaves it `None` and closes the channel instead.
    pub fn stopped(&self) -> tokio::sync::watch::Receiver<Option<String>> {
        self.stopped.clone()
    }
}

/// Binds `fs` on an ephemeral `127.0.0.1` port and serves it until
/// aborted, logging an ERROR and flagging [`NfsServer::stopped`] if it
/// ever ends on its own. `label` names the drive in that log line.
#[doc(hidden)]
pub async fn start_server(fs: VfsNfs, label: String) -> anyhow::Result<NfsServer> {
    use anyhow::Context;
    use nfs3_server::tcp::{NFSTcp, NFSTcpListener};

    // Loopback keeps the network out, not the other local processes —
    // another account, or a sandboxed app of this user allowed outgoing
    // connections. Every handle starts with this generation, so a random
    // one makes them unguessable (upstream uses the bind time in ms); the
    // policy refuses callers that are neither this user nor root, and every
    // MNT after the mount's own, so the root handle never leaves the kernel.
    // See `vendor/nfs3_server/src/caller_policy.rs`.
    let generation = random_u64().context("reading a random nfs handle generation")?;
    let mut listener = NFSTcpListener::bind_with_generation("127.0.0.1:0", fs, generation)
        .await
        .context("binding the in-process nfs3 server to 127.0.0.1:0")?;
    listener.set_caller_policy(
        nfs3_server::caller_policy::CallerPolicy::new(vec![process_uid(), 0]).single_mount(),
    );
    let port = listener.get_listen_port();
    let server = tokio::spawn(async move { listener.handle_forever().await });
    Ok(supervise(server, label, port))
}

fn random_u64() -> std::io::Result<u64> {
    use std::io::Read;
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}

/// Watches the server task: its end is reported unless it was aborted.
fn supervise(
    server: JoinHandle<std::io::Result<()>>,
    label: String,
    port: u16,
) -> NfsServer {
    let task = server.abort_handle();
    let (stopped_tx, stopped) = tokio::sync::watch::channel(None);
    tokio::spawn(async move {
        let reason = match server.await {
            Ok(Ok(())) => "the nfs server stopped".to_string(),
            Ok(Err(err)) => format!("the nfs server stopped: {err}"),
            Err(err) if err.is_cancelled() => return, // unmounted on purpose
            Err(err) => format!("the nfs server panicked: {err}"),
        };
        tracing::error!(
            drive = %label,
            port,
            %reason,
            "vfs: the nfs server behind a mounted drive died; the drive is unreachable until remounted"
        );
        stopped_tx.send_replace(Some(reason));
    });
    NfsServer { port, task, stopped }
}

/// Test-only RPC tally of one mount, see [`VfsNfs::with_rpc_counts`].
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RpcCounts {
    pub getattr: AtomicU64,
    pub lookup: AtomicU64,
    pub readdirplus: AtomicU64,
    pub read: AtomicU64,
    pub write: AtomicU64,
}

/// [`NEVER_INDEX_NAME`]'s attributes: an empty read-only file whose times
/// never move.
fn never_index_attr() -> fattr3 {
    to_fattr3(&FrontendAttr {
        fileid: NEVER_INDEX_FILEID,
        is_dir: false,
        size: 0,
        mode: 0o400,
        uid: process_uid(),
        gid: process_gid(),
        nlink: 1,
        mtime_secs: 0,
        atime_secs: 0,
        ctime_secs: 0,
        time_nsecs: 0,
    })
}

/// One whole READ against the facade: attrs for the eof flag, then an
/// open/read/close of a facade handle (open-per-operation, see the module
/// doc). Runs as its own task so it can outlive the RPC that started it.
async fn read_once(vfs: Arc<Vfs>, node: NodeId, offset: u64, count: u32) -> ReadResult {
    let attr = vfs
        .getattr(node)
        .await
        .map_err(|e| to_errno(&e))?
        .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;

    let handle = vfs.open(node).await.map_err(|e| to_errno(&e))?;
    let result = vfs.read(handle, offset, count).await;
    // Best-effort close: a failed read still releases the cache retain
    // this open() took. `close`'s own error (handle already gone) would
    // only ever fire if something else raced this exact handle id, which
    // nothing can — it is local to this call.
    crate::frontend_util::close_logged(&vfs, handle).await;
    let data = result.map_err(|e| to_errno(&e))?;
    let eof = offset.saturating_add(data.len() as u64) >= attr.size;
    Ok((data.to_vec(), eof))
}

impl NfsReadFileSystem for VfsNfs {
    type Handle = FileHandleU64;

    fn root_dir(&self) -> FileHandleU64 {
        FileHandleU64::new(self.vfs.tree().root().0)
    }

    async fn lookup(
        &self,
        dirid: &FileHandleU64,
        filename: &filename3<'_>,
    ) -> Result<FileHandleU64, nfsstat3> {
        self.count(|c| &c.lookup);
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        if self.is_never_index(dirid, &name) {
            return Ok(FileHandleU64::new(NEVER_INDEX_FILEID));
        }
        match self.vfs.lookup(parent, &name).await {
            Ok(Some((id, _attr))) => Ok(FileHandleU64::new(id.0)),
            // Lookup miss: the facade's `Ok(None)`, not an error — routed
            // through the SAME `to_nfsstat3` conversion as everything else
            // (see the module doc and `FrontendErrno::NotFound`'s doc).
            Ok(None) => Err(to_nfsstat3(FrontendErrno::NotFound)),
            Err(err) => Err(to_errno(&err)),
        }
    }

    async fn getattr(&self, id: &FileHandleU64) -> Result<fattr3, nfsstat3> {
        self.count(|c| &c.getattr);
        if id.as_u64() == NEVER_INDEX_FILEID {
            return Ok(never_index_attr());
        }
        let node = NodeId(id.as_u64());
        match self.vfs.getattr(node).await {
            Ok(Some(attr)) => Ok(to_fattr3(&to_frontend_attr(node.0, &attr))),
            Ok(None) => Err(to_nfsstat3(FrontendErrno::NotFound)),
            Err(err) => Err(to_errno(&err)),
        }
    }

    async fn read(
        &self,
        id: &FileHandleU64,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        self.count(|c| &c.read);
        // A READ that cannot finish within `READ_JUKEBOX_AFTER` (cold
        // hydration from a slow server) is answered with JUKEBOX and its
        // task parked here; the client's resend of the same READ picks the
        // same task back up instead of starting a second download. See
        // `READ_JUKEBOX_AFTER` for why holding the RPC open instead turns
        // a slow server into EIO for the app.
        if let Some(log) = &self.read_log {
            log.lock().unwrap().push(count);
        }
        if id.as_u64() == NEVER_INDEX_FILEID {
            return Ok((Vec::new(), true));
        }
        let key: ReadKey = (id.as_u64(), offset, count);
        let mut task = {
            let mut slow = self.slow_reads.lock().unwrap();
            slow.retain(|_, (since, _)| since.elapsed() < ABANDONED_READ_TTL);
            match slow.remove(&key) {
                Some((_, task)) => task,
                None => tokio::spawn(read_once(self.vfs.clone(), NodeId(key.0), offset, count)),
            }
        };
        match tokio::time::timeout(self.read_jukebox_after, &mut task).await {
            Ok(joined) => joined.unwrap_or(Err(nfsstat3::NFS3ERR_IO)),
            Err(_) => {
                self.slow_reads.lock().unwrap().insert(key, (Instant::now(), task));
                Err(nfsstat3::NFS3ERR_JUKEBOX)
            }
        }
    }

    async fn readdirplus(
        &self,
        dirid: &FileHandleU64,
        cookie: u64,
    ) -> Result<impl ReadDirPlusIterator<FileHandleU64>, nfsstat3> {
        self.count(|c| &c.readdirplus);
        let dir = NodeId(dirid.as_u64());
        let mut listing = self.vfs.readdir(dir).await.map_err(|e| to_errno(&e))?;
        // The root's own sidecar, the one junk file listed: otherwise the
        // client, holding this listing, answers every lookup of it with
        // ENOENT without asking, and the root's FinderInfo (the drive
        // folder's custom icon) can be neither set nor read once Finder
        // has listed the folder. See `crate::appledouble`. Last, so the
        // cookies of the real entries do not move.
        if dir == self.vfs.tree().root() {
            if let Ok(Some(sidecar)) = self.vfs.lookup(dir, ROOT_SIDECAR_NAME).await {
                listing.push(sidecar);
            }
        }
        let start = cookie as usize;
        if start > listing.len() {
            // Per the module doc: a directory reshuffle between two paged
            // calls can put a previously valid cookie past the end of the
            // fresh snapshot.
            return Err(nfsstat3::NFS3ERR_BAD_COOKIE);
        }
        Ok(VfsReadDirIter { listing, index: start })
    }

    /// The client clamps its `rsize`/`wsize` to `rtmax`/`wtmax` (and to
    /// `rtpref`/`wtpref` when smaller, `kext/nfs_vfsops.c`), so these are
    /// what makes `MOUNT_OPTS`' `rsize=1048576,wsize=1048576` real: one READ
    /// is one block of the vfs cache, and 1 MiB is also the macOS kext's own
    /// ceiling (`NFS_MAXDATA`). Same values `nfs3_server`'s default answers
    /// with today, stated here so they cannot drift with the crate.
    async fn fsinfo(&self, root_fileid: &FileHandleU64) -> Result<FSINFO3resok, nfsstat3> {
        let io_size = BLOCK_SIZE as u32;
        let obj_attributes = match self.getattr(root_fileid).await {
            Ok(attr) => post_op_attr::Some(attr),
            Err(_) => post_op_attr::None,
        };
        Ok(FSINFO3resok {
            obj_attributes,
            rtmax: io_size,
            rtpref: io_size,
            rtmult: io_size,
            wtmax: io_size,
            wtpref: io_size,
            wtmult: io_size,
            dtpref: io_size,
            maxfilesize: 128 * 1024 * 1024 * 1024,
            time_delta: nfstime3 { seconds: 0, nseconds: 1_000_000 },
            properties: FSF3_SYMLINK | FSF3_HOMOGENEOUS | FSF3_CANSETTIME,
        })
    }

    async fn readlink(&self, _id: &FileHandleU64) -> Result<nfspath3<'_>, nfsstat3> {
        // The facade has no symlink concept at all.
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

impl NfsFileSystem for VfsNfs {
    async fn setattr(&self, id: &FileHandleU64, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        if id.as_u64() == NEVER_INDEX_FILEID {
            // Read-only: a resize is refused, a mode/time change is a no-op
            // like everywhere else on this mount.
            return match setattr.size {
                Nfs3Option::Some(_) => Err(nfsstat3::NFS3ERR_ACCES),
                Nfs3Option::None => Ok(never_index_attr()),
            };
        }
        let node = NodeId(id.as_u64());
        // D4: the only `sattr3` field this facade can act on is `size`
        // (truncate) — mode/uid/gid/atime/mtime have no server-side
        // counterpart to persist them to, so a client setting them sees the
        // call succeed (matching D7's fixed attrs) without silently
        // pretending to honor a value it can't keep.
        if let Nfs3Option::Some(size) = setattr.size {
            let vfs = self.vfs.clone();
            self.run_or_jukebox((SlowOp::Resize, node.0, size, 0), async move {
                let handle = vfs.open(node).await.map_err(|e| to_errno(&e))?;
                let result = vfs.truncate(handle, size).await;
                crate::frontend_util::close_logged(&vfs, handle).await;
                result.map(|_| ()).map_err(|e| to_errno(&e))
            })
            .await?;
        }
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        Ok(to_fattr3(&to_frontend_attr(node.0, &attr)))
    }

    async fn write(
        &self,
        id: &FileHandleU64,
        offset: u64,
        data: &[u8],
        _stable: stable_how,
    ) -> Result<(fattr3, stable_how), nfsstat3> {
        self.count(|c| &c.write);
        if id.as_u64() == NEVER_INDEX_FILEID {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }
        let node = NodeId(id.as_u64());
        let vfs = self.vfs.clone();
        let bytes = data.to_vec();
        self.run_or_jukebox((SlowOp::Write, node.0, offset, data.len() as u64), async move {
            let handle = vfs.open(node).await.map_err(|e| to_errno(&e))?;
            let result = vfs.write(handle, offset, &bytes).await;
            crate::frontend_util::close_logged(&vfs, handle).await;
            result.map(|_| ()).map_err(|e| to_errno(&e))
        })
        .await?;
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        // `Vfs::write` lands synchronously in the local draft store before
        // this returns — FILE_SYNC is an honest description of that, not an
        // aspirational one, regardless of what the client requested.
        Ok((to_fattr3(&to_frontend_attr(node.0, &attr)), stable_how::FILE_SYNC))
    }

    async fn create(
        &self,
        dirid: &FileHandleU64,
        filename: &filename3<'_>,
        _attr: sattr3,
    ) -> Result<(FileHandleU64, fattr3), nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        if self.is_never_index(dirid, &name) {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        // The `sattr3` the client may pass (mode, etc.) is ignored — see
        // `setattr`'s doc: the facade has nowhere to persist it, and D7
        // fixes every entry's reported mode regardless.
        let (node, handle) = self.vfs.create(parent, &name).await.map_err(|e| to_errno(&e))?;
        crate::frontend_util::close_logged(&self.vfs, handle).await;
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        Ok((FileHandleU64::new(node.0), to_fattr3(&to_frontend_attr(node.0, &attr))))
    }

    async fn create_exclusive(
        &self,
        dirid: &FileHandleU64,
        filename: &filename3<'_>,
        createverf: createverf3,
    ) -> Result<FileHandleU64, nfsstat3> {
        // `Vfs::create`'s EEXIST guard gives exclusive create's essential
        // guarantee (never overwrite an existing name). RFC 1813 also asks
        // that a RESENT call (same verifier: its reply was lost) succeed
        // with the file it created: the verifier is remembered here, in
        // memory, for recent creates — the facade has no attribute to keep
        // it in, and a resend arrives within seconds, not across restarts.
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        if self.is_never_index(dirid, &name) {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        let key = (dirid.as_u64(), name.clone());
        match self.vfs.create(parent, &name).await {
            Ok((node, handle)) => {
                crate::frontend_util::close_logged(&self.vfs, handle).await;
                let mut recent = self.exclusive_creates.lock().unwrap();
                if recent.len() >= 1024 {
                    recent.clear();
                }
                recent.insert(key, (createverf.0, node.0));
                Ok(FileHandleU64::new(node.0))
            }
            Err(err) => {
                let errno = to_errno(&err);
                let remembered = self.exclusive_creates.lock().unwrap().get(&key).copied();
                if let (nfsstat3::NFS3ERR_EXIST, Some((verf, fileid))) = (errno, remembered) {
                    if verf == createverf.0 {
                        if let Ok(Some((id, _))) = self.vfs.lookup(parent, &name).await {
                            if id.0 == fileid {
                                return Ok(FileHandleU64::new(fileid));
                            }
                        }
                    }
                }
                Err(errno)
            }
        }
    }

    async fn mkdir(
        &self,
        dirid: &FileHandleU64,
        dirname: &filename3<'_>,
    ) -> Result<(FileHandleU64, fattr3), nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(dirname)?;
        if self.is_never_index(dirid, &name) {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        let node = self.vfs.mkdir(parent, &name).await.map_err(|e| to_errno(&e))?;
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        Ok((FileHandleU64::new(node.0), to_fattr3(&to_frontend_attr(node.0, &attr))))
    }

    async fn remove(&self, dirid: &FileHandleU64, filename: &filename3<'_>) -> Result<(), nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        if self.is_never_index(dirid, &name) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }
        self.vfs.unlink(parent, &name).await.map_err(|e| to_errno(&e))
    }

    async fn rename<'a>(
        &self,
        from_dirid: &FileHandleU64,
        from_filename: &filename3<'a>,
        to_dirid: &FileHandleU64,
        to_filename: &filename3<'a>,
    ) -> Result<(), nfsstat3> {
        let from_parent = NodeId(from_dirid.as_u64());
        let to_parent = NodeId(to_dirid.as_u64());
        let from_name = name_str(from_filename)?;
        let to_name = name_str(to_filename)?;
        if self.is_never_index(from_dirid, &from_name) || self.is_never_index(to_dirid, &to_name) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }
        self.vfs
            .rename(from_parent, &from_name, to_parent, &to_name)
            .await
            .map_err(|e| to_errno(&e))
    }

    async fn symlink<'a>(
        &self,
        _dirid: &FileHandleU64,
        _linkname: &filename3<'a>,
        _symlink: &nfspath3<'a>,
        _attr: &sattr3,
    ) -> Result<(FileHandleU64, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn commit(&self, _id: &FileHandleU64, _offset: u64, _count: u32) -> Result<(), nfsstat3> {
        // Every write already lands synchronously in the draft store before
        // `write` returns (see its own doc) — there is nothing left here to
        // flush.
        Ok(())
    }
}

/// Iterates one directory snapshot for `readdirplus` — see the module doc
/// for the cookie scheme.
struct VfsReadDirIter {
    listing: Vec<(NodeId, NodeAttr)>,
    index: usize,
}

impl ReadDirPlusIterator<FileHandleU64> for VfsReadDirIter {
    async fn next(&mut self) -> NextResult<DirEntryPlus<FileHandleU64>> {
        if self.index >= self.listing.len() {
            return NextResult::Eof;
        }
        let (id, attr) = &self.listing[self.index];
        let entry = DirEntryPlus {
            fileid: id.0,
            name: attr.name.clone().into_bytes().into(),
            cookie: (self.index + 1) as u64,
            name_attributes: Some(to_fattr3(&to_frontend_attr(id.0, attr))),
            name_handle: Some(FileHandleU64::new(id.0)),
        };
        self.index += 1;
        NextResult::Ok(entry)
    }
}

/// Decodes an NFS wire filename into a `String`, refusing anything that
/// isn't valid UTF-8 — the facade's `remote_path`s are always UTF-8 (they
/// round-trip through `serde_json`), so a non-UTF-8 name could never
/// resolve to anything anyway.
fn name_str(name: &filename3<'_>) -> Result<String, nfsstat3> {
    std::str::from_utf8(name.as_ref())
        .map(str::to_string)
        .map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

/// The nfs3-specific half of D2's mapping: turns the portable
/// [`FrontendErrno`] into `nfs3_server`'s own `nfsstat3`. The ONE place this
/// adapter turns any classified outcome (including a literal
/// [`FrontendErrno::NotFound`] constructed at a lookup/getattr-miss call
/// site, not just an [`classify_error`]'d `anyhow::Error`) into a wire code —
/// see the module doc's mutation-testing note.
fn to_nfsstat3(errno: FrontendErrno) -> nfsstat3 {
    match errno {
        FrontendErrno::NotFound => nfsstat3::NFS3ERR_NOENT,
        FrontendErrno::Stale => nfsstat3::NFS3ERR_STALE,
        FrontendErrno::Exist => nfsstat3::NFS3ERR_EXIST,
        // D2 judgment call (documented per its directive to "judge" and
        // disclose): NFSv3 has no direct "handle busy" code. `JUKEBOX`
        // ("the server can't complete this right now, retry the request")
        // is the closest fit semantically — the client is expected to retry,
        // which is exactly what closing the busy handle and retrying does
        // for `RenameBusyError`. `ACCES` was the other candidate but would
        // misleadingly suggest a permissions problem, which this isn't.
        // Reachable through THIS adapter despite handles being per-RPC (see
        // the module doc): `nfs3_server` dispatches each incoming RPC on
        // its own tokio task, so a slow WRITE's facade handle can still be
        // open when a concurrent RENAME on the same path arrives and trips
        // `Vfs::rename`'s `is_subtree_open` guard. No deterministic test drives
        // that overlap through the trait object (it depends on the tokio
        // scheduler), so this mapping is pinned by a direct unit test
        // instead — real AND end-to-end-testable once the FUSE adapter
        // (Task 4) lands, since `fuser` handles persist across calls by
        // design rather than by scheduler timing.
        FrontendErrno::Busy => nfsstat3::NFS3ERR_JUKEBOX,
        // D2 (phase 4, deliverable D): NFSv3's direct, purpose-built code
        // for exactly this condition — a real REMOVE/RMDIR-equivalent
        // refusal, unlike `Busy`'s judgment-call stand-in above.
        FrontendErrno::NotEmpty => nfsstat3::NFS3ERR_NOTEMPTY,
        // Phase 4 task 3, R1: NFSv3's direct, purpose-built code for this
        // exact condition (RFC 1813's own RENAME procedure lists it).
        FrontendErrno::IsDir => nfsstat3::NFS3ERR_ISDIR,
        // Junk (`crate::junk`): NFSv3's own codes for both conditions.
        FrontendErrno::Access => nfsstat3::NFS3ERR_ACCES,
        FrontendErrno::CrossDevice => nfsstat3::NFS3ERR_XDEV,
        FrontendErrno::NoSpace => nfsstat3::NFS3ERR_NOSPC,
        FrontendErrno::FileTooBig => nfsstat3::NFS3ERR_FBIG,
        FrontendErrno::Io => nfsstat3::NFS3ERR_IO,
    }
}

/// Classifies then converts an `anyhow::Error` from any `Vfs` call in one
/// step.
fn to_errno(err: &anyhow::Error) -> nfsstat3 {
    to_nfsstat3(classify_error(err))
}

/// Converts the portable [`FrontendAttr`] into `nfs3_server`'s `fattr3`.
fn to_fattr3(attr: &FrontendAttr) -> fattr3 {
    fattr3 {
        type_: if attr.is_dir { ftype3::NF3DIR } else { ftype3::NF3REG },
        mode: attr.mode,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        used: attr.size,
        rdev: specdata3::default(),
        fsid: 0,
        fileid: attr.fileid,
        atime: to_nfstime3(attr.atime_secs, attr.time_nsecs),
        mtime: to_nfstime3(attr.mtime_secs, attr.time_nsecs),
        ctime: to_nfstime3(attr.ctime_secs, attr.time_nsecs),
    }
}

/// `NodeAttr::mtime_secs` is a signed unix timestamp; `nfstime3::seconds` is
/// an unsigned 32-bit one. Clamped rather than wrapped: a negative or
/// far-future timestamp (neither of which a well-behaved server should ever
/// send) becomes the nearest representable bound instead of silently
/// aliasing to an unrelated time.
fn to_nfstime3(secs: i64, nsecs: u32) -> nfstime3 {
    match u32::try_from(secs) {
        Ok(seconds) => nfstime3 { seconds, nseconds: nsecs.min(999_999_999) },
        Err(_) => nfstime3 { seconds: if secs < 0 { 0 } else { u32::MAX }, nseconds: 0 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::{DirNotEmptyError, RenameBusyError, StaleHandleError, UnlinkBusyError};

    #[test]
    fn not_found_maps_to_noent() {
        assert_eq!(to_nfsstat3(FrontendErrno::NotFound), nfsstat3::NFS3ERR_NOENT);
    }

    #[test]
    fn stale_handle_error_maps_to_stale() {
        let err = anyhow::Error::new(StaleHandleError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_STALE);
    }

    #[test]
    fn rename_busy_error_maps_to_jukebox() {
        let err = anyhow::Error::new(RenameBusyError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_JUKEBOX);
    }

    #[test]
    fn unlink_busy_error_maps_to_jukebox() {
        let err = anyhow::Error::new(UnlinkBusyError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_JUKEBOX);
    }

    #[test]
    fn dir_not_empty_error_maps_to_notempty() {
        let err = anyhow::Error::new(DirNotEmptyError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_NOTEMPTY);
    }

    #[test]
    fn eexist_marker_maps_to_exist() {
        let err = anyhow::anyhow!("EEXIST: an entry named \"dup.txt\" already exists");
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_EXIST);
    }

    #[test]
    fn an_unrecognized_error_maps_to_io() {
        let err = anyhow::anyhow!("some transient failure");
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_IO);
    }

    #[test]
    fn junk_refusals_map_to_acces_and_xdev() {
        assert_eq!(to_nfsstat3(FrontendErrno::Access), nfsstat3::NFS3ERR_ACCES);
        assert_eq!(to_nfsstat3(FrontendErrno::CrossDevice), nfsstat3::NFS3ERR_XDEV);
    }

    #[test]
    fn negative_mtime_clamps_to_zero() {
        assert_eq!(to_nfstime3(-5, 7).seconds, 0);
    }

    /// The server's accept loop returning an error (EMFILE once killed
    /// it) must reach whoever owns the mount instead of going unnoticed.
    #[tokio::test]
    async fn a_server_that_ends_on_its_own_is_reported_stopped() {
        let server = tokio::spawn(async {
            Err(std::io::Error::from_raw_os_error(libc::EMFILE))
        });
        let server = supervise(server, "drive-1".into(), 4242);
        let mut stopped = server.stopped();
        tokio::time::timeout(Duration::from_secs(2), stopped.wait_for(Option::is_some))
            .await
            .expect("the stop must be reported")
            .expect("the channel must still be open");
        assert!(stopped.borrow().as_deref().unwrap().contains("stopped"));
    }

    #[tokio::test]
    async fn a_panicking_server_is_reported_stopped() {
        let server = tokio::spawn(async { panic!("boom") });
        let server = supervise(server, "drive-1".into(), 4242);
        let mut stopped = server.stopped();
        tokio::time::timeout(Duration::from_secs(2), stopped.wait_for(Option::is_some))
            .await
            .expect("the stop must be reported")
            .unwrap();
        assert!(stopped.borrow().as_deref().unwrap().contains("panicked"));
    }

    /// Unmounting aborts the server on purpose: nothing to report, the
    /// channel just closes.
    #[tokio::test]
    async fn an_aborted_server_is_not_reported_stopped() {
        let server = tokio::spawn(std::future::pending::<std::io::Result<()>>());
        let server = supervise(server, "drive-1".into(), 4242);
        let mut stopped = server.stopped();
        server.abort();
        let changed = tokio::time::timeout(Duration::from_secs(2), stopped.changed())
            .await
            .expect("the channel must close");
        assert!(changed.is_err(), "a deliberate stop must close the channel, not report");
        assert!(stopped.borrow().is_none());
    }
}
