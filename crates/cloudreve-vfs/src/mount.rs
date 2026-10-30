//! Mount lifecycle for the on-demand VFS, across both frontends (phase 3,
//! D1/D4/D5/D6): [`mount`] wires the platform-appropriate frontend up to a
//! real OS mount (`nfs3_server` + `mount_nfs` on macOS, `fuser` +
//! libfuse/`fusermount3` on Linux), returns a [`MountedVfs`] RAII handle,
//! and [`cleanup_stale_mount`] recovers a mountpoint left behind by an
//! unclean shutdown.
//!
//! Platform dispatch is entirely `cfg`-gated INSIDE this one file (D1):
//! each OS gets its own private `{macos,linux}_impl` submodule, re-exported
//! under the SAME three names (`mount`, `MountedVfs`, `cleanup_stale_mount`)
//! so callers never see which branch compiled. Only one of the two
//! submodules exists in any given build — this crate declares no fallback
//! for any third OS (matching the status quo before this task: the macOS
//! branch previously had no Linux counterpart at all, just a whole-file
//! `#[cfg(target_os = "macos")]` gate in `lib.rs`, which this task relaxes
//! to per-item gating here so both branches can share this one file and its
//! `run_ok`/`resolve_mountpoint_for_comparison` helpers below).
//!
//! **Verification status:** the `macos_impl` module is exercised by
//! `tests/mounted_macos.rs` on this development machine (macOS). The
//! `linux_impl` module below is `#[cfg(target_os = "linux")]`-gated and has
//! never compiled or run anywhere but a Linux CI runner (Task 5) — every
//! doc claim about it follows from reading the vendored `fuser` crate
//! source, not from execution. See `fuse.rs`'s module doc for the same
//! caveat about `VfsFuse` itself.
//!
//! ## Phase 4 (this task): mount robustness
//!
//! Three carried obligations, all shared (OS-agnostic) machinery below:
//!
//! - **Pre-clean.** `mount()` now calls `cleanup_stale_mount` FIRST, before
//!   ever attempting its own bind/attach — D5's original intent, which
//!   previously only ran when a caller remembered to invoke it by hand.
//!   Best-effort: a pre-clean failure is logged and the mount is attempted
//!   anyway, so a pre-clean bug never masks (or replaces) the real mount
//!   attempt's own, more specific error.
//! - **Bounded shell-outs.** Every subprocess call this module makes now
//!   runs through [`blocking_bounded`]/[`run_ok_bounded`]: `spawn_blocking`
//!   (so a wedged command blocks a blocking-pool thread, never an async
//!   worker) plus `tokio::time::timeout` (so a wedged command still fails
//!   the call rather than hanging it forever). [`MOUNT_BUDGET`],
//!   [`UMOUNT_STEP_BUDGET`], and [`DETECT_BUDGET`] are the three documented
//!   budgets. The one deliberate exception is `MountedVfs`'s `Drop`
//!   fallback, which cannot `.await` at all (see `macos_impl`'s
//!   `escalate_unmount_sync` doc) and keeps the pre-existing, unbounded,
//!   best-effort behavior — disclosed there, not silently narrowed here.
//! - **Two-instance false positive.** A mount-table match at a given path
//!   only proves SOME mount from `127.0.0.1` (NFS) / some `fuse*` mount
//!   (Linux) sits there — it cannot by itself tell "our own crashed
//!   instance" apart from "a currently-alive sibling (another running copy
//!   of this app, or a relaunch that reused the same mountpoint before the
//!   old one fully exited) still correctly serving it". [`mount`] now
//!   records an identity marker at `<cache_dir>/mount.port` on every
//!   successful bind (macOS: the ephemeral TCP port `nfs3_server` bound;
//!   Linux: this process's pid — there is no port at all for a FUSE mount,
//!   see `linux_impl`'s doc for why a pid is the closest analogous
//!   identity signal), and [`cleanup_stale_mount`] only ever force-unmounts
//!   a mount-table match once that SPECIFIC recorded owner is provably
//!   unreachable/dead — never on a bare path+source match alone. The
//!   decision itself is pure/testable independent of any real OS call: see
//!   [`is_stale`].
//!
//! ## Shared helpers
//!
//! `run_ok`/`run_ok_bounded`, `resolve_mountpoint_for_comparison`, the
//! mount-marker read/write helpers, and `is_stale` are pure or
//! OS-agnostic-I/O logic used identically by both platforms' stale-mount
//! detection and unmount escalation, so they live here, unconditionally
//! compiled, rather than duplicated per OS. Their unit tests (bottom of
//! this file) are the ONLY tests in this module that run on both CI
//! platforms — everything mount-specific is real-mount E2E,
//! `tests/mounted_{macos,linux}.rs`.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};

/// Total budget for the whole blocking OS attach step inside `mount()`
/// (macOS: `mount_nfs`; see `linux_impl`'s doc for why `fuser::
/// spawn_mount2` deliberately stays OUTSIDE this budget). Generous next to
/// a healthy attach (sub-second in practice against a live in-process
/// server on this machine) but still finite: a wedged mount helper must
/// fail the call, not hang the caller — and everything transitively
/// awaiting it — forever.
const MOUNT_BUDGET: Duration = Duration::from_secs(15);

/// Budget for ONE rung of an unmount escalation chain (plain `umount`, then
/// `umount -f`, etc. — see each platform's `escalate_unmount`), not the
/// whole chain. Each rung gets its own fresh budget so an early rung
/// wedging (rather than cleanly failing) can't eat into the time a LATER,
/// more forceful rung would have needed.
const UMOUNT_STEP_BUDGET: Duration = Duration::from_secs(10);

/// Budget for a single, local, read-only diagnostic step: listing the
/// mount table, reading `/proc/mounts`, or a loopback TCP liveness probe.
/// Much shorter than the two budgets above since none of these talk to the
/// (possibly dead) mount itself — only to the OS's own bookkeeping, or a
/// bare loopback socket — so a healthy answer is near-instant; this budget
/// exists purely so a pathological hang here can't stall detection.
const DETECT_BUDGET: Duration = Duration::from_secs(5);

/// Runs `cmd args...`, returning whether it exited successfully. Spawn
/// failures (binary missing, etc.) count as failure too — callers' own
/// escalation chains just move on to the next attempt. Synchronous:
/// callers on the async paths below always reach this through
/// [`run_ok_bounded`] instead; the one exception is `MountedVfs::Drop`'s
/// fallback, which cannot `.await` — see `macos_impl::escalate_unmount_sync`.
fn run_ok(cmd: &str, args: &[&OsStr]) -> bool {
    Command::new(cmd).args(args).output().map(|o| o.status.success()).unwrap_or(false)
}

/// Runs a blocking closure off the async executor via `spawn_blocking`,
/// bounded by `budget`. Every mount/umount shell-out (and the loopback
/// liveness probe) in this module goes through this, directly or via
/// [`run_ok_bounded`] — a real subprocess or socket call can block for the
/// OS's own timeout duration (or genuinely hang), which must never stall
/// the calling task indefinitely. `budget` elapsing, or the blocking
/// closure itself panicking, both surface as `Err`: callers already treat
/// "this attempt failed" uniformly regardless of which.
async fn blocking_bounded<T, F>(budget: Duration, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::time::timeout(budget, tokio::task::spawn_blocking(f))
        .await
        .context("blocking shell-out exceeded its deadline")?
        .context("blocking shell-out task panicked")
}

/// Async, bounded counterpart of [`run_ok`] for a single escalation rung.
/// Takes owned `args` (not borrowed `&OsStr`s) since the closure it builds
/// must be `'static` to cross into `spawn_blocking`.
async fn run_ok_bounded(cmd: &'static str, args: Vec<OsString>, budget: Duration) -> bool {
    blocking_bounded(budget, move || {
        let arg_refs: Vec<&OsStr> = args.iter().map(OsString::as_os_str).collect();
        run_ok(cmd, &arg_refs)
    })
    .await
    .unwrap_or(false)
}

/// Resolves `mountpoint`'s ancestor symlinks WITHOUT ever stat-ing
/// `mountpoint` itself — that stat is exactly the operation that can stall
/// (macOS: for a soft-mount's whole timeout against a dead NFS server;
/// Linux: against a FUSE mount whose reader thread is gone, the kernel
/// blocks most syscalls on it until it's unmounted) against precisely the
/// dead mount this function exists to help detect. Canonicalizes the
/// PARENT directory instead (always a plain, un-mounted local directory —
/// it can't be the mount, so it can't stall) and re-joins `mountpoint`'s
/// own final path component onto the result. Both platforms' own mount
/// tables (`mount`'s output on macOS, `/proc/mounts` on Linux) report
/// fully-resolved paths (e.g. macOS's `/tmp` -> `/private/tmp`), so this is
/// what makes each platform's string comparison line up.
///
/// Falls back to the raw, un-resolved path when there's no parent to
/// canonicalize (`mountpoint` has no parent component, e.g. `/`) or the
/// parent lookup itself fails for some other reason.
fn resolve_mountpoint_for_comparison(mountpoint: &Path) -> PathBuf {
    let (Some(parent), Some(file_name)) = (mountpoint.parent(), mountpoint.file_name()) else {
        return mountpoint.to_path_buf();
    };
    std::fs::canonicalize(parent)
        .map(|canonical_parent| canonical_parent.join(file_name))
        .unwrap_or_else(|_| mountpoint.to_path_buf())
}

/// Where [`mount`] records its identity marker (this task's two-instance
/// fix) — see the module doc's "Two-instance false positive" section for
/// what each platform stores there and why.
fn mount_marker_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("mount.port")
}

/// Persists `value` (a port number on macOS, a pid on Linux — see the
/// module doc) as this mount's identity marker. Best-effort from every call
/// site (a failure here only degrades a FUTURE crash's pre-clean back to
/// the pre-this-task "leave it for a human" behavior — it must never fail
/// the mount that's already succeeded by the time this runs).
fn write_mount_marker(cache_dir: &Path, value: &str) -> Result<()> {
    std::fs::create_dir_all(cache_dir)
        .with_context(|| format!("failed to create cache dir {}", cache_dir.display()))?;
    let path = mount_marker_path(cache_dir);
    std::fs::write(&path, value)
        .with_context(|| format!("failed to write mount marker {}", path.display()))
}

/// Reads back whatever [`write_mount_marker`] last stored, trimmed.
/// `None` covers both "never written" and "unreadable" alike — either way
/// there is nothing to positively attribute a mount-table match to.
fn read_mount_marker(cache_dir: &Path) -> Option<String> {
    std::fs::read_to_string(mount_marker_path(cache_dir)).ok().map(|s| s.trim().to_string())
}

/// Best-effort delete of the marker — called once a mount is known gone
/// (cleanly unmounted, or force-cleaned as stale) so a stale leftover
/// marker never outlives the mount it described. Never fails the caller:
/// a failure here only means a future pre-clean might (harmlessly) find a
/// marker pointing at an owner that, on re-probing, is already gone.
fn remove_mount_marker(cache_dir: &Path) {
    let path = mount_marker_path(cache_dir);
    if let Err(err) = std::fs::remove_file(&path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), %err, "vfs mount: failed to remove the mount marker");
        }
    }
}

/// Decides whether a mount-table match at some path should be treated as
/// stale, given what's known about OUR recorded identity marker for it —
/// the pure decision core of this task's two-instance fix, deliberately
/// factored out from any real OS call (`is_our_nfs_mount`/
/// `is_our_fuse_mount` are the only callers) so it can be pinned by a
/// deterministic unit test independent of any real mount.
///
/// - No mount-table match at all: never stale — nothing to clean.
/// - A match, but no marker was ever recorded for this cache_dir (`None`):
///   NOT treated as stale. Conservative on purpose: without a positive
///   record of having mounted here ourselves, this match cannot be
///   attributed to a dead instance of ours, so leaving it alone beats
///   risking a force-unmount of someone else's live mount.
/// - A match, a marker WAS recorded, and its owner (the process on the
///   other end — a TCP listener on macOS, a pid on Linux) is still alive:
///   NOT stale. This is the two-instance false positive itself: a
///   currently-alive sibling still legitimately owns this mountpoint.
/// - A match, a marker WAS recorded, and its owner is dead: stale.
fn is_stale(mount_table_match: bool, marker_owner_alive: Option<bool>) -> bool {
    if !mount_table_match {
        return false;
    }
    matches!(marker_owner_alive, Some(false))
}

#[cfg(target_os = "macos")]
mod macos_impl {
    //! macOS mount lifecycle (phase 3, D1/D4/D5; phase 4 this task):
    //! shells out to the OS's built-in `/sbin/mount_nfs` to attach
    //! [`VfsNfs`]'s in-process `nfs3_server` listener — rclone's own
    //! `nfsmount` pattern, verified live against this crate's
    //! `nfs3_server`. See [`MOUNT_OPTS`]'s doc for exactly what the OS
    //! accepted.
    //!
    //! ## Lifecycle
    //!
    //! [`MountedVfs`] owns the server's `JoinHandle`, the mountpoint, and
    //! (this task) the `cache_dir` its identity marker lives under.
    //! `unmount()` runs the umount escalation chain (`umount`, then `umount
    //! -f`, then `diskutil unmount force`) and, only once that succeeds,
    //! aborts the server task — unmounting the OS side while the server can
    //! still answer is safer than aborting first and leaving the kernel's
    //! NFS client stuck talking to nothing while it tries to actually tear
    //! the mount down. `Drop` performs the same best-effort sequence,
    //! logging rather than panicking — the `server_task: Option<...>` field
    //! doubles as its own "already cleaned up" sentinel: `unmount()`
    //! `.take()`s it out only AFTER a successful OS-level unmount, so `Drop`
    //! naturally no-ops on the happy path and only ever retries cleanup when
    //! something was left unfinished.
    //!
    //! [`cleanup_stale_mount`] is the D5 crash-recovery path: this
    //! in-process server cannot outlive the process that spawned it, so ANY
    //! mountpoint the OS still lists as an NFS mount sourced from
    //! `127.0.0.1` is EITHER left over from an earlier, uncleanly-
    //! terminated run of this app, OR (this task's fix) a currently-alive
    //! sibling instance's legitimate mount — see the module doc's
    //! "Two-instance false positive" section and [`is_our_nfs_mount`] for
    //! how the two are told apart. Detection never probes the mountpoint
    //! itself (no `read_dir`/`statfs`, which would risk hanging on exactly
    //! the dead mount it's trying to diagnose) — only `/sbin/mount`'s own
    //! listing and, for the port-liveness check, a bare loopback TCP
    //! connect (never to the mountpoint, always to `127.0.0.1:<port>`
    //! directly).

    use std::ffi::{OsStr, OsString};
    use std::net::TcpStream;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;

    use anyhow::{bail, Context, Result};
    use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
    use tokio::task::JoinHandle;

    use super::{
        blocking_bounded, read_mount_marker, remove_mount_marker, resolve_mountpoint_for_comparison,
        run_ok, run_ok_bounded, write_mount_marker, DETECT_BUDGET, MOUNT_BUDGET, UMOUNT_STEP_BUDGET,
    };
    use crate::nfs::VfsNfs;
    use crate::vfs::Vfs;

    /// mount_nfs option string, minus the `port=`/`mountport=` pair this
    /// function fills in per-mount (the server binds an ephemeral port, so
    /// there is no fixed value to hardcode here). Verified live on this
    /// machine (macOS 26 / Darwin 25.6.0) against `mount_nfs`'s BSD/Apple
    /// implementation:
    ///   - `vers=3` is REQUIRED — omitting it let the client negotiate a
    ///     higher version the in-process server (NFSv3-only) can't speak,
    ///     which failed the mount outright. The upstream `nfs3_server`
    ///     README shows the same flag for its own macOS quick-start
    ///     example.
    ///   - `soft` (+ `retrans`/`timeo`) is required for the crash-recovery
    ///     test: without it, an RPC against a mount whose server has died
    ///     blocks forever instead of returning ETIMEDOUT. `timeo` is in
    ///     DECISECONDS per `mount_nfs(8)` — `timeo=30` is 3 real seconds per
    ///     retry, `retrans=2` bounds the whole soft-timeout window to ~9s.
    ///   - `nolocks`: this server implements no NLM (network lock manager);
    ///     without it, mount_nfs's own lock-daemon handshake stalls the
    ///     mount.
    const MOUNT_OPTS: &str = "nolocks,vers=3,tcp,soft,timeo=30,retrans=2";

    /// Mounts `vfs` as a real NFS drive at `mountpoint` (D1). `mountpoint`
    /// must already exist as an empty directory. `volume_name` is advisory
    /// ONLY on macOS: plain `mount_nfs` has no volname option, so Finder
    /// shows the mountpoint's own directory NAME instead — callers that
    /// want a specific drive name in Finder should name the mountpoint
    /// directory itself, not rely on this argument (it is still accepted
    /// and stored, matching the public surface the Linux branch needs —
    /// `fuser`'s `fsname`/`subtype` options DO use it there). `cache_dir`
    /// (this task) is where the mount's identity marker is written on
    /// success and where a future call's pre-clean step looks for one —
    /// callers should pass the SAME `cache_dir` they gave `Vfs::new` for
    /// this drive.
    ///
    /// Pre-cleans (this task, D5's original intent): calls
    /// [`cleanup_stale_mount`] on `mountpoint`/`cache_dir` FIRST,
    /// best-effort — a pre-clean failure is logged, never returned, so it
    /// can never mask the real mount attempt's own, more specific error.
    ///
    /// Async — not a bookkeeping choice but a genuine requirement:
    /// `nfs3_server` only exposes an async bind (`NFSTcpListener::bind`,
    /// which itself awaits `tokio::net::TcpListener::bind`), so there is no
    /// way to obtain the server's ephemeral port synchronously. Every
    /// caller in this codebase (Tauri commands, tests) already runs inside
    /// a tokio runtime.
    pub async fn mount(
        vfs: Arc<Vfs>,
        mountpoint: &Path,
        volume_name: &str,
        cache_dir: &Path,
    ) -> Result<MountedVfs> {
        // `volume_name` is genuinely unused on macOS (see the fn doc) — held
        // here only so the parameter's purpose is documented at the call
        // site rather than silently swallowed.
        let _ = volume_name;

        if let Err(err) = cleanup_stale_mount(mountpoint, cache_dir).await {
            tracing::warn!(
                mountpoint = %mountpoint.display(),
                ?err,
                "vfs mount: pre-clean failed, attempting to mount anyway"
            );
        }

        let fs = VfsNfs::new(vfs);
        let listener = NFSTcpListener::bind("127.0.0.1:0", fs)
            .await
            .context("binding the in-process nfs3 server to 127.0.0.1:0")?;
        let port = listener.get_listen_port();

        let server_task: JoinHandle<()> = tokio::spawn(async move {
            if let Err(err) = listener.handle_forever().await {
                tracing::warn!(?err, "vfs nfs3 server task exited");
            }
        });

        let mountpoint_owned = mountpoint.to_path_buf();
        let mount_result =
            blocking_bounded(MOUNT_BUDGET, move || run_mount_nfs(&mountpoint_owned, port))
                .await
                .and_then(std::convert::identity);
        if let Err(err) = mount_result {
            // The OS never attached, so there is nothing mounted to clean up
            // — just stop the now-useless server task.
            server_task.abort();
            return Err(err);
        }

        if let Err(err) = write_mount_marker(cache_dir, &port.to_string()) {
            tracing::warn!(
                ?err,
                "vfs mount: failed to persist the mount.port marker — a future crash at this \
                 mountpoint may not self-heal via pre-clean"
            );
        }

        Ok(MountedVfs {
            mountpoint: mountpoint.to_path_buf(),
            cache_dir: cache_dir.to_path_buf(),
            server_task: Some(server_task),
        })
    }

    /// Runs `/sbin/mount_nfs` against the in-process server listening on
    /// `127.0.0.1:<port>`, attaching it at `mountpoint`. See [`MOUNT_OPTS`]'s
    /// doc for why each flag is there. Synchronous: always reached through
    /// [`blocking_bounded`] (see `mount`'s body), never called directly.
    fn run_mount_nfs(mountpoint: &Path, port: u16) -> Result<()> {
        let mp = mountpoint.to_str().context("mountpoint path must be valid utf-8")?;
        let opts = format!("{MOUNT_OPTS},port={port},mountport={port}");
        let output = Command::new("/sbin/mount_nfs")
            .args(["-o", &opts, "127.0.0.1:/", mp])
            .output()
            .context("spawning /sbin/mount_nfs")?;
        if !output.status.success() {
            bail!(
                "mount_nfs failed (status {:?}): {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    /// An active mount produced by [`mount`]. Dropping it performs a
    /// best-effort unmount (logged, never panics) — see the module doc for
    /// the exact ordering and the `server_task: Option` "already cleaned
    /// up" sentinel.
    pub struct MountedVfs {
        mountpoint: PathBuf,
        cache_dir: PathBuf,
        server_task: Option<JoinHandle<()>>,
    }

    impl MountedVfs {
        /// Unmounts cleanly: OS-level umount escalation chain first (each
        /// rung bounded — see [`UMOUNT_STEP_BUDGET`]), then this mount's
        /// identity marker is removed (best-effort — see
        /// `remove_mount_marker`'s doc), then the in-process server task is
        /// aborted. Order matters — see the module doc.
        pub async fn unmount(mut self) -> Result<()> {
            escalate_unmount(&self.mountpoint).await?;
            remove_mount_marker(&self.cache_dir);
            if let Some(task) = self.server_task.take() {
                task.abort();
            }
            Ok(())
        }

        /// Test-only: aborts the in-process nfs3 server's ACCEPT-LOOP task
        /// WITHOUT performing any OS-level unmount, and flips `MountedVfs`'s
        /// internal sentinel so `Drop` no-ops afterward (see the module
        /// doc). Aborting this one `JoinHandle` does NOT by itself make the
        /// server dead — `nfs3_server` spawns each accepted connection's RPC
        /// loop as its own DETACHED task (see `tcp.rs`), which keeps
        /// answering the kernel's already-established TCP connection after
        /// this call returns. A test that needs a GENUINELY dead server (to
        /// exercise [`cleanup_stale_mount`] realistically) must kill the
        /// whole runtime the server ran on instead — see
        /// `tests/mounted_macos.rs`'s crash-recovery test, which runs
        /// `mount()` on its own dedicated `tokio::runtime::Runtime` and
        /// `shutdown_background()`s it (dropping every task on it, accept
        /// loop and detached connection handlers alike) before calling this
        /// method purely to suppress `Drop`'s now-redundant OS-unmount
        /// attempt on a mount the test still needs intact.
        #[doc(hidden)]
        pub fn abort_server_for_tests(mut self) {
            if let Some(task) = self.server_task.take() {
                task.abort();
            }
        }
    }

    impl Drop for MountedVfs {
        fn drop(&mut self) {
            // `server_task` is `None` only once `unmount()` (or
            // `abort_server_for_tests`) already handled cleanup — see the
            // module doc's sentinel note.
            if let Some(task) = self.server_task.take() {
                if let Err(err) = escalate_unmount_sync(&self.mountpoint) {
                    tracing::warn!(
                        mountpoint = %self.mountpoint.display(),
                        ?err,
                        "best-effort unmount on drop failed"
                    );
                }
                task.abort();
            }
        }
    }

    /// D5: crash recovery (phase 4 this task: now also the two-instance
    /// fix). This in-process server cannot outlive the process that
    /// spawned it, so any mountpoint the OS still lists as an NFS mount
    /// sourced from `127.0.0.1` is either a genuine leftover from an
    /// earlier, uncleanly-terminated run of THIS app, or a currently-alive
    /// sibling's legitimate mount — [`is_our_nfs_mount`] tells the two
    /// apart via the `cache_dir`-scoped port marker before this
    /// force-unmounts anything. Returns whether anything was cleaned.
    pub async fn cleanup_stale_mount(mountpoint: &Path, cache_dir: &Path) -> Result<bool> {
        if !is_our_nfs_mount(mountpoint, cache_dir).await? {
            return Ok(false);
        }
        escalate_unmount_stale(mountpoint).await?;
        remove_mount_marker(cache_dir);
        Ok(true)
    }

    /// Parses `/sbin/mount`'s own listing (no arguments — every current
    /// mount) looking for an entry whose local path is `mountpoint` and
    /// whose source is an NFS mount from `127.0.0.1`; if one is found,
    /// consults `cache_dir`'s recorded port marker and probes whether that
    /// SPECIFIC port is still alive (a bare loopback TCP connect — see
    /// [`is_port_alive`]) before concluding this match is actually stale.
    /// `mountpoint` is resolved through its ancestor symlinks (macOS's temp
    /// directories commonly go through one, e.g. `/tmp` -> `/private/tmp`)
    /// via [`resolve_mountpoint_for_comparison`] before comparing — see
    /// that function's doc for why the mountpoint itself is never touched
    /// here. The actual stale/not-stale decision is [`super::is_stale`],
    /// pure and unit-tested independent of both OS calls this function
    /// makes.
    async fn is_our_nfs_mount(mountpoint: &Path, cache_dir: &Path) -> Result<bool> {
        let canonical = resolve_mountpoint_for_comparison(mountpoint).to_string_lossy().into_owned();

        let mount_table_match: bool = blocking_bounded(DETECT_BUDGET, move || -> Result<bool> {
            let output = Command::new("/sbin/mount").output().context("running /sbin/mount")?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                // Line shape: "<source> on <path> (<options>)".
                let Some((source, rest)) = line.split_once(" on ") else { continue };
                let Some((path_str, opts)) = rest.rsplit_once(" (") else { continue };
                if path_str == canonical && source.starts_with("127.0.0.1:") && opts.contains("nfs") {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
        .and_then(std::convert::identity)?;

        if !mount_table_match {
            return Ok(false);
        }

        let marker_owner_alive = match read_mount_marker(cache_dir).and_then(|m| m.parse::<u16>().ok())
        {
            Some(port) => Some(is_port_alive(port).await),
            None => None,
        };

        Ok(super::is_stale(mount_table_match, marker_owner_alive))
    }

    /// Whether something is currently listening (and accepting a raw TCP
    /// connect) on `127.0.0.1:<port>` — the two-instance liveness probe.
    /// Deliberately just a bare connect, no NFS-level RPC: a TCP handshake
    /// already answers "is the process that bound this port still alive
    /// and accepting connections", and doing nothing more here means
    /// nothing more that can itself hang. Never touches the mountpoint.
    async fn is_port_alive(port: u16) -> bool {
        blocking_bounded(DETECT_BUDGET, move || TcpStream::connect(("127.0.0.1", port)).is_ok())
            .await
            .unwrap_or(false)
    }

    /// Escalating unmount for an explicit, live `unmount()` call: plain
    /// `umount`, then `umount -f`, then `diskutil unmount force` — the
    /// sequence D4 specifies. A healthy, currently-served mount is expected
    /// to yield to the very first, gentlest rung, so this always starts
    /// there — see [`escalate_unmount_stale`] for the pre-diagnosed-dead
    /// path, which skips it.
    async fn escalate_unmount(mountpoint: &Path) -> Result<()> {
        escalate_unmount_from(mountpoint, false).await
    }

    /// Escalating unmount for [`cleanup_stale_mount`]'s path: the mount was
    /// already independently diagnosed dead (`is_our_nfs_mount` already
    /// confirmed its recorded port is unreachable) before this is ever
    /// called, so the plain `umount` rung would only wait out its own
    /// failure against a server that's already known to be gone — this
    /// starts directly at `umount -f`, halving the ~10s dead-mount latency
    /// `MOUNT_OPTS`'s `timeo=30,retrans=2` soft-timeout budget would
    /// otherwise cost the FIRST rung for nothing.
    async fn escalate_unmount_stale(mountpoint: &Path) -> Result<()> {
        escalate_unmount_from(mountpoint, true).await
    }

    async fn escalate_unmount_from(mountpoint: &Path, skip_plain_umount: bool) -> Result<()> {
        let path = mountpoint.as_os_str().to_os_string();
        if !skip_plain_umount
            && run_ok_bounded("/sbin/umount", vec![path.clone()], UMOUNT_STEP_BUDGET).await
        {
            return Ok(());
        }
        if run_ok_bounded(
            "/sbin/umount",
            vec![OsString::from("-f"), path.clone()],
            UMOUNT_STEP_BUDGET,
        )
        .await
        {
            return Ok(());
        }
        if run_ok_bounded(
            "/usr/sbin/diskutil",
            vec![OsString::from("unmount"), OsString::from("force"), path],
            UMOUNT_STEP_BUDGET,
        )
        .await
        {
            return Ok(());
        }
        bail!("failed to unmount {} after exhausting the escalation chain", mountpoint.display());
    }

    /// Synchronous counterpart of [`escalate_unmount`], used ONLY by `Drop`
    /// (which cannot `.await` — there is no async destructor in Rust). Kept
    /// as a byte-for-byte copy of the pre-this-task escalation: unbounded,
    /// best-effort, logged rather than propagated. Deliberately NOT
    /// upgraded to reach for `tokio::task::block_in_place` +
    /// `Handle::current().block_on(..)` to get the same bounded behavior
    /// the async paths now have — that combination panics outside a
    /// multi-thread runtime, and worse, can panic against a runtime that is
    /// ALREADY shutting down, which is exactly the state
    /// `tests/mounted_macos.rs`'s crash-recovery test's dedicated runtime is
    /// in by the time cleanup runs. A hung command in this one fallback
    /// path still blocks whatever thread drops the value, exactly as it
    /// always has — this is a disclosed, deliberate scope limit of this
    /// task, not an oversight.
    fn escalate_unmount_sync(mountpoint: &Path) -> Result<()> {
        let path = mountpoint.as_os_str();
        if run_ok("/sbin/umount", &[path]) {
            return Ok(());
        }
        if run_ok("/sbin/umount", &[OsStr::new("-f"), path]) {
            return Ok(());
        }
        if run_ok("/usr/sbin/diskutil", &[OsStr::new("unmount"), OsStr::new("force"), path]) {
            return Ok(());
        }
        bail!("failed to unmount {} after exhausting the escalation chain", mountpoint.display());
    }
}

#[cfg(target_os = "macos")]
pub use macos_impl::{cleanup_stale_mount, mount, MountedVfs};

#[cfg(target_os = "linux")]
mod linux_impl {
    //! Linux mount lifecycle (phase 3, D1/D6; phase 4 this task):
    //! [`VfsFuse`] mounted via `fuser::spawn_mount2`, which itself spawns
    //! the request-dispatch loop on a dedicated OS thread and returns a
    //! `fuser::BackgroundSession` immediately (`session.rs::
    //! BackgroundSession::new`) — so, unlike the macOS branch, there is no
    //! separate "spawn our own background task" step here: `fuser` already
    //! does it.
    //!
    //! **UNVERIFIED**: this module has never compiled or run — see this
    //! file's own top doc and `fuse.rs`'s module doc for what that means
    //! and what it's based on instead (reading the vendored `fuser =
    //! "0.15.1"` source). This task's own changes here (pre-clean, bounded
    //! shell-outs, the pid-based two-instance marker) are held to the SAME
    //! caveat — none of it has run.
    //!
    //! ## Why `default-features = false` on the `fuser` dependency
    //!
    //! `fuser`'s default `libfuse` feature links against the system
    //! `libfuse`/`libfuse3` via `pkg-config` AT BUILD TIME (`fuser`'s own
    //! `build.rs`) — meaning a CI runner would need `libfuse3-dev` (headers
    //! + `.pc` file), not merely the `fuse3` runtime package the phase-3
    //! plan's Task 5 step already commits to installing
    //! (`apt-get install -y fuse3`, which provides the `fusermount3` binary
    //! and `/dev/fuse` udev rule, NOT the dev headers). Disabling the
    //! `libfuse` feature switches `fuser` to its "pure-rust" mount
    //! implementation (`build.rs`: "Building without libfuse is only
    //! supported on Linux", and `mnt/fuse_pure.rs`) — no C library link step
    //! at all. At RUNTIME it still needs exactly what Task 5 already plans
    //! to install: `/dev/fuse` (present on GitHub-hosted Ubuntu runners) and
    //! the `fusermount`/`fusermount3` SETUID helper binary as a fallback
    //! when a direct `mount(2)` syscall is refused for lacking
    //! `CAP_SYS_ADMIN` (`fuse_pure.rs`'s `fuse_mount_pure`). **Disclosed
    //! divergence from the brief**, which only said "fuser = latest 0.x,
    //! pin what resolves" without specifying features — `Cargo.toml` pins
    //! `fuser = "0.15"` with `default-features = false`; see this task's
    //! report for the full reasoning.
    //!
    //! ## Unmount (D6: "session drop + fusermount -u escalation")
    //!
    //! This is not something this module hand-rolls — it is what dropping
    //! `fuser::BackgroundSession` already does. `BackgroundSession`'s
    //! `_mount: Option<Mount>` field is the ONLY thing keeping the mount
    //! attached; dropping it runs `mnt::fuse_pure::Mount`'s own `Drop`
    //! impl, which — already, inside the dependency — tries a direct
    //! `umount`/`umount2` syscall first and, only on `EPERM` (unprivileged
    //! caller), falls back to spawning the setuid `fusermount3 -u -q -z`
    //! helper. That IS the "session drop + fusermount -u escalation" D6
    //! asks for; [`MountedVfs::unmount`] below only adds a verification
    //! step on top (fuser's own `Mount::Drop` returns nothing and only
    //! LOGS a failure via the `log` crate, which this crate's `tracing`
    //! subscriber does not currently bridge — see this task's report for
    //! that gap), and [`escalate_unmount`] (used by [`cleanup_stale_mount`]
    //! only) adds the SAME extra `umount -f`/lazy-detach rungs the macOS
    //! branch has, for the crash-recovery path where nothing is left
    //! running to even attempt fuser's own escalation.
    //!
    //! Deliberately does NOT call `BackgroundSession::join()`: it also
    //! joins the background read-dispatch thread, but does so via a
    //! DOUBLE `.unwrap()` internally (`session.rs`: the `JoinHandle`'s own
    //! result, then the loop's inner `io::Result`) — a panic there would
    //! violate this crate's "never panic on teardown" discipline more than
    //! the (harmless) cost of not waiting for that thread to fully exit:
    //! once the kernel-level unmount syscall inside `Mount::drop()`
    //! succeeds, the mountpoint is already a plain directory again from
    //! userspace's perspective, regardless of whether the now-orphaned
    //! read-dispatch thread has noticed yet (its next blocking read from
    //! `/dev/fuse` will get `ENODEV` and it will exit on its own).
    //!
    //! ## Stale-mount detection (phase 4 this task: two-instance)
    //!
    //! [`is_our_fuse_mount`] reads `/proc/mounts` (a virtual procfs file —
    //! reading it never blocks on a dead mount, same "side-effect-free"
    //! property the macOS branch's `/sbin/mount` parse relies on) and looks
    //! for a line whose target field is `mountpoint` (resolved via the
    //! shared [`super::resolve_mountpoint_for_comparison`]) and whose fstype
    //! field starts with `"fuse"` (`fuse`, or `fuse.<subtype>` — this
    //! module's `mount()` sets `Subtype("cloudreve")`, which the standard
    //! FUSE mount helpers report as fstype `fuse.cloudreve`; the `starts_
    //! with` check does not depend on getting that exact string right).
    //!
    //! A match alone is no longer treated as proof of staleness (same fix
    //! as the macOS branch, see the top module doc): FUSE has no TCP port
    //! to probe liveness against, so this branch records THIS PROCESS'S
    //! pid at `<cache_dir>/mount.port` on a successful mount instead, and
    //! [`cleanup_stale_mount`] only force-unmounts once that recorded pid
    //! is confirmed dead (`kill(pid, 0)` returning `ESRCH`) — the closest
    //! analogous identity signal to the macOS branch's port-liveness probe
    //! available for a mount type with no network endpoint at all.
    //! **Weaker than the macOS check, disclosed rather than hidden**: a pid
    //! can be reused by an unrelated process after this one exits, in which
    //! case a stale marker would (incorrectly) read as "still alive" and
    //! this mount would NOT be pre-cleaned — the conservative failure mode
    //! (leaving a genuinely stale mount for a human) rather than the unsafe
    //! one (force-unmounting a live sibling), consistent with [`super::
    //! is_stale`]'s documented default for "cannot positively confirm
    //! either way".
    //!
    //! **Known limitation, disclosed rather than handled**: `/proc/mounts`
    //! escapes whitespace/backslashes in paths as octal (`proc(5)`, e.g. a
    //! space becomes `\040`); this parser does not unescape. Every
    //! mountpoint this crate ever constructs comes from a caller-chosen
    //! directory (in practice, `tempfile::tempdir()` in tests) that does
    //! not contain such characters, so this is a real but narrow gap, not
    //! exercised by any test here.

    use std::ffi::{OsStr, OsString};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use anyhow::{bail, Context, Result};

    use super::{
        blocking_bounded, read_mount_marker, remove_mount_marker, resolve_mountpoint_for_comparison,
        run_ok_bounded, write_mount_marker, DETECT_BUDGET, MOUNT_BUDGET, UMOUNT_STEP_BUDGET,
    };
    use crate::fuse::VfsFuse;
    use crate::vfs::Vfs;

    /// Mounts `vfs` as a real FUSE drive at `mountpoint` (D1). `mountpoint`
    /// must already exist as an empty directory. `volume_name` becomes
    /// fuser's `FSName` mount option (shown as the mount's source in
    /// `mount`/`df` output) — unlike macOS's `mount_nfs`, which has no
    /// equivalent, this genuinely reaches userspace tooling here. A fixed
    /// `Subtype("cloudreve")` is also set (see the module doc's stale-mount
    /// detection section for why). `cache_dir` (this task) mirrors the
    /// macOS branch's parameter of the same name — see its doc.
    ///
    /// Pre-cleans (this task, D5's original intent, mirroring the macOS
    /// branch): calls [`cleanup_stale_mount`] on `mountpoint`/`cache_dir`
    /// FIRST, best-effort.
    ///
    /// Async for parity with the macOS branch's signature, not because
    /// `fuser::spawn_mount2` itself is async (it isn't — it performs a
    /// blocking `mount(2)`/`fusermount3` call synchronously before
    /// returning). Called directly rather than via `spawn_blocking` +
    /// `tokio::time::timeout` — the ONE call in this module that does NOT
    /// go through [`blocking_bounded`], a deliberate, narrower exception
    /// than it might first appear: every OTHER blocking call this task
    /// bounds is a real subprocess (or `/proc` read) that can wedge waiting
    /// on something external (a dead peer, a wedged mount table); this one
    /// is a single local `mount(2)`-class syscall through `fuser`, with no
    /// dead-peer wait built into it the way macOS's `mount_nfs` `soft`/
    /// `timeo` negotiation has. It is also the harder call to move safely
    /// without ever having compiled this module: `spawn_blocking` demands
    /// `VfsFuse: Send + 'static`, unverified here. Kept as the pre-existing
    /// direct call rather than risk an unverifiable `Send` bound on a
    /// module that cannot be compile-checked on this development machine —
    /// disclosed, not silently narrowed; see this task's report.
    ///
    /// `tokio::runtime::Handle::current()` captures the CALLING runtime,
    /// which `VfsFuse` then `block_on`s every facade call against from
    /// fuser's dedicated request-dispatch thread for the rest of the
    /// mount's life — see `fuse.rs`'s module doc for why this is sound and
    /// what it requires (the runtime must keep running for the mount's
    /// whole lifetime; every caller in this codebase already runs inside
    /// one long-lived tokio runtime, same assumption the macOS branch
    /// already documents).
    pub async fn mount(
        vfs: Arc<Vfs>,
        mountpoint: &Path,
        volume_name: &str,
        cache_dir: &Path,
    ) -> Result<MountedVfs> {
        if let Err(err) = cleanup_stale_mount(mountpoint, cache_dir).await {
            tracing::warn!(
                mountpoint = %mountpoint.display(),
                ?err,
                "vfs mount: pre-clean failed, attempting to mount anyway"
            );
        }

        let rt = tokio::runtime::Handle::current();
        let fs = VfsFuse::new(vfs, rt);
        let options = [
            fuser::MountOption::FSName(volume_name.to_string()),
            fuser::MountOption::Subtype("cloudreve".to_string()),
            fuser::MountOption::RW,
        ];
        let session = fuser::spawn_mount2(fs, mountpoint, &options).with_context(|| {
            format!(
                "fuser spawn_mount2 failed for {} — commonly a missing /dev/fuse device or a \
                 missing fusermount/fusermount3 helper binary; see mount()'s doc for what this \
                 mount needs at runtime",
                mountpoint.display()
            )
        })?;

        if let Err(err) = write_mount_marker(cache_dir, &std::process::id().to_string()) {
            tracing::warn!(
                ?err,
                "vfs mount: failed to persist the mount.port marker — a future crash at this \
                 mountpoint may not self-heal via pre-clean"
            );
        }

        Ok(MountedVfs {
            mountpoint: mountpoint.to_path_buf(),
            cache_dir: cache_dir.to_path_buf(),
            session: Some(session),
        })
    }

    /// An active mount produced by [`mount`]. Dropping it performs a
    /// best-effort unmount that cannot panic (fuser's own `Mount::Drop`
    /// contains no `.unwrap()`/`panic!` anywhere in its body) — see the
    /// module doc for the full unmount story.
    pub struct MountedVfs {
        mountpoint: PathBuf,
        cache_dir: PathBuf,
        session: Option<fuser::BackgroundSession>,
    }

    impl MountedVfs {
        /// Unmounts cleanly. See the module doc for why dropping `session`
        /// IS the actual unmount (fuser's own escalation), and why this
        /// does not call `BackgroundSession::join()`. Async (this task, for
        /// signature parity with the macOS branch): the verification step
        /// below (confirming the mount is really gone) now goes through
        /// [`blocking_bounded`] rather than an unbounded direct
        /// `/proc/mounts` read.
        pub async fn unmount(mut self) -> Result<()> {
            if let Some(session) = self.session.take() {
                drop(session);
            }
            if is_our_fuse_mount(&self.mountpoint).await? {
                bail!(
                    "failed to unmount {} — a fuse mount is still present after dropping the \
                     session",
                    self.mountpoint.display()
                );
            }
            remove_mount_marker(&self.cache_dir);
            Ok(())
        }

        /// Test-only: abandons the session WITHOUT dropping it (via
        /// `std::mem::forget`), so no unmount is ever attempted for it —
        /// simulating a crashed process that never got the chance to clean
        /// up. Flips `MountedVfs`'s internal sentinel the same way the
        /// macOS branch's own hook of the same name does, so `Drop`
        /// no-ops afterward.
        ///
        /// Weaker than the macOS branch's version of this hook in one
        /// respect, disclosed rather than hidden: macOS's hook needs a
        /// whole dedicated-runtime-shutdown dance to make the server
        /// GENUINELY unreachable, because `nfs3_server` spawns a detached
        /// background task per TCP connection that keeps answering after
        /// the accept loop alone is killed. FUSE has no such fan-out — the
        /// ENTIRE mount is served by exactly one dedicated OS thread (see
        /// `fuse.rs`'s module doc), so there is no second background task
        /// this hook needs to separately hunt down; leaking the session is
        /// already the strongest "abandon this mount" available short of
        /// actually crashing the process. This does mean the abandoned
        /// mount here is a LIVE, still-perfectly-functional server nobody
        /// bothered to unmount, rather than a genuinely dead one — a
        /// weaker crash simulation than the macOS test's, but sufficient
        /// for what `cleanup_stale_mount` is actually proving: that it
        /// force-detects and force-unmounts a mount at a known path,
        /// without probing whether whatever is behind it is alive (see
        /// this file's own `cleanup_stale_mount` doc — neither branch
        /// does liveness probing beyond this task's own pid/port check).
        /// See `tests/mounted_linux.rs`'s crash-recovery test's own doc for
        /// the same point made from the test's side.
        #[doc(hidden)]
        pub fn abort_server_for_tests(mut self) {
            if let Some(session) = self.session.take() {
                std::mem::forget(session);
            }
        }
    }

    impl Drop for MountedVfs {
        fn drop(&mut self) {
            if self.session.is_some() {
                tracing::debug!(
                    mountpoint = %self.mountpoint.display(),
                    "vfs: dropping a fuse mount without an explicit unmount() call — best-effort \
                     cleanup via fuser's own Mount::Drop escalation (see the module doc)"
                );
            }
            // `session`'s own field-drop, which happens automatically right
            // after this function returns, is what actually performs the
            // unmount — see the module doc. Nothing else to do here, and
            // nothing here can panic.
        }
    }

    /// D5: crash recovery, the Linux counterpart of the macOS branch's
    /// function of the same name (phase 4 this task: also the pid-based
    /// two-instance fix) — see the module doc's "Stale-mount detection"
    /// section for the detection strategy and its known limitations.
    pub async fn cleanup_stale_mount(mountpoint: &Path, cache_dir: &Path) -> Result<bool> {
        let mount_table_match = is_our_fuse_mount(mountpoint).await?;
        if !mount_table_match {
            return Ok(false);
        }
        let marker_owner_alive =
            match read_mount_marker(cache_dir).and_then(|m| m.parse::<i32>().ok()) {
                Some(pid) => Some(is_pid_alive(pid)),
                None => None,
            };
        if !super::is_stale(mount_table_match, marker_owner_alive) {
            return Ok(false);
        }
        escalate_unmount(mountpoint).await?;
        remove_mount_marker(cache_dir);
        Ok(true)
    }

    async fn is_our_fuse_mount(mountpoint: &Path) -> Result<bool> {
        let canonical = resolve_mountpoint_for_comparison(mountpoint).to_string_lossy().into_owned();
        blocking_bounded(DETECT_BUDGET, move || -> Result<bool> {
            let contents =
                std::fs::read_to_string("/proc/mounts").context("reading /proc/mounts")?;
            for line in contents.lines() {
                // Line shape (proc(5)): "<source> <target> <fstype> <options> <freq> <passno>",
                // whitespace-separated.
                let mut fields = line.split_whitespace();
                let Some(_source) = fields.next() else { continue };
                let Some(target) = fields.next() else { continue };
                let Some(fstype) = fields.next() else { continue };
                if target == canonical && fstype.starts_with("fuse") {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
        .and_then(std::convert::identity)
    }

    /// Whether pid `pid` currently names a live process — `kill(pid, 0)`
    /// sends no signal, only checking permission/existence: `ESRCH` means
    /// dead, anything else (success, or `EPERM` — the process exists but we
    /// lack permission to signal it, still proof it's alive) means alive.
    /// See the module doc's "Stale-mount detection" section for the known
    /// pid-reuse weakness this inherits.
    fn is_pid_alive(pid: i32) -> bool {
        // SAFETY: `kill` with signal 0 sends nothing; it only queries
        // existence/permission, and takes no pointer arguments — there is
        // nothing here for the caller to uphold beyond a valid `pid`
        // value, which every `i32` is.
        let result = unsafe { libc::kill(pid, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    /// Escalating unmount for the crash-recovery path, where nothing is
    /// left running to attempt fuser's own drop-time escalation (see the
    /// module doc): `fusermount3 -u`, then legacy `fusermount -u`, then
    /// `umount -f`, then a lazy `umount -l` detach as the final,
    /// guaranteed-to-succeed resort — the Linux analogue of the macOS
    /// branch's `diskutil unmount force` last rung, and the same
    /// `MNT_DETACH` mechanism `fuser`'s own pure-rust unmount path falls
    /// back to internally (`mnt/fuse_pure.rs`'s `fuse_unmount_pure`).
    ///
    /// Deliberately bare command names (`"fusermount3"`, not an absolute
    /// path like the macOS branch's `"/sbin/umount"`): Linux distros place
    /// these under different prefixes (`/usr/bin`, `/bin`, sometimes
    /// symlinked between them), so resolving them through `$PATH` is more
    /// portable across distros than hardcoding one — this is the same
    /// lookup fuser's own `fuse_pure.rs::detect_fusermount_bin` performs.
    /// Every rung is bounded (this task) — see [`UMOUNT_STEP_BUDGET`].
    async fn escalate_unmount(mountpoint: &Path) -> Result<()> {
        let path = mountpoint.as_os_str().to_os_string();
        if run_ok_bounded("fusermount3", vec![OsString::from("-u"), path.clone()], UMOUNT_STEP_BUDGET)
            .await
        {
            return Ok(());
        }
        if run_ok_bounded("fusermount", vec![OsString::from("-u"), path.clone()], UMOUNT_STEP_BUDGET)
            .await
        {
            return Ok(());
        }
        if run_ok_bounded("umount", vec![OsString::from("-f"), path.clone()], UMOUNT_STEP_BUDGET).await
        {
            return Ok(());
        }
        if run_ok_bounded("umount", vec![OsString::from("-l"), path], UMOUNT_STEP_BUDGET).await {
            return Ok(());
        }
        bail!("failed to unmount {} after exhausting the escalation chain", mountpoint.display());
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::{cleanup_stale_mount, mount, MountedVfs};

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic regression proof (macOS review Important-1),
    /// independent of any real mount or kernel attribute-cache timing:
    /// builds a symlinked ancestor exactly like `/tmp` -> `/private/tmp`,
    /// then points `mountpoint` at a FINAL component that never exists —
    /// `std::fs::canonicalize` on the full path is then GUARANTEED to fail
    /// (ENOENT), no network or mount involved, deterministically exercising
    /// the exact branch the old buggy code took against a genuinely dead
    /// mount.
    ///
    /// Old (buggy) code: `canonicalize(mountpoint)` fails -> falls back to
    /// the RAW, un-resolved path — which still contains the symlink
    /// component and so never matches the OS's own resolved output.
    /// New (fixed) code: only the PARENT is canonicalized (and it exists,
    /// so that always succeeds) -> the result is fully resolved regardless
    /// of whether the final component (the "mount") is reachable at all.
    ///
    /// This logic is shared verbatim by both platforms' stale-mount
    /// detection (see the module doc), so this test — unlike everything
    /// else in this file — runs and is meaningful on BOTH CI platforms.
    #[test]
    fn resolves_through_a_symlinked_ancestor_even_when_the_final_component_is_unreachable() {
        let base = tempfile::tempdir().unwrap();
        let real_target = base.path().join("real-target");
        std::fs::create_dir(&real_target).unwrap();
        let symlinked_ancestor = base.path().join("symlinked-ancestor");
        std::os::unix::fs::symlink(&real_target, &symlinked_ancestor)
            .expect("create the symlinked ancestor directory");

        // Never created — canonicalize on the full path is guaranteed to
        // fail, exactly mirroring "the mountpoint itself can't be stat'd"
        // without needing a real dead server to produce that failure.
        let mountpoint = symlinked_ancestor.join("never-created");

        let resolved = resolve_mountpoint_for_comparison(&mountpoint);

        // Expected: the symlink is resolved (result lives under the real,
        // canonical target — itself re-canonicalized in case this machine's
        // own tempdir root traverses another symlink, e.g. macOS's
        // `/var` -> `/private/var`), and the un-created final component is
        // preserved as a plain path segment rather than causing a fallback
        // to the raw, symlink-containing path.
        let expected = std::fs::canonicalize(&real_target).unwrap().join("never-created");
        assert_eq!(
            resolved, expected,
            "must resolve through the symlinked ancestor even though the final \
             component doesn't exist — a fallback to the raw path here is exactly \
             review Important-1's false negative"
        );
        assert!(
            !resolved.to_string_lossy().contains("symlinked-ancestor"),
            "the resolved path must not still contain the unresolved symlink component: {resolved:?}"
        );
    }

    /// No parent to canonicalize (e.g. the root path) must degrade to the
    /// raw path rather than panicking.
    #[test]
    fn a_path_with_no_parent_falls_back_to_the_raw_path() {
        let root = Path::new("/");
        assert_eq!(resolve_mountpoint_for_comparison(root), root);
    }

    /// An ordinary, non-symlinked, existing path resolves to itself
    /// (canonicalize is idempotent on an already-canonical path).
    #[test]
    fn an_ordinary_existing_path_resolves_to_its_own_canonical_form() {
        let dir = tempfile::tempdir().unwrap();
        let mountpoint = dir.path().join("mnt");
        std::fs::create_dir(&mountpoint).unwrap();

        let resolved = resolve_mountpoint_for_comparison(&mountpoint);
        let expected = std::fs::canonicalize(dir.path()).unwrap().join("mnt");
        assert_eq!(resolved, expected);
    }

    // -----------------------------------------------------------------
    // Phase 4 (this task), Step 1(b): the two-instance false-positive fix
    // — pure decision logic, independent of any real OS mount or marker
    // file. `is_stale` is the ONLY thing `is_our_nfs_mount`/
    // `is_our_fuse_mount` defer to for the actual stale/not-stale call, so
    // pinning it here pins both platforms' behavior at once.
    // -----------------------------------------------------------------

    /// No match in the mount table at all: never stale, regardless of any
    /// marker — there is nothing to clean either way.
    #[test]
    fn is_stale_is_false_with_no_mount_table_match() {
        assert!(!is_stale(false, None));
        assert!(!is_stale(false, Some(true)));
        assert!(!is_stale(false, Some(false)));
    }

    /// A mount-table match, but no marker ever recorded: conservatively NOT
    /// treated as stale — this is exactly what protects a mountpoint this
    /// process never mounted itself from being force-unmounted.
    #[test]
    fn is_stale_is_false_with_a_match_but_no_recorded_marker() {
        assert!(!is_stale(true, None));
    }

    /// The two-instance false positive itself: a match, AND a marker was
    /// recorded, AND that marker's owner (port/pid) is still alive — this
    /// must NOT be treated as stale, or a currently-running sibling's
    /// perfectly healthy mount would be force-unmounted out from under it.
    #[test]
    fn is_stale_is_false_when_the_recorded_owner_is_still_alive() {
        assert!(
            !is_stale(true, Some(true)),
            "a live sibling's own mount must never be treated as stale"
        );
    }

    /// A match, a marker was recorded, and that owner is confirmed dead:
    /// THIS is the genuinely-stale case — our own crashed instance's
    /// leftover mount.
    #[test]
    fn is_stale_is_true_when_the_recorded_owner_is_dead() {
        assert!(is_stale(true, Some(false)));
    }
}
