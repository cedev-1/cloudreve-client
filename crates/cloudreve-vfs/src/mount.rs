//! macOS mount lifecycle for the on-demand VFS (phase 3, D1/D4/D5).
//!
//! `mount()` spawns [`VfsNfs`] behind `nfs3_server`'s own TCP listener on
//! `127.0.0.1:0` (an ephemeral port), then shells out to the OS's built-in
//! `/sbin/mount_nfs` to attach that server as a real mount — rclone's own
//! `nfsmount` pattern, verified live against this crate's `nfs3_server`
//! (see the module's bottom doc comment for exactly what the OS accepted).
//! Everything below is `#[cfg(target_os = "macos")]`-gated at the
//! declaration in `lib.rs`, not per-item in this file: **Task 4 will need
//! to relax that** (declare the module unconditionally, or per-OS) once it
//! adds the Linux/FUSE branch into this same file — see D1's "platform
//! dispatch inside via `cfg`" note. Right now the whole module is honestly
//! macOS-only because that's the only platform this code has ever run on.
//!
//! ## Lifecycle
//!
//! [`MountedVfs`] owns the server's `JoinHandle` and the mountpoint.
//! `unmount()` runs the umount escalation chain (`umount`, then `umount
//! -f`, then `diskutil unmount force`) and, only once that succeeds, aborts
//! the server task — matching the plan's "umount escalation chain + server
//! abort" order (unmounting the OS side while the server can still answer
//! is safer than aborting first and leaving the kernel's NFS client stuck
//! talking to nothing while it tries to actually tear the mount down).
//! `Drop` performs the same best-effort sequence, logging rather than
//! panicking — the `server_task: Option<...>` field doubles as its own
//! "already cleaned up" sentinel: `unmount()` `.take()`s it out only AFTER
//! a successful OS-level unmount, so `Drop` naturally no-ops on the happy
//! path and only ever retries cleanup when something was left unfinished
//! (an early return from a failed `unmount()`, or a value that was never
//! explicitly unmounted at all).
//!
//! [`cleanup_stale_mount`] is the D5 crash-recovery path: this in-process
//! server cannot outlive the process that spawned it, so ANY mountpoint the
//! OS still lists as an NFS mount sourced from `127.0.0.1` is necessarily
//! left over from an earlier, uncleanly-terminated run — there is no
//! "currently healthy" case to distinguish it from. Detection is therefore
//! a single, side-effect-free check against `/sbin/mount`'s own listing
//! (no probing `read_dir`/`statfs` against the mountpoint itself, which
//! would risk hanging on exactly the dead mount it's trying to diagnose).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::nfs::VfsNfs;
use crate::vfs::Vfs;

/// mount_nfs option string, minus the `port=`/`mountport=` pair this
/// function fills in per-mount (the server binds an ephemeral port, so
/// there is no fixed value to hardcode here). Verified live on this
/// machine (macOS 26 / Darwin 25.6.0) against `mount_nfs`'s BSD/Apple
/// implementation:
///   - `vers=3` is REQUIRED — omitting it let the client negotiate a
///     higher version the in-process server (NFSv3-only) can't speak,
///     which failed the mount outright. The upstream `nfs3_server` README
///     shows the same flag for its own macOS quick-start example.
///   - `soft` (+ `retrans`/`timeo`) is required for the crash-recovery
///     test: without it, an RPC against a mount whose server has died
///     blocks forever instead of returning ETIMEDOUT. `timeo` is in
///     DECISECONDS per `mount_nfs(8)` — `timeo=30` is 3 real seconds per
///     retry, `retrans=2` bounds the whole soft-timeout window to ~9s.
///   - `nolocks`: this server implements no NLM (network lock manager);
///     without it, mount_nfs's own lock-daemon handshake stalls the mount.
const MOUNT_OPTS: &str = "nolocks,vers=3,tcp,soft,timeo=30,retrans=2";

/// Mounts `vfs` as a real NFS drive at `mountpoint` (D1). `mountpoint` must
/// already exist as an empty directory. `volume_name` is advisory ONLY on
/// macOS: plain `mount_nfs` has no volname option, so Finder shows the
/// mountpoint's own directory NAME instead — callers that want a specific
/// drive name in Finder should name the mountpoint directory itself, not
/// rely on this argument (it is still accepted and stored, matching the
/// public surface Task 4's Linux branch needs — `fuser`'s `fsname`/
/// `subtype` options DO use it there).
///
/// Async — not a bookkeeping choice but a genuine requirement: `nfs3_server`
/// only exposes an async bind (`NFSTcpListener::bind`, which itself awaits
/// `tokio::net::TcpListener::bind`), so there is no way to obtain the
/// server's ephemeral port synchronously. Every caller in this codebase
/// (Tauri commands, tests) already runs inside a tokio runtime.
pub async fn mount(vfs: Arc<Vfs>, mountpoint: &Path, volume_name: &str) -> Result<MountedVfs> {
    // `volume_name` is genuinely unused on macOS (see the fn doc) — held
    // here only so the parameter's purpose is documented at the call site
    // rather than silently swallowed.
    let _ = volume_name;

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

    if let Err(err) = run_mount_nfs(mountpoint, port) {
        // The OS never attached, so there is nothing mounted to clean up —
        // just stop the now-useless server task.
        server_task.abort();
        return Err(err);
    }

    Ok(MountedVfs { mountpoint: mountpoint.to_path_buf(), server_task: Some(server_task) })
}

/// Runs `/sbin/mount_nfs` against the in-process server listening on
/// `127.0.0.1:<port>`, attaching it at `mountpoint`. See [`MOUNT_OPTS`]'s
/// doc for why each flag is there.
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

/// An active mount produced by [`mount`]. Dropping it performs a best-effort
/// unmount (logged, never panics) — see the module doc for the exact
/// ordering and the `server_task: Option` "already cleaned up" sentinel.
pub struct MountedVfs {
    mountpoint: PathBuf,
    server_task: Option<JoinHandle<()>>,
}

impl MountedVfs {
    /// Unmounts cleanly: OS-level umount escalation chain first, then the
    /// in-process server task is aborted. Order matters — see the module
    /// doc.
    pub fn unmount(mut self) -> Result<()> {
        escalate_unmount(&self.mountpoint)?;
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
        Ok(())
    }

    /// Test-only: aborts the in-process nfs3 server's ACCEPT-LOOP task
    /// WITHOUT performing any OS-level unmount, and flips `MountedVfs`'s
    /// internal sentinel so `Drop` no-ops afterward (see the module doc).
    /// Correction from an earlier version of this doc (review Important-2):
    /// aborting this one `JoinHandle` does NOT by itself make the server
    /// dead — `nfs3_server` spawns each accepted connection's RPC loop as
    /// its own DETACHED task (see `tcp.rs`), which keeps answering the
    /// kernel's already-established TCP connection after this call returns.
    /// A test that needs a GENUINELY dead server (to exercise
    /// [`cleanup_stale_mount`] realistically) must kill the whole runtime
    /// the server ran on instead — see `tests/mounted_macos.rs`'s
    /// crash-recovery test, which runs `mount()` on its own dedicated
    /// `tokio::runtime::Runtime` and `shutdown_background()`s it (dropping
    /// every task on it, accept loop and detached connection handlers
    /// alike) before calling this method purely to suppress `Drop`'s
    /// now-redundant OS-unmount attempt on a mount the test still needs
    /// intact. A free-standing test hook was considered instead of a
    /// method, but `MountedVfs` is the only thing holding the private
    /// `JoinHandle` — a consuming method keeps that field private while
    /// giving tests exactly the one operation they need.
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
            if let Err(err) = escalate_unmount(&self.mountpoint) {
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

/// D5: crash recovery. This in-process server cannot outlive the process
/// that spawned it, so any mountpoint the OS still lists as an NFS mount
/// sourced from `127.0.0.1` is necessarily a leftover from an earlier,
/// uncleanly-terminated run (see the module doc for why no separate
/// liveness probe is needed). Force-unmounts it if found. Returns whether
/// anything was cleaned.
pub fn cleanup_stale_mount(mountpoint: &Path) -> Result<bool> {
    if !is_our_nfs_mount(mountpoint)? {
        return Ok(false);
    }
    escalate_unmount(mountpoint)?;
    Ok(true)
}

/// Parses `/sbin/mount`'s own listing (no arguments — every current mount)
/// looking for an entry whose local path is `mountpoint` and whose source
/// is an NFS mount from `127.0.0.1`. `mountpoint` is resolved through its
/// ancestor symlinks (macOS's temp directories commonly go through one,
/// e.g. `/tmp` -> `/private/tmp`) via [`resolve_mountpoint_for_comparison`]
/// before comparing — see that function's doc for why the mountpoint
/// itself is never touched here (review Important-1: the OLD code called
/// `canonicalize` directly on `mountpoint`, which stats the mount itself;
/// against a genuinely dead server that stalled for the whole soft-mount
/// timeout and then silently fell back to the un-resolved path, which
/// never string-matches `mount`'s canonical output — a false negative on
/// exactly the crash this function exists to detect).
fn is_our_nfs_mount(mountpoint: &Path) -> Result<bool> {
    let canonical = resolve_mountpoint_for_comparison(mountpoint);
    let canonical = canonical.to_string_lossy();

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
}

/// Resolves `mountpoint`'s ancestor symlinks WITHOUT ever stat-ing
/// `mountpoint` itself — that stat is exactly the operation that can stall
/// for the whole soft-mount timeout against a genuinely dead server (review
/// Important-1). Canonicalizes the PARENT directory instead (always a
/// plain, un-mounted local directory — it can't be the mount, so it can't
/// stall) and re-joins `mountpoint`'s own final path component onto the
/// result. `mount`'s own listing always reports the fully-resolved form
/// (macOS's `/tmp` -> `/private/tmp`, `/var` -> `/private/var`), so this is
/// what makes the string comparison in [`is_our_nfs_mount`] line up.
///
/// Falls back to the raw, un-resolved path when there's no parent to
/// canonicalize (`mountpoint` has no parent component, e.g. `/`) or the
/// parent lookup itself fails for some other reason — the same degrade the
/// old code had for its own canonicalize failure, just narrowed to a call
/// that can't hang.
fn resolve_mountpoint_for_comparison(mountpoint: &Path) -> PathBuf {
    let (Some(parent), Some(file_name)) = (mountpoint.parent(), mountpoint.file_name()) else {
        return mountpoint.to_path_buf();
    };
    std::fs::canonicalize(parent)
        .map(|canonical_parent| canonical_parent.join(file_name))
        .unwrap_or_else(|_| mountpoint.to_path_buf())
}

/// Escalating unmount: plain `umount`, then `umount -f`, then `diskutil
/// unmount force` — the sequence D4 specifies, each one only attempted
/// after the previous one fails.
fn escalate_unmount(mountpoint: &Path) -> Result<()> {
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

/// Runs `cmd args...`, returning whether it exited successfully. Spawn
/// failures (binary missing, etc.) count as failure too — the caller's
/// escalation chain just moves on to the next attempt.
fn run_ok(cmd: &str, args: &[&OsStr]) -> bool {
    Command::new(cmd).args(args).output().map(|o| o.status.success()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic regression proof for review Important-1, independent of
    /// any real mount or kernel attribute-cache timing (see the phase-3
    /// Task-3 "Fix round 1" report for why the integration-level crash test
    /// alone couldn't reliably reproduce this: canonicalize on a
    /// freshly-mounted root came back from cache too fast to observe the
    /// stall/false-negative in that test's timing window). Builds a
    /// symlinked ancestor exactly like `/tmp` -> `/private/tmp`, then points
    /// `mountpoint` at a FINAL component that never exists — `std::fs::
    /// canonicalize` on the full path is then GUARANTEED to fail (ENOENT),
    /// no network or mount involved, deterministically exercising the
    /// exact branch the old buggy code took against a genuinely dead mount.
    ///
    /// Old (buggy) code: `canonicalize(mountpoint)` fails -> falls back to
    /// the RAW, un-resolved path — which still contains the symlink
    /// component and so never matches `/sbin/mount`'s resolved output.
    /// New (fixed) code: only the PARENT is canonicalized (and it exists,
    /// so that always succeeds) -> the result is fully resolved regardless
    /// of whether the final component (the "mount") is reachable at all.
    #[test]
    fn resolves_through_a_symlinked_ancestor_even_when_the_final_component_is_unreachable() {
        let base = tempfile::tempdir().unwrap();
        let real_target = base.path().join("real-target");
        std::fs::create_dir(&real_target).unwrap();
        let symlinked_ancestor = base.path().join("symlinked-ancestor");
        std::os::unix::fs::symlink(&real_target, &symlinked_ancestor).unwrap();

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
}
