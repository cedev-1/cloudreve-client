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
//! ## Shared helpers
//!
//! `run_ok` (run a command, report success/failure) and
//! `resolve_mountpoint_for_comparison` (resolve a mountpoint's ancestor
//! symlinks without ever stat-ing the mountpoint itself — see that
//! function's own doc for why) are pure, OS-agnostic logic used identically
//! by both platforms' stale-mount detection and unmount escalation, so they
//! live here, unconditionally compiled, rather than duplicated per OS. Their
//! unit tests (bottom of this file) are the ONLY tests in this module that
//! run on both CI platforms — everything mount-specific is real-mount E2E,
//! `tests/mounted_{macos,linux}.rs`.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Runs `cmd args...`, returning whether it exited successfully. Spawn
/// failures (binary missing, etc.) count as failure too — callers' own
/// escalation chains just move on to the next attempt.
fn run_ok(cmd: &str, args: &[&OsStr]) -> bool {
    Command::new(cmd).args(args).output().map(|o| o.status.success()).unwrap_or(false)
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

#[cfg(target_os = "macos")]
mod macos_impl {
    //! macOS mount lifecycle (phase 3, D1/D4/D5): shells out to the OS's
    //! built-in `/sbin/mount_nfs` to attach [`VfsNfs`]'s in-process
    //! `nfs3_server` listener — rclone's own `nfsmount` pattern, verified
    //! live against this crate's `nfs3_server`. See [`MOUNT_OPTS`]'s doc for
    //! exactly what the OS accepted.
    //!
    //! ## Lifecycle
    //!
    //! [`MountedVfs`] owns the server's `JoinHandle` and the mountpoint.
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
    //! `127.0.0.1` is necessarily left over from an earlier, uncleanly-
    //! terminated run — there is no "currently healthy" case to distinguish
    //! it from. Detection is therefore a single, side-effect-free check
    //! against `/sbin/mount`'s own listing (no probing `read_dir`/`statfs`
    //! against the mountpoint itself, which would risk hanging on exactly
    //! the dead mount it's trying to diagnose).

    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;

    use anyhow::{bail, Context, Result};
    use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
    use tokio::task::JoinHandle;

    use super::{resolve_mountpoint_for_comparison, run_ok};
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
    /// `fuser`'s `fsname`/`subtype` options DO use it there).
    ///
    /// Async — not a bookkeeping choice but a genuine requirement:
    /// `nfs3_server` only exposes an async bind (`NFSTcpListener::bind`,
    /// which itself awaits `tokio::net::TcpListener::bind`), so there is no
    /// way to obtain the server's ephemeral port synchronously. Every
    /// caller in this codebase (Tauri commands, tests) already runs inside
    /// a tokio runtime.
    pub async fn mount(vfs: Arc<Vfs>, mountpoint: &Path, volume_name: &str) -> Result<MountedVfs> {
        // `volume_name` is genuinely unused on macOS (see the fn doc) — held
        // here only so the parameter's purpose is documented at the call
        // site rather than silently swallowed.
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
            // The OS never attached, so there is nothing mounted to clean up
            // — just stop the now-useless server task.
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

    /// An active mount produced by [`mount`]. Dropping it performs a
    /// best-effort unmount (logged, never panics) — see the module doc for
    /// the exact ordering and the `server_task: Option` "already cleaned
    /// up" sentinel.
    pub struct MountedVfs {
        mountpoint: PathBuf,
        server_task: Option<JoinHandle<()>>,
    }

    impl MountedVfs {
        /// Unmounts cleanly: OS-level umount escalation chain first, then
        /// the in-process server task is aborted. Order matters — see the
        /// module doc.
        pub fn unmount(mut self) -> Result<()> {
            escalate_unmount(&self.mountpoint)?;
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

    /// D5: crash recovery. This in-process server cannot outlive the
    /// process that spawned it, so any mountpoint the OS still lists as an
    /// NFS mount sourced from `127.0.0.1` is necessarily a leftover from an
    /// earlier, uncleanly-terminated run (see the module doc for why no
    /// separate liveness probe is needed). Force-unmounts it if found.
    /// Returns whether anything was cleaned.
    pub fn cleanup_stale_mount(mountpoint: &Path) -> Result<bool> {
        if !is_our_nfs_mount(mountpoint)? {
            return Ok(false);
        }
        escalate_unmount(mountpoint)?;
        Ok(true)
    }

    /// Parses `/sbin/mount`'s own listing (no arguments — every current
    /// mount) looking for an entry whose local path is `mountpoint` and
    /// whose source is an NFS mount from `127.0.0.1`. `mountpoint` is
    /// resolved through its ancestor symlinks (macOS's temp directories
    /// commonly go through one, e.g. `/tmp` -> `/private/tmp`) via
    /// [`resolve_mountpoint_for_comparison`] before comparing — see that
    /// function's doc for why the mountpoint itself is never touched here.
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
}

#[cfg(target_os = "macos")]
pub use macos_impl::{cleanup_stale_mount, mount, MountedVfs};

#[cfg(target_os = "linux")]
mod linux_impl {
    //! Linux mount lifecycle (phase 3, D1/D6): [`VfsFuse`] mounted via
    //! `fuser::spawn_mount2`, which itself spawns the request-dispatch
    //! loop on a dedicated OS thread and returns a `fuser::BackgroundSession`
    //! immediately (`session.rs::BackgroundSession::new`) — so, unlike the
    //! macOS branch, there is no separate "spawn our own background task"
    //! step here: `fuser` already does it.
    //!
    //! **UNVERIFIED**: this module has never compiled or run — see this
    //! file's own top doc and `fuse.rs`'s module doc for what that means
    //! and what it's based on instead (reading the vendored `fuser =
    //! "0.15.1"` source).
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
    //! ## Stale-mount detection
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
    //! Same reasoning as the macOS branch for why this can't be confused
    //! with someone else's unrelated mount: this in-process server cannot
    //! outlive the process that spawned it, so ANY fuse mount still
    //! present at a path this code itself manages is necessarily ours.
    //!
    //! **Known limitation, disclosed rather than handled**: `/proc/mounts`
    //! escapes whitespace/backslashes in paths as octal (`proc(5)`, e.g. a
    //! space becomes `\040`); this parser does not unescape. Every
    //! mountpoint this crate ever constructs comes from a caller-chosen
    //! directory (in practice, `tempfile::tempdir()` in tests) that does
    //! not contain such characters, so this is a real but narrow gap, not
    //! exercised by any test here.

    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use anyhow::{bail, Context, Result};

    use super::{resolve_mountpoint_for_comparison, run_ok};
    use crate::fuse::VfsFuse;
    use crate::vfs::Vfs;

    /// Mounts `vfs` as a real FUSE drive at `mountpoint` (D1). `mountpoint`
    /// must already exist as an empty directory. `volume_name` becomes
    /// fuser's `FSName` mount option (shown as the mount's source in
    /// `mount`/`df` output) — unlike macOS's `mount_nfs`, which has no
    /// equivalent, this genuinely reaches userspace tooling here. A fixed
    /// `Subtype("cloudreve")` is also set (see the module doc's stale-mount
    /// detection section for why).
    ///
    /// Async for parity with the macOS branch's signature, not because
    /// `fuser::spawn_mount2` itself is async (it isn't — it performs a
    /// blocking `mount(2)`/`fusermount3` call synchronously before
    /// returning). Called directly rather than via `spawn_blocking`,
    /// mirroring the SAME accepted precedent the macOS branch already sets
    /// with its own blocking `Command::output()` call inside `async fn
    /// mount` — a one-off setup call, not a hot path.
    ///
    /// `tokio::runtime::Handle::current()` captures the CALLING runtime,
    /// which `VfsFuse` then `block_on`s every facade call against from
    /// fuser's dedicated request-dispatch thread for the rest of the
    /// mount's life — see `fuse.rs`'s module doc for why this is sound and
    /// what it requires (the runtime must keep running for the mount's
    /// whole lifetime; every caller in this codebase already runs inside
    /// one long-lived tokio runtime, same assumption the macOS branch
    /// already documents).
    pub async fn mount(vfs: Arc<Vfs>, mountpoint: &Path, volume_name: &str) -> Result<MountedVfs> {
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
        Ok(MountedVfs { mountpoint: mountpoint.to_path_buf(), session: Some(session) })
    }

    /// An active mount produced by [`mount`]. Dropping it performs a
    /// best-effort unmount that cannot panic (fuser's own `Mount::Drop`
    /// contains no `.unwrap()`/`panic!` anywhere in its body) — see the
    /// module doc for the full unmount story.
    pub struct MountedVfs {
        mountpoint: PathBuf,
        session: Option<fuser::BackgroundSession>,
    }

    impl MountedVfs {
        /// Unmounts cleanly. See the module doc for why dropping `session`
        /// IS the actual unmount (fuser's own escalation), and why this
        /// does not call `BackgroundSession::join()`.
        pub fn unmount(mut self) -> Result<()> {
            if let Some(session) = self.session.take() {
                drop(session);
            }
            if is_our_fuse_mount(&self.mountpoint)? {
                bail!(
                    "failed to unmount {} — a fuse mount is still present after dropping the \
                     session",
                    self.mountpoint.display()
                );
            }
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
        /// does liveness probing). See `tests/mounted_linux.rs`'s
        /// crash-recovery test's own doc for the same point made from the
        /// test's side.
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
    /// function of the same name — see the module doc's "Stale-mount
    /// detection" section for the detection strategy and its known
    /// limitation.
    pub fn cleanup_stale_mount(mountpoint: &Path) -> Result<bool> {
        if !is_our_fuse_mount(mountpoint)? {
            return Ok(false);
        }
        escalate_unmount(mountpoint)?;
        Ok(true)
    }

    fn is_our_fuse_mount(mountpoint: &Path) -> Result<bool> {
        let canonical = resolve_mountpoint_for_comparison(mountpoint);
        let canonical = canonical.to_string_lossy();

        let contents = std::fs::read_to_string("/proc/mounts").context("reading /proc/mounts")?;
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
    fn escalate_unmount(mountpoint: &Path) -> Result<()> {
        let path = mountpoint.as_os_str();
        if run_ok("fusermount3", &[OsStr::new("-u"), path]) {
            return Ok(());
        }
        if run_ok("fusermount", &[OsStr::new("-u"), path]) {
            return Ok(());
        }
        if run_ok("umount", &[OsStr::new("-f"), path]) {
            return Ok(());
        }
        if run_ok("umount", &[OsStr::new("-l"), path]) {
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
}
