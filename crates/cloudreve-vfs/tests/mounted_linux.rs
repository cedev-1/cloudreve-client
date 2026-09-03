//! Mounted end-to-end tests (phase 3, Task 4): a REAL `fuser` FUSE mount of
//! the wiremock-backed `Vfs`, driven exclusively through `std::fs` — the
//! same API a file manager would use. These are the only tests in this
//! crate that touch the OS mount table on Linux; every path (including a
//! panic) cleans up after itself via `MountpointGuard` below (HYGIENE
//! requirement: never leave a stale mount on the runner).
//!
//! **UNVERIFIED**: this whole file is `#[cfg(target_os = "linux")]`-gated
//! and has never run — it was written by reading `fuser`'s vendored source
//! and mirroring `tests/mounted_macos.rs`'s already-passing patterns, not
//! by watching it pass. It is exercised for the first time by Task 5's
//! Linux CI job. See `crate::mount`'s `linux_impl` module doc and
//! `crate::fuse`'s module doc for what every claim below is based on.
#![cfg(target_os = "linux")]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::mount;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, VfsTestEnv};

/// Best-effort force-unmount of `path` on drop, regardless of whether
/// anything is actually mounted there — every escalation step is allowed to
/// fail silently (harmless if nothing's mounted). Constructed BEFORE the
/// first `mount::mount` call in every test and held until the very end, so
/// a panicking assertion mid-test (which skips any explicit `unmount()`
/// call) still can't leave the runner with a stray mount behind. Mirrors
/// `mounted_macos.rs`'s `MountpointGuard`, using the Linux escalation chain
/// instead (see `mount.rs`'s `linux_impl::escalate_unmount`).
struct MountpointGuard(PathBuf);

impl Drop for MountpointGuard {
    fn drop(&mut self) {
        let path = self.0.to_string_lossy().to_string();
        let _ = std::process::Command::new("fusermount3").args(["-u", &path]).output();
        let _ = std::process::Command::new("fusermount").args(["-u", &path]).output();
        let _ = std::process::Command::new("umount").args(["-f", &path]).output();
        let _ = std::process::Command::new("umount").args(["-l", &path]).output();
    }
}

/// Runs a blocking `std::fs` closure off the async executor with a bounded
/// deadline, so a bug in mount/cleanup that leaves the OS call hanging fails
/// the test instead of hanging the whole suite. Identical to
/// `mounted_macos.rs`'s helper of the same name (duplicated per test file,
/// matching that file's own convention of not sharing it via `common`).
async fn blocking_with_timeout<T, F>(f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::time::timeout(Duration::from_secs(15), tokio::task::spawn_blocking(f))
        .await
        .expect("blocking std::fs call timed out")
        .expect("blocking task panicked")
}

async fn new_test_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    Arc::new(vfs)
}

/// Polls until `env.uploaded_content(name)` matches `expected`, calling
/// `vfs.wait_for_writeback_idle()` before each check, or panics past a
/// bounded deadline. Needed specifically because of a FUSE quirk this
/// crate's NFS frontend never has to deal with (CI fix round 1 diagnosis):
/// `release()` — the call that actually reaches `Vfs::close()` and arms the
/// write-back debounce — is fire-and-forget from the KERNEL's perspective
/// on `close(2)`. `fuser::Filesystem::release`'s own doc says so plainly:
/// "The filesystem may reply with an error, but error values are not
/// returned to close()... which triggered the release" — only possible
/// because the VFS's underlying `f_op->release` hook has a `void` return
/// type, unlike `f_op->flush` (fuser's `flush`), which IS synchronous with
/// `close(2)` and IS what can propagate a write error back to it. So
/// `std::fs::write(...)` returning successfully proves CREATE, WRITE, and
/// FLUSH all completed — it proves NOTHING about whether RELEASE (and
/// therefore the debounce arm this whole assertion depends on) has even
/// been dispatched by the kernel yet, let alone processed by `VfsFuse`'s
/// one dedicated dispatch thread. Contrast `nfs.rs`, whose own `write` RPC
/// handler calls `Vfs::close` INLINE before ever replying (see its module
/// doc) — no such gap exists there, which is why `mounted_macos.rs`'s
/// equivalent test needs no analogous helper.
async fn wait_for_upload(vfs: &Vfs, env: &VfsTestEnv, name: &str, expected: &[u8]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        vfs.wait_for_writeback_idle().await;
        if env.uploaded_content(name).as_deref() == Some(expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "upload of {name} never landed within the timeout — last seen: {:?}",
            env.uploaded_content(name)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The whole feature, through the OS: file-manager-equivalent std::fs calls
/// against a real FUSE mount of an in-process `Vfs`.
#[tokio::test(flavor = "multi_thread")]
async fn a_mounted_drive_lists_reads_and_writes_through_std_fs() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;
    env.expect_uploads().await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    // List: the mocked remote file is visible through the real mount.
    let dir_for_list = mountpoint.clone();
    let names: Vec<String> = blocking_with_timeout(move || {
        std::fs::read_dir(&dir_for_list)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect()
    })
    .await;
    assert!(names.contains(&"hello.txt".to_string()), "hello.txt missing from {names:?}");

    // On-demand read: the exact bytes come back through the mount.
    let read_path = mountpoint.join("hello.txt");
    let content = blocking_with_timeout(move || std::fs::read(&read_path).unwrap()).await;
    assert_eq!(content, b"hello world");

    // Write-through: a brand new file created via std::fs reaches the mock
    // server's upload endpoint after the debounce.
    let write_path = mountpoint.join("new.txt");
    blocking_with_timeout(move || std::fs::write(&write_path, b"created through the mount").unwrap())
        .await;

    // See `wait_for_upload`'s doc: `std::fs::write` returning does NOT mean
    // `release()` (and therefore `Vfs::close`'s debounce arm) has run yet —
    // that's fire-and-forget from the kernel's perspective on `close(2)`.
    wait_for_upload(&vfs, &env, "new.txt", b"created through the mount").await;

    mounted.unmount().await.expect("unmount should succeed");

    // Unmounted: the directory is a plain empty local dir again.
    let dir_for_check = mountpoint.clone();
    let after_unmount: Vec<_> =
        blocking_with_timeout(move || std::fs::read_dir(&dir_for_check).unwrap().collect::<Vec<_>>())
            .await;
    assert!(after_unmount.is_empty(), "unmounted dir should be empty again, saw {after_unmount:?}");
}

/// Crash recovery: `abort_server_for_tests` abandons the session without
/// unmounting it (see its own doc — a `std::mem::forget`, not a kill),
/// leaving the mountpoint attached but nothing left that will ever run its
/// own cleanup. `cleanup_stale_mount` must detect and force-unmount it so
/// the SAME directory can be mounted again.
///
/// Deliberately weaker than `mounted_macos.rs`'s crash test in one respect,
/// disclosed rather than hidden: that test needs a whole dedicated-runtime-
/// shutdown dance to make its server GENUINELY unreachable, because
/// `nfs3_server` spawns a detached background task per TCP connection that
/// keeps answering after the accept loop alone is killed. FUSE has no such
/// fan-out — this mount is served by exactly ONE dedicated OS thread (see
/// `fuse.rs`'s module doc), so leaking the `BackgroundSession` here already
/// abandons the whole thing; there is no second background task to
/// separately hunt down. The mount left behind by `abort_server_for_tests`
/// is therefore a LIVE, still-answering server nobody unmounted — not a
/// genuinely dead one — but that difference doesn't matter for what this
/// test is actually proving: `cleanup_stale_mount` never probes whether
/// anything is alive behind a mount (see its own doc, both platforms);
/// it force-detects and force-unmounts by path alone, which works
/// identically against a live-but-abandoned mount or a truly dead one.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_mount_from_a_crashed_server_is_cleaned_and_remountable() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    // Simulate a crash: abandon the session without ever unmounting it —
    // see this test's own doc for why this is a sufficient (if weaker than
    // macOS's) stand-in for a genuinely dead server.
    mounted.abort_server_for_tests();

    let cleaned = tokio::time::timeout(
        Duration::from_secs(15),
        mount::cleanup_stale_mount(&mountpoint, env.cache_dir()),
    )
    .await
    .expect("cleanup_stale_mount timed out")
    .expect("cleanup_stale_mount should not error");
    assert!(cleaned, "a mount left behind by an abandoned session must be detected as stale");

    // Mounting again on the SAME directory must succeed, and a read through
    // the fresh mount must actually work.
    let mounted2 = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("remount after cleanup should succeed");

    let read_path = mountpoint.join("hello.txt");
    let content = blocking_with_timeout(move || std::fs::read(&read_path).unwrap()).await;
    assert_eq!(content, b"hello world");

    mounted2.unmount().await.expect("unmount should succeed");
}

/// `cleanup_stale_mount` on a directory that was never mounted at all is a
/// harmless no-op — it must never report cleaning something it didn't.
#[tokio::test(flavor = "multi_thread")]
async fn cleanup_on_a_plain_directory_is_a_noop() {
    let env = VfsTestEnv::new().await;
    let mp = tempfile::tempdir().unwrap();
    let cleaned = mount::cleanup_stale_mount(mp.path(), env.cache_dir()).await.unwrap();
    assert!(!cleaned, "an ordinary, never-mounted directory must never be reported as cleaned");
}

/// Phase 4 (this task), Step 1(a) counterpart: `mount()` now pre-cleans a
/// deliberately stale leftover at the SAME mountpoint before attempting its
/// own attach. Mirrors `mounted_macos.rs`'s test of the same intent.
///
/// The Linux two-instance marker is a PID, not a port (see `mount.rs`'s
/// `linux_impl` module doc — FUSE has no network endpoint to probe), so
/// this test needs a marker pointing at a PROVABLY DEAD pid to make
/// `cleanup_stale_mount` actually treat the leftover as stale — this
/// process's own (very much alive) pid, which `mount()` records by
/// default, would not do. Spawns and waits out a trivial child process
/// (`/bin/true`) purely to obtain a pid guaranteed dead the instant
/// `wait()` returns, then overwrites the marker `mount()` wrote with it —
/// the same "recorded owner is provably gone" state a real crash leaves
/// behind, without needing this test's own process to die.
#[tokio::test(flavor = "multi_thread")]
async fn mounting_over_a_stale_leftover_mountpoint_succeeds_via_pre_clean() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let stale = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");
    stale.abort_server_for_tests();

    // Overwrite the marker `mount()` just wrote (this process's own, live
    // pid) with one that is definitively dead — see the test's own doc.
    let dead_pid = blocking_with_timeout(|| {
        let mut child = std::process::Command::new("/bin/true").spawn().expect("spawn /bin/true");
        let pid = child.id();
        child.wait().expect("wait for /bin/true");
        pid
    })
    .await;
    std::fs::write(env.cache_dir().join("mount.port"), dead_pid.to_string())
        .expect("overwrite the mount marker with a dead pid");

    // No explicit `cleanup_stale_mount` call here — `mount()` must pre-clean
    // this leftover internally before attaching its own fresh mount.
    let mounted = tokio::time::timeout(
        Duration::from_secs(15),
        mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir()),
    )
    .await
    .expect("mount() timed out")
    .expect("mount() must pre-clean the stale leftover and succeed, not refuse or hang");

    let read_path = mountpoint.join("hello.txt");
    let content = blocking_with_timeout(move || std::fs::read(&read_path).unwrap()).await;
    assert_eq!(content, b"hello world", "the fresh mount must actually be functional after pre-clean");

    mounted.unmount().await.expect("unmount should succeed");
}

/// The phase-2 rename-with-open-handle debt (D3, `RenameBusyError`) reaching
/// the OS as a REAL `EBUSY` through a real FUSE mount — this is the
/// end-to-end proof `nfs.rs`'s own module doc predicted would only become
/// possible once this task landed, because FUSE handles persist across
/// SEPARATE calls (unlike NFSv3, which has no OPEN/CLOSE at all): opening
/// `hello.txt` and keeping the `File` alive genuinely leaves a handle
/// registered in `Vfs::open_files` for as long as this test holds it, no
/// scheduler race required (see `fuse.rs`'s module doc for why FUSE's
/// single-threaded, non-concurrent request dispatch makes this
/// deterministic rather than timing-dependent).
#[tokio::test(flavor = "multi_thread")]
async fn renaming_an_open_file_through_the_mount_fails_with_ebusy() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;
    // CI fix round 1: the FIRST rename (while busy) never reaches the API
    // at all — `Vfs::rename`'s EBUSY guard returns before any HTTP call —
    // but the SECOND, now-unblocked rename is a REAL `rename_file` API
    // call (hello.txt is a genuine remote file, not a draft) and needs a
    // mock to succeed against, same as `namespace_ops.rs`'s rename tests.
    // Without this, that second rename failed with a generic (unmocked)
    // error, classified as `Io` -> `EIO` — which is exactly what CI saw,
    // and specifically NOT `EBUSY`, ruling out a release()-timing race as
    // the cause here (see `wait_for_upload`'s doc for where that race
    // DOES matter, in the other test).
    env.expect_namespace_ops().await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted =
        mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
            .await
            .expect("mount should succeed");

    let open_path = mountpoint.join("hello.txt");
    // Opened and kept alive for the rest of this test: the fd genuinely
    // stays open in the kernel, and `Vfs::open_files` genuinely keeps the
    // corresponding facade handle registered — see the test's own doc.
    let open_file = blocking_with_timeout(move || std::fs::File::open(&open_path).unwrap()).await;

    let from = mountpoint.join("hello.txt");
    let to = mountpoint.join("renamed.txt");
    let rename_err = blocking_with_timeout(move || std::fs::rename(&from, &to).unwrap_err()).await;
    assert_eq!(
        rename_err.raw_os_error(),
        Some(libc::EBUSY),
        "renaming a file with an open handle through the mount must surface EBUSY, got {rename_err:?}"
    );

    // Close the handle, then the same rename must succeed — the guard is a
    // refusal, not a permanent lock. Polled rather than a single immediate
    // retry: RELEASE (see `wait_for_upload`'s doc on the other test) is
    // fire-and-forget from the kernel's perspective on `close(2)`, so
    // `Vfs::open_files` may not have dropped this handle the very instant
    // `drop(open_file)` returns — a bare retry could still see `EBUSY` and
    // wrongly look like a regression. Anything OTHER than `EBUSY` fails
    // immediately: this loop tolerates exactly the one known race, nothing
    // else. (Not proven necessary by CI fix round 1's diagnosis — that
    // failure was the missing `expect_namespace_ops` mock below, which
    // surfaced as `EIO`, not `EBUSY` — added defensively since the same
    // release()-timing gap applies here in principle.)
    blocking_with_timeout(move || drop(open_file)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let from = mountpoint.join("hello.txt");
        let to = mountpoint.join("renamed.txt");
        match blocking_with_timeout(move || std::fs::rename(&from, &to)).await {
            Ok(()) => break,
            Err(err) if err.raw_os_error() == Some(libc::EBUSY) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "rename stayed EBUSY long after the handle was closed"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("rename after closing the handle should succeed, got {err:?}"),
        }
    }

    let renamed_path = mountpoint.join("renamed.txt");
    let names: Vec<String> = blocking_with_timeout(move || {
        std::fs::read_dir(renamed_path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect()
    })
    .await;
    assert!(names.contains(&"renamed.txt".to_string()), "renamed.txt missing from {names:?}");
    assert!(!names.contains(&"hello.txt".to_string()), "hello.txt should be gone, saw {names:?}");

    mounted.unmount().await.expect("unmount should succeed");
}
