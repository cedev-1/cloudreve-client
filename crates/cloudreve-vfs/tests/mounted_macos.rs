//! Mounted end-to-end tests (phase 3, Task 3): a REAL `mount_nfs` mount of
//! the wiremock-backed `Vfs`, driven exclusively through `std::fs` — the
//! same API Finder/Explorer would use. These are the only tests in this
//! crate that touch the OS mount table; every path (including a panic)
//! cleans up after itself via `MountpointGuard` below (HYGIENE requirement:
//! never leave a stale mount on this machine).
#![cfg(target_os = "macos")]

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
/// call) still can't leave this machine with a stray mount behind.
struct MountpointGuard(PathBuf);

impl Drop for MountpointGuard {
    fn drop(&mut self) {
        let path = self.0.to_string_lossy().to_string();
        let _ = std::process::Command::new("/sbin/umount").arg(&path).output();
        let _ = std::process::Command::new("/sbin/umount").args(["-f", &path]).output();
        let _ = std::process::Command::new("/usr/sbin/diskutil")
            .args(["unmount", "force", &path])
            .output();
    }
}

/// Runs a blocking `std::fs` closure off the async executor with a bounded
/// deadline, so a bug in mount/cleanup that leaves the OS call hanging fails
/// the test instead of hanging the whole suite (required for the crash-
/// recovery test, cheap insurance everywhere else).
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

/// Counts how many entries in `/sbin/mount`'s listing name `path` as their
/// local mountpoint — used by the pre-clean test to prove the stale
/// leftover was actually detached before the fresh mount attached, rather
/// than macOS silently STACKING the new mount on top of the old, still-
/// registered one (which would make a plain `std::fs::read`/`unmount()`
/// success alone a false-positive proof of pre-clean: mount stacking means
/// the read could come from the NEW top mount while the dead OLD one is
/// still there underneath, invisible to every other assertion this file
/// otherwise makes).
fn mount_table_entry_count(path: &std::path::Path) -> usize {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let canonical = canonical.to_string_lossy();
    let output = std::process::Command::new("/sbin/mount").output().expect("run /sbin/mount");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| {
            line.rsplit_once(" on ")
                .and_then(|(_, rest)| rest.rsplit_once(" ("))
                .is_some_and(|(p, _)| p == canonical)
        })
        .count()
}

async fn new_test_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    Arc::new(vfs)
}

/// The whole feature, through the OS: Finder-equivalent std::fs calls
/// against a real mount_nfs mount of an in-process server.
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

    vfs.wait_for_writeback_idle().await;

    assert_eq!(
        env.uploaded_content("new.txt").as_deref(),
        Some(&b"created through the mount"[..]),
        "the mock server never received new.txt's content"
    );

    mounted.unmount().await.expect("unmount should succeed");

    // Unmounted: the directory is a plain empty local dir again.
    let dir_for_check = mountpoint.clone();
    let after_unmount: Vec<_> =
        blocking_with_timeout(move || std::fs::read_dir(&dir_for_check).unwrap().collect::<Vec<_>>())
            .await;
    assert!(after_unmount.is_empty(), "unmounted dir should be empty again, saw {after_unmount:?}");
}

/// Crash recovery: killing the in-process server so it is GENUINELY dead —
/// not just its accept loop — leaves the kernel-level NFS mount registered
/// but pointing at a dead port, exactly what a crashed app leaves behind.
/// `cleanup_stale_mount` must detect and force-unmount it so the SAME
/// directory can be mounted again.
///
/// Two things this test deliberately goes out of its way to get right, both
/// fallout from the phase-3 review's Important findings:
///
/// - **Genuine deadness (Important-2).** `nfs3_server` spawns each accepted
///   connection's RPC loop as its own DETACHED tokio task (`tcp.rs`'s
///   `handle_forever`) — aborting only the accept-loop `JoinHandle`
///   (`abort_server_for_tests`'s literal effect) leaves that per-connection
///   task alive, still answering the kernel's already-established TCP
///   connection. The review confirmed this empirically: a "stale" mount
///   produced that way kept serving reads. To make the server ACTUALLY
///   dead, this test mounts on its own dedicated `tokio::runtime::Runtime`
///   and `shutdown_background()`s that runtime — which drops every task on
///   it, accept loop and detached connection handlers alike,
///   unconditionally. `abort_server_for_tests` is still called afterward,
///   but purely for its OTHER effect (see its doc): flipping `MountedVfs`'s
///   internal sentinel so `Drop` doesn't ALSO try an OS-level unmount of a
///   mount this test needs to survive until `cleanup_stale_mount` runs —
///   by the time it's called, the actual killing already happened via the
///   runtime shutdown, so the task-abort itself is a harmless no-op.
/// - **A genuinely symlinked ancestor (Important-1).** `cleanup_stale_
///   mount`'s detection must resolve a mountpoint path through a symlinked
///   ancestor directory (macOS's real `/tmp` -> `/private/tmp`, `/var` ->
///   `/private/var`) WITHOUT ever stat-ing the mountpoint itself — that
///   stat is exactly what can stall against a genuinely dead server (see
///   `mount.rs`'s `resolve_mountpoint_for_comparison`). Rather than rely on
///   this machine's own `TMPDIR` happening to traverse a symlink, this test
///   builds one explicitly: a real directory, a symlink pointing at it, and
///   the mountpoint reached only THROUGH that symlink — so it proves the
///   fix regardless of any particular machine's temp-dir layout.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_mount_from_a_crashed_server_is_cleaned_and_remountable() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;

    let vfs = new_test_vfs(&env).await;

    // mountpoint = <base>/symlinked-ancestor/mnt, where `symlinked-ancestor`
    // is a real symlink to `<base>/real-target` — deliberately mirroring the
    // `/tmp` -> `/private/tmp` shape (mountpoint's PARENT is the symlink,
    // not the mountpoint itself) rather than relying on this machine's own
    // temp-dir layout to happen to go through one.
    let base = tempfile::tempdir().unwrap();
    let real_target = base.path().join("real-target");
    std::fs::create_dir(&real_target).unwrap();
    let symlinked_ancestor = base.path().join("symlinked-ancestor");
    std::os::unix::fs::symlink(&real_target, &symlinked_ancestor)
        .expect("create the symlinked ancestor directory");
    let mountpoint = symlinked_ancestor.join("mnt");
    std::fs::create_dir(&mountpoint).expect("create the mountpoint through the symlink");

    let _guard = MountpointGuard(mountpoint.clone());

    // Mount on a DEDICATED runtime so it — and every task it spawns,
    // including nfs3_server's detached per-connection handlers — can be
    // torn down completely and unconditionally, not just have its accept
    // loop aborted. See the test's own doc above.
    let dedicated = tokio::runtime::Runtime::new().expect("build a dedicated runtime");
    let vfs_for_dead_server = vfs.clone();
    let mountpoint_for_dead_server = mountpoint.clone();
    let cache_dir_for_dead_server = env.cache_dir().to_path_buf();
    let mounted = dedicated
        .spawn(async move {
            mount::mount(
                vfs_for_dead_server,
                &mountpoint_for_dead_server,
                "CloudreveTest",
                &cache_dir_for_dead_server,
            )
            .await
        })
        .await
        .expect("mount task panicked")
        .expect("mount should succeed");

    // Simulate a crash: the whole runtime the server ran on is torn down at
    // once — accept loop AND every detached per-connection task with it.
    // `abort_server_for_tests` afterward only suppresses `MountedVfs::
    // Drop`'s own OS-level unmount attempt (see its doc); the actual
    // killing already happened above.
    dedicated.shutdown_background();
    mounted.abort_server_for_tests();

    let cleaned = tokio::time::timeout(
        Duration::from_secs(15),
        mount::cleanup_stale_mount(&mountpoint, env.cache_dir()),
    )
    .await
    .expect("cleanup_stale_mount timed out")
    .expect("cleanup_stale_mount should not error");
    assert!(
        cleaned,
        "a mount left behind by a genuinely dead in-process server must be detected as stale"
    );

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

/// Phase 4 (this task), Step 1(a): `mount()` now pre-cleans a deliberately
/// stale leftover at the SAME mountpoint before attempting its own attach —
/// D5's original intent, previously only reachable by a caller remembering
/// to call `cleanup_stale_mount` by hand first. Constructs a genuinely dead
/// leftover mount exactly like the crash-recovery test above (dedicated
/// runtime, `shutdown_background()`), but this time calls `mount::mount`
/// DIRECTLY on the same mountpoint with NO explicit `cleanup_stale_mount`
/// call in between — the pre-clean must happen internally for this to
/// succeed at all.
#[tokio::test(flavor = "multi_thread")]
async fn mounting_over_a_stale_leftover_mountpoint_succeeds_via_pre_clean() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    // Leave a genuinely dead mount behind, exactly like the crash-recovery
    // test: a dedicated runtime, killed outright.
    let dedicated = tokio::runtime::Runtime::new().expect("build a dedicated runtime");
    let vfs_for_dead_server = vfs.clone();
    let mountpoint_for_dead_server = mountpoint.clone();
    let cache_dir_for_dead_server = env.cache_dir().to_path_buf();
    let stale = dedicated
        .spawn(async move {
            mount::mount(
                vfs_for_dead_server,
                &mountpoint_for_dead_server,
                "CloudreveTest",
                &cache_dir_for_dead_server,
            )
            .await
        })
        .await
        .expect("mount task panicked")
        .expect("mount should succeed");
    dedicated.shutdown_background();
    stale.abort_server_for_tests();

    // No explicit `cleanup_stale_mount` call here — `mount()` must pre-clean
    // this leftover internally before attaching its own fresh mount.
    let mounted = tokio::time::timeout(
        Duration::from_secs(15),
        mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir()),
    )
    .await
    .expect("mount() timed out")
    .expect("mount() must pre-clean the stale leftover and succeed, not refuse or hang");

    // The decisive check: exactly ONE nfs entry at this path, not two. If
    // `mount()` skipped pre-clean, macOS would STACK the fresh mount on top
    // of the still-registered dead one instead of refusing outright — a
    // bare "mount() succeeded and reads work" assertion alone cannot tell
    // that apart from a genuine pre-clean, since reads would come from the
    // new top mount either way.
    assert_eq!(
        mount_table_entry_count(&mountpoint),
        1,
        "the stale leftover must be detached before the fresh mount attaches, not stacked \
         underneath it"
    );

    let read_path = mountpoint.join("hello.txt");
    let content = blocking_with_timeout(move || std::fs::read(&read_path).unwrap()).await;
    assert_eq!(content, b"hello world", "the fresh mount must actually be functional after pre-clean");

    mounted.unmount().await.expect("unmount should succeed");

    assert_eq!(
        mount_table_entry_count(&mountpoint),
        0,
        "after the fresh mount's own clean unmount, nothing must remain registered at this path \
         — a stacked leftover would still show one entry here"
    );
}

