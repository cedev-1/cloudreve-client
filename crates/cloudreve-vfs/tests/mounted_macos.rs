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


/// Advisory locks through the mount must WORK, not fail with ENOTSUP:
/// SQLite, Office and Python's `filelock` all take an `flock`/`fcntl` lock
/// before touching a file and give up when the syscall itself is refused.
/// The in-process server has no NLM, so the mount must handle locks on the
/// client side (`locallocks`) rather than rejecting them (`nolocks`).
#[tokio::test(flavor = "multi_thread")]
async fn flock_and_fcntl_locks_succeed_through_the_mount() {
    use std::os::fd::AsRawFd;

    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("db.sqlite", 11, "e1")]).await;
    env.serve_file_content("db.sqlite", b"hello world").await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let path = mountpoint.join("db.sqlite");
    let (flock_res, fcntl_res) = blocking_with_timeout(move || {
        let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let fd = file.as_raw_fd();

        let flock_res = if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            unsafe { libc::flock(fd, libc::LOCK_UN) };
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        };

        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_RDLCK as libc::c_short;
        lock.l_whence = libc::SEEK_SET as libc::c_short;
        let fcntl_res = if unsafe { libc::fcntl(fd, libc::F_SETLK, &lock) } == 0 {
            lock.l_type = libc::F_UNLCK as libc::c_short;
            unsafe { libc::fcntl(fd, libc::F_SETLK, &lock) };
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        };
        (flock_res, fcntl_res)
    })
    .await;

    mounted.unmount().await.expect("unmount should succeed");

    assert!(flock_res.is_ok(), "flock through the mount failed: {flock_res:?}");
    assert!(fcntl_res.is_ok(), "fcntl F_SETLK through the mount failed: {fcntl_res:?}");
}

/// A cold read whose download takes 12s (a slow Cloudreve server) must
/// still return the right bytes through the mount, not EIO/ETIMEDOUT: the
/// `soft` mount (kept for crash recovery) gives up on an RPC left
/// unanswered for a few retransmit timeouts, so the server answers
/// `NFS3ERR_JUKEBOX` while the fetch runs and the kernel resends the READ.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_slower_than_the_soft_timeout_still_returns_the_bytes() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("slow.txt", 11, "e1")]).await;
    env.serve_file_content("slow.txt", b"hello world").await;
    env.slow_down_downloads("slow.txt", Duration::from_secs(12));

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let read_path = mountpoint.join("slow.txt");
    let content = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::task::spawn_blocking(move || std::fs::read(&read_path)),
    )
    .await
    .expect("the read through the mount hung")
    .expect("blocking task panicked");

    mounted.unmount().await.expect("unmount should succeed");

    assert_eq!(content.expect("a slow read must not fail"), b"hello world");
}

/// MEASUREMENT (item 1 of the NFS hardening wave): how long can the macOS
/// client wait for an unanswered RPC before its soft timeout hands the app
/// an error? JUKEBOX is disabled (threshold 10 minutes) so the server
/// really holds each call for the whole delay. Three probes, each preceded
/// by fast warm-up traffic that trains the client's RTT estimator the way
/// an ordinary warm mount would have:
/// - `read`: `std::fs::read`, which goes through the buffer cache — its
///   READ is issued as an async readahead buffer (`nfs_bio.c`).
/// - `mmap`: `/usr/bin/cmp` against a local copy; cmp mmaps regular
///   files, so the READ comes from a synchronous pagein.
/// - `lookup`: `stat` of a file in a directory whose listing is slow, so
///   the LOOKUP itself waits on the network.
/// Results go straight to the process's stderr (bypassing libtest's
/// capture) as `SOFT-BUDGET` lines.
///
/// Ignored: it takes ~80s and asserts nothing. Its results are recorded on
/// `nfs::READ_JUKEBOX_AFTER` and `mount.rs`'s `MOUNT_OPTS`. Rerun with
/// `cargo test -p cloudreve-vfs --test mounted_macos -- --ignored
/// measure_soft_timeout_budget_for_held_rpcs`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn measure_soft_timeout_budget_for_held_rpcs() {
    use std::io::Write;
    use std::os::unix::process::ExitStatusExt;

    const DELAYS_MS: [u64; 6] = [250, 500, 1000, 2000, 4000, 8000];
    let variants: [(&str, &str); 2] = [
        ("rto", "locallocks,mutejukebox,vers=3,tcp,soft,timeo=30,retrans=2"),
        ("dumbtimer", "locallocks,mutejukebox,vers=3,tcp,soft,dumbtimer,timeo=30,retrans=2"),
    ];
    let content = b"hello world";
    let local = tempfile::tempdir().unwrap();
    let local_copy = local.path().join("copy.txt");
    std::fs::write(&local_copy, content).unwrap();

    let env = VfsTestEnv::new().await;
    let mut listing = Vec::new();
    for (label, _) in &variants {
        for d in DELAYS_MS {
            for w in 0..4 {
                let name = format!("warm-{label}-{d}-{w}.txt");
                listing.push(remote_file(&name, 11, "e1"));
                env.serve_file_content(&name, content).await;
            }
            for probe in ["read", "mmap"] {
                let name = format!("{probe}-{label}-{d}.txt");
                listing.push(remote_file(&name, 11, "e1"));
                env.serve_file_content(&name, content).await;
                env.slow_down_downloads(&name, Duration::from_millis(d));
            }
            let dir = format!("dir-{label}-{d}");
            listing.push(common::remote_dir(&dir));
            let mut inside = remote_file("x.txt", 11, "e1");
            inside["path"] = serde_json::json!(format!("{}/{dir}/x.txt", common::REMOTE_BASE));
            env.set_remote_files_at(&dir, vec![inside]).await;
            env.slow_down_listing(&dir, Duration::from_millis(d));
        }
    }
    env.set_remote_files(listing).await;
    let vfs = new_test_vfs(&env).await;

    let report = |label: &str, probe: &str, d: u64, started: std::time::Instant, outcome: String| {
        let _ = writeln!(
            std::io::stderr(),
            "SOFT-BUDGET variant={label} probe={probe} delay_ms={d} elapsed_ms={} outcome={outcome}",
            started.elapsed().as_millis()
        );
    };
    let io_outcome = |res: &std::io::Result<()>| match res {
        Ok(()) => "OK".to_string(),
        Err(e) => format!("ERR {e} (os {:?})", e.raw_os_error()),
    };

    for (label, opts) in &variants {
        let mp = tempfile::tempdir().unwrap();
        let mountpoint = mp.path().to_path_buf();
        let _guard = MountpointGuard(mountpoint.clone());
        let mounted = mount::mount_with_nfs_tuning_for_tests(
            vfs.clone(),
            &mountpoint,
            env.cache_dir(),
            Duration::from_secs(600),
            opts,
        )
        .await
        .expect("mount should succeed");

        for d in DELAYS_MS {
            let warm = |mountpoint: PathBuf| async move {
                for w in 0..4 {
                    let p = mountpoint.join(format!("warm-{label}-{d}-{w}.txt"));
                    blocking_with_timeout(move || std::fs::read(&p).unwrap()).await;
                }
            };

            warm(mountpoint.clone()).await;
            let p = mountpoint.join(format!("read-{label}-{d}.txt"));
            let started = std::time::Instant::now();
            let res = tokio::time::timeout(
                Duration::from_secs(60),
                tokio::task::spawn_blocking(move || {
                    std::fs::read(&p).and_then(|b| {
                        if b == b"hello world" {
                            Ok(())
                        } else {
                            Err(std::io::Error::other("wrong bytes"))
                        }
                    })
                }),
            )
            .await
            .expect("read hung past 60s")
            .unwrap();
            report(label, "read", d, started, io_outcome(&res));

            warm(mountpoint.clone()).await;
            let p = mountpoint.join(format!("mmap-{label}-{d}.txt"));
            let copy = local_copy.clone();
            let started = std::time::Instant::now();
            let status = tokio::time::timeout(
                Duration::from_secs(60),
                tokio::task::spawn_blocking(move || {
                    std::process::Command::new("/usr/bin/cmp").arg(&p).arg(&copy).output()
                }),
            )
            .await
            .expect("cmp hung past 60s")
            .unwrap()
            .unwrap();
            let outcome = match (status.status.code(), status.status.signal()) {
                (Some(0), _) => "OK".to_string(),
                (code, signal) => format!(
                    "ERR code={code:?} signal={signal:?} stderr={}",
                    String::from_utf8_lossy(&status.stderr).trim()
                ),
            };
            report(label, "mmap", d, started, outcome);

            warm(mountpoint.clone()).await;
            let p = mountpoint.join(format!("dir-{label}-{d}")).join("x.txt");
            let started = std::time::Instant::now();
            let res = tokio::time::timeout(
                Duration::from_secs(60),
                tokio::task::spawn_blocking(move || std::fs::metadata(&p).map(|_| ())),
            )
            .await
            .expect("stat hung past 60s")
            .unwrap();
            report(label, "lookup", d, started, io_outcome(&res));
        }
        mounted.unmount().await.expect("unmount should succeed");
    }
}

/// A cold block that takes about a second to download (an ordinary remote
/// Cloudreve server) must be answered directly, not deferred with JUKEBOX:
/// every JUKEBOX costs the app a 4s client-side sleep before the resend.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_second_read_is_answered_without_the_jukebox_sleep() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("remote.txt", 11, "e1")]).await;
    env.serve_file_content("remote.txt", b"hello world").await;
    env.slow_down_downloads("remote.txt", Duration::from_millis(1000));

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let read_path = mountpoint.join("remote.txt");
    let started = std::time::Instant::now();
    let content = blocking_with_timeout(move || std::fs::read(&read_path)).await;
    let elapsed = started.elapsed();

    mounted.unmount().await.expect("unmount should succeed");

    assert_eq!(content.expect("the read must succeed"), b"hello world");
    assert!(
        elapsed < Duration::from_millis(3000),
        "a 1s download must not pay the 4s JUKEBOX sleep, the read took {elapsed:?}"
    );
}

/// A LOOKUP waiting on a slow directory listing (a big folder on a remote
/// server) must not fail with ETIMEDOUT: without `dumbtimer` the client
/// times LOOKUPs with an RTO learned from millisecond loopback replies and
/// gave up after under 2s on CI. The warm-up trains that estimator the way
/// any mount in use would have.
#[tokio::test(flavor = "multi_thread")]
async fn a_lookup_in_a_slowly_listed_folder_does_not_time_out() {
    let env = VfsTestEnv::new().await;
    let mut listing = vec![common::remote_dir("big")];
    for i in 0..30 {
        listing.push(remote_file(&format!("warm-{i}.txt"), 11, "e1"));
    }
    env.set_remote_files(listing).await;
    let mut inside = remote_file("x.txt", 11, "e1");
    inside["path"] = serde_json::json!(format!("{}/big/x.txt", common::REMOTE_BASE));
    env.set_remote_files_at("big", vec![inside]).await;
    env.slow_down_listing("big", Duration::from_millis(4000));

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());

    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let warm_dir = mountpoint.clone();
    blocking_with_timeout(move || {
        for i in 0..30 {
            std::fs::metadata(warm_dir.join(format!("warm-{i}.txt"))).unwrap();
        }
    })
    .await;

    let p = mountpoint.join("big").join("x.txt");
    let res = blocking_with_timeout(move || std::fs::metadata(&p).map(|m| m.len())).await;

    mounted.unmount().await.expect("unmount should succeed");

    assert_eq!(res.expect("the lookup must wait for the listing, not time out"), 11);
}

/// Runs `cmd args...` to completion off the executor (bounded like every
/// other blocking call here), returning whether it exited 0 and its stderr.
async fn run_tool(cmd: &'static str, args: Vec<std::ffi::OsString>) -> (bool, String) {
    blocking_with_timeout(move || {
        let out = std::process::Command::new(cmd).args(&args).output().expect("spawn tool");
        (out.status.success(), String::from_utf8_lossy(&out.stderr).trim().to_string())
    })
    .await
}

/// The ordinary ways a macOS user copies a file that carries extended
/// attributes onto the mount (`cp`, `cp -p`, `ditto`), then sets and reads
/// back an xattr on a file inside it. NFSv3 has no xattrs, so each of
/// these makes the kernel write an AppleDouble `._name` sidecar. Returns
/// `(label, exit 0, stderr)` per step.
async fn macos_copy_matrix(mountpoint: &std::path::Path) -> Vec<(&'static str, bool, String)> {
    let src_dir = tempfile::tempdir().unwrap();
    let src = src_dir.path().join("tagged.txt");
    std::fs::write(&src, b"payload").unwrap();
    let (ok, err) = run_tool(
        "/usr/bin/xattr",
        vec!["-w".into(), "com.apple.metadata:test".into(), "v".into(), src.clone().into()],
    )
    .await;
    assert!(ok, "setting an xattr on the local source failed: {err}");

    let m = |name: &str| mountpoint.join(name).into_os_string();
    let mut results = Vec::new();
    let (ok, err) = run_tool("/bin/cp", vec![src.clone().into(), m("cp.txt")]).await;
    results.push(("cp", ok, err));
    let (ok, err) = run_tool("/bin/cp", vec!["-p".into(), src.clone().into(), m("cp-p.txt")]).await;
    results.push(("cp -p", ok, err));
    let (ok, err) = run_tool("/usr/bin/ditto", vec![src.clone().into(), m("ditto.txt")]).await;
    results.push(("ditto", ok, err));
    let (ok, err) = run_tool(
        "/usr/bin/xattr",
        vec!["-w".into(), "com.apple.metadata:extra".into(), "v2".into(), m("cp.txt")],
    )
    .await;
    results.push(("xattr -w", ok, err));
    let (ok, err) =
        run_tool("/usr/bin/xattr", vec!["-p".into(), "com.apple.metadata:extra".into(), m("cp.txt")]).await;
    results.push(("xattr -p", ok, err));
    results
}

/// The deciding test for the macOS junk policy (see `crate::junk`): every
/// ordinary copy path must succeed, the real files must reach the server,
/// and no `._*` sidecar (nor any `.DS_Store`) may ever be uploaded.
#[tokio::test(flavor = "multi_thread")]
async fn macos_copies_of_a_file_with_xattrs_succeed_and_never_upload_junk() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    let vfs = new_test_vfs(&env).await;
    // The mock upload endpoint does not add what it receives to the
    // listing, so an uploaded file vanishes from the mount on the next
    // re-list: hold the uploads back until the whole matrix has run.
    vfs.set_debounce_for_tests(Duration::from_secs(5));

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let results = macos_copy_matrix(&mountpoint).await;
    let touch_ds = mountpoint.join(".DS_Store");
    let ds = blocking_with_timeout(move || std::fs::write(&touch_ds, b"Bud1")).await;
    vfs.wait_for_writeback_idle().await;
    mounted.unmount().await.expect("unmount should succeed");

    for (label, ok, err) in &results {
        assert!(ok, "`{label}` through the mount failed: {err}");
    }
    assert!(ds.is_ok(), "writing a .DS_Store failed: {ds:?}");
    for name in ["cp.txt", "cp-p.txt", "ditto.txt"] {
        assert_eq!(env.uploaded_content(name).as_deref(), Some(&b"payload"[..]), "{name} not uploaded");
    }
    let junk: Vec<String> = env
        .uploaded_names()
        .into_iter()
        .filter(|n| n.starts_with("._") || n.eq_ignore_ascii_case(".DS_Store"))
        .collect();
    assert!(junk.is_empty(), "macOS metadata reached the server: {junk:?}");
}

/// `rm -rf` of a folder holding junk (a sidecar written by the kernel, a
/// Finder `.DS_Store`) must succeed: whatever the policy, the final rmdir
/// must not trip over junk nobody can see. And a `.DS_Store` that already
/// sits on the server is not shown in the mount.
#[tokio::test(flavor = "multi_thread")]
async fn rm_rf_of_a_folder_holding_junk_succeeds_and_server_junk_stays_hidden() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file(".DS_Store", 4, "e1"), remote_file("a.txt", 1, "e2")]).await;
    env.serve_file_content(".DS_Store", b"Bud1").await;
    env.serve_file_content("a.txt", b"a").await;
    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let root = mountpoint.clone();
    let names: Vec<String> = blocking_with_timeout(move || {
        std::fs::read_dir(&root).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect()
    })
    .await;

    let folder = mountpoint.join("folder");
    let f = folder.clone();
    blocking_with_timeout(move || std::fs::create_dir(&f).unwrap()).await;
    let src_dir = tempfile::tempdir().unwrap();
    let src = src_dir.path().join("x.txt");
    std::fs::write(&src, b"x").unwrap();
    let (ok, err) =
        run_tool("/usr/bin/xattr", vec!["-w".into(), "com.apple.metadata:t".into(), "v".into(), src.clone().into()])
            .await;
    assert!(ok, "{err}");
    let (cp_ok, cp_err) = run_tool("/bin/cp", vec![src.into(), folder.join("x.txt").into()]).await;
    let ds = folder.join(".DS_Store");
    let _ = blocking_with_timeout(move || std::fs::write(&ds, b"Bud1")).await;
    vfs.wait_for_writeback_idle().await;
    let (rm_ok, rm_err) = run_tool("/bin/rm", vec!["-rf".into(), folder.clone().into()]).await;
    let f = folder.clone();
    let still_there = blocking_with_timeout(move || f.exists()).await;
    mounted.unmount().await.expect("unmount should succeed");

    assert_eq!(names, vec!["a.txt".to_string()], "server-side .DS_Store must not be listed");
    assert!(cp_ok, "cp into the folder failed: {cp_err}");
    assert!(rm_ok, "rm -rf failed: {rm_err}");
    assert!(!still_there, "the folder must be gone");
    assert!(
        env.last_deleted_uris().contains(&common::uri_of("folder")),
        "the folder itself must be deleted on the server, last delete was {:?}",
        env.last_deleted_uris()
    );
}

/// MEASUREMENT (item 2 of the NFS hardening wave): the same copy matrix as
/// `macos_copies_of_a_file_with_xattrs_succeed_and_never_upload_junk`, under
/// the policy hf-mount ships instead — sidecar creation refused with EACCES
/// (`Vfs::refuse_local_only_junk_for_tests`). Logs `JUNK-POLICY` lines to
/// the process's stderr; asserts nothing.
///
/// Ignored: its result is recorded in `crate::junk`'s module doc. Rerun
/// with `cargo test -p cloudreve-vfs --test mounted_macos -- --ignored
/// measure_macos_copies_when_sidecars_are_refused`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn measure_macos_copies_when_sidecars_are_refused() {
    use std::io::Write;

    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    let vfs = new_test_vfs(&env).await;
    vfs.refuse_local_only_junk_for_tests();

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");
    let results = macos_copy_matrix(&mountpoint).await;
    mounted.unmount().await.expect("unmount should succeed");

    for (label, ok, err) in results {
        let _ = writeln!(std::io::stderr(), "JUNK-POLICY policy=refuse step={label:?} exit0={ok} stderr={err:?}");
    }
}

/// The options `/sbin/mount` lists for the mount at `path`, e.g.
/// `nfs, nodev, nosuid, nobrowse, mounted by runner`.
fn mount_table_options(path: &std::path::Path) -> Option<String> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let canonical = canonical.to_string_lossy().to_string();
    let output = std::process::Command::new("/sbin/mount").output().expect("run /sbin/mount");
    String::from_utf8_lossy(&output.stdout).lines().find_map(|line| {
        let (_, rest) = line.rsplit_once(" on ")?;
        let (p, opts) = rest.rsplit_once(" (")?;
        (p == canonical).then(|| opts.trim_end_matches(')').to_string())
    })
}

/// Spotlight must stay off the mount: indexing it would download every
/// file. The mount is `nobrowse` (no Desktop/sidebar volume — the folder
/// itself stays where the user put it) and its root holds an empty,
/// read-only `.metadata_never_index`, Apple's documented opt-out.
///
/// `mdutil -s` cannot prove the marker works: on the macos-latest runner it
/// answers "Error: unknown indexing state." for this mount (exit 0) —
/// Spotlight does not manage the volume at all. What it can show is that
/// indexing is never reported enabled; the marker's own presence and
/// read-only-ness are asserted directly.
#[tokio::test(flavor = "multi_thread")]
async fn the_mount_is_nobrowse_and_opts_out_of_spotlight() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let options = mount_table_options(&mountpoint);
    let marker = mountpoint.join(".metadata_never_index");
    let m = marker.clone();
    let meta = blocking_with_timeout(move || std::fs::metadata(&m).map(|m| m.len())).await;
    let m = marker.clone();
    let write = blocking_with_timeout(move || std::fs::write(&m, b"x")).await;
    let root = mountpoint.clone();
    let names: Vec<String> = blocking_with_timeout(move || {
        std::fs::read_dir(&root).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect()
    })
    .await;
    let (mdutil_ok, mdutil_out) = blocking_with_timeout({
        let mp = mountpoint.clone();
        move || {
            let out = std::process::Command::new("/usr/bin/mdutil").arg("-s").arg(&mp).output().unwrap();
            let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
            (out.status.success(), text)
        }
    })
    .await;
    mounted.unmount().await.expect("unmount should succeed");

    let options = options.expect("the mount must be in the mount table");
    assert!(options.split(", ").any(|o| o == "nobrowse"), "not nobrowse: {options}");
    assert_eq!(meta.expect(".metadata_never_index must exist at the root"), 0);
    assert!(write.is_err(), ".metadata_never_index must be read-only");
    assert_eq!(names, vec!["a.txt".to_string()], ".metadata_never_index must not be listed");
    assert!(
        !mdutil_out.to_lowercase().contains("indexing enabled"),
        "Spotlight must not index the mount, mdutil -s said (exit0={mdutil_ok}): {mdutil_out}"
    );
}

/// The Cloudreve logo `cloudreve_sync::sidebar` puts on the drive folder,
/// set the way the app sets it (`NSWorkspace.setIcon(_:forFile:options:)`,
/// through JXA here) on the mounted root. AppKit sets the custom-icon bit in
/// the root's `com.apple.FinderInfo` and stores the image either in an
/// `Icon\r` file's resource fork (a folder) or, the mount root being a
/// volume root, in `.VolumeIcon.icns` (the first CI run found no `Icon\r`
/// fork; the `DRIVE-ICON` line logs which). NFSv3 has no xattrs: the
/// FinderInfo lands in an AppleDouble sidecar. All of it must work (Finder
/// reads the icon back from there) and none of it may reach the server.
#[tokio::test(flavor = "multi_thread")]
async fn the_drive_folder_icon_set_on_the_mount_root_reads_back_and_is_never_uploaded() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let icns = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src-tauri/icons/icon.icns");
    let mp2 = mountpoint.clone();
    let (set_out, finder_info, fork_len, volume_icon_len, names) = blocking_with_timeout(move || {
        let script = "ObjC.import('AppKit'); function run(a) { \
            var img = $.NSImage.alloc.initWithContentsOfFile(a[1]); \
            return $.NSWorkspace.sharedWorkspace.setIconForFileOptions(img, a[0], 0); }";
        let out = std::process::Command::new("/usr/bin/osascript")
            .args(["-l", "JavaScript", "-e", script])
            .arg(&mp2)
            .arg(&icns)
            .output()
            .unwrap();
        let set_out = format!("{}{}", String::from_utf8_lossy(&out.stdout).trim(), String::from_utf8_lossy(&out.stderr).trim());
        let info = std::process::Command::new("/usr/bin/xattr")
            .args(["-px", "com.apple.FinderInfo"])
            .arg(&mp2)
            .output()
            .unwrap();
        let finder_info = String::from_utf8_lossy(&info.stdout).split_whitespace().map(str::to_string).collect::<Vec<_>>();
        let fork_len = std::process::Command::new("/usr/bin/xattr")
            .args(["-px", "com.apple.ResourceFork"])
            .arg(mp2.join("Icon\r"))
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).split_whitespace().count())
            .unwrap_or(0);
        let names: Vec<String> =
            std::fs::read_dir(&mp2).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        // The mount root is a VOLUME root: AppKit may store its icon as the
        // volume icon instead of an `Icon\r` file.
        let volume_icon_len = std::fs::metadata(mp2.join(".VolumeIcon.icns")).map(|m| m.len()).unwrap_or(0);
        (set_out, finder_info, fork_len, volume_icon_len, names)
    })
    .await;
    vfs.wait_for_writeback_idle().await;
    mounted.unmount().await.expect("unmount should succeed");

    assert_eq!(set_out, "true", "NSWorkspace refused the icon on the mount root");
    // FinderInfo bytes 8-9 are the Finder flags; 0x0400 = kHasCustomIcon.
    assert_eq!(finder_info.len(), 32, "no FinderInfo on the root: {finder_info:?}");
    let flags = u8::from_str_radix(&finder_info[8], 16).unwrap();
    assert!(flags & 0x04 != 0, "the custom-icon bit is not set: {finder_info:?}");
    eprintln!("DRIVE-ICON Icon\\r resource fork: {fork_len} bytes, .VolumeIcon.icns: {volume_icon_len} bytes");
    assert!(
        fork_len > 1000 || volume_icon_len > 1000,
        "the icon image is nowhere: Icon\\r fork {fork_len} bytes, .VolumeIcon.icns {volume_icon_len} bytes"
    );
    // The icon file stays hidden like any junk; the root's own sidecar
    // `._.` (its FinderInfo) is the one junk name listed, see
    // `cloudreve_vfs::appledouble`.
    let mut names = names;
    names.sort();
    assert_eq!(names, vec!["._.".to_string(), "a.txt".to_string()], "only the root's sidecar may be listed");
    assert!(
        env.uploaded_names().is_empty() && env.create_file_call_count() == 0,
        "the folder icon reached the server: {:?}",
        env.uploaded_names()
    );
}

/// The root's FinderInfo (the drive folder's custom-icon bit) stays readable
/// and writable once the root has been listed, which Finder does the moment
/// it shows the folder. It lives in the root's `._.` sidecar, seeded before
/// mounting the way the app does (`Vfs::put_local_only`); unlisted, the
/// client answered its lookup with ENOENT on its own after a listing: the
/// bit read as absent and `xattr -w` failed with EPERM (measured on CI,
/// see `cloudreve_vfs::appledouble`).
#[tokio::test(flavor = "multi_thread")]
async fn the_mount_root_finder_info_is_still_read_and_written_once_the_root_was_listed() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let vfs = new_test_vfs(&env).await;
    let mut finder_info = [0u8; 32];
    finder_info[8] = 0x04;
    let sidecar = cloudreve_vfs::appledouble::finder_info_sidecar(finder_info);
    vfs.put_local_only(vfs.tree().root(), cloudreve_vfs::appledouble::ROOT_SIDECAR_NAME, sidecar).await.unwrap();

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let mp2 = mountpoint.clone();
    let (listed, seeded, written, rewritten) = blocking_with_timeout(move || {
        let read = |p: &std::path::Path| {
            let out = std::process::Command::new("/usr/bin/xattr").args(["-px", "com.apple.FinderInfo"]).arg(p).output().unwrap();
            String::from_utf8_lossy(&out.stdout).split_whitespace().collect::<String>()
        };
        let listed = std::fs::read_dir(&mp2).unwrap().count();
        let seeded = read(&mp2);
        // A label (bits 1-3 of byte 9) on top of the custom-icon bit.
        let written = std::process::Command::new("/usr/bin/xattr")
            .args(["-wx", "com.apple.FinderInfo", "0000000000000000040c00000000000000000000000000000000000000000000"])
            .arg(&mp2)
            .status()
            .unwrap()
            .success();
        let _ = std::fs::read_dir(&mp2).unwrap().count();
        (listed, seeded, written, read(&mp2))
    })
    .await;
    mounted.unmount().await.expect("unmount should succeed");

    assert!(listed >= 1, "the root listing came back empty");
    assert_eq!(seeded, format!("{:0<16}04{:0<46}", "", ""), "the seeded FinderInfo is not read back after a listing");
    assert!(written, "setting the root's FinderInfo after a listing failed");
    assert_eq!(rewritten, format!("{:0<16}040c{:0<44}", "", ""), "the new FinderInfo did not stick");
    assert_eq!(env.uploaded_names(), Vec::<String>::new(), "the sidecar reached the server");
}

/// `rsize=1048576`: the client reads in 1 MiB chunks — one block of the vfs
/// cache per READ — instead of the 32 KiB default, which cost 32 READs (and
/// as many facade calls) per block.
#[tokio::test(flavor = "multi_thread")]
async fn reading_a_4_mib_file_through_the_mount_uses_1_mib_reads() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..4 * 1_048_576u32).map(|i| (i % 253) as u8).collect();
    env.set_remote_files(vec![remote_file("big.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("big.bin", &body).await;
    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mount::mount_with_read_log_for_tests(vfs.clone(), &mountpoint, env.cache_dir(), log.clone())
        .await
        .expect("mount should succeed");

    let p = mountpoint.join("big.bin");
    let content = blocking_with_timeout(move || std::fs::read(&p).unwrap()).await;
    mounted.unmount().await.expect("unmount should succeed");

    assert!(content == body, "wrong bytes read back");
    let reads = log.lock().unwrap().clone();
    assert_eq!(reads.iter().max().copied(), Some(1_048_576), "READ sizes: {reads:?}");
    assert!(reads.len() <= 8, "4 MiB should take about 4 READs, took {}: {reads:?}", reads.len());
}

/// The downloader idiom through the OS: open `dl.part`, write, rename it to
/// its final name while the fd stays open, keep writing, close. The kernel
/// keeps writing by the fileid it holds, so every byte must reach the
/// server under the final name and nothing under the old one.
#[tokio::test(flavor = "multi_thread")]
async fn writing_through_an_fd_kept_open_across_a_rename_uploads_every_byte() {
    use std::io::Write;

    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    let vfs = new_test_vfs(&env).await;
    // The mock upload endpoint does not add what it receives to the
    // listing: hold uploads back until the sequence is done.
    vfs.set_debounce_for_tests(Duration::from_secs(5));

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let first: Vec<u8> = (0..65_536u32).map(|i| (i % 241) as u8).collect();
    let second: Vec<u8> = (0..65_536u32).map(|i| (i % 239) as u8).collect();
    let (part, done) = (mountpoint.join("dl.part"), mountpoint.join("dl.bin"));
    let (a, b) = (first.clone(), second.clone());
    let res = blocking_with_timeout(move || -> std::io::Result<()> {
        let mut f = std::fs::File::create(&part)?;
        f.write_all(&a)?;
        std::fs::rename(&part, &done)?;
        f.write_all(&b)?;
        f.sync_all()?;
        drop(f);
        Ok(())
    })
    .await;
    vfs.wait_for_writeback_idle().await;
    mounted.unmount().await.expect("unmount should succeed");

    res.expect("open/write/rename/write/close through the mount");
    let mut expected = first;
    expected.extend_from_slice(&second);
    let uploaded = env.uploaded_content("dl.bin");
    assert_eq!(uploaded.as_ref().map(Vec::len), Some(expected.len()), "dl.bin uploaded size");
    assert!(uploaded.as_deref() == Some(&expected[..]), "dl.bin must hold every byte written");
    assert_eq!(env.uploaded_content("dl.part"), None, "nothing may be uploaded under the old name");
}

// ---------------------------------------------------------------------
// Remote changes reaching the mount: what another device or the web UI
// did must show through `stat`/`read_dir` within a few seconds, with no
// action from the user. The change goes through `Vfs::remote_changed`,
// the same entry point the sync crate's SSE handler calls.
// ---------------------------------------------------------------------

/// Upper bound on how long a remote change may take to show through the
/// mount, as the product promises it.
const REMOTE_CHANGE_VISIBLE_WITHIN: Duration = Duration::from_secs(3);

/// How long a poll keeps watching, so a red run reports how far off it is
/// instead of only "not within 3 s".
const REMOTE_CHANGE_POLL_FOR: Duration = Duration::from_secs(10);

/// A file of `{REMOTE_BASE}/{dir}` as the list API returns it.
fn file_in(dir: &str, name: &str, size: i64, etag: &str, updated_at: &str) -> serde_json::Value {
    serde_json::json!({
        "type": 0,
        "id": format!("file-{dir}-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": updated_at,
        "size": size,
        "path": format!("{}/{dir}/{name}", common::REMOTE_BASE),
        "primary_entity": etag,
    })
}

/// Polls `check` (a blocking `std::fs` probe) every 50 ms until it holds or
/// [`REMOTE_CHANGE_POLL_FOR`] runs out. Returns how long it took.
async fn poll_until<F>(check: F) -> Option<Duration>
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    let check = Arc::new(check);
    let start = std::time::Instant::now();
    while start.elapsed() < REMOTE_CHANGE_POLL_FOR {
        let c = check.clone();
        if blocking_with_timeout(move || c()).await {
            return Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

fn names_in(dir: &std::path::Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries.filter_map(|e| e.ok()?.file_name().into_string().ok()).collect(),
        Err(_) => Vec::new(),
    }
}

/// Mounts a drive whose `docs/` folder holds `files`, then browses it the
/// way Finder would (list the root, list `docs/`, stat every file) so the
/// kernel has everything cached before the remote change lands.
async fn mount_and_browse_docs(
    env: &VfsTestEnv,
    files: Vec<serde_json::Value>,
) -> (Arc<Vfs>, mount::MountedVfs, PathBuf, MountpointGuard) {
    env.set_remote_files(vec![common::remote_dir("docs")]).await;
    env.set_remote_files_at("docs", files).await;
    let vfs = new_test_vfs(env).await;
    let mp = tempfile::tempdir().unwrap().keep();
    let guard = MountpointGuard(mp.clone());
    // `VFS_TEST_MOUNT_OPTS` replaces the mount options, nothing else (same
    // server, same folder refresh): how CI shows these tests go red
    // without `actimeo=1,nonegnamecache`.
    let mounted = match std::env::var("VFS_TEST_MOUNT_OPTS") {
        Ok(opts) => {
            let counts = Arc::new(cloudreve_vfs::nfs::RpcCounts::default());
            mount::mount_with_rpc_counts_for_tests(vfs.clone(), &mp, env.cache_dir(), &opts, counts).await
        }
        Err(_) => mount::mount(vfs.clone(), &mp, "CloudreveTest", env.cache_dir()).await,
    }
    .expect("mount should succeed");
    // `VFS_TEST_NO_NUDGE` turns the folder refresh off: how CI shows the
    // kqueue/FSEvents checks go red without it.
    if std::env::var_os("VFS_TEST_NO_NUDGE").is_some() {
        vfs.stop_nudging().await;
    }
    let docs = mp.join("docs");
    let root = mp.clone();
    let d = docs.clone();
    blocking_with_timeout(move || {
        names_in(&root);
        for name in names_in(&d) {
            let _ = std::fs::metadata(d.join(name));
        }
    })
    .await;
    (vfs, mounted, mp, guard)
}

fn report_latency(label: &str, took: Option<Duration>) {
    use std::io::Write;
    let shown = took.map_or_else(|| format!("not within {REMOTE_CHANGE_POLL_FOR:?}"), |d| format!("{d:?}"));
    let _ = writeln!(std::io::stderr(), "REMOTE-CHANGE-LATENCY {label}: {shown}");
}

fn assert_within_bound(label: &str, took: Option<Duration>) {
    report_latency(label, took);
    let took = took.unwrap_or_else(|| panic!("{label}: not visible within {REMOTE_CHANGE_POLL_FOR:?}"));
    assert!(
        took <= REMOTE_CHANGE_VISIBLE_WITHIN,
        "{label}: took {took:?}, more than {REMOTE_CHANGE_VISIBLE_WITHIN:?}"
    );
}

/// (a) A file edited on the web shows its new size through `stat`.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_modified_remotely_shows_its_new_size_within_3s() {
    let env = VfsTestEnv::new().await;
    let (vfs, mounted, mp, _guard) =
        mount_and_browse_docs(&env, vec![file_in("docs", "report.txt", 5, "e1", "2026-01-02T00:00:00Z")]).await;
    let p = mp.join("docs/report.txt");
    let q = p.clone();
    assert_eq!(blocking_with_timeout(move || std::fs::metadata(&q).unwrap().len()).await, 5);

    env.set_remote_files_at("docs", vec![file_in("docs", "report.txt", 9, "e2", "2026-03-01T10:00:00Z")]).await;
    vfs.remote_changed(&format!("{}/docs/report.txt", common::REMOTE_BASE)).await;
    let took = poll_until(move || std::fs::metadata(&p).map(|m| m.len()).ok() == Some(9)).await;
    mounted.unmount().await.expect("unmount should succeed");

    assert_within_bound("modified size via stat", took);
}

/// (b) A name the mount already answered "no such file" for, then created
/// remotely, becomes visible — the kernel's negative name cache must not
/// hide it.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_created_remotely_after_a_failed_lookup_becomes_visible_within_3s() {
    let env = VfsTestEnv::new().await;
    let (vfs, mounted, mp, _guard) =
        mount_and_browse_docs(&env, vec![file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z")]).await;
    let p = mp.join("docs/new.txt");
    let q = p.clone();
    let before = blocking_with_timeout(move || {
        let first = std::fs::metadata(&q).is_err();
        first && std::fs::metadata(&q).is_err()
    })
    .await;
    assert!(before, "docs/new.txt must not exist yet");

    env.set_remote_files_at(
        "docs",
        vec![
            file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z"),
            file_in("docs", "new.txt", 4, "n1", "2026-03-01T10:00:00Z"),
        ],
    )
    .await;
    vfs.remote_changed(&format!("{}/docs/new.txt", common::REMOTE_BASE)).await;
    let took = poll_until(move || std::fs::metadata(&p).is_ok()).await;
    mounted.unmount().await.expect("unmount should succeed");

    assert_within_bound("created after ENOENT via stat", took);
}

/// (c) A file deleted remotely leaves the folder listing.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_deleted_remotely_disappears_from_the_listing_within_3s() {
    let env = VfsTestEnv::new().await;
    let (vfs, mounted, mp, _guard) = mount_and_browse_docs(
        &env,
        vec![
            file_in("docs", "keep.txt", 1, "e1", "2026-01-02T00:00:00Z"),
            file_in("docs", "gone.txt", 2, "e2", "2026-01-02T00:00:00Z"),
        ],
    )
    .await;
    let docs = mp.join("docs");
    let d = docs.clone();
    assert!(blocking_with_timeout(move || names_in(&d)).await.contains(&"gone.txt".to_string()));

    env.set_remote_files_at("docs", vec![file_in("docs", "keep.txt", 1, "e1", "2026-01-02T00:00:00Z")]).await;
    vfs.remote_changed(&format!("{}/docs/gone.txt", common::REMOTE_BASE)).await;
    let took = poll_until(move || {
        let names = names_in(&docs);
        names.contains(&"keep.txt".to_string())
            && !names.contains(&"gone.txt".to_string())
            && std::fs::metadata(docs.join("gone.txt")).is_err()
    })
    .await;
    mounted.unmount().await.expect("unmount should succeed");

    assert_within_bound("deleted gone from read_dir and stat", took);
}

/// (d) A file renamed remotely shows under its new name only.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_renamed_remotely_shows_the_new_name_and_not_the_old_within_3s() {
    let env = VfsTestEnv::new().await;
    let (vfs, mounted, mp, _guard) =
        mount_and_browse_docs(&env, vec![file_in("docs", "draft.txt", 3, "e1", "2026-01-02T00:00:00Z")]).await;
    let docs = mp.join("docs");

    env.set_remote_files_at("docs", vec![file_in("docs", "final.txt", 3, "e1", "2026-01-02T00:00:00Z")]).await;
    // What the SSE handler does for a rename: both ends.
    vfs.remote_changed(&format!("{}/docs/draft.txt", common::REMOTE_BASE)).await;
    vfs.remote_changed(&format!("{}/docs/final.txt", common::REMOTE_BASE)).await;
    let took = poll_until(move || {
        let names = names_in(&docs);
        names == vec!["final.txt".to_string()]
            && std::fs::metadata(docs.join("final.txt")).is_ok()
            && std::fs::metadata(docs.join("draft.txt")).is_err()
    })
    .await;
    mounted.unmount().await.expect("unmount should succeed");

    assert_within_bound("renamed via read_dir and stat", took);
}

/// Waits up to `within` for a `NOTE_WRITE` on the kqueue `kq`. Returns
/// how long it took, or `None`.
fn wait_note_write(kq: i32, within: Duration) -> Option<Duration> {
    let start = std::time::Instant::now();
    loop {
        let left = within.checked_sub(start.elapsed())?;
        let timeout =
            libc::timespec { tv_sec: left.as_secs() as libc::time_t, tv_nsec: left.subsec_nanos() as libc::c_long };
        // SAFETY: an all-zero kevent is a valid out-buffer.
        let mut ev: libc::kevent = unsafe { std::mem::zeroed() };
        // SAFETY: `kq` is a live kqueue, `ev` a valid buffer of one event.
        let n = unsafe { libc::kevent(kq, std::ptr::null(), 0, &mut ev, 1, &timeout) };
        if n > 0 && ev.fflags & libc::NOTE_WRITE != 0 {
            return Some(start.elapsed());
        }
        if n < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            return None;
        }
    }
}

/// An open Finder window learns about a folder's change from the kernel
/// (kqueue/FSEvents), which only reports operations done locally through
/// the VFS. A remote change goes through `Vfs::remote_changed` — the SSE
/// entry point — and must reach a `NOTE_WRITE` watch on the folder, opened
/// `O_EVTONLY` through the mount the way Finder watches it, within 3 s.
/// What Finder then re-reads must already hold the new file, and the
/// refresh must neither upload anything nor show up in the listing.
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_change_wakes_a_kqueue_watch_on_the_open_folder_within_3s() {
    use std::os::unix::ffi::OsStrExt;

    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    let (vfs, mounted, mp, _guard) =
        mount_and_browse_docs(&env, vec![file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z")]).await;
    let docs = mp.join("docs");

    let path = std::ffi::CString::new(docs.as_os_str().as_bytes()).unwrap();
    // SAFETY: plain syscalls on a NUL-terminated path / fresh descriptors.
    let (fd, kq) = unsafe { (libc::open(path.as_ptr(), libc::O_EVTONLY), libc::kqueue()) };
    assert!(fd >= 0 && kq >= 0, "open O_EVTONLY / kqueue failed");
    let change = libc::kevent {
        ident: fd as usize,
        filter: libc::EVFILT_VNODE,
        flags: libc::EV_ADD | libc::EV_CLEAR,
        fflags: libc::NOTE_WRITE,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: registers one well-formed change on a live kqueue.
    let rc = unsafe { libc::kevent(kq, &change, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
    assert_eq!(rc, 0, "kevent register failed");
    // Registering the watch makes the NFS client poll the folder's
    // attributes (`nfs_vnop_monitor`; `nfs_mount_sock_thread` re-polls
    // monitored vnodes every 5 s), and that poll alone also reports a moved
    // mtime. Let the first poll pass, then require three changes in a row
    // to each wake the watch within 1 s: a 5 s poll cannot do that.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    blocking_with_timeout(move || while wait_note_write(kq, Duration::from_millis(10)).is_some() {}).await;

    let mut files = vec![file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z")];
    let mut woke = Vec::new();
    for round in 0..3 {
        files.push(file_in("docs", &format!("b{round}.txt"), 2, "e2", "2026-03-01T10:00:00Z"));
        env.set_remote_files_at("docs", files.clone()).await;
        vfs.remote_changed(&format!("{}/docs/b{round}.txt", common::REMOTE_BASE)).await;
        woke.push(blocking_with_timeout(move || wait_note_write(kq, Duration::from_secs(5))).await);
        tokio::time::sleep(Duration::from_millis(700)).await;
        blocking_with_timeout(move || while wait_note_write(kq, Duration::from_millis(10)).is_some() {}).await;
    }
    let d = docs.clone();
    let listed = blocking_with_timeout(move || {
        let mut names = names_in(&d);
        names.sort();
        names
    })
    .await;
    // SAFETY: closing descriptors this test opened.
    unsafe {
        libc::close(kq);
        libc::close(fd);
    }
    vfs.wait_for_writeback_idle().await;
    mounted.unmount().await.expect("unmount should succeed");

    for (round, took) in woke.iter().enumerate() {
        report_latency(&format!("kqueue NOTE_WRITE on docs/ #{round}"), *took);
    }
    for took in &woke {
        let took = took.expect("no NOTE_WRITE on the open folder: Finder would never refresh it");
        assert!(took <= Duration::from_secs(1), "NOTE_WRITE took {took:?}");
    }
    assert_eq!(listed, ["a.txt", "b0.txt", "b1.txt", "b2.txt"], "what Finder re-reads");
    assert_eq!(env.upload_session_count(), 0, "a refresh must never upload anything");
}

/// Snapshot of a mount's RPC tally plus the HTTP listing requests the mock
/// Cloudreve server answered.
#[derive(Debug, Clone, Copy, Default)]
struct Cost {
    getattr: u64,
    lookup: u64,
    readdirplus: u64,
    read: u64,
    write: u64,
    http_list: usize,
}

fn cost_now(counts: &cloudreve_vfs::nfs::RpcCounts, env: &VfsTestEnv) -> Cost {
    use std::sync::atomic::Ordering::Relaxed;
    Cost {
        getattr: counts.getattr.load(Relaxed),
        lookup: counts.lookup.load(Relaxed),
        readdirplus: counts.readdirplus.load(Relaxed),
        read: counts.read.load(Relaxed),
        write: counts.write.load(Relaxed),
        http_list: env.list_request_count(),
    }
}

fn cost_since(a: Cost, b: Cost) -> Cost {
    Cost {
        getattr: b.getattr - a.getattr,
        lookup: b.lookup - a.lookup,
        readdirplus: b.readdirplus - a.readdirplus,
        read: b.read - a.read,
        write: b.write - a.write,
        http_list: b.http_list - a.http_list,
    }
}

/// Measurement, not a pass/fail test (run on CI with `--ignored
/// --nocapture`): what a scripted workload costs at the NFS adapter (RPCs)
/// and at the Cloudreve server (HTTP listings), and how fast it runs, under
/// the production mount options versus the ones before `actimeo=1,
/// nonegnamecache` (`BASELINE_OPTS`), each twice. Workload: `ls -lR` of a
/// 20 x 100 = 2,000-file tree, the same again 2 s later (attribute caches
/// of a short `actimeo` expired, the tree's 5 s listing TTL not) and 7 s
/// later (both expired), a 200 MB sequential read, a 50 MB write (upload
/// held back by a long debounce: only the mount is measured).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn measure_rpc_and_http_cost_of_mount_options() {
    use std::io::{Read, Write};

    const BASELINE_OPTS: &str = "locallocks,mutejukebox,vers=3,tcp,soft,dumbtimer,timeo=30,retrans=2,\
                                 nobrowse,rsize=1048576,wsize=1048576,readahead=4";
    // Each set twice, interleaved, so run-to-run noise on the shared
    // runner shows next to any difference between the sets.
    let production = mount::production_mount_opts_for_tests().to_string();
    let option_sets = [
        ("baseline#1", BASELINE_OPTS.to_string()),
        ("production#1", production.clone()),
        ("baseline#2", BASELINE_OPTS.to_string()),
        ("production#2", production),
    ];

    const MB: usize = 1_048_576;
    let big: Vec<u8> = (0..200 * MB).map(|i| (i % 251) as u8).collect();
    for (label, opts) in option_sets {
        let env = VfsTestEnv::new().await;
        let mut root = vec![remote_file("big.bin", big.len() as i64, "e1")];
        for d in 0..20 {
            let dir = format!("d{d:02}");
            root.push(common::remote_dir(&dir));
            let files = (0..100).map(|f| file_in(&dir, &format!("f{f:03}.txt"), 10, "e", "2026-01-02T00:00:00Z")).collect();
            env.set_remote_files_at(&dir, files).await;
        }
        env.set_remote_files(root).await;
        env.serve_file_content("big.bin", &big).await;
        let vfs = new_test_vfs(&env).await;
        vfs.set_debounce_for_tests(Duration::from_secs(600));
        let mp = tempfile::tempdir().unwrap().keep();
        let _guard = MountpointGuard(mp.clone());
        let counts = Arc::new(cloudreve_vfs::nfs::RpcCounts::default());
        let mounted = mount::mount_with_rpc_counts_for_tests(vfs.clone(), &mp, env.cache_dir(), &opts, counts.clone())
            .await
            .expect("mount should succeed");

        let report = |phase: &str, before: Cost, took: Duration, extra: String| {
            let c = cost_since(before, cost_now(&counts, &env));
            let _ = writeln!(
                std::io::stderr(),
                "MOUNT-COST opts={label} phase={phase} took={took:?} getattr={} lookup={} readdirplus={} read={} write={} http_list={} {extra}",
                c.getattr, c.lookup, c.readdirplus, c.read, c.write, c.http_list
            );
        };
        let ls = |mp: PathBuf| move || {
            let out = std::process::Command::new("/bin/ls").arg("-lR").arg(&mp).output().unwrap();
            String::from_utf8_lossy(&out.stdout).lines().filter(|l| l.contains(".txt")).count()
        };

        for (phase, pause) in [("ls-lR-cold", 0u64), ("ls-lR-after-2s", 2), ("ls-lR-after-7s", 7)] {
            tokio::time::sleep(Duration::from_secs(pause)).await;
            let before = cost_now(&counts, &env);
            let start = std::time::Instant::now();
            let n = blocking_with_timeout(ls(mp.clone())).await;
            report(phase, before, start.elapsed(), format!("files_listed={n}"));
        }

        let before = cost_now(&counts, &env);
        let start = std::time::Instant::now();
        let p = mp.join("big.bin");
        let read_bytes = blocking_with_timeout(move || {
            let mut f = std::fs::File::open(&p).unwrap();
            let mut buf = vec![0u8; MB];
            let mut total = 0usize;
            loop {
                let n = f.read(&mut buf).unwrap();
                if n == 0 {
                    break total;
                }
                total += n;
            }
        })
        .await;
        let took = start.elapsed();
        report("read-200MB", before, took, format!("bytes={read_bytes} MBps={:.1}", read_bytes as f64 / MB as f64 / took.as_secs_f64()));

        let before = cost_now(&counts, &env);
        let start = std::time::Instant::now();
        let p = mp.join("written.bin");
        blocking_with_timeout(move || {
            let mut f = std::fs::File::create(&p).unwrap();
            let chunk = vec![7u8; MB];
            for _ in 0..50 {
                f.write_all(&chunk).unwrap();
            }
            f.sync_all().unwrap();
        })
        .await;
        let took = start.elapsed();
        report("write-50MB", before, took, format!("MBps={:.1}", 50.0 / took.as_secs_f64()));

        mounted.unmount().await.expect("unmount should succeed");
    }
}

/// Minimal FSEvents binding for the Finder-level check below.
mod fsevents {
    use std::ffi::{c_char, c_void, CString};
    use std::sync::Mutex;

    type Log = Mutex<Vec<(String, u32)>>;

    #[repr(C)]
    struct StreamContext {
        version: isize,
        info: *mut c_void,
        retain: *const c_void,
        release: *const c_void,
        copy_description: *const c_void,
    }

    type Callback = extern "C" fn(*const c_void, *mut c_void, usize, *mut c_void, *const u32, *const u64);

    #[link(name = "CoreServices", kind = "framework")]
    extern "C" {
        fn FSEventStreamCreate(
            allocator: *const c_void,
            callback: Callback,
            context: *const StreamContext,
            paths: *const c_void,
            since_when: u64,
            latency: f64,
            flags: u32,
        ) -> *mut c_void;
        fn FSEventStreamSetDispatchQueue(stream: *mut c_void, queue: *mut c_void);
        fn FSEventStreamStart(stream: *mut c_void) -> u8;
        fn FSEventStreamStop(stream: *mut c_void);
        fn FSEventStreamInvalidate(stream: *mut c_void);
        fn FSEventStreamRelease(stream: *mut c_void);
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    extern "C" {
        static kCFTypeArrayCallBacks: c_void;
        fn CFStringCreateWithCString(alloc: *const c_void, s: *const c_char, encoding: u32) -> *const c_void;
        fn CFArrayCreate(alloc: *const c_void, values: *const *const c_void, n: isize, cb: *const c_void) -> *const c_void;
    }

    extern "C" {
        fn dispatch_queue_create(label: *const c_char, attr: *const c_void) -> *mut c_void;
    }

    extern "C" fn on_events(_: *const c_void, info: *mut c_void, n: usize, paths: *mut c_void, flags: *const u32, _: *const u64) {
        // SAFETY: `info` is the log `Watch` keeps alive; without
        // `UseCFTypes`, `paths` is an array of `n` C strings and `flags`
        // of `n` flags.
        let log = unsafe { &*(info as *const Log) };
        let paths = paths as *const *const c_char;
        for i in 0..n {
            let (path, flag) = unsafe {
                (std::ffi::CStr::from_ptr(*paths.add(i)).to_string_lossy().into_owned(), *flags.add(i))
            };
            log.lock().unwrap().push((path, flag));
        }
    }

    /// An FSEvents stream on one directory, the API Finder's own folder
    /// views refresh from.
    pub struct Watch {
        stream: *mut c_void,
        log: Box<Log>,
    }

    // SAFETY: the stream is only started/stopped from the owning thread;
    // events arrive on its own dispatch queue and only touch `log`.
    unsafe impl Send for Watch {}

    impl Watch {
        pub fn start(dir: &std::path::Path) -> Self {
            let log: Box<Log> = Box::new(Mutex::new(Vec::new()));
            let path = CString::new(dir.to_str().unwrap()).unwrap();
            let ctx = StreamContext {
                version: 0,
                info: &*log as *const Log as *mut c_void,
                retain: std::ptr::null(),
                release: std::ptr::null(),
                copy_description: std::ptr::null(),
            };
            // SAFETY: plain CoreFoundation/FSEvents calls with valid,
            // NUL-terminated strings; the context outlives the call (it is
            // copied by FSEventStreamCreate).
            let stream = unsafe {
                let s = CFStringCreateWithCString(std::ptr::null(), path.as_ptr(), 0x0800_0100);
                let paths = CFArrayCreate(std::ptr::null(), &s, 1, &kCFTypeArrayCallBacks as *const c_void);
                // kFSEventStreamEventIdSinceNow; NoDefer | FileEvents.
                let stream =
                    FSEventStreamCreate(std::ptr::null(), on_events, &ctx, paths, u64::MAX, 0.05, 0x02 | 0x10);
                let queue = dispatch_queue_create(c"cloudreve.test.fsevents".as_ptr(), std::ptr::null());
                FSEventStreamSetDispatchQueue(stream, queue);
                assert_eq!(FSEventStreamStart(stream), 1, "FSEventStreamStart failed");
                stream
            };
            Self { stream, log }
        }

        pub fn seen(&self) -> bool {
            !self.log.lock().unwrap().is_empty()
        }

        /// Every (path, flags) received since the last call, cleared.
        pub fn take(&self) -> Vec<(String, u32)> {
            std::mem::take(&mut *self.log.lock().unwrap())
        }
    }

    impl Drop for Watch {
        fn drop(&mut self) {
            // SAFETY: the stream was created and started by `start`.
            unsafe {
                FSEventStreamStop(self.stream);
                FSEventStreamInvalidate(self.stream);
                FSEventStreamRelease(self.stream);
            }
        }
    }
}

/// The same refresh seen through FSEvents, the API Finder's folder views
/// listen to: an event for the open folder must arrive within 3 s of a
/// remote change, with no local action.
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_change_reaches_an_fsevents_stream_on_the_open_folder_within_3s() {
    let env = VfsTestEnv::new().await;
    let (vfs, mounted, mp, _guard) =
        mount_and_browse_docs(&env, vec![file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z")]).await;
    let docs = std::fs::canonicalize(mp.join("docs")).unwrap();
    let watch = fsevents::Watch::start(&docs);
    // The stream registers with fseventsd asynchronously and only gets
    // events newer than that: prove it live with a local operation first
    // (a local-only junk file, never uploaded), then start from silence.
    // The probe is the production refresh itself, run by hand on a folder
    // whose listing the kernel holds in full: it must work then too (an
    // AppleDouble `._` probe failed with ENOENT exactly there, see
    // `nudge::NUDGE_NAME`).
    let probe = cloudreve_vfs::nudge::touch_through(mp.clone());
    let mut warmup_errors = Vec::new();
    let warm_start = std::time::Instant::now();
    while !watch.seen() && warm_start.elapsed() < Duration::from_secs(10) {
        let p = probe.clone();
        if let Err(err) = blocking_with_timeout(move || p("docs")).await {
            warmup_errors.push(err.to_string());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let live = watch.seen();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let warmup_events = watch.take();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let spurious = watch.take();

    env.set_remote_files_at(
        "docs",
        vec![
            file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z"),
            file_in("docs", "b.txt", 2, "e2", "2026-03-01T10:00:00Z"),
        ],
    )
    .await;
    vfs.remote_changed(&format!("{}/docs/b.txt", common::REMOTE_BASE)).await;
    let start = std::time::Instant::now();
    let mut took = None;
    while start.elapsed() < Duration::from_secs(5) {
        if watch.seen() {
            took = Some(start.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let change_events = watch.take();
    drop(watch);
    mounted.unmount().await.expect("unmount should succeed");

    {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr(),
            "FSEVENTS warmup={warmup_events:?} warmup_errors={warmup_errors:?} spurious={spurious:?} change={change_events:?}"
        );
    }
    report_latency("FSEvents on docs/", took);
    assert!(warmup_errors.is_empty(), "the folder refresh failed on a freshly listed folder: {warmup_errors:?}");
    assert!(live, "the FSEvents stream never saw a local operation on the folder: nothing to measure");
    assert!(spurious.is_empty(), "an event arrived before any change: the check would prove nothing");
    let took = took.expect("no FSEvents event for the open folder: Finder would never refresh it");
    assert!(took <= REMOTE_CHANGE_VISIBLE_WITHIN, "FSEvents took {took:?}");
}
