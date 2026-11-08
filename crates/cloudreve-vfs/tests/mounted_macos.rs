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
#[tokio::test(flavor = "multi_thread")]
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
