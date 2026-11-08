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
/// CI fix round (post-Task-3 two-instance fix): `cleanup_stale_mount` is
/// no longer "force-detect and force-unmount by path alone" — see
/// `mount.rs`'s `linux_impl` module doc's "Stale-mount detection" section.
/// It now additionally consults the `<cache_dir>/mount.port` marker
/// `mount()` records (this process's pid on Linux — there is no port for a
/// FUSE mount) and only treats a mount-table match as stale once that
/// SPECIFIC recorded owner is confirmed dead. `abort_server_for_tests`
/// alone (a `mem::forget`, not a real process death) leaves the mount's
/// OWNER PID pointing at THIS VERY TEST PROCESS, which is very much still
/// running — under the new semantics that is indistinguishable from a
/// live sibling instance's healthy mount, and `cleanup_stale_mount`
/// correctly refuses to touch it (this is the two-instance guarantee
/// working as designed, not a bug: a real crash always means a dead pid,
/// and an in-process test can't produce one just by abandoning a session
/// while the test's own process keeps running). So this test now also
/// overwrites the marker with a PROVABLY DEAD pid — the same technique
/// `mounting_over_a_stale_leftover_mountpoint_succeeds_via_pre_clean`
/// above already uses: spawn and wait out a trivial child process
/// (`/bin/true`), then write its (now guaranteed-dead) pid over the real
/// one `mount()` recorded — making the marker's owner genuinely
/// unreachable without needing this test's own process to die, which is
/// what a real crash actually leaves behind.
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
/// test is actually proving: `cleanup_stale_mount` never probes the mount
/// or session itself for liveness (see its own doc, both platforms); once
/// its OWNER is confirmed dead, it force-detects and force-unmounts by
/// path alone, which works identically against a live-but-abandoned mount
/// or a truly dead one.
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

    // Overwrite the marker `mount()` just wrote (this process's own, live
    // pid) with one that is definitively dead — see the test's own doc.
    // Without this, the marker's owner is THIS test process, still alive,
    // and `cleanup_stale_mount` would (correctly, under the two-instance
    // fix) refuse to touch it.
    let dead_pid = blocking_with_timeout(|| {
        let mut child = std::process::Command::new("/bin/true").spawn().expect("spawn /bin/true");
        let pid = child.id();
        child.wait().expect("wait for /bin/true");
        pid
    })
    .await;
    std::fs::write(env.cache_dir().join("mount.port"), dead_pid.to_string())
        .expect("overwrite the mount marker with a dead pid");

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

/// `mv a b` then writing to `b` right away: the kernel moves its dentry and
/// keeps using the inode it already had for `a` for as long as the entry
/// stays cached (5s), without a fresh LOOKUP — so the renamed file must
/// still answer to that inode.
#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_file_is_writable_through_the_kernels_cached_inode() {
    use std::io::Write;

    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 5, "e1")]).await;
    env.serve_file_content("a.txt", b"hello").await;
    env.serve_file_content("b.txt", b"hello").await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;

    let vfs = new_test_vfs(&env).await;

    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let mounted = mount::mount(vfs.clone(), &mountpoint, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");

    let (a, b) = (mountpoint.join("a.txt"), mountpoint.join("b.txt"));
    let res = blocking_with_timeout(move || -> std::io::Result<Vec<u8>> {
        std::fs::metadata(&a)?;
        std::fs::rename(&a, &b)?;
        let mut f = std::fs::OpenOptions::new().append(true).open(&b)?;
        f.write_all(b" world")?;
        drop(f);
        std::fs::read(&b)
    })
    .await;

    let content = res.expect("rename then append through the mount");
    assert_eq!(content, b"hello world");
    wait_for_upload(&vfs, &env, "b.txt", b"hello world").await;
    assert_eq!(env.uploaded_content("a.txt"), None);
    mounted.unmount().await.expect("unmount should succeed");
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
/// way a file manager would (list the root, list `docs/`, stat every file) so the
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
    let mounted = mount::mount(vfs.clone(), &mp, "CloudreveTest", env.cache_dir())
        .await
        .expect("mount should succeed");
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


/// Waits up to `within` for an `IN_CREATE`/`IN_DELETE` on the inotify
/// instance `ino`. Returns how long it took, or `None`.
fn wait_inotify(ino: i32, within: Duration) -> Option<Duration> {
    let start = std::time::Instant::now();
    loop {
        let left = within.checked_sub(start.elapsed())?;
        let mut pfd = libc::pollfd { fd: ino, events: libc::POLLIN, revents: 0 };
        // SAFETY: one valid pollfd.
        let n = unsafe { libc::poll(&mut pfd, 1, left.as_millis() as libc::c_int) };
        if n == 0 {
            return None;
        }
        if n > 0 {
            let mut buf = [0u8; 4096];
            // SAFETY: reads into a buffer of the given length.
            let len = unsafe { libc::read(ino, buf.as_mut_ptr().cast(), buf.len()) };
            if len >= std::mem::size_of::<libc::inotify_event>() as isize {
                // SAFETY: the kernel wrote at least one whole inotify_event.
                let ev: libc::inotify_event = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast()) };
                if ev.mask & (libc::IN_CREATE | libc::IN_DELETE) != 0 {
                    return Some(start.elapsed());
                }
            }
        }
    }
}

/// A file manager (Nautilus/GIO, Dolphin) learns about a folder's change
/// from inotify, which FUSE only feeds for operations done locally through
/// the VFS — the kernel's `notify_inval_*` paths emit no fsnotify event on
/// the parent. A remote change goes through `Vfs::remote_changed` and must
/// reach an inotify watch on the folder within 3 s; what the file manager
/// then re-reads must hold the new file; nothing is uploaded.
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_change_wakes_an_inotify_watch_on_the_open_folder_within_3s() {
    use std::os::unix::ffi::OsStrExt;

    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    let (vfs, mounted, mp, _guard) =
        mount_and_browse_docs(&env, vec![file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z")]).await;
    let docs = mp.join("docs");

    let path = std::ffi::CString::new(docs.as_os_str().as_bytes()).unwrap();
    // SAFETY: a plain syscall returning a fresh descriptor.
    let ino = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
    assert!(ino >= 0, "inotify_init1 failed");
    // SAFETY: a live inotify descriptor and a NUL-terminated path.
    let wd = unsafe { libc::inotify_add_watch(ino, path.as_ptr(), libc::IN_CREATE | libc::IN_DELETE) };
    assert!(wd >= 0, "inotify_add_watch failed: {}", std::io::Error::last_os_error());

    env.set_remote_files_at(
        "docs",
        vec![
            file_in("docs", "a.txt", 1, "e1", "2026-01-02T00:00:00Z"),
            file_in("docs", "b.txt", 2, "e2", "2026-03-01T10:00:00Z"),
        ],
    )
    .await;
    vfs.remote_changed(&format!("{}/docs/b.txt", common::REMOTE_BASE)).await;
    let woke = blocking_with_timeout(move || wait_inotify(ino, Duration::from_secs(5))).await;
    let d = docs.clone();
    let listed = blocking_with_timeout(move || {
        let mut names = names_in(&d);
        names.sort();
        names
    })
    .await;
    // SAFETY: closing a descriptor this test opened.
    unsafe { libc::close(ino) };
    vfs.wait_for_writeback_idle().await;
    mounted.unmount().await.expect("unmount should succeed");

    report_latency("inotify on docs/", woke);
    let woke = woke.expect("no inotify event on the open folder: the file manager would never refresh it");
    assert!(woke <= REMOTE_CHANGE_VISIBLE_WITHIN, "inotify took {woke:?}");
    assert_eq!(listed, vec!["a.txt".to_string(), "b.txt".to_string()], "what the file manager re-reads");
    assert_eq!(env.upload_session_count(), 0, "a refresh must never upload anything");
}
