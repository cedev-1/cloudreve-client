//! Mounted Linux tests for what a desktop (GNOME/KDE) and its apps do to a
//! drive: trash, office lock files, editors' atomic saves, free space,
//! a slow download next to a folder listing, and a real app crash.
//!
//! A real `fuser` mount of the wiremock-backed `Vfs`, driven through plain
//! syscalls, like `tests/mounted_linux.rs`. Runs on the ubuntu CI job (and
//! any Linux machine with `/dev/fuse` and `fusermount3`).
#![cfg(target_os = "linux")]

mod common;

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cloudreve_vfs::mount;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, VfsTestEnv};

/// Force-unmounts `path` on drop, whatever happened in the test.
struct MountpointGuard(PathBuf);

impl Drop for MountpointGuard {
    fn drop(&mut self) {
        let path = self.0.to_string_lossy().to_string();
        let _ = std::process::Command::new("fusermount3").args(["-u", &path]).output();
        let _ = std::process::Command::new("fusermount3").args(["-uz", &path]).output();
    }
}

async fn blocking<T, F>(f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::time::timeout(Duration::from_secs(20), tokio::task::spawn_blocking(f))
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

async fn wait_for_upload(vfs: &Vfs, env: &VfsTestEnv, name: &str, expected: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        vfs.wait_for_writeback_idle().await;
        if env.uploaded_content(name).as_deref() == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "upload of {name} never landed — last seen: {:?}",
            env.uploaded_content(name).map(|b| String::from_utf8_lossy(&b).into_owned())
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn mounted(env: &VfsTestEnv, mountpoint: &Path) -> (Arc<Vfs>, mount::MountedVfs) {
    let vfs = new_test_vfs(env).await;
    let handle = mount::mount(vfs.clone(), mountpoint, "Cloudreve", env.cache_dir())
        .await
        .expect("mount should succeed");
    (vfs, handle)
}

/// Nautilus/Dolphin move a file on another volume into `.Trash-<uid>` at
/// its root. That folder is refused (EACCES) — the file manager then asks
/// "delete permanently?" — while an ordinary folder is created normally.
/// Without the refusal, "trashed" files stayed in the cloud in a hidden
/// folder that used quota and showed up on every other device.
#[tokio::test(flavor = "multi_thread")]
async fn the_desktop_trash_folder_is_refused_but_ordinary_folders_are_created() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![]).await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (_vfs, handle) = mounted(&env, &mountpoint).await;

    let root = mountpoint.clone();
    let (trash, trash_legacy, photos) = blocking(move || {
        let uid = unsafe { libc::getuid() };
        (
            std::fs::create_dir(root.join(format!(".Trash-{uid}"))),
            std::fs::create_dir(root.join(".Trash")),
            std::fs::create_dir(root.join("Photos")),
        )
    })
    .await;

    assert_eq!(trash.unwrap_err().raw_os_error(), Some(libc::EACCES));
    assert_eq!(trash_legacy.unwrap_err().raw_os_error(), Some(libc::EACCES));
    photos.expect("an ordinary folder must be created");
    assert_eq!(env.create_file_call_count(), 1, "only Photos may reach the server");

    handle.unmount().await.unwrap();
}

/// LibreOffice keeps `.~lock.<name>#` next to an open document, MS Office
/// `~$<name>`, KDE writes `.directory`: they must work (LibreOffice reads
/// its lock back) but never be uploaded. The real document still uploads.
#[tokio::test(flavor = "multi_thread")]
async fn office_lock_files_work_on_the_mount_and_never_upload() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![]).await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (vfs, handle) = mounted(&env, &mountpoint).await;

    let root = mountpoint.clone();
    let lock_back = blocking(move || {
        let lock = root.join(".~lock.Budget 2026.ods#");
        std::fs::write(&lock, b",ana,ana-laptop,09.11.2026 10:00,file:///home/ana/.config/libreoffice/4;")
            .unwrap();
        std::fs::write(root.join("~$Budget.docx"), b"ana").unwrap();
        std::fs::write(root.join(".directory"), b"[Dolphin]\nViewMode=1\n").unwrap();
        std::fs::write(root.join("Budget 2026.ods"), b"spreadsheet bytes").unwrap();
        let back = std::fs::read(&lock).unwrap();
        std::fs::remove_file(&lock).unwrap();
        back
    })
    .await;
    assert!(lock_back.starts_with(b",ana,"), "the lock file must read back what was written");

    wait_for_upload(&vfs, &env, "Budget 2026.ods", b"spreadsheet bytes").await;
    assert_eq!(env.uploaded_names(), vec!["Budget 2026.ods".to_string()]);

    handle.unmount().await.unwrap();
}

/// gedit (GLib's `g_file_replace`), Kate and LibreOffice save by writing a
/// temp file next to the document and renaming it over the original. The
/// server must end up with the new content under the real name, and the
/// temp name must never be uploaded.
#[tokio::test(flavor = "multi_thread")]
async fn an_editor_atomic_save_replaces_the_file_and_uploads_only_the_real_name() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("notes.txt", 9, "e1")]).await;
    env.serve_file_content("notes.txt", b"old notes").await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    // The real 2 s upload delay, not the suite's 20 ms: the editor's
    // rename must land before the temp file's upload starts, exactly as
    // in production (a 20 ms window is too short on a loaded runner).
    let (vfs, _rx) = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
        .unwrap();
    let vfs = Arc::new(vfs);
    let handle = mount::mount(vfs.clone(), &mountpoint, "Cloudreve", env.cache_dir()).await.unwrap();

    let root = mountpoint.clone();
    let read_back = blocking(move || {
        assert_eq!(std::fs::read(root.join("notes.txt")).unwrap(), b"old notes");
        let tmp = root.join(".goutputstream-K3Q9X2");
        std::fs::write(&tmp, b"new notes, saved by gedit").unwrap();
        std::fs::rename(&tmp, root.join("notes.txt")).unwrap();
        std::fs::read(root.join("notes.txt")).unwrap()
    })
    .await;
    assert_eq!(read_back, b"new notes, saved by gedit");

    wait_for_upload(&vfs, &env, "notes.txt", b"new notes, saved by gedit").await;
    assert!(
        !env.uploaded_names().iter().any(|n| n.starts_with(".goutputstream")),
        "the temp name must never reach the server: {:?}",
        env.uploaded_names()
    );

    handle.unmount().await.unwrap();
}

/// `mv -n` and other "no clobber" tools use `renameat2(RENAME_NOREPLACE)`.
/// It must never overwrite the destination: the kernel refuses it for this
/// FUSE protocol level (EINVAL, then coreutils falls back to a checked
/// rename), or the filesystem answers EEXIST. Pins that a future protocol
/// bump cannot silently turn it into an overwrite.
#[tokio::test(flavor = "multi_thread")]
async fn rename_noreplace_never_overwrites_the_destination() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("keep.txt", 4, "e1"), remote_file("other.txt", 5, "e2")])
        .await;
    env.serve_file_content("keep.txt", b"keep").await;
    env.serve_file_content("other.txt", b"other").await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (_vfs, handle) = mounted(&env, &mountpoint).await;

    let root = mountpoint.clone();
    let (rc, errno, keep) = blocking(move || {
        let from = CString::new(root.join("other.txt").as_os_str().as_bytes()).unwrap();
        let to = CString::new(root.join("keep.txt").as_os_str().as_bytes()).unwrap();
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        let errno = std::io::Error::last_os_error().raw_os_error();
        (rc, errno, std::fs::read(root.join("keep.txt")).unwrap())
    })
    .await;

    assert_eq!(rc, -1, "RENAME_NOREPLACE onto an existing file must fail");
    assert!(matches!(errno, Some(libc::EINVAL) | Some(libc::EEXIST)), "unexpected errno {errno:?}");
    assert_eq!(keep, b"keep");
    assert_eq!(env.rename_call_count(), 0);

    handle.unmount().await.unwrap();
}

/// `df`, Nautilus and Dolphin read the free space of the drive. fuser's
/// default answers 0 blocks, so the drive looked full (Nautilus refuses to
/// copy onto a volume without room).
#[tokio::test(flavor = "multi_thread")]
async fn the_mount_reports_free_space() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (_vfs, handle) = mounted(&env, &mountpoint).await;

    let root = mountpoint.clone();
    let (free, total) = blocking(move || {
        let path = CString::new(root.as_os_str().as_bytes()).unwrap();
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(path.as_ptr(), &mut st) };
        assert_eq!(rc, 0, "statvfs failed: {}", std::io::Error::last_os_error());
        (st.f_bavail as u64 * st.f_frsize as u64, st.f_blocks as u64 * st.f_frsize as u64)
    })
    .await;

    let gib = 1024 * 1024 * 1024u64;
    assert!(free >= 100 * gib, "free space reported: {free} bytes");
    assert!(total >= free);

    handle.unmount().await.unwrap();
}

/// Opening a big file waits on the network. Meanwhile the user keeps
/// browsing: listing the folder and opening a small file must answer
/// promptly instead of waiting for that download to finish (FUSE requests
/// used to be served one at a time).
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_download_does_not_freeze_browsing() {
    let env = VfsTestEnv::new().await;
    let big: Vec<u8> = (0..=255u8).cycle().take(2 * 1024 * 1024).collect();
    env.set_remote_files(vec![
        remote_file("movie.mkv", big.len() as i64, "e1"),
        remote_file("todo.txt", 4, "e2"),
    ])
    .await;
    env.serve_file_content("movie.mkv", &big).await;
    env.serve_file_content("todo.txt", b"milk").await;
    env.slow_down_downloads("movie.mkv", Duration::from_secs(4));
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (_vfs, handle) = mounted(&env, &mountpoint).await;

    // Warm the listing so the slow part is only the movie's bytes.
    let root = mountpoint.clone();
    blocking(move || std::fs::read_dir(&root).unwrap().count()).await;

    let movie = mountpoint.join("movie.mkv");
    let slow_read = tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut f = std::fs::File::open(&movie).unwrap();
        let mut buf = vec![0u8; 4096];
        f.read_exact(&mut buf).unwrap();
        buf
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let root = mountpoint.clone();
    let (took, names, todo) = blocking(move || {
        let start = Instant::now();
        let names: Vec<String> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        let todo = std::fs::read(root.join("todo.txt")).unwrap();
        (start.elapsed(), names, todo)
    })
    .await;

    assert!(names.contains(&"todo.txt".to_string()));
    assert_eq!(todo, b"milk");
    assert!(
        took < Duration::from_millis(2000),
        "browsing waited {took:?} behind a download (the download takes 4 s)"
    );
    let head = slow_read.await.unwrap();
    assert_eq!(head, big[..4096]);

    handle.unmount().await.unwrap();
}

/// The first write to a big remote file downloads it whole first (the
/// draft must hold every byte before the new ones land). Browsing must not
/// wait behind that download, and the upload must hold the old bytes plus
/// the appended ones.
#[tokio::test(flavor = "multi_thread")]
async fn appending_to_a_slow_big_file_does_not_freeze_browsing() {
    let env = VfsTestEnv::new().await;
    let big: Vec<u8> = (0..=255u8).cycle().take(2 * 1024 * 1024).collect();
    env.set_remote_files(vec![
        remote_file("export.log", big.len() as i64, "e1"),
        remote_file("todo.txt", 4, "e2"),
    ])
    .await;
    env.serve_file_content("export.log", &big).await;
    env.serve_file_content("todo.txt", b"milk").await;
    env.slow_down_downloads("export.log", Duration::from_secs(4));
    env.expect_uploads().await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (vfs, handle) = mounted(&env, &mountpoint).await;

    let root = mountpoint.clone();
    blocking(move || std::fs::read_dir(&root).unwrap().count()).await;

    let log = mountpoint.join("export.log");
    let append = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&log)?;
        f.write_all(b"\nlast line")?;
        Ok(())
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let root = mountpoint.clone();
    let (took, names, todo) = blocking(move || {
        let start = Instant::now();
        let names: Vec<String> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        let todo = std::fs::read(root.join("todo.txt")).unwrap();
        (start.elapsed(), names, todo)
    })
    .await;

    assert!(names.contains(&"export.log".to_string()));
    assert_eq!(todo, b"milk");
    assert!(
        took < Duration::from_millis(2000),
        "browsing waited {took:?} behind the first write's download (the download takes 4 s)"
    );
    let appended = tokio::time::timeout(Duration::from_secs(30), append)
        .await
        .expect("the append hung")
        .unwrap();
    assert!(appended.is_ok(), "appending to a slowly served file failed: {appended:?}");

    let mut expected = big.clone();
    expected.extend_from_slice(b"\nlast line");
    wait_for_upload(&vfs, &env, "export.log", &expected).await;
    handle.unmount().await.unwrap();
}

const CHILD_MOUNTPOINT: &str = "CLOUDREVE_TEST_CHILD_MOUNTPOINT";
const CHILD_CACHE: &str = "CLOUDREVE_TEST_CHILD_CACHE";

/// Not a test on its own: the "app" `a_killed_app_..` below starts and then
/// kills with SIGKILL. Mounts and waits forever.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "helper process for a_killed_app_leaves_a_dead_mount_the_next_launch_recovers"]
async fn crash_helper_child() {
    let (Ok(mountpoint), Ok(cache)) = (std::env::var(CHILD_MOUNTPOINT), std::env::var(CHILD_CACHE)) else {
        return;
    };
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 5, "e1")]).await;
    let (vfs, _rx) = Vfs::new(env.client(), common::REMOTE_BASE.into(), Path::new(&cache), DEFAULT_CACHE_MAX_BYTES)
        .unwrap();
    let _mounted = mount::mount(Arc::new(vfs), Path::new(&mountpoint), "Cloudreve", Path::new(&cache))
        .await
        .expect("child mount");
    std::fs::write(Path::new(&cache).join("child-ready"), b"1").unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// The app is killed (crash, OOM killer, logout without a clean quit)
/// while the drive is mounted. The kernel keeps a dead mount: every access
/// fails with "Transport endpoint is not connected" and the folder shows
/// as broken in the file manager. The next launch must clean it up and
/// mount again on the same folder — here one whose name has a space, like
/// most users' "Cloud Drive" (`/proc/mounts` escapes it as `\040`).
#[tokio::test(flavor = "multi_thread")]
async fn a_killed_app_leaves_a_dead_mount_the_next_launch_recovers() {
    let base = tempfile::tempdir().unwrap();
    let mountpoint = base.path().join("Cloud Drive");
    std::fs::create_dir(&mountpoint).unwrap();
    let child_cache = base.path().join("child-cache");
    std::fs::create_dir(&child_cache).unwrap();
    let _guard = MountpointGuard(mountpoint.clone());

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_helper_child", "--exact", "--ignored", "--nocapture"])
        .env(CHILD_MOUNTPOINT, &mountpoint)
        .env(CHILD_CACHE, &child_cache)
        .spawn()
        .expect("spawn the app process");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !child_cache.join("child-ready").exists() {
        assert!(Instant::now() < deadline, "the app process never mounted");
        assert!(child.try_wait().unwrap().is_none(), "the app process exited early");
        std::thread::sleep(Duration::from_millis(50));
    }

    child.kill().unwrap(); // SIGKILL: no unmount, no cleanup.
    child.wait().unwrap();

    let mp = mountpoint.clone();
    let dead = blocking(move || std::fs::metadata(&mp).err().and_then(|e| e.raw_os_error())).await;
    assert_eq!(dead, Some(libc::ENOTCONN), "a killed FUSE server leaves a disconnected mount");

    // Next launch: same drive (same cache dir, holding the dead process's
    // marker), same folder.
    let cleaned = mount::cleanup_stale_mount(&mountpoint, &child_cache).await.unwrap();
    assert!(cleaned, "the dead mount must be recognized and removed");

    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 5, "e1")]).await;
    let vfs = new_test_vfs(&env).await;
    let handle = mount::mount(vfs, &mountpoint, "Cloudreve", &child_cache).await.expect("remount");
    let mp = mountpoint.clone();
    let names: Vec<String> = blocking(move || {
        std::fs::read_dir(&mp).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect()
    })
    .await;
    assert_eq!(names, vec!["hello.txt".to_string()]);
    handle.unmount().await.unwrap();
}

/// Same crash, but the drive's cache folder (and its record of who
/// mounted) is gone — the user cleared `~/.cloudreve`, or the drive was
/// re-added. A disconnected mount is still proof nobody serves it.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_mount_without_any_record_is_still_recovered() {
    let base = tempfile::tempdir().unwrap();
    let mountpoint = base.path().join("Cloudreve");
    std::fs::create_dir(&mountpoint).unwrap();
    let child_cache = base.path().join("child-cache");
    std::fs::create_dir(&child_cache).unwrap();
    let _guard = MountpointGuard(mountpoint.clone());

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_helper_child", "--exact", "--ignored", "--nocapture"])
        .env(CHILD_MOUNTPOINT, &mountpoint)
        .env(CHILD_CACHE, &child_cache)
        .spawn()
        .expect("spawn the app process");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !child_cache.join("child-ready").exists() {
        assert!(Instant::now() < deadline, "the app process never mounted");
        std::thread::sleep(Duration::from_millis(50));
    }
    child.kill().unwrap();
    child.wait().unwrap();

    let fresh_cache = base.path().join("fresh-cache");
    std::fs::create_dir(&fresh_cache).unwrap();
    let cleaned = mount::cleanup_stale_mount(&mountpoint, &fresh_cache).await.unwrap();
    assert!(cleaned, "a disconnected mount must be removed even without a marker");
    let mp = mountpoint.clone();
    let entries = blocking(move || std::fs::read_dir(&mp).map(|d| d.count())).await;
    assert_eq!(entries.unwrap(), 0, "the folder is a plain empty folder again");
}
