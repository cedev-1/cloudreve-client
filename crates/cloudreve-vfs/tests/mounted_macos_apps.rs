//! What Mac apps do to a document on the drive, through the very Foundation
//! calls they make (`tests/support/macos_file_apis.swift`, run with the
//! system `swift`): an NSDocument "safe save" (TextEdit, Preview, Pages),
//! Finder's "Move to Trash", and Finder tags — against a real NFS mount of
//! the wiremock-backed `Vfs`, like `tests/mounted_macos.rs`.
//!
//! Each test prints what Foundation answered (`--nocapture` on CI), so the
//! log records the actual behavior, not only the assertion.
#![cfg(target_os = "macos")]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cloudreve_vfs::mount;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, VfsTestEnv};

struct MountpointGuard(PathBuf);

impl Drop for MountpointGuard {
    fn drop(&mut self) {
        let path = self.0.to_string_lossy().to_string();
        let _ = std::process::Command::new("/sbin/umount").arg(&path).output();
        let _ = std::process::Command::new("/sbin/umount").args(["-f", &path]).output();
        let _ = std::process::Command::new("/usr/sbin/diskutil").args(["unmount", "force", &path]).output();
    }
}

/// Runs the Swift helper: `swift macos_file_apis.swift <args>`. A clean
/// environment, so a Nix (or other) toolchain on `PATH` cannot shadow the
/// system Swift.
async fn foundation(args: &[&str]) -> String {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/macos_file_apis.swift");
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let output = tokio::time::timeout(
        Duration::from_secs(180),
        tokio::task::spawn_blocking(move || {
            std::process::Command::new("/usr/bin/swift")
                .arg(&script)
                .args(&args)
                .env_clear()
                .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
                .env("HOME", std::env::var("HOME").unwrap_or_default())
                .output()
                .expect("run /usr/bin/swift")
        }),
    )
    .await
    .expect("swift timed out")
    .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let line = stdout.lines().last().unwrap_or_default().to_string();
    eprintln!("foundation {:?} -> {line}\n{}", output.status, String::from_utf8_lossy(&output.stderr));
    line
}

async fn mounted(env: &VfsTestEnv, mountpoint: &Path) -> (Arc<Vfs>, mount::MountedVfs) {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(200));
    let vfs = Arc::new(vfs);
    let handle = mount::mount(vfs.clone(), mountpoint, "Cloudreve", env.cache_dir()).await.expect("mount");
    (vfs, handle)
}

async fn wait_for_upload(vfs: &Vfs, env: &VfsTestEnv, name: &str, expected: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        vfs.wait_for_writeback_idle().await;
        if env.uploaded_content(name).as_deref() == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "upload of {name} never landed — uploaded so far: {:?}",
            env.uploaded_names()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// TextEdit/Preview/Pages save through NSDocument: a temp copy written in
/// the "item replacement" folder Foundation picks for the volume, swapped
/// in with `replaceItemAt`. On this drive that folder cannot live on the
/// volume (`.TemporaryItems` is refused), so Foundation must fall back
/// without failing the save. The server must get the new content under
/// the document's name, and nothing else.
#[tokio::test(flavor = "multi_thread")]
async fn an_nsdocument_safe_save_updates_the_file_and_uploads_only_it() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("Letter.txt", 9, "e1")]).await;
    env.serve_file_content("Letter.txt", b"Dear Ana,").await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (vfs, handle) = mounted(&env, &mountpoint).await;

    let doc = mountpoint.join("Letter.txt");
    let answer = foundation(&["safe-save", doc.to_str().unwrap(), "Dear Ana, see you Monday."]).await;
    assert!(answer.starts_with("ok"), "the save failed: {answer}");

    let doc_for_read = doc.clone();
    let content =
        tokio::task::spawn_blocking(move || std::fs::read(&doc_for_read).unwrap()).await.unwrap();
    assert_eq!(content, b"Dear Ana, see you Monday.");

    wait_for_upload(&vfs, &env, "Letter.txt", b"Dear Ana, see you Monday.").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_names(), vec!["Letter.txt".to_string()], "only the document may be uploaded");

    handle.unmount().await.unwrap();
}

/// Finder's "Move to Trash" on this drive. A network volume has no trash
/// of its own here (`.Trashes` is refused), so the file must either stay
/// put with an error (Finder then offers "Delete Immediately") or end up
/// in the user's own Trash — never in a hidden trash folder on the server.
#[tokio::test(flavor = "multi_thread")]
async fn move_to_trash_never_creates_a_trash_folder_on_the_server() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("old draft.txt", 5, "e1")]).await;
    env.serve_file_content("old draft.txt", b"draft").await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (_vfs, handle) = mounted(&env, &mountpoint).await;

    let doc = mountpoint.join("old draft.txt");
    let answer = foundation(&["trash", doc.to_str().unwrap()]).await;

    let doc_for_check = doc.clone();
    let still_there = tokio::task::spawn_blocking(move || doc_for_check.exists()).await.unwrap();
    if answer.starts_with("ok") {
        // Moved to the user's own Trash (a copy on this Mac), deleted here.
        assert!(!still_there, "trashed but still on the drive: {answer}");
        assert!(!answer.contains(&*mountpoint.to_string_lossy()), "trashed onto the drive itself: {answer}");
        assert_eq!(env.delete_call_count(), 1);
        if let Some(path) = answer.strip_prefix("ok trashed=") {
            let _ = std::fs::remove_file(path);
        }
    } else {
        assert!(still_there, "a failed trash must leave the file in place: {answer}");
        assert_eq!(env.delete_call_count(), 0);
    }
    assert_eq!(env.create_file_call_count(), 0, "no trash folder may be created on the server");
    assert!(env.uploaded_names().is_empty(), "nothing may be uploaded: {:?}", env.uploaded_names());

    handle.unmount().await.unwrap();
}

/// A Finder tag is an extended attribute, which this NFS mount keeps in a
/// local AppleDouble sidecar: setting one works and reads back, and never
/// reaches the server (tags are forgotten when the app quits — documented
/// limit).
#[tokio::test(flavor = "multi_thread")]
async fn a_finder_tag_works_locally_and_is_never_uploaded() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("Invoice.pdf", 3, "e1")]).await;
    env.serve_file_content("Invoice.pdf", b"pdf").await;
    let mp = tempfile::tempdir().unwrap();
    let mountpoint = mp.path().to_path_buf();
    let _guard = MountpointGuard(mountpoint.clone());
    let (vfs, handle) = mounted(&env, &mountpoint).await;

    let doc = mountpoint.join("Invoice.pdf");
    let answer = foundation(&["tag", doc.to_str().unwrap(), "Red"]).await;
    assert!(answer.starts_with("ok"), "tagging failed: {answer}");
    assert!(answer.contains("Red"), "the tag did not read back: {answer}");

    tokio::time::sleep(Duration::from_millis(600)).await;
    vfs.wait_for_writeback_idle().await;
    assert!(env.uploaded_names().is_empty(), "nothing may be uploaded: {:?}", env.uploaded_names());
    assert_eq!(env.create_file_call_count(), 0);

    handle.unmount().await.unwrap();
}
