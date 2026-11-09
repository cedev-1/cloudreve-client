//! The REAL macOS sidebar backend (`sidebar::platform_backend()`, what the
//! `DriveManager` installs) against Finder's real Favorites list, on a
//! throwaway folder. Proves on every CI run that the deprecated
//! `LSSharedFileList` still adds, finds and removes an entry, and that the
//! icon lands on the folder.
//!
//! `#[ignore]`d: it edits the Finder sidebar of whoever runs it (briefly —
//! the guard below removes the entry even on a panic). CI runs it on a
//! throwaway runner with `-- --ignored`; locally:
//! `cargo test -p cloudreve-sync --test finder_sidebar_macos -- --ignored`.
#![cfg(target_os = "macos")]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cloudreve_sync::sidebar::{self, SidebarBackend};
use common::TestEnv;

/// Takes the folder back out of Favorites whatever happens.
struct Cleanup(Arc<dyn SidebarBackend>, PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = self.0.remove(&self.1);
    }
}

/// The custom-icon bit (kHasCustomIcon, 0x0400 in the Finder flags at
/// bytes 8-9) of `folder`'s `com.apple.FinderInfo`.
fn has_custom_icon(folder: &Path) -> bool {
    let out = Command::new("/usr/bin/xattr").args(["-px", "com.apple.FinderInfo"]).arg(folder).output().unwrap();
    let bytes: Vec<u8> = String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect();
    bytes.len() == 32 && bytes[8] & 0x04 != 0
}

#[test]
#[ignore]
fn a_folder_is_added_to_finder_favorites_with_the_logo_and_removed_again() {
    // Under `/tmp` (a symlink to `/private/tmp`): Finder resolves the
    // entry to the real path, the drive folder may be given either way.
    let tmp = tempfile::Builder::new().prefix("cloudreve-sidebar-").tempdir_in("/tmp").unwrap();
    let folder = tmp.path().join("Cloudreve Sidebar Test");
    std::fs::create_dir(&folder).unwrap();
    let backend = sidebar::platform_backend();
    let cleanup = Cleanup(backend.clone(), folder.clone());

    // Everything is observed first and asserted only after the entry is
    // gone: a failing assertion once crashed the process (SIGTRAP, AppKit
    // loaded, libtest capturing output) before the guard could run, and
    // left a dead entry in a developer's sidebar.
    let listed_before = backend.contains(&folder).unwrap();
    backend.add(&folder, "Cloudreve").unwrap();
    let listed_after_add = backend.contains(&folder).unwrap();
    // Every launch of an app that lost its `added` flag calls add again.
    backend.add(&folder, "Cloudreve").unwrap();
    backend.remove(&folder).unwrap();
    let listed_after_one_remove = backend.contains(&folder).unwrap();

    backend.add(&folder, "Cloudreve").unwrap();
    backend.set_icon(&folder).unwrap();
    let icon_set = has_custom_icon(&folder);
    let icon_file = folder.join("Icon\r").exists();

    backend.remove(&folder).unwrap();
    backend.clear_icon(&folder).unwrap();
    let listed_at_end = backend.contains(&folder).unwrap();
    let icon_at_end = has_custom_icon(&folder);
    drop(cleanup);

    assert!(!listed_before, "a fresh folder cannot already be a favorite");
    assert!(listed_after_add, "LSSharedFileList did not add the folder");
    assert!(!listed_after_one_remove, "a second add left a duplicate behind");
    assert!(icon_set, "the folder has no custom icon");
    assert!(icon_file, "AppKit stores the image in Icon\\r");
    assert!(!listed_at_end, "the entry is still in Favorites");
    assert!(!icon_at_end, "the custom icon was not cleared");
}

/// Force-unmounts `path` on drop, whatever the test did.
struct UnmountGuard(PathBuf);

impl Drop for UnmountGuard {
    fn drop(&mut self) {
        let _ = Command::new("/sbin/umount").arg("-f").arg(&self.0).output();
    }
}

/// `(listed in Favorites, custom icon on the folder)`, read off the async
/// runtime: the NFS server answering those reads runs on it.
async fn observe(backend: Arc<dyn SidebarBackend>, folder: PathBuf) -> (bool, bool) {
    tokio::task::spawn_blocking(move || (backend.contains(&folder).unwrap_or(false), has_custom_icon(&folder)))
        .await
        .unwrap()
}

/// The production path end to end on a REAL on-demand mount: `Mount::start`
/// puts the mounted folder in Favorites and sets the logo on the NFS root;
/// after a relaunch (old NFS server gone, a new one on another port) the
/// entry made on the old mount still resolves to the folder — so the app
/// neither duplicates it nor believes the user removed it — and the logo,
/// lost with the old server's memory, is back; removing the drive takes the
/// entry away.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn an_on_demand_drive_keeps_its_favorite_and_logo_across_a_relaunch() {
    let backend = sidebar::platform_backend();
    let mut env = TestEnv::with_real_mount_and_sidebar(backend.clone()).await;
    let folder = env.sync_dir.clone();
    let _unmount = UnmountGuard(folder.clone());
    let cleanup = Cleanup(backend.clone(), folder.clone());

    let first = observe(backend.clone(), folder.clone()).await;
    let first_settings = env.mount.get_config().await.sidebar;

    env.mount.shutdown().await;
    let while_unmounted = observe(backend.clone(), folder.clone()).await;
    let mountpoint_entries = std::fs::read_dir(&folder).map(|d| d.count()).unwrap_or(usize::MAX);
    env.restart_mount().await;
    let after_relaunch = observe(backend.clone(), folder.clone()).await;

    env.mount.delete().await.unwrap();
    let after_delete = observe(backend.clone(), folder.clone()).await;
    drop(cleanup);

    assert_eq!(first, (true, true), "(listed, logo) right after the first start");
    assert!(first_settings.added, "the entry was not remembered");
    assert!(while_unmounted.0, "the entry must survive the app quitting");
    assert_eq!(mountpoint_entries, 0, "something was written into the bare mountpoint");
    assert_eq!(after_relaunch, (true, true), "(listed, logo) after a relaunch on a new NFS server");
    assert!(!after_delete.0, "removing the drive left it in Favorites");
}
