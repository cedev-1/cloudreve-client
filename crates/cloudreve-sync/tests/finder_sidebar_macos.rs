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
    // A second drive's folder, also tagged: removing one drive must leave
    // the other's entry alone.
    let other = tmp.path().join("Cloudreve Second Drive");
    std::fs::create_dir(&other).unwrap();
    let cleanup_other = Cleanup(backend.clone(), other.clone());
    backend.add(&other, "Second").unwrap();

    // Everything is observed first and asserted only after the entry is
    // gone: a crash (a SIGTRAP from native code never unwinds) would skip
    // the guard and leave a dead entry in a developer's sidebar — which a
    // `CFRelease(NULL)` bug in the backend once did.
    let listed_before = backend.contains(&folder).unwrap();
    backend.add(&folder, "Cloudreve").unwrap();
    let listed_after_add = backend.contains(&folder).unwrap();
    // Every launch of an app that lost its `added` flag calls add again.
    backend.add(&folder, "Cloudreve").unwrap();
    backend.remove(&folder).unwrap();
    let listed_after_one_remove = backend.contains(&folder).unwrap();
    let other_kept = backend.contains(&other).unwrap();
    backend.remove(&other).unwrap();
    drop(cleanup_other);

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
    assert!(other_kept, "removing one drive's entry took another drive's entry too");
    assert!(icon_set, "the folder has no custom icon");
    assert!(icon_file, "AppKit stores the image in Icon\\r");
    assert!(!listed_at_end, "the entry is still in Favorites");
    assert!(!icon_at_end, "the custom icon was not cleared");
}

/// An on-demand drive's folder is a volume root: its logo is a volume icon
/// (`.VolumeIcon.icns` + the custom-icon bit), written without AppKit and
/// from a non-main thread — what `Mount::start` does. A real HFS+ RAM
/// volume stands in for the NFS mount (which this test cannot reach
/// locally); the real NFS root is covered by the test below, on CI.
/// Not ignored: it never touches the Finder sidebar.
#[test]
fn the_logo_on_a_volume_root_is_a_volume_icon_set_from_any_thread() {
    let Some(disk) = common::RamDisk::new(64, "CloudreveIconTest") else {
        eprintln!("SKIP: no RAM volume could be created");
        return;
    };
    let root = disk.mount_point.clone();
    let backend = sidebar::platform_backend();
    // Whatever else the root's FinderInfo holds must survive the flag.
    let existing = "0102030400000000000000000000000000000000000000000000000000000000";
    let ok = Command::new("/usr/bin/xattr").args(["-wx", "com.apple.FinderInfo", existing]).arg(&root).status().unwrap();
    assert!(ok.success());

    let b = backend.clone();
    let r = root.clone();
    std::thread::spawn(move || b.set_icon(&r)).join().unwrap().unwrap();
    let icon_set = has_custom_icon(&root);
    let kept = Command::new("/usr/bin/xattr").args(["-px", "com.apple.FinderInfo"]).arg(&root).output().unwrap();
    let kept = String::from_utf8_lossy(&kept.stdout).split_whitespace().take(4).collect::<Vec<_>>().join("");
    let icns = std::fs::read(root.join(".VolumeIcon.icns")).unwrap_or_default();
    let no_icon_file = !root.join("Icon\r").exists();

    backend.clear_icon(&root).unwrap();
    let cleared = !has_custom_icon(&root) && !root.join(".VolumeIcon.icns").exists();

    assert!(icon_set, "the volume root has no custom-icon bit");
    assert_eq!(kept, "01020304", "setting the flag wiped the rest of the FinderInfo");
    assert!(icns.len() > 1000 && &icns[..4] == b"icns", "no icns image at the volume root ({} bytes)", icns.len());
    assert!(no_icon_file, "a volume root must not get an Icon\\r file");
    assert!(cleared, "clearing left the volume icon behind");
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
    tokio::task::spawn_blocking(move || {
        eprintln!("E2E   resolving Favorites");
        let listed = backend.contains(&folder).unwrap_or(false);
        eprintln!("E2E   listed={listed}, reading FinderInfo");
        (listed, has_custom_icon(&folder))
    })
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
    // `E2E` checkpoints: a crash in native code kills the process without
    // a panic message; the last line printed says where.
    eprintln!("E2E start");
    let backend = sidebar::platform_backend();
    let mut env = TestEnv::with_real_mount_and_sidebar(backend.clone()).await;
    eprintln!("E2E mounted and started");
    let folder = env.sync_dir.clone();
    let _unmount = UnmountGuard(folder.clone());
    let cleanup = Cleanup(backend.clone(), folder.clone());

    let first = observe(backend.clone(), folder.clone()).await;
    let first_settings = env.mount.get_config().await.sidebar;
    eprintln!("E2E first observed {first:?}");

    env.mount.shutdown().await;
    eprintln!("E2E shut down (unmounted)");
    let while_unmounted = observe(backend.clone(), folder.clone()).await;
    eprintln!("E2E observed while unmounted {while_unmounted:?}");
    let mountpoint_entries = std::fs::read_dir(&folder).map(|d| d.count()).unwrap_or(usize::MAX);
    eprintln!("E2E mountpoint holds {mountpoint_entries} entries, relaunching");
    env.restart_mount().await;
    let after_relaunch = observe(backend.clone(), folder.clone()).await;
    eprintln!("E2E relaunched, observed {after_relaunch:?}");

    eprintln!("E2E deleting the drive");
    env.mount.delete().await.unwrap();
    eprintln!("E2E deleted");
    let after_delete = observe(backend.clone(), folder.clone()).await;
    drop(cleanup);

    assert_eq!(first, (true, true), "(listed, logo) right after the first start");
    assert!(first_settings.added, "the entry was not remembered");
    assert!(while_unmounted.0, "the entry must survive the app quitting");
    assert_eq!(mountpoint_entries, 0, "something was written into the bare mountpoint");
    assert_eq!(after_relaunch, (true, true), "(listed, logo) after a relaunch on a new NFS server");
    assert!(!after_delete.0, "removing the drive left it in Favorites");
}

/// The icon macOS RESOLVES for `path` (`NSURLEffectiveIconKey`, what Finder
/// and every open panel draw), rendered at 32x32, against the bundled
/// Cloudreve icon rendered the same way: the mean absolute difference per
/// RGBA byte (0 = identical; the generic file-server icon a bare NFS root
/// gets is ~74 away, measured on CI). Read in a FRESH process (this test
/// binary re-run on `resolved_icon_child`): IconServices caches icons per
/// process, so an in-process read can repeat an answer made before the
/// icon changed.
fn resolved_icon_distance_to_logo(path: &Path) -> f64 {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["resolved_icon_child", "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env("CLOUDREVE_ICON_PROBE", path)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Not line-anchored: the harness prints "test ... " on the same line.
    stdout
        .split("ICON-DISTANCE=")
        .nth(1)
        .and_then(|d| d.split_whitespace().next()?.parse().ok())
        .unwrap_or_else(|| panic!("the icon probe printed no distance: {stdout}{}", String::from_utf8_lossy(&out.stderr)))
}

/// Within this of the logo, the resolved icon IS the logo (measured on CI:
/// 0.0 for a volume icon, 1.6 for a folder's `Icon\r`, against 70+ for
/// every generic icon).
const SAME_ICON: f64 = 8.0;

/// The child half of [`resolved_icon_distance_to_logo`]. Does nothing
/// unless that function runs it.
#[test]
#[ignore]
fn resolved_icon_child() {
    use objc2::rc::{autoreleasepool, Retained};
    use objc2::runtime::AnyObject;
    use objc2::AllocAnyThread;
    use objc2_app_kit::{NSBitmapImageRep, NSCompositingOperation, NSDeviceRGBColorSpace, NSGraphicsContext, NSImage};
    use objc2_foundation::{NSData, NSPoint, NSRect, NSSize, NSString, NSURLEffectiveIconKey, NSURL};

    let Ok(path) = std::env::var("CLOUDREVE_ICON_PROBE") else { return };
    fn render(image: &NSImage) -> Vec<u8> {
        const PX: isize = 32;
        let rep = unsafe {
            NSBitmapImageRep::initWithBitmapDataPlanes_pixelsWide_pixelsHigh_bitsPerSample_samplesPerPixel_hasAlpha_isPlanar_colorSpaceName_bytesPerRow_bitsPerPixel(
                NSBitmapImageRep::alloc(), std::ptr::null_mut(), PX, PX, 8, 4, true, false, NSDeviceRGBColorSpace, 0, 0)
        }
        .unwrap();
        let ctx = NSGraphicsContext::graphicsContextWithBitmapImageRep(&rep).unwrap();
        NSGraphicsContext::saveGraphicsState_class();
        NSGraphicsContext::setCurrentContext(Some(&ctx));
        let rect = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(PX as f64, PX as f64));
        image.drawInRect_fromRect_operation_fraction(rect, NSRect::ZERO, NSCompositingOperation::Copy, 1.0);
        ctx.flushGraphics();
        NSGraphicsContext::restoreGraphicsState_class();
        unsafe { std::slice::from_raw_parts(rep.bitmapData(), (PX * PX * 4) as usize) }.to_vec()
    }
    autoreleasepool(|_| {
        let icns = include_bytes!("../../../src-tauri/icons/icon.icns");
        let logo = render(&NSImage::initWithData(NSImage::alloc(), &NSData::with_bytes(icns)).unwrap());
        let url = NSURL::fileURLWithPath(&NSString::from_str(&path));
        let mut value: Option<Retained<AnyObject>> = None;
        unsafe { url.getResourceValue_forKey_error(&mut value, NSURLEffectiveIconKey) }.unwrap();
        let icon = render(&value.expect("no effective icon").downcast::<NSImage>().unwrap());
        let distance = icon.iter().zip(&logo).map(|(a, b)| (*a as f64 - *b as f64).abs()).sum::<f64>() / logo.len() as f64;
        println!("ICON-DISTANCE={distance}");
    });
}

/// A mirror drive's folder is a plain folder: the logo `set_icon` puts on
/// it is the icon macOS resolves for it. Not ignored: it never touches the
/// Finder sidebar.
#[test]
fn a_mirror_drive_folder_resolves_to_the_cloudreve_logo() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("Cloudreve");
    std::fs::create_dir(&folder).unwrap();
    let backend = sidebar::platform_backend();

    let before = resolved_icon_distance_to_logo(&folder);
    backend.set_icon(&folder).unwrap();
    let after = resolved_icon_distance_to_logo(&folder);

    assert!(before > SAME_ICON, "a bare folder already resolves to the logo ({before})");
    assert!(after < SAME_ICON, "the folder does not resolve to the logo: distance {after}");
}

/// Plays Finder on the drive folder: lists it the moment it appears,
/// before the app gets to set its icon — what Finder does to a Favorites
/// entry whose volume just mounted, and to any window showing it.
/// Also records the icon the folder resolves to at that moment: what
/// Finder draws before the app has written anything through the mount.
struct BrowsedFirst(Arc<dyn SidebarBackend>, std::sync::Mutex<Vec<f64>>);

impl SidebarBackend for BrowsedFirst {
    fn contains(&self, folder: &Path) -> anyhow::Result<bool> {
        self.0.contains(folder)
    }
    fn add(&self, folder: &Path, label: &str) -> anyhow::Result<()> {
        self.0.add(folder, label)
    }
    fn remove(&self, folder: &Path) -> anyhow::Result<()> {
        self.0.remove(folder)
    }
    fn set_icon(&self, folder: &Path) -> anyhow::Result<()> {
        let listed = std::fs::read_dir(folder).map(|d| d.count());
        let distance = resolved_icon_distance_to_logo(folder);
        eprintln!("RESOLVED listed the folder before set_icon: {listed:?}, its icon is {distance} from the logo");
        self.1.lock().unwrap().push(distance);
        self.0.set_icon(folder)
    }
    fn clear_icon(&self, folder: &Path) -> anyhow::Result<()> {
        self.0.clear_icon(folder)
    }
    fn volume_root_files(&self, icon: bool) -> Vec<(&'static str, Vec<u8>)> {
        self.0.volume_root_files(icon)
    }
}

/// The on-demand drive's folder (the NFS mount root) resolves to the
/// Cloudreve logo even though Finder listed it before the app set the
/// icon, and again after a relaunch (new server, empty in-memory overlay).
/// It does from the moment it is mounted, before `set_icon` runs.
/// The root's FinderInfo lives in an AppleDouble `._.` inside the root;
/// once the client holds a listing of the root without it, Apple's NFS
/// client answers every `._.` lookup with ENOENT on its own (see
/// `cloudreve_vfs::nudge::NUDGE_NAME`) — the custom-icon bit could then be
/// neither set nor read, and Finder drew the generic volume icon.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn an_on_demand_drive_folder_resolves_to_the_cloudreve_logo_even_once_finder_listed_it() {
    let real = sidebar::platform_backend();
    let browsed = Arc::new(BrowsedFirst(real.clone(), Default::default()));
    let backend: Arc<dyn SidebarBackend> = browsed.clone();
    let mut env = TestEnv::with_real_mount_and_sidebar(backend.clone()).await;
    let folder = env.sync_dir.clone();
    let _unmount = UnmountGuard(folder.clone());
    let cleanup = Cleanup(real.clone(), folder.clone());

    let f = folder.clone();
    let first = tokio::task::spawn_blocking(move || {
        let fresh = resolved_icon_distance_to_logo(&f);
        // Browsed again later: the answer must not depend on luck.
        let _ = std::fs::read_dir(&f).map(|d| d.count());
        (fresh, resolved_icon_distance_to_logo(&f))
    })
    .await
    .unwrap();
    env.mount.shutdown().await;
    env.restart_mount().await;
    let f = folder.clone();
    let after_relaunch = tokio::task::spawn_blocking(move || resolved_icon_distance_to_logo(&f)).await.unwrap();
    env.mount.delete().await.unwrap();
    drop(cleanup);

    eprintln!("RESOLVED distances to the logo: first {first:?}, after a relaunch {after_relaunch}");
    // Before `set_icon`, at both starts: the seeded root alone shows the
    // logo, so Finder never gets to cache the generic icon first.
    let before_set_icon = browsed.1.lock().unwrap().clone();
    assert_eq!(before_set_icon.len(), 2, "set_icon did not run at both starts: {before_set_icon:?}");
    assert!(before_set_icon.iter().all(|d| *d < SAME_ICON), "the folder showed another icon before set_icon ran: {before_set_icon:?}");
    assert!(first.0 < SAME_ICON, "the mounted drive folder does not resolve to the logo: distance {}", first.0);
    assert!(first.1 < SAME_ICON, "the logo was lost once the folder was browsed again: distance {}", first.1);
    assert!(after_relaunch < SAME_ICON, "the logo is gone after a relaunch: distance {after_relaunch}");
}
