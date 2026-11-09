#![cfg(target_os = "macos")]
mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cloudreve_sync::sidebar::{self, SidebarBackend};
use objc2::rc::{autoreleasepool, Retained};
use objc2::runtime::AnyObject;
use objc2::AllocAnyThread;
use objc2_app_kit::*;
use objc2_foundation::*;

const PX: isize = 32;

fn pixels(img: &NSImage) -> Vec<u8> {
    unsafe {
        let rep = NSBitmapImageRep::initWithBitmapDataPlanes_pixelsWide_pixelsHigh_bitsPerSample_samplesPerPixel_hasAlpha_isPlanar_colorSpaceName_bytesPerRow_bitsPerPixel(
            NSBitmapImageRep::alloc(), std::ptr::null_mut(), PX, PX, 8, 4, true, false, NSDeviceRGBColorSpace, 0, 0).unwrap();
        let ctx = NSGraphicsContext::graphicsContextWithBitmapImageRep(&rep).unwrap();
        NSGraphicsContext::saveGraphicsState_class();
        NSGraphicsContext::setCurrentContext(Some(&ctx));
        let r = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(PX as f64, PX as f64));
        img.drawInRect_fromRect_operation_fraction(r, NSRect::ZERO, NSCompositingOperation::Copy, 1.0);
        ctx.flushGraphics();
        NSGraphicsContext::restoreGraphicsState_class();
        std::slice::from_raw_parts(rep.bitmapData(), (PX * PX * 4) as usize).to_vec()
    }
}

fn diff(a: &[u8], b: &[u8]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (*x as i32 - *y as i32).abs() as f64).sum::<f64>() / a.len() as f64
}

fn res(path: &str, key: &NSString) -> Option<Retained<AnyObject>> {
    let url = NSURL::fileURLWithPath(&NSString::from_str(path));
    let mut v: Option<Retained<AnyObject>> = None;
    unsafe { url.getResourceValue_forKey_error(&mut v, key).ok()? };
    v
}

fn measure(tag: &str, p: &str) {
    autoreleasepool(|_| {
        let logo = NSImage::initWithData(NSImage::alloc(), &NSData::with_bytes(include_bytes!("../../../src-tauri/icons/icon.icns"))).unwrap();
        let logo = pixels(&logo);
        let ws = NSWorkspace::sharedWorkspace();
        #[allow(deprecated)]
        let srvr = pixels(&ws.iconForFileType(&NSString::from_str("'srvr'")));
        #[allow(deprecated)]
        let hdsk = pixels(&ws.iconForFileType(&NSString::from_str("'hdsk'")));
        #[allow(deprecated)]
        let fldr = pixels(&ws.iconForFileType(&NSString::from_str("'fldr'")));
        let root = pixels(&ws.iconForFile(&NSString::from_str("/")));
        let eff = res(p, unsafe { NSURLEffectiveIconKey }).map(|i| pixels(&i.downcast::<NSImage>().unwrap()));
        let wsi = pixels(&ws.iconForFile(&NSString::from_str(p)));
        let d = |v: &[u8]| format!("logo={:.1} hdsk={:.1} root={:.1} srvr={:.1} fldr={:.1}", diff(v, &logo), diff(v, &hdsk), diff(v, &root), diff(v, &srvr), diff(v, &fldr));
        let keys = unsafe { [("isVolume", NSURLIsVolumeKey), ("local", NSURLVolumeIsLocalKey), ("browsable", NSURLVolumeIsBrowsableKey), ("internal", NSURLVolumeIsInternalKey), ("fmt", NSURLVolumeLocalizedFormatDescriptionKey)] };
        let kv: Vec<String> = keys.iter().map(|(n, k)| format!("{n}={:?}", res(p, k).map(|o| format!("{o:?}")))).collect();
        eprintln!("PROBE[{tag}] {p}\n  effective: {}\n  ws: {}\n  {}", eff.as_deref().map(d).unwrap_or("nil".into()), d(&wsi), kv.join(" "));
    });
}

fn measure_fresh(tag: &str, p: &Path) {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["probe_only", "--exact", "--nocapture", "--ignored"])
        .env("PROBE", p).env("PROBE_TAG", tag)
        .output().unwrap();
    for l in String::from_utf8_lossy(&out.stderr).lines().filter(|l| !l.starts_with("test ") && !l.is_empty()) {
        eprintln!("{l}");
    }
}

fn sh(cmd: &str) {
    let out = Command::new("/bin/sh").arg("-c").arg(cmd).output().unwrap();
    eprintln!("SH $ {cmd}\n{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
}

#[test]
#[ignore]
fn probe_only() {
    if let Ok(p) = std::env::var("PROBE") {
        measure(&std::env::var("PROBE_TAG").unwrap_or_default(), &p);
    }
}

struct Spy(Arc<dyn SidebarBackend>);
impl SidebarBackend for Spy {
    fn contains(&self, _: &Path) -> anyhow::Result<bool> { Ok(true) }
    fn add(&self, _: &Path, _: &str) -> anyhow::Result<()> { Ok(()) }
    fn remove(&self, _: &Path) -> anyhow::Result<()> { Ok(()) }
    fn set_icon(&self, folder: &Path) -> anyhow::Result<()> {
        measure_fresh("nfs: before set_icon", folder);
        sh(&format!("ls -la '{}'", folder.display()));
        let r = self.0.set_icon(folder);
        eprintln!("set_icon -> {r:?}");
        measure("nfs: after set_icon (in-proc)", &folder.to_string_lossy());
        measure_fresh("nfs: after set_icon (fresh proc)", folder);
        sh(&format!("ls -la '{0}' '{0}/.VolumeIcon.icns'; xattr -l '{0}'; xattr -px com.apple.FinderInfo '{0}'; mount | grep '{0}'", folder.display()));
        r
    }
    fn clear_icon(&self, folder: &Path) -> anyhow::Result<()> { self.0.clear_icon(folder) }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn probe_icons() {
    let real = sidebar::platform_backend();
    // Mirror: a plain folder through the real backend.
    let tmp = tempfile::tempdir().unwrap();
    let plain = tmp.path().join("Plain");
    std::fs::create_dir(&plain).unwrap();
    real.set_icon(&plain).unwrap();
    measure_fresh("plain folder after set_icon", &plain);
    // HFS RAM volume root.
    if let Some(disk) = common::RamDisk::new(64, "CrProbe") {
        real.set_icon(&disk.mount_point).unwrap();
        measure_fresh("hfs volume after set_icon", &disk.mount_point);
    }
    let backend: Arc<dyn SidebarBackend> = Arc::new(Spy(real.clone()));
    let mut env = common::TestEnv::with_real_mount_and_sidebar(backend.clone()).await;
    let folder = env.sync_dir.clone();
    let f2 = folder.clone();
    tokio::task::spawn_blocking(move || {
        // AppKit's own path on the NFS root.
        let script = "ObjC.import('AppKit'); function run(a) { var img = $.NSImage.alloc.initWithContentsOfFile(a[1]); return $.NSWorkspace.sharedWorkspace.setIconForFileOptions(img, a[0], 0); }";
        let icns = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src-tauri/icons/icon.icns");
        sh(&format!("osascript -l JavaScript -e \"{script}\" '{}' '{}'", f2.display(), icns.display()));
        measure_fresh("nfs: after AppKit setIcon", &f2);
        sh(&format!("ls -la '{0}'; xattr -l '{0}'", f2.display()));
    }).await.unwrap();
    eprintln!("=== relaunch");
    env.restart_mount().await;
    env.mount.delete().await.unwrap();
    let _ = Command::new("/sbin/umount").arg("-f").arg(&folder).output();
    let _: PathBuf = folder;
}
