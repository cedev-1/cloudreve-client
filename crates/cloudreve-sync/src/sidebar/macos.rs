//! macOS sidebar backend: Finder's Favorites through `LSSharedFileList`,
//! the folder icon through `NSWorkspace.setIcon(_:forFile:options:)`.
//!
//! `LSSharedFileList` has been deprecated since OS X 10.11 with no
//! replacement — the only supported way to get a Favorites entry is a File
//! Provider extension, which an unsigned app cannot ship. It still works:
//! measured on macOS 27.0.1 (build 26A434) from an unsigned, unsandboxed
//! binary, `LSSharedFileListInsertItemURL` added a folder that a fresh
//! process listed back, inserting the same URL twice left one entry, and
//! `LSSharedFileListItemRemove` took it away. The alternatives are worse:
//! `sfltool` only lists/clears/resets, and the `.sfl3` store under
//! `~/Library/Application Support/com.apple.sharedfilelist` is TCC-protected
//! (EPERM even to read it from a terminal) and owned by `sharedfilelistd`.
//! `tests/finder_sidebar_macos.rs` re-proves the round trip on every CI run,
//! so the day Apple removes the API, CI says so. Entries are found by a tag
//! property set at insert time, never by resolving them (see [`TAG`]).
//!
//! The folder gets the Cloudreve icon (Get Info, Finder windows, Dock), but
//! the Favorites row does NOT show it: Finder draws only template glyphs in
//! its sidebar (`ForceTemplateIcons`), and a folder with a custom icon gets
//! the generic folder glyph there (the NFS volume root, the generic network
//! one) — checked by eye on macOS 27.0.1. The row's Cloudreve glyph comes
//! from the Finder Sync extension instead (`super::finder_sync`, fed by
//! `set_glyph_folders`). A mirror folder gets
//! its icon through `NSWorkspace` (an `Icon\r` file); an on-demand drive's folder
//! is a volume root and gets a volume icon instead ([`set_volume_icon`]). Its
//! files are seeded into the vfs before mounting
//! (`SidebarBackend::volume_root_files`): written only after mounting, the
//! custom-icon bit could no longer be set once Finder had listed the
//! folder, which kept the generic volume icon.

use std::ffi::c_void;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use cloudreve_vfs::appledouble;
use objc2::rc::autoreleasepool;
use objc2::AllocAnyThread;
use objc2_app_kit::{NSImage, NSWorkspace, NSWorkspaceIconCreationOptions};
use objc2_foundation::{NSData, NSString};

use super::finder_sync::FinderSyncGlyph;
use super::SidebarBackend;

const ICON_ICNS: &[u8] = include_bytes!("../../../../src-tauri/icons/icon.icns");

type CFTypeRef = *const c_void;

/// Item property tagging the Favorites entries this app added, valued with
/// the drive folder's path. Entries are found by it, never by resolving
/// their bookmark: the app looks its entry up right when the on-demand
/// volume is NOT mounted (removing a drive, after its unmount; the toggle
/// of a paused drive), and resolving an entry of an unmounted volume
/// returns NULL (measured on a detached RAM volume, macOS 27) — the entry
/// could not be found, so not removed. Resolving also stats the target,
/// which a dead NFS server can hang. A property read touches neither the
/// file system nor the network. Measured on macOS 27.0.1: set at insert time, a fresh process
/// reads it back. Per-item properties are what SidebarFavorites relies on
/// too (`com.apple.LSSharedFileList.OverrideIcon.OSType`).
const TAG: &std::ffi::CStr = c"org.cloudreve.client.sync-path";

#[link(name = "CoreServices", kind = "framework")]
unsafe extern "C" {
    static kLSSharedFileListFavoriteItems: CFTypeRef;
    static kLSSharedFileListItemLast: CFTypeRef;
    fn LSSharedFileListCreate(alloc: CFTypeRef, list_type: CFTypeRef, options: CFTypeRef) -> CFTypeRef;
    fn LSSharedFileListCopySnapshot(list: CFTypeRef, seed: *mut u32) -> CFTypeRef;
    fn LSSharedFileListItemCopyProperty(item: CFTypeRef, name: CFTypeRef) -> CFTypeRef;
    fn LSSharedFileListInsertItemURL(
        list: CFTypeRef,
        after: CFTypeRef,
        name: CFTypeRef,
        icon: CFTypeRef,
        url: CFTypeRef,
        properties: CFTypeRef,
        properties_to_clear: CFTypeRef,
    ) -> CFTypeRef;
    fn LSSharedFileListItemRemove(list: CFTypeRef, item: CFTypeRef) -> i32;
}

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    static kCFTypeDictionaryKeyCallBacks: c_void;
    static kCFTypeDictionaryValueCallBacks: c_void;
    fn CFRelease(cf: CFTypeRef);
    fn CFEqual(a: CFTypeRef, b: CFTypeRef) -> u8;
    fn CFArrayGetCount(array: CFTypeRef) -> isize;
    fn CFArrayGetValueAtIndex(array: CFTypeRef, index: isize) -> CFTypeRef;
    fn CFURLCreateFromFileSystemRepresentation(alloc: CFTypeRef, buf: *const u8, len: isize, is_dir: u8) -> CFTypeRef;
    fn CFStringCreateWithBytes(alloc: CFTypeRef, bytes: *const u8, len: isize, encoding: u32, external: u8) -> CFTypeRef;
    fn CFDictionaryCreate(
        alloc: CFTypeRef,
        keys: *const CFTypeRef,
        values: *const CFTypeRef,
        count: isize,
        key_callbacks: *const c_void,
        value_callbacks: *const c_void,
    ) -> CFTypeRef;
}

const UTF8: u32 = 0x0800_0100; // kCFStringEncodingUTF8

/// Owns one CoreFoundation reference.
struct Cf(CFTypeRef);

impl Cf {
    fn new(ptr: CFTypeRef) -> Option<Self> {
        // Not `then_some(Self(ptr))`: that builds the `Cf` eagerly and drops
        // it on NULL, and `CFRelease(NULL)` traps (SIGTRAP) — which is what
        // killed the first CI runs as soon as any CF call returned NULL.
        if ptr.is_null() { None } else { Some(Self(ptr)) }
    }
}

impl Drop for Cf {
    fn drop(&mut self) {
        unsafe { CFRelease(self.0) }
    }
}

fn favorites() -> Result<Cf> {
    let list = unsafe { LSSharedFileListCreate(std::ptr::null(), kLSSharedFileListFavoriteItems, std::ptr::null()) };
    Cf::new(list).context("LSSharedFileListCreate(FavoriteItems) returned NULL")
}

fn url_of(path: &Path) -> Result<Cf> {
    let bytes = path.as_os_str().as_bytes();
    let url = unsafe { CFURLCreateFromFileSystemRepresentation(std::ptr::null(), bytes.as_ptr(), bytes.len() as isize, 1) };
    Cf::new(url).with_context(|| format!("not a valid file URL: {}", path.display()))
}

fn cf_string(s: &[u8]) -> Result<Cf> {
    Cf::new(unsafe { CFStringCreateWithBytes(std::ptr::null(), s.as_ptr(), s.len() as isize, UTF8, 0) })
        .context("CFStringCreateWithBytes returned NULL")
}

/// The tag value for `folder`: its path exactly as the drive config holds
/// it (no `canonicalize`: that would stat a possibly dead mount).
fn tag_value(folder: &Path) -> Result<Cf> {
    cf_string(folder.as_os_str().as_bytes())
}

/// The Favorites items this app tagged for `folder`, with the list they
/// belong to and the snapshot they borrow from (kept alive by the caller).
fn tagged_items(folder: &Path) -> Result<(Cf, Cf, Vec<CFTypeRef>)> {
    let list = favorites()?;
    let mut seed = 0u32;
    let snapshot = Cf::new(unsafe { LSSharedFileListCopySnapshot(list.0, &mut seed) })
        .context("LSSharedFileListCopySnapshot returned NULL")?;
    let key = cf_string(TAG.to_bytes())?;
    let want = tag_value(folder)?;
    let mut items = Vec::new();
    for i in 0..unsafe { CFArrayGetCount(snapshot.0) } {
        let item = unsafe { CFArrayGetValueAtIndex(snapshot.0, i) };
        if let Some(value) = Cf::new(unsafe { LSSharedFileListItemCopyProperty(item, key.0) })
            && unsafe { CFEqual(value.0, want.0) } != 0
        {
            items.push(item);
        }
    }
    Ok((list, snapshot, items))
}

/// Whether `folder` is the root of a mounted volume (an on-demand drive's
/// NFS mount): a different device than its parent.
fn is_mount_point(folder: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(folder), folder.parent().map(std::fs::metadata)) {
        (Ok(dir), Some(Ok(parent))) => dir.dev() != parent.dev(),
        _ => false,
    }
}

const FINDER_INFO: &std::ffi::CStr = c"com.apple.FinderInfo";
/// kHasCustomIcon, in the Finder flags at bytes 8-9 of FinderInfo.
const HAS_CUSTOM_ICON: u8 = 0x04;

/// The 32-byte FinderInfo of `path` with the custom-icon bit set or
/// cleared. Everything else in it (type, creator, label) is kept.
fn set_custom_icon_flag(path: &Path, on: bool) -> Result<()> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut info = [0u8; 32];
    let read = unsafe { libc::getxattr(c_path.as_ptr(), FINDER_INFO.as_ptr(), info.as_mut_ptr().cast(), info.len(), 0, 0) };
    if read != 32 {
        info = [0u8; 32];
    }
    if on {
        info[8] |= HAS_CUSTOM_ICON;
    } else {
        info[8] &= !HAS_CUSTOM_ICON;
    }
    let rc = unsafe { libc::setxattr(c_path.as_ptr(), FINDER_INFO.as_ptr(), info.as_ptr().cast(), info.len(), 0, 0) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("failed to set the FinderInfo of {}", path.display()));
    }
    Ok(())
}

/// A volume root's icon, the way Finder has always read it: the image in
/// `.VolumeIcon.icns` at the root, the custom-icon bit in the root's
/// FinderInfo. AppKit writes the same thing for a volume root (measured on
/// an HFS+ RAM volume), but plain file I/O keeps AppKit off the NFS mount
/// and off this blocking thread, and its result is pinned by tests
/// (`tests/finder_sidebar_macos.rs`). On the mount both names
/// are local-only junk (`cloudreve_vfs::junk`): never uploaded.
/// On the mount the FinderInfo lives in the root's `._.` sidecar, which
/// only exists because `volume_root_files` seeded it before mounting
/// (see `cloudreve_vfs::appledouble`): this is what the settings toggle
/// and every start go through, the seed is what the first look gets.
fn set_volume_icon(root: &Path) -> Result<()> {
    let icns = root.join(".VolumeIcon.icns");
    std::fs::write(&icns, ICON_ICNS).with_context(|| format!("failed to write {}", icns.display()))?;
    set_custom_icon_flag(root, true)
}

fn clear_volume_icon(root: &Path) -> Result<()> {
    match std::fs::remove_file(root.join(".VolumeIcon.icns")) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("failed to remove .VolumeIcon.icns"),
    }
    set_custom_icon_flag(root, false)
}

pub struct FinderSidebar {
    glyph: FinderSyncGlyph,
}

impl FinderSidebar {
    pub fn new() -> Self {
        Self { glyph: FinderSyncGlyph::for_this_app() }
    }
}

impl SidebarBackend for FinderSidebar {
    fn contains(&self, folder: &Path) -> Result<bool> {
        Ok(!tagged_items(folder)?.2.is_empty())
    }

    fn add(&self, folder: &Path, _label: &str) -> Result<()> {
        // Finder labels a Favorites row with the folder's own name whatever
        // name is passed here (cocoa-dev, "LSSharedFileListInsertItemURL
        // does not honor the display name or icon", 2013), so none is.
        let list = favorites()?;
        let url = url_of(folder)?;
        let key = cf_string(TAG.to_bytes())?;
        let value = tag_value(folder)?;
        let properties = Cf::new(unsafe {
            CFDictionaryCreate(
                std::ptr::null(),
                &key.0,
                &value.0,
                1,
                &kCFTypeDictionaryKeyCallBacks,
                &kCFTypeDictionaryValueCallBacks,
            )
        })
        .context("CFDictionaryCreate returned NULL")?;
        // Inserting a URL already in Favorites updates that entry (one row,
        // measured): a re-add, or the folder already there by hand, ends up
        // as one tagged entry.
        let item = unsafe {
            LSSharedFileListInsertItemURL(
                list.0,
                kLSSharedFileListItemLast,
                std::ptr::null(),
                std::ptr::null(),
                url.0,
                properties.0,
                std::ptr::null(),
            )
        };
        let item = Cf::new(item).with_context(|| format!("Finder refused to add {} to Favorites", folder.display()))?;
        drop(item);
        Ok(())
    }

    fn remove(&self, folder: &Path) -> Result<()> {
        let (list, _snapshot, items) = tagged_items(folder)?;
        for item in items {
            let status = unsafe { LSSharedFileListItemRemove(list.0, item) };
            if status != 0 {
                bail!("LSSharedFileListItemRemove failed for {}: OSStatus {status}", folder.display());
            }
        }
        Ok(())
    }

    fn set_icon(&self, folder: &Path) -> Result<()> {
        if is_mount_point(folder) {
            return set_volume_icon(folder);
        }
        autoreleasepool(|_| {
            let data = NSData::with_bytes(ICON_ICNS);
            let image = NSImage::initWithData(NSImage::alloc(), &data).context("the bundled icon.icns does not decode")?;
            let path = NSString::from_str(&folder.to_string_lossy());
            if !NSWorkspace::sharedWorkspace().setIcon_forFile_options(Some(&image), &path, NSWorkspaceIconCreationOptions(0)) {
                bail!("NSWorkspace refused to set the icon of {}", folder.display());
            }
            Ok(())
        })
    }

    fn clear_icon(&self, folder: &Path) -> Result<()> {
        if is_mount_point(folder) {
            return clear_volume_icon(folder);
        }
        autoreleasepool(|_| {
            let path = NSString::from_str(&folder.to_string_lossy());
            if !NSWorkspace::sharedWorkspace().setIcon_forFile_options(None, &path, NSWorkspaceIconCreationOptions(0)) {
                bail!("NSWorkspace refused to clear the icon of {}", folder.display());
            }
            Ok(())
        })
    }

    fn volume_root_files(&self, icon: bool) -> Vec<(&'static str, Vec<u8>)> {
        // The sidecar is seeded even without the logo: `set_icon` from the
        // settings toggle can only set the bit in a sidecar that exists.
        let finder_info = appledouble::with_custom_icon([0; 32], icon);
        let mut files = vec![(appledouble::ROOT_SIDECAR_NAME, appledouble::finder_info_sidecar(finder_info))];
        if icon {
            files.push((".VolumeIcon.icns", ICON_ICNS.to_vec()));
        }
        files
    }

    fn set_glyph_folders(&self, folders: &[std::path::PathBuf]) -> Result<()> {
        self.glyph.publish(folders)
    }

    fn glyph_folder_remounted(&self) {
        self.glyph.remounted();
    }
}
