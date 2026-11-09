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
//! so the day Apple removes the API, CI says so.
//!
//! The Favorites row shows the folder's own custom icon (Finder redraws the
//! row from it — what SidebarFavorites' documentation calls the custom icon
//! "fighting" its sidebar glyph on macOS 26), so setting the Cloudreve icon
//! on the folder is what puts the logo in the sidebar.

use std::ffi::c_void;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use objc2::rc::autoreleasepool;
use objc2::AllocAnyThread;
use objc2_app_kit::{NSImage, NSWorkspace, NSWorkspaceIconCreationOptions};
use objc2_foundation::{NSData, NSString};

use super::SidebarBackend;

const ICON_ICNS: &[u8] = include_bytes!("../../../../src-tauri/icons/icon.icns");

type CFTypeRef = *const c_void;

// kLSSharedFileListNoUserInteraction | kLSSharedFileListDoNotMountVolumes:
// resolving an entry must never pop a dialog nor mount a network volume
// (a stale entry for an old 127.0.0.1 NFS mount must not be remounted).
const RESOLVE_FLAGS: u32 = 1 | 2;

#[link(name = "CoreServices", kind = "framework")]
unsafe extern "C" {
    static kLSSharedFileListFavoriteItems: CFTypeRef;
    static kLSSharedFileListItemLast: CFTypeRef;
    fn LSSharedFileListCreate(alloc: CFTypeRef, list_type: CFTypeRef, options: CFTypeRef) -> CFTypeRef;
    fn LSSharedFileListCopySnapshot(list: CFTypeRef, seed: *mut u32) -> CFTypeRef;
    fn LSSharedFileListItemCopyResolvedURL(item: CFTypeRef, flags: u32, error: *mut CFTypeRef) -> CFTypeRef;
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
    fn CFRelease(cf: CFTypeRef);
    fn CFArrayGetCount(array: CFTypeRef) -> isize;
    fn CFArrayGetValueAtIndex(array: CFTypeRef, index: isize) -> CFTypeRef;
    fn CFURLCreateFromFileSystemRepresentation(alloc: CFTypeRef, buf: *const u8, len: isize, is_dir: u8) -> CFTypeRef;
    fn CFURLGetFileSystemRepresentation(url: CFTypeRef, resolve: u8, buf: *mut u8, max: isize) -> u8;
}

/// Owns one CoreFoundation reference.
struct Cf(CFTypeRef);

impl Cf {
    fn new(ptr: CFTypeRef) -> Option<Self> {
        (!ptr.is_null()).then_some(Self(ptr))
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

fn path_of_url(url: CFTypeRef) -> Option<PathBuf> {
    let mut buf = vec![0u8; 4096];
    let ok = unsafe { CFURLGetFileSystemRepresentation(url, 1, buf.as_mut_ptr(), buf.len() as isize) };
    if ok == 0 {
        return None;
    }
    let len = buf.iter().position(|&b| b == 0)?;
    Some(PathBuf::from(std::ffi::OsStr::from_bytes(&buf[..len])))
}

/// `folder` as Finder resolves it (`/tmp` → `/private/tmp`), or as given
/// when it cannot be resolved.
fn normalized(folder: &Path) -> PathBuf {
    std::fs::canonicalize(folder).unwrap_or_else(|_| folder.to_path_buf())
}

/// The Favorites items pointing to `folder`, with the list they belong to.
fn matching_items(folder: &Path) -> Result<(Cf, Cf, Vec<CFTypeRef>)> {
    let list = favorites()?;
    let mut seed = 0u32;
    let snapshot = Cf::new(unsafe { LSSharedFileListCopySnapshot(list.0, &mut seed) })
        .context("LSSharedFileListCopySnapshot returned NULL")?;
    let target = normalized(folder);
    let mut items = Vec::new();
    for i in 0..unsafe { CFArrayGetCount(snapshot.0) } {
        let item = unsafe { CFArrayGetValueAtIndex(snapshot.0, i) };
        let Some(url) = Cf::new(unsafe { LSSharedFileListItemCopyResolvedURL(item, RESOLVE_FLAGS, std::ptr::null_mut()) })
        else {
            continue;
        };
        if path_of_url(url.0).is_some_and(|p| p == target || p == folder) {
            items.push(item);
        }
    }
    // `items` borrow from `snapshot`, which the caller keeps alive.
    Ok((list, snapshot, items))
}

pub struct FinderSidebar;

impl SidebarBackend for FinderSidebar {
    fn contains(&self, folder: &Path) -> Result<bool> {
        Ok(!matching_items(folder)?.2.is_empty())
    }

    fn add(&self, folder: &Path, _label: &str) -> Result<()> {
        // Finder labels a Favorites row with the folder's own name whatever
        // name is passed here (cocoa-dev, "LSSharedFileListInsertItemURL
        // does not honor the display name or icon", 2013), so none is.
        let list = favorites()?;
        let url = url_of(folder)?;
        let item = unsafe {
            LSSharedFileListInsertItemURL(
                list.0,
                kLSSharedFileListItemLast,
                std::ptr::null(),
                std::ptr::null(),
                url.0,
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        let item = Cf::new(item).with_context(|| format!("Finder refused to add {} to Favorites", folder.display()))?;
        drop(item);
        Ok(())
    }

    fn remove(&self, folder: &Path) -> Result<()> {
        let (list, _snapshot, items) = matching_items(folder)?;
        for item in items {
            let status = unsafe { LSSharedFileListItemRemove(list.0, item) };
            if status != 0 {
                bail!("LSSharedFileListItemRemove failed for {}: OSStatus {status}", folder.display());
            }
        }
        Ok(())
    }

    fn set_icon(&self, folder: &Path) -> Result<()> {
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
        autoreleasepool(|_| {
            let path = NSString::from_str(&folder.to_string_lossy());
            if !NSWorkspace::sharedWorkspace().setIcon_forFile_options(None, &path, NSWorkspaceIconCreationOptions(0)) {
                bail!("NSWorkspace refused to clear the icon of {}", folder.display());
            }
            Ok(())
        })
    }
}
