//! OS metadata junk (macOS, plus Linux desktops and office suites): names
//! the OS or an app writes on its own and that must never reach the Cloudreve server.
//!
//! NFSv3 has no extended attributes, so whenever macOS sets one on a file
//! of the mount (quarantine and provenance on every download, Finder tags,
//! resource forks, `cp`/`ditto` preserving them) the kernel falls back to
//! an AppleDouble sidecar named `._<file>` next to it. Finder adds a
//! `.DS_Store` to every folder it shows. Uploading those would litter the
//! user's cloud (and every other device syncing it) with files nobody
//! created on purpose.
//!
//! Two kinds, decided per name:
//!
//! - [`JunkKind::LocalOnly`] — `._*`, `.DS_Store`, `.VolumeIcon.icns`, `Icon\r`, and
//!   the app's own `.cloudreve-refresh` (`crate::nudge`):
//!   creating, writing, reading, renaming and deleting them all SUCCEED,
//!   but the bytes live only in the facade's in-memory overlay (see
//!   `Vfs`'s `junk` field) and are never uploaded. Refusing them instead
//!   (what hf-mount and fusekit ship: EACCES on create) makes ordinary
//!   copies fail. Measured on the macos-latest CI runner with a file
//!   carrying one xattr (`measure_macos_copies_when_sidecars_are_refused`
//!   in `tests/mounted_macos.rs`): `cp` and `cp -p` exit 1 ("could not copy
//!   extended attributes to ...: Operation not permitted"), `ditto` exits 1
//!   on its temp file, `xattr -w` fails with EPERM. With the overlay all
//!   four succeed. The cost of the overlay is that xattrs set
//!   through the mount are forgotten when the app quits.
//! - [`JunkKind::Reserved`] — the macOS volume-level folders (Spotlight
//!   index, Trash, FSEvents log, `.TemporaryItems`, revisions, AFP
//!   leftovers) and `__MACOSX` (zip extraction debris): refused with
//!   EACCES. The overlay holds plain files only, never folders; none of
//!   the copy paths above needs one. Not verified against Finder itself:
//!   it is expected to delete immediately on a network volume instead of
//!   using `.Trashes`, and Cocoa safe-saves to fall back to a sibling temp
//!   file when `.TemporaryItems` cannot be created.
//!
//! Both kinds are hidden from listings, including copies that already sit
//! on the server (uploaded by another client): a Cloudreve-side
//! `.DS_Store` is never shown, never resolved by name, and never
//! overwritten by the local one. A folder holding nothing but junk
//! therefore lists as empty and `rmdir` deletes it (the server takes the
//! junk with it) instead of failing with ENOTEMPTY on a folder that looks
//! empty to every tool.
//!
//! The list is written down independently of `cloudreve-sync`'s
//! `drive/ignore.rs` (this crate cannot depend on it) but stays consistent
//! with its macOS entries: same names, same case-insensitive matching
//! (the OS is not consistent about the case it writes). Additions here
//! that the mirror engine does not ignore: `.VolumeIcon.icns` and
//! `__MACOSX`.

/// What a junk name is allowed to do on the mount. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JunkKind {
    LocalOnly,
    Reserved,
}

// `crate::nudge::NUDGE_NAME`: the app's own folder-refresh probe.
// `Icon\r`: a folder's custom icon (`NSWorkspace.setIcon` on the drive
// root, see `cloudreve_sync::sidebar`), its bytes in the `._Icon\r` sidecar.
// `.directory`: KDE Dolphin's per-folder view settings.
const LOCAL_ONLY_NAMES: &[&str] =
    &[".DS_Store", ".VolumeIcon.icns", ".cloudreve-refresh", "Icon\r", ".directory"];

/// Lock files an office suite keeps next to an open document and deletes
/// on close: LibreOffice's `.~lock.<name>#` and MS Office's `~$<name>`.
/// They are never renamed onto a real name, unlike the `~WRL*.tmp`,
/// `lu*.tmp` and `.goutputstream-*` temp files Word, LibreOffice and GTK
/// apps save through: those must stay ordinary files (junk cannot be
/// renamed onto a real name, so classifying them would break every save).
fn is_office_lock_file(name: &str) -> bool {
    (name.starts_with(".~lock.") && name.ends_with('#') && name.len() > ".~lock.#".len())
        || (name.starts_with("~$") && name.len() > 2)
}

/// GNOME (gio) and KDE trash a file that lives on another volume by moving
/// it into `<volume root>/.Trash-<uid>` (or a pre-existing `.Trash/<uid>`).
/// Refused like macOS's `.Trashes`: the file manager then offers to delete
/// permanently, instead of "trashing" into a hidden cloud folder that
/// keeps using quota and shows up on every other device.
fn is_linux_trash_dir(name: &str) -> bool {
    if name.eq_ignore_ascii_case(".Trash") {
        return true;
    }
    match name.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case(".Trash-") => {
            let uid = &name[7..];
            !uid.is_empty() && uid.bytes().all(|b| b.is_ascii_digit())
        }
        _ => false,
    }
}

const RESERVED_NAMES: &[&str] = &[
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    ".TemporaryItems",
    ".DocumentRevisions-V100",
    ".AppleDouble",
    ".AppleDB",
    ".AppleDesktop",
    "__MACOSX",
    // Served read-only at the volume root by the NFS adapter
    // (`nfs::NEVER_INDEX_NAME`); reserved everywhere else.
    ".metadata_never_index",
];

/// Classifies one path component (a bare name, never a path).
pub fn classify(name: &str) -> Option<JunkKind> {
    if name.starts_with("._")
        || LOCAL_ONLY_NAMES.iter().any(|n| n.eq_ignore_ascii_case(name))
        || is_office_lock_file(name)
    {
        return Some(JunkKind::LocalOnly);
    }
    if RESERVED_NAMES.iter().any(|n| n.eq_ignore_ascii_case(name)) || is_linux_trash_dir(name) {
        return Some(JunkKind::Reserved);
    }
    None
}

/// Whether `remote_path`'s own name is local-only junk. Only the last
/// component matters: the overlay never holds folders, so nothing local-
/// only can sit under a junk name.
pub fn is_local_only_path(remote_path: &str) -> bool {
    let name = remote_path.rsplit('/').next().unwrap_or(remote_path);
    classify(name) == Some(JunkKind::LocalOnly)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appledouble_sidecars_and_finder_files_are_local_only() {
        for name in ["._photo.jpg", "._.DS_Store", "._", ".DS_Store", ".ds_store", ".VolumeIcon.icns", "Icon\r"] {
            assert_eq!(classify(name), Some(JunkKind::LocalOnly), "{name}");
        }
    }

    #[test]
    fn volume_folders_and_zip_debris_are_reserved() {
        for name in [".Spotlight-V100", ".Trashes", ".trashes", ".fseventsd", ".TemporaryItems", "__MACOSX"] {
            assert_eq!(classify(name), Some(JunkKind::Reserved), "{name}");
        }
    }

    #[test]
    fn ordinary_and_lookalike_names_are_not_junk() {
        for name in ["photo.jpg", ".gitignore", "_photo.jpg", ".DS_Store.txt", "My.DS_Store", "Trashes", ".env", "Icon", "Icon.png"] {
            assert_eq!(classify(name), None, "{name}");
        }
    }

    /// Linux desktops: KDE folder view settings, LibreOffice and MS Office
    /// lock files are written while a document is open and must never reach
    /// the cloud (every other device would see them appear and vanish).
    #[test]
    fn linux_desktop_and_office_lock_files_are_local_only() {
        for name in [
            ".directory",
            ".~lock.Budget 2026.ods#",
            ".~lock.report.docx#",
            "~$report.docx",
            "~$Q3 slides.pptx",
        ] {
            assert_eq!(classify(name), Some(JunkKind::LocalOnly), "{name}");
        }
    }

    /// GNOME (gio) and KDE trash a file on another volume by moving it into
    /// `.Trash-<uid>` at the volume root. Refusing that folder makes the
    /// file manager offer to delete permanently instead of silently keeping
    /// the "trashed" file in the cloud under a hidden folder.
    #[test]
    fn volume_trash_folders_of_linux_desktops_are_reserved() {
        for name in [".Trash", ".Trash-1000", ".Trash-1001", ".trash-1000"] {
            assert_eq!(classify(name), Some(JunkKind::Reserved), "{name}");
        }
    }

    /// Temp files that editors rename ONTO the real name when saving must
    /// stay ordinary files: junk can never be renamed onto a real name, so
    /// classifying them would make every save fail.
    #[test]
    fn atomic_save_temp_files_and_lookalikes_are_not_junk() {
        for name in [
            ".goutputstream-ABC123",
            "~WRL0001.tmp",
            "lu12345abc.tmp",
            ".report.txt.swp",
            "notes.txt~",
            ".hidden",
            ".~lock.ods",
            "~report.docx",
            ".Trash-files",
            ".Trash-",
            "Trash-1000",
        ] {
            assert_eq!(classify(name), None, "{name}");
        }
    }

    #[test]
    fn only_the_last_path_component_decides() {
        assert!(is_local_only_path("cloudreve://my/sync/docs/._report.pdf"));
        assert!(!is_local_only_path("cloudreve://my/sync/._dir/report.pdf"));
    }
}
