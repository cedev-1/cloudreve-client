//! macOS metadata junk: names the OS writes on its own and that must never
//! reach the Cloudreve server.
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
//! - [`JunkKind::LocalOnly`] — `._*`, `.DS_Store`, `.VolumeIcon.icns`:
//!   creating, writing, reading, renaming and deleting them all SUCCEED,
//!   but the bytes live only in the facade's in-memory overlay (see
//!   `Vfs`'s `junk` field) and are never uploaded. Refusing them instead
//!   (what hf-mount and fusekit ship: EACCES on create) makes ordinary
//!   copies fail: macOS `cp` exits 1 ("could not copy extended attributes")
//!   and `xattr -w` fails whenever the sidecar cannot be created — see
//!   `tests/mounted_macos.rs`. The cost of the overlay is that xattrs set
//!   through the mount are forgotten when the app quits.
//! - [`JunkKind::Reserved`] — the macOS volume-level folders (Spotlight
//!   index, Trash, FSEvents log, `.TemporaryItems`, revisions, AFP
//!   leftovers) and `__MACOSX` (zip extraction debris): refused with
//!   EACCES. The overlay holds plain files only, never folders, and none
//!   of these is needed for a copy or a save to succeed: Finder deletes
//!   immediately on a network volume instead of using `.Trashes`, and
//!   fseventsd/Spotlight only write their folders on volumes they manage.
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

const LOCAL_ONLY_NAMES: &[&str] = &[".DS_Store", ".VolumeIcon.icns"];

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
];

/// Classifies one path component (a bare name, never a path).
pub fn classify(name: &str) -> Option<JunkKind> {
    if name.starts_with("._") || LOCAL_ONLY_NAMES.iter().any(|n| n.eq_ignore_ascii_case(name)) {
        return Some(JunkKind::LocalOnly);
    }
    if RESERVED_NAMES.iter().any(|n| n.eq_ignore_ascii_case(name)) {
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
        for name in ["._photo.jpg", "._.DS_Store", "._", ".DS_Store", ".ds_store", ".VolumeIcon.icns"] {
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
        for name in ["photo.jpg", ".gitignore", "_photo.jpg", ".DS_Store.txt", "My.DS_Store", "Trashes", ".env"] {
            assert_eq!(classify(name), None, "{name}");
        }
    }

    #[test]
    fn only_the_last_path_component_decides() {
        assert!(is_local_only_path("cloudreve://my/sync/docs/._report.pdf"));
        assert!(!is_local_only_path("cloudreve://my/sync/._dir/report.pdf"));
    }
}
