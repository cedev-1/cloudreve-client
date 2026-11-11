//! What the app and its Finder Sync extension agree on.
//!
//! Finder draws only template glyphs in its sidebar (`ForceTemplateIcons`),
//! so a drive folder's own colour icon never shows there. The one way to put
//! a custom glyph on a sidebar row without a File Provider (which an unsigned
//! app cannot ship) is a Finder Sync extension: Finder asks every enabled
//! extension whether a row's folder is one of its `directoryURLs`, and if so
//! draws the icon of the app CONTAINING the extension — the
//! `CFBundleIcons → CFBundlePrimaryIcon → CFBundleSymbolName` symbol from
//! that app's `Assets.car` (measured by the SidebarFavorites project on
//! macOS 26.6; a `CFBundleIconFile` .icns gives a generic glyph, and the
//! appex's own `CFBundleIcons` is ignored).
//!
//! So Cloudreve.app embeds a tiny helper app, `CloudreveSidebar.app`, whose
//! only job is to carry that symbol and contain the extension. The main
//! app's own icon (Dock, Launchpad) is left alone.
//!
//! The extension is sandboxed (PlugInKit refuses an unsandboxed Finder Sync
//! extension) and ad-hoc signed, so it cannot share an app group container
//! with the app. It reads the folder list from [`FOLDERS_FILE`], which its
//! entitlements open read-only (and nothing else under `~/.cloudreve`, where
//! the drive credentials live), and re-reads it on [`RELOAD_NOTIFICATION`].

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The helper app's bundle id. Prefixed by the main app's (`cloudreve.desktop`).
pub const HOST_BUNDLE_ID: &str = "cloudreve.desktop.sidebar";
/// The extension's bundle id: what `pluginkit -e use -i` takes.
pub const EXTENSION_BUNDLE_ID: &str = "cloudreve.desktop.sidebar.FinderSync";
/// Objective-C class name of the extension's principal class
/// (`NSExtensionPrincipalClass`).
pub const PRINCIPAL_CLASS: &str = "CloudreveFinderSync";
/// Distributed notification the app posts after rewriting the folder list or
/// after a drive's volume was mounted or unmounted. Name only: a sandboxed
/// process does not receive a notification's `userInfo`.
pub const RELOAD_NOTIFICATION: &str = "cloudreve.desktop.sidebar.reload";

/// The helper app, relative to the main app's `.app` directory.
pub const HELPER_IN_APP: &str = "Contents/Helpers/CloudreveSidebar.app";
/// The extension, relative to the main app's `.app` directory.
pub const EXTENSION_IN_APP: &str = "Contents/Helpers/CloudreveSidebar.app/Contents/PlugIns/CloudreveFinderSync.appex";

/// The folder list, relative to the user's home. Its directory is exactly
/// what the extension's sandbox may read (see `Extension.entitlements`).
pub const FOLDERS_FILE: &str = ".cloudreve/finder-sync/folders.json";

/// [`FOLDERS_FILE`] under `home` — the REAL home: inside the sandbox
/// `$HOME` is the extension's container.
pub fn folders_file(home: &Path) -> PathBuf {
    home.join(FOLDERS_FILE)
}

#[derive(Serialize, Deserialize, Default)]
struct FolderList {
    folders: Vec<String>,
}

/// The file's content for `folders`: sorted and without duplicates, so the
/// same set of drives always writes the same bytes (the app skips rewriting,
/// and re-registering, an unchanged list).
pub fn encode_folders(folders: &[PathBuf]) -> String {
    let mut folders: Vec<String> = folders.iter().map(|f| f.to_string_lossy().into_owned()).collect();
    folders.sort();
    folders.dedup();
    serde_json::to_string_pretty(&FolderList { folders }).expect("a list of strings always serializes")
}

/// The folders in a file written by [`encode_folders`]. Anything unreadable
/// is no folder at all: the extension then decorates nothing, which is the
/// state the sidebar would be in without it.
pub fn decode_folders(content: &str) -> Vec<PathBuf> {
    serde_json::from_str::<FolderList>(content)
        .map(|list| list.folders.into_iter().filter(|f| f.starts_with('/')).map(PathBuf::from).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_extension_reads_back_the_folders_the_app_wrote() {
        let written = vec![
            PathBuf::from("/Users/ana/Cloudreve"),
            PathBuf::from("/Users/ana/Documents/Équipe partagée"),
        ];
        assert_eq!(decode_folders(&encode_folders(&written)), written);
    }

    #[test]
    fn the_same_drives_in_another_order_write_the_same_file() {
        let a = encode_folders(&[PathBuf::from("/Users/ana/Work"), PathBuf::from("/Users/ana/Cloudreve")]);
        let b = encode_folders(&[
            PathBuf::from("/Users/ana/Cloudreve"),
            PathBuf::from("/Users/ana/Work"),
            PathBuf::from("/Users/ana/Work"),
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn no_drive_in_the_sidebar_writes_an_empty_list() {
        assert!(decode_folders(&encode_folders(&[])).is_empty());
    }

    #[test]
    fn a_damaged_file_decorates_nothing() {
        assert!(decode_folders("").is_empty());
        assert!(decode_folders("{\"folders\": [\"/Users/ana/Clou").is_empty());
        assert!(decode_folders("[\"/Users/ana/Cloudreve\"]").is_empty());
    }

    #[test]
    fn a_relative_path_is_never_handed_to_finder() {
        let list = decode_folders("{\"folders\": [\"Cloudreve\", \"/Users/ana/Cloudreve\"]}");
        assert_eq!(list, vec![PathBuf::from("/Users/ana/Cloudreve")]);
    }

    #[test]
    fn the_folder_list_lives_in_the_real_home_not_the_sandbox_container() {
        assert_eq!(
            folders_file(Path::new("/Users/ana")),
            PathBuf::from("/Users/ana/.cloudreve/finder-sync/folders.json")
        );
    }
}
