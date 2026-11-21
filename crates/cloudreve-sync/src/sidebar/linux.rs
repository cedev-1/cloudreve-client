//! Linux sidebar backend: the GTK bookmarks file (GNOME Files, Nemo,
//! Caja, Thunar, GTK file choosers) and, when the user has KDE, its places
//! file (Dolphin). Edits are the pure functions of [`super::places`],
//! written back atomically (temp file + rename, the file's mode kept).
//!
//! The logo: the app icon is installed as a theme icon in the user's own
//! hicolor theme (`$XDG_DATA_HOME/icons`, works for a `.deb` and an
//! AppImage alike), which the KDE place names. GTK bookmarks have no icon
//! field: GNOME shows the folder's GVfs `metadata::custom-icon` (set with
//! `gio`, stored in `~/.local/share/gvfs-metadata`, never inside the
//! folder — nothing for a FUSE drive to upload) in its file view; its
//! sidebar keeps the generic folder symbol whatever is set.
//!
//! Compiled on every Unix so its tests run on the macOS dev machine too;
//! only selected by [`super::platform_backend`] on Linux.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use super::{places, SidebarBackend};

/// Theme icon name the KDE place points to. Distinct from whatever name a
/// distro package installs the app icon under, so removing one never
/// breaks the other.
pub const ICON_NAME: &str = "cloudreve-drive";

const ICON_PNG: &[u8] = include_bytes!("../../../../src-tauri/icons/128x128@2x.png");

pub struct LinuxPlaces {
    pub gtk_bookmarks: PathBuf,
    pub kde_places: PathBuf,
    /// `.../icons/hicolor/256x256/apps`.
    pub icon_dir: PathBuf,
    /// Run `gio` for the folder icon. Off in tests: GVfs metadata is the
    /// user's real desktop state.
    pub use_gio: bool,
}

impl LinuxPlaces {
    /// Locations from the XDG base directory variables, falling back to
    /// `~/.config` and `~/.local/share`.
    pub fn from_env() -> Option<Self> {
        let home = dirs::home_dir()?;
        let xdg = |var: &str, fallback: &str| {
            std::env::var_os(var)
                .map(PathBuf::from)
                .filter(|p| p.is_absolute())
                .unwrap_or_else(|| home.join(fallback))
        };
        let config = xdg("XDG_CONFIG_HOME", ".config");
        let data = xdg("XDG_DATA_HOME", ".local/share");
        Some(Self::with_dirs(&config, &data, true))
    }

    pub fn with_dirs(config_home: &Path, data_home: &Path, use_gio: bool) -> Self {
        Self {
            gtk_bookmarks: config_home.join("gtk-3.0").join("bookmarks"),
            kde_places: data_home.join("user-places.xbel"),
            icon_dir: data_home.join("icons").join("hicolor").join("256x256").join("apps"),
            use_gio,
        }
    }

    pub fn icon_file(&self) -> PathBuf {
        self.icon_dir.join(format!("{ICON_NAME}.png"))
    }

    fn install_icon(&self) -> Result<()> {
        let target = self.icon_file();
        if std::fs::read(&target).ok().as_deref() == Some(ICON_PNG) {
            return Ok(());
        }
        std::fs::create_dir_all(&self.icon_dir)
            .with_context(|| format!("failed to create {}", self.icon_dir.display()))?;
        write_atomically(&target, ICON_PNG)
    }

    fn gio(&self, args: &[&std::ffi::OsStr]) {
        if !self.use_gio {
            return;
        }
        match Command::new("gio").args(args).output() {
            Ok(out) if out.status.success() => {}
            Ok(out) => tracing::debug!(target: "sidebar", stderr = %String::from_utf8_lossy(&out.stderr), "gio refused the folder icon"),
            Err(err) => tracing::debug!(target: "sidebar", error = %format_args!("{err:#}"), "gio is not available"),
        }
    }
}

/// Contents of `path`, empty when it does not exist yet.
fn read_or_empty(path: &Path) -> Result<String> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(s),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Replaces `path` with `contents` in one rename, so a file manager
/// reading it concurrently never sees half a file. Keeps the old mode.
fn write_atomically(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_file_name(format!(
        ".{}.cloudreve-tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("places")
    ));
    std::fs::write(&tmp, contents).with_context(|| format!("failed to write {}", tmp.display()))?;
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&tmp, meta.permissions());
    }
    std::fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))
}

/// KDE place ids are `<unix seconds>/<counter>`; the counter only has to
/// be unique among the user's places.
fn kde_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}/{}", now.as_secs(), 1000 + now.subsec_micros() % 9000)
}

impl SidebarBackend for LinuxPlaces {
    fn contains(&self, folder: &Path) -> Result<bool> {
        Ok(places::gtk_contains(&read_or_empty(&self.gtk_bookmarks)?, folder)
            || places::xbel_contains(&read_or_empty(&self.kde_places)?, folder))
    }

    fn add(&self, folder: &Path, label: &str) -> Result<()> {
        // GTK: always (creating the file is how GNOME itself starts it).
        if let Some(edited) = places::gtk_add(&read_or_empty(&self.gtk_bookmarks)?, folder, label) {
            if let Some(dir) = self.gtk_bookmarks.parent() {
                std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
            }
            write_atomically(&self.gtk_bookmarks, edited.as_bytes())?;
        }
        // KDE: only for a user who has it — Dolphin creates the file with
        // its default places on first run, and one invented here would
        // replace those defaults. A missing file reads as "" and `xbel_add`
        // leaves a document without `</xbel>` alone, so none is written.
        if let Some(edited) = places::xbel_add(&read_or_empty(&self.kde_places)?, folder, label, ICON_NAME, &kde_id())
        {
            self.install_icon()?;
            write_atomically(&self.kde_places, edited.as_bytes())?;
        }
        Ok(())
    }

    fn remove(&self, folder: &Path) -> Result<()> {
        if let Some(edited) = places::gtk_remove(&read_or_empty(&self.gtk_bookmarks)?, folder) {
            write_atomically(&self.gtk_bookmarks, edited.as_bytes())?;
        }
        if let Some(edited) = places::xbel_remove(&read_or_empty(&self.kde_places)?, folder) {
            write_atomically(&self.kde_places, edited.as_bytes())?;
        }
        Ok(())
    }

    fn set_icon(&self, folder: &Path) -> Result<()> {
        self.install_icon()?;
        let uri = places::file_uri(&self.icon_file());
        self.gio(&["set".as_ref(), folder.as_os_str(), "metadata::custom-icon".as_ref(), uri.as_ref()]);
        Ok(())
    }

    fn clear_icon(&self, folder: &Path) -> Result<()> {
        self.gio(&["set".as_ref(), "-t".as_ref(), "unset".as_ref(), folder.as_os_str(), "metadata::custom-icon".as_ref()]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> (tempfile::TempDir, LinuxPlaces) {
        let tmp = tempfile::tempdir().unwrap();
        let places = LinuxPlaces::with_dirs(&tmp.path().join("config"), &tmp.path().join("data"), false);
        (tmp, places)
    }

    const DOLPHIN_DEFAULTS: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE xbel>\n<xbel xmlns:bookmark=\"http://www.freedesktop.org/standards/desktop-bookmarks\">\n <bookmark href=\"file:///home/ana\">\n  <title>Home</title>\n </bookmark>\n</xbel>\n";

    #[test]
    fn a_gnome_user_without_a_bookmarks_file_gets_one_listing_the_drive() {
        let (_tmp, places) = env();
        places.add(Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();
        assert_eq!(std::fs::read_to_string(&places.gtk_bookmarks).unwrap(), "file:///home/ana/Cloudreve Cloudreve\n");
        assert!(places.contains(Path::new("/home/ana/Cloudreve")).unwrap());
        assert!(!places.kde_places.exists(), "no KDE file may be invented for a GNOME user");
    }

    #[test]
    fn a_kde_user_gets_a_dolphin_place_with_the_cloudreve_icon_installed() {
        let (_tmp, places) = env();
        std::fs::create_dir_all(places.kde_places.parent().unwrap()).unwrap();
        std::fs::write(&places.kde_places, DOLPHIN_DEFAULTS).unwrap();

        places.add(Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();

        let xbel = std::fs::read_to_string(&places.kde_places).unwrap();
        assert!(xbel.contains(" <bookmark href=\"file:///home/ana\">\n  <title>Home</title>"), "the defaults must stay: {xbel}");
        assert!(xbel.contains("<bookmark href=\"file:///home/ana/Cloudreve\">"), "{xbel}");
        assert!(xbel.contains("<bookmark:icon name=\"cloudreve-drive\"/>"), "{xbel}");
        assert_eq!(std::fs::read(places.icon_file()).unwrap(), ICON_PNG);
        // A theme lookup by that name finds a real PNG.
        assert!(places.icon_file().ends_with("icons/hicolor/256x256/apps/cloudreve-drive.png"));
        assert_eq!(&ICON_PNG[1..4], b"PNG");
    }

    #[test]
    fn adding_twice_leaves_a_single_entry_in_each_file() {
        let (_tmp, places) = env();
        std::fs::create_dir_all(places.kde_places.parent().unwrap()).unwrap();
        std::fs::write(&places.kde_places, DOLPHIN_DEFAULTS).unwrap();
        places.add(Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();
        places.add(Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();
        let gtk = std::fs::read_to_string(&places.gtk_bookmarks).unwrap();
        let xbel = std::fs::read_to_string(&places.kde_places).unwrap();
        assert_eq!(gtk.matches("file:///home/ana/Cloudreve").count(), 1);
        assert_eq!(xbel.matches("file:///home/ana/Cloudreve").count(), 1);
    }

    #[test]
    fn removing_the_drive_restores_both_files_and_keeps_the_users_own_entries() {
        let (_tmp, places) = env();
        std::fs::create_dir_all(places.gtk_bookmarks.parent().unwrap()).unwrap();
        std::fs::write(&places.gtk_bookmarks, "file:///home/ana/Documents\n").unwrap();
        std::fs::create_dir_all(places.kde_places.parent().unwrap()).unwrap();
        std::fs::write(&places.kde_places, DOLPHIN_DEFAULTS).unwrap();

        places.add(Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();
        places.remove(Path::new("/home/ana/Cloudreve")).unwrap();

        assert_eq!(std::fs::read_to_string(&places.gtk_bookmarks).unwrap(), "file:///home/ana/Documents\n");
        assert_eq!(std::fs::read_to_string(&places.kde_places).unwrap(), DOLPHIN_DEFAULTS);
        assert!(!places.contains(Path::new("/home/ana/Cloudreve")).unwrap());
    }

    #[test]
    fn an_entry_found_only_in_kde_places_counts_as_listed() {
        let (_tmp, places) = env();
        std::fs::create_dir_all(places.kde_places.parent().unwrap()).unwrap();
        std::fs::write(&places.kde_places, DOLPHIN_DEFAULTS).unwrap();
        assert!(places.contains(Path::new("/home/ana")).unwrap());
    }

    #[test]
    fn removing_from_files_that_do_not_exist_is_not_an_error_and_creates_nothing() {
        let (_tmp, places) = env();
        places.remove(Path::new("/home/ana/Cloudreve")).unwrap();
        assert!(!places.gtk_bookmarks.exists());
        assert!(!places.kde_places.exists());
    }

    #[cfg(unix)]
    #[test]
    fn rewriting_a_bookmarks_file_keeps_its_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let (_tmp, places) = env();
        std::fs::create_dir_all(places.gtk_bookmarks.parent().unwrap()).unwrap();
        std::fs::write(&places.gtk_bookmarks, "file:///home/ana/Documents\n").unwrap();
        std::fs::set_permissions(&places.gtk_bookmarks, std::fs::Permissions::from_mode(0o600)).unwrap();
        places.add(Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();
        let mode = std::fs::metadata(&places.gtk_bookmarks).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
