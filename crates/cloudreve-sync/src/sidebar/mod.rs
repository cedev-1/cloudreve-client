//! "Show in sidebar": every drive's folder gets an entry in the file
//! manager's sidebar (Finder Favorites on macOS, GTK bookmarks and KDE
//! places on Linux) and the Cloudreve logo as its folder icon — what
//! Dropbox does, so a non-technical user finds the drive without knowing
//! where it lives on disk.
//!
//! The policy lives here, the OS calls behind [`SidebarBackend`]:
//!
//! - On by default ([`SidebarSettings::default`]), added ONCE per drive:
//!   `added` remembers it was done, so a user who removes the entry by hand
//!   does not get it back at every launch. The toggle in the drive settings
//!   is the way back.
//! - The folder icon is re-applied at every start while enabled: on an
//!   on-demand drive it lives in the mount's in-memory junk overlay
//!   (`cloudreve_vfs::junk`), which is gone after a relaunch.
//! - The icon is written INTO the folder (macOS: an `Icon\r` file plus
//!   xattrs), so an on-demand drive only gets it while its volume is
//!   mounted: written into the bare mountpoint, `Icon\r` would make the
//!   next mount refuse a non-empty directory.
//! - Removing the drive removes the entry (only if this code added it).

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[cfg(unix)]
pub mod places;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(unix)]
pub mod linux;

/// Per-drive sidebar state, persisted in `drives.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SidebarSettings {
    /// The user wants the entry (the settings toggle).
    pub enabled: bool,
    /// The entry was added since `enabled` last turned on. Never re-added
    /// while set, whatever the sidebar holds now.
    pub added: bool,
}

impl Default for SidebarSettings {
    fn default() -> Self {
        Self { enabled: true, added: false }
    }
}

/// The OS side. Every method is synchronous and quick (a plist round trip
/// through `sharedfilelistd`, or a small text file), and is called from a
/// blocking context.
pub trait SidebarBackend: Send + Sync {
    fn contains(&self, folder: &Path) -> Result<bool>;
    /// Must not duplicate an entry that is already there.
    fn add(&self, folder: &Path, label: &str) -> Result<()>;
    /// Removes every entry pointing to `folder`; none is not an error.
    fn remove(&self, folder: &Path) -> Result<()>;
    fn set_icon(&self, folder: &Path) -> Result<()>;
    fn clear_icon(&self, folder: &Path) -> Result<()>;
}

/// Does nothing: platforms without a backend, and every `Mount` until the
/// `DriveManager` installs the real one (tests never touch the real
/// sidebar of the machine they run on).
pub struct NoSidebar;

impl SidebarBackend for NoSidebar {
    fn contains(&self, _: &Path) -> Result<bool> {
        Ok(false)
    }
    fn add(&self, _: &Path, _: &str) -> Result<()> {
        Ok(())
    }
    fn remove(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    fn set_icon(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    fn clear_icon(&self, _: &Path) -> Result<()> {
        Ok(())
    }
}

/// The real backend for the platform the app runs on.
pub fn platform_backend() -> Arc<dyn SidebarBackend> {
    #[cfg(target_os = "macos")]
    {
        Arc::new(macos::FinderSidebar)
    }
    #[cfg(target_os = "linux")]
    {
        match linux::LinuxPlaces::from_env() {
            Some(places) => Arc::new(places),
            None => Arc::new(NoSidebar),
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Arc::new(NoSidebar)
    }
}

/// A drive folder as the policy sees it.
pub struct DriveFolder<'a> {
    pub path: &'a Path,
    pub label: &'a str,
    /// Whether files may be written into `path` right now (see the module
    /// doc): always for a mirror drive, only while mounted on-demand.
    pub icon_writable: bool,
}

/// Drive start (every launch, and right after the drive is added).
/// Returns the settings to persist. Never fails: a sidebar that cannot be
/// edited must not keep the drive from starting; `added` then stays unset
/// so the next launch tries again.
pub fn at_start(backend: &dyn SidebarBackend, folder: &DriveFolder, settings: SidebarSettings) -> SidebarSettings {
    if !settings.enabled {
        return settings;
    }
    let mut settings = settings;
    if !settings.added {
        match backend.add(folder.path, folder.label) {
            Ok(()) => settings.added = true,
            Err(err) => {
                tracing::warn!(target: "sidebar", folder = %folder.path.display(), ?err, "failed to add the drive folder to the sidebar");
            }
        }
    }
    if folder.icon_writable
        && let Err(err) = backend.set_icon(folder.path)
    {
        tracing::warn!(target: "sidebar", folder = %folder.path.display(), ?err, "failed to set the drive folder icon");
    }
    settings
}

/// The settings toggle. Unlike [`at_start`], turning the entry on adds it
/// even if it was added before (the user asked for it back).
pub fn set_enabled(
    backend: &dyn SidebarBackend,
    folder: &DriveFolder,
    enabled: bool,
) -> Result<SidebarSettings> {
    if enabled {
        backend.add(folder.path, folder.label)?;
        if folder.icon_writable
            && let Err(err) = backend.set_icon(folder.path)
        {
            // The entry is what the user asked for; the logo is a bonus.
            tracing::warn!(target: "sidebar", folder = %folder.path.display(), ?err, "failed to set the drive folder icon");
        }
        Ok(SidebarSettings { enabled: true, added: true })
    } else {
        backend.remove(folder.path)?;
        if folder.icon_writable
            && let Err(err) = backend.clear_icon(folder.path)
        {
            tracing::warn!(target: "sidebar", folder = %folder.path.display(), ?err, "failed to clear the drive folder icon");
        }
        Ok(SidebarSettings { enabled: false, added: false })
    }
}

/// The drive is being removed: take back what this code added.
pub fn on_drive_removed(backend: &dyn SidebarBackend, folder: &DriveFolder, settings: SidebarSettings) {
    if !settings.added {
        return;
    }
    if let Err(err) = backend.remove(folder.path) {
        tracing::warn!(target: "sidebar", folder = %folder.path.display(), ?err, "failed to remove the drive folder from the sidebar");
    }
    if folder.icon_writable
        && let Err(err) = backend.clear_icon(folder.path)
    {
        tracing::warn!(target: "sidebar", folder = %folder.path.display(), ?err, "failed to clear the drive folder icon");
    }
}

/// What the toggle shows: enabled AND the entry still there (a user who
/// removed it by hand sees it off, and turning it on brings it back).
/// When the sidebar cannot be read, the stored state answers.
pub fn is_shown(backend: &dyn SidebarBackend, folder: &Path, settings: SidebarSettings) -> bool {
    settings.enabled && backend.contains(folder).unwrap_or(settings.added)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::sync::Mutex;

    /// An in-memory sidebar: which folders it lists, which carry the icon,
    /// and every call made.
    #[derive(Default)]
    pub(crate) struct FakeSidebar {
        pub entries: Mutex<BTreeSet<PathBuf>>,
        pub icons: Mutex<BTreeSet<PathBuf>>,
        pub calls: Mutex<Vec<String>>,
        pub broken: std::sync::atomic::AtomicBool,
    }

    impl FakeSidebar {
        pub fn lists(&self, p: &str) -> bool {
            self.entries.lock().unwrap().contains(Path::new(p))
        }
        pub fn has_icon(&self, p: &str) -> bool {
            self.icons.lock().unwrap().contains(Path::new(p))
        }
        /// The user drags the entry out of the sidebar.
        pub fn user_removes(&self, p: &str) {
            self.entries.lock().unwrap().remove(Path::new(p));
        }
        fn fail(&self) -> Result<()> {
            if self.broken.load(std::sync::atomic::Ordering::Relaxed) {
                anyhow::bail!("sharedfilelistd unavailable");
            }
            Ok(())
        }
    }

    impl SidebarBackend for FakeSidebar {
        fn contains(&self, folder: &Path) -> Result<bool> {
            self.fail()?;
            Ok(self.entries.lock().unwrap().contains(folder))
        }
        fn add(&self, folder: &Path, label: &str) -> Result<()> {
            self.fail()?;
            self.calls.lock().unwrap().push(format!("add {} {label}", folder.display()));
            self.entries.lock().unwrap().insert(folder.to_path_buf());
            Ok(())
        }
        fn remove(&self, folder: &Path) -> Result<()> {
            self.fail()?;
            self.calls.lock().unwrap().push(format!("remove {}", folder.display()));
            self.entries.lock().unwrap().remove(folder);
            Ok(())
        }
        fn set_icon(&self, folder: &Path) -> Result<()> {
            self.fail()?;
            self.icons.lock().unwrap().insert(folder.to_path_buf());
            Ok(())
        }
        fn clear_icon(&self, folder: &Path) -> Result<()> {
            self.fail()?;
            self.icons.lock().unwrap().remove(folder);
            Ok(())
        }
    }

    fn mirror(path: &str) -> DriveFolder<'_> {
        DriveFolder { path: Path::new(path), label: "Cloudreve", icon_writable: true }
    }

    #[test]
    fn a_new_drive_appears_in_the_sidebar_with_the_logo() {
        let fake = FakeSidebar::default();
        let after = at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        assert!(fake.lists("/Users/ana/Cloudreve"));
        assert!(fake.has_icon("/Users/ana/Cloudreve"));
        assert_eq!(after, SidebarSettings { enabled: true, added: true });
        assert_eq!(*fake.calls.lock().unwrap(), vec!["add /Users/ana/Cloudreve Cloudreve".to_string()]);
    }

    #[test]
    fn an_entry_the_user_removed_is_not_added_back_at_the_next_launch() {
        let fake = FakeSidebar::default();
        let s = at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        fake.user_removes("/Users/ana/Cloudreve");
        let s2 = at_start(&fake, &mirror("/Users/ana/Cloudreve"), s);
        assert!(!fake.lists("/Users/ana/Cloudreve"));
        assert_eq!(s2, s);
        assert!(!is_shown(&fake, Path::new("/Users/ana/Cloudreve"), s2), "the toggle must show it off");
    }

    #[test]
    fn the_icon_is_reapplied_at_every_launch_while_enabled() {
        let fake = FakeSidebar::default();
        let s = at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        // Gone with the on-demand overlay after a relaunch.
        fake.icons.lock().unwrap().clear();
        at_start(&fake, &mirror("/Users/ana/Cloudreve"), s);
        assert!(fake.has_icon("/Users/ana/Cloudreve"));
    }

    #[test]
    fn an_unmounted_on_demand_folder_gets_the_entry_but_nothing_written_into_it() {
        let fake = FakeSidebar::default();
        let folder = DriveFolder { path: Path::new("/Users/ana/Cloud"), label: "Cloud", icon_writable: false };
        let s = at_start(&fake, &folder, SidebarSettings::default());
        assert!(fake.lists("/Users/ana/Cloud"));
        assert!(!fake.has_icon("/Users/ana/Cloud"));
        assert!(s.added);
    }

    #[test]
    fn a_drive_whose_toggle_is_off_is_left_out_of_the_sidebar() {
        let fake = FakeSidebar::default();
        let off = SidebarSettings { enabled: false, added: false };
        assert_eq!(at_start(&fake, &mirror("/Users/ana/Cloudreve"), off), off);
        assert!(!fake.lists("/Users/ana/Cloudreve"));
        assert!(!fake.has_icon("/Users/ana/Cloudreve"));
    }

    #[test]
    fn a_failing_sidebar_does_not_mark_the_entry_added_so_the_next_launch_retries() {
        let fake = FakeSidebar::default();
        fake.broken.store(true, std::sync::atomic::Ordering::Relaxed);
        let s = at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        assert_eq!(s, SidebarSettings::default());
        fake.broken.store(false, std::sync::atomic::Ordering::Relaxed);
        at_start(&fake, &mirror("/Users/ana/Cloudreve"), s);
        assert!(fake.lists("/Users/ana/Cloudreve"));
    }

    #[test]
    fn turning_the_toggle_on_brings_back_an_entry_the_user_removed() {
        let fake = FakeSidebar::default();
        at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        fake.user_removes("/Users/ana/Cloudreve");
        let s = set_enabled(&fake, &mirror("/Users/ana/Cloudreve"), true).unwrap();
        assert!(fake.lists("/Users/ana/Cloudreve"));
        assert!(is_shown(&fake, Path::new("/Users/ana/Cloudreve"), s));
    }

    #[test]
    fn turning_the_toggle_off_removes_the_entry_and_the_logo_until_turned_back_on() {
        let fake = FakeSidebar::default();
        at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        let s = set_enabled(&fake, &mirror("/Users/ana/Cloudreve"), false).unwrap();
        assert!(!fake.lists("/Users/ana/Cloudreve"));
        assert!(!fake.has_icon("/Users/ana/Cloudreve"));
        assert_eq!(s, SidebarSettings { enabled: false, added: false });
        // Stays off across launches.
        at_start(&fake, &mirror("/Users/ana/Cloudreve"), s);
        assert!(!fake.lists("/Users/ana/Cloudreve"));
        let s = set_enabled(&fake, &mirror("/Users/ana/Cloudreve"), true).unwrap();
        assert_eq!(s, SidebarSettings { enabled: true, added: true });
        assert!(fake.lists("/Users/ana/Cloudreve") && fake.has_icon("/Users/ana/Cloudreve"));
    }

    #[test]
    fn a_toggle_that_cannot_reach_the_sidebar_reports_the_error() {
        let fake = FakeSidebar::default();
        fake.broken.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(set_enabled(&fake, &mirror("/Users/ana/Cloudreve"), true).is_err());
    }

    #[test]
    fn removing_the_drive_takes_its_entry_and_logo_away() {
        let fake = FakeSidebar::default();
        let s = at_start(&fake, &mirror("/Users/ana/Cloudreve"), SidebarSettings::default());
        on_drive_removed(&fake, &mirror("/Users/ana/Cloudreve"), s);
        assert!(!fake.lists("/Users/ana/Cloudreve"));
        assert!(!fake.has_icon("/Users/ana/Cloudreve"));
    }

    #[test]
    fn removing_a_drive_never_touches_an_entry_this_code_did_not_add() {
        let fake = FakeSidebar::default();
        // The user had the folder in Favorites on their own, toggle off.
        fake.entries.lock().unwrap().insert(PathBuf::from("/Users/ana/Work"));
        on_drive_removed(&fake, &mirror("/Users/ana/Work"), SidebarSettings { enabled: false, added: false });
        assert!(fake.lists("/Users/ana/Work"));
    }
}
