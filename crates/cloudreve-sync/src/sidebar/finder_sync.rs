//! The Cloudreve glyph on the drive folders' Finder sidebar rows, through
//! the app's Finder Sync extension (`crates/cloudreve-finder-sync`, whose
//! crate doc explains the mechanism).
//!
//! The app's half: write the folder list the extension reads, register and
//! enable the extension while at least one drive wants the glyph (disable
//! it when none does, so no process runs for nothing), and tell it to
//! re-read — after a new list, and after a drive's volume was mounted or
//! unmounted (Finder forgets a monitored folder whose node changed).
//!
//! Registration goes through `pluginkit`, the only way without System
//! Settings, measured on macOS 27.0.1 with an ad-hoc signed copy of the
//! extension: `pluginkit -a` registered it (not always on the first call,
//! hence the retries), `pluginkit -e use` enabled it, and Finder started it
//! right away. Only from an app bundle: a `cargo run` binary has no
//! extension, and must not touch the real list in `~/.cloudreve` either.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use cloudreve_finder_sync::{encode_folders, EXTENSION_BUNDLE_ID, EXTENSION_IN_APP, RELOAD_NOTIFICATION};
use objc2_foundation::{NSDistributedNotificationCenter, NSString};

const LSREGISTER: &str =
    "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister";

/// What one change of the folder list does to the extension.
#[derive(Debug, PartialEq, Eq)]
enum Registration {
    /// Register (idempotent) and enable the extension at this path.
    Enable(PathBuf),
    /// No drive wants the glyph: the extension is turned off.
    Disable,
}

fn registration(extension: &Path, folders: &[PathBuf]) -> Registration {
    if folders.is_empty() { Registration::Disable } else { Registration::Enable(extension.to_path_buf()) }
}

/// The extension inside the app bundle running `exe`
/// (`<X>.app/Contents/MacOS/<exe>`), if the bundle has one.
fn extension_of(exe: &Path) -> Option<PathBuf> {
    let app = exe.parent()?.parent()?.parent()?;
    let extension = app.join(EXTENSION_IN_APP);
    extension.is_dir().then_some(extension)
}

pub(super) struct FinderSyncGlyph {
    /// `None` outside an app bundle: then nothing at all is done.
    extension: Option<PathBuf>,
    folders_file: Option<PathBuf>,
    /// The file content last published by this process. A fresh process
    /// always publishes once (the app may have moved or been updated).
    published: Mutex<Published>,
    /// Registers and enables the extension (`enable`; replaced in tests).
    register: fn(&Path) -> Result<()>,
}

#[derive(Default)]
struct Published {
    content: Option<String>,
    /// PlugInKit refused the extension once in this process: not retried
    /// (five attempts a second apart) on every save. The next launch tries
    /// again.
    registration_failed: bool,
}

impl FinderSyncGlyph {
    pub(super) fn for_this_app() -> Self {
        let extension = std::env::current_exe().ok().and_then(|exe| extension_of(&exe));
        let folders_file = dirs::home_dir().map(|home| cloudreve_finder_sync::folders_file(&home));
        Self { extension, folders_file, published: Mutex::new(Published::default()), register: enable }
    }

    pub(super) fn publish(&self, folders: &[PathBuf]) -> Result<()> {
        let (Some(extension), Some(file)) = (&self.extension, &self.folders_file) else {
            return Ok(());
        };
        let content = encode_folders(folders);
        // Held throughout: two saves in a row must not interleave their
        // pluginkit calls.
        let mut published = self.published.lock().unwrap();
        if published.content.as_deref() == Some(content.as_str()) {
            return Ok(());
        }
        write_atomically(file, &content)?;
        let mut result = Ok(());
        match registration(extension, folders) {
            Registration::Enable(_) if published.registration_failed => {}
            Registration::Enable(extension) => {
                if let Err(err) = (self.register)(&extension) {
                    published.registration_failed = true;
                    result = Err(err);
                }
            }
            Registration::Disable => disable(),
        }
        reload();
        published.content = Some(content);
        result
    }

    pub(super) fn remounted(&self) {
        if self.extension.is_some() {
            reload();
        }
    }
}

fn write_atomically(file: &Path, content: &str) -> Result<()> {
    let dir = file.parent().context("the folder list has no directory")?;
    std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let tmp = file.with_extension("json.tmp");
    std::fs::write(&tmp, content).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, file).with_context(|| format!("failed to replace {}", file.display()))
}

fn run(program: &str, args: &[&std::ffi::OsStr]) -> Result<std::process::Output> {
    Command::new(program).args(args).output().with_context(|| format!("failed to run {program}"))
}

fn is_registered() -> bool {
    run("/usr/bin/pluginkit", &["-m".as_ref(), "-A".as_ref(), "-i".as_ref(), EXTENSION_BUNDLE_ID.as_ref()])
        .map(|out| String::from_utf8_lossy(&out.stdout).contains(EXTENSION_BUNDLE_ID))
        .unwrap_or(false)
}

fn enable(extension: &Path) -> Result<()> {
    // The helper app (`…/CloudreveSidebar.app/Contents/PlugIns/X.appex`):
    // Launch Services registering it is what made a missed `pluginkit -a`
    // show up, in the measurements above.
    if let Some(helper) = extension.ancestors().nth(3)
        && let Err(err) = run(LSREGISTER, &["-f".as_ref(), helper.as_os_str()])
    {
        tracing::warn!(target: "sidebar", ?err, "lsregister failed");
    }
    let mut registered = false;
    for _ in 0..5 {
        run("/usr/bin/pluginkit", &["-a".as_ref(), extension.as_os_str()])?;
        if is_registered() {
            registered = true;
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    if !registered {
        bail!("PlugInKit did not register {}", extension.display());
    }
    let out = run("/usr/bin/pluginkit", &["-e".as_ref(), "use".as_ref(), "-i".as_ref(), EXTENSION_BUNDLE_ID.as_ref()])?;
    if !out.status.success() {
        bail!("pluginkit -e use failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    tracing::info!(target: "sidebar", extension = %extension.display(), "Finder Sync extension enabled");
    Ok(())
}

fn disable() {
    match run("/usr/bin/pluginkit", &["-e".as_ref(), "ignore".as_ref(), "-i".as_ref(), EXTENSION_BUNDLE_ID.as_ref()]) {
        Ok(_) => tracing::info!(target: "sidebar", "Finder Sync extension disabled: no drive in the sidebar"),
        Err(err) => tracing::warn!(target: "sidebar", ?err, "failed to disable the Finder Sync extension"),
    }
}

/// Tells a running extension to re-read the list and re-apply it (name
/// only: a sandboxed process gets no `userInfo`). Nobody listening is fine.
fn reload() {
    let center = NSDistributedNotificationCenter::defaultCenter();
    unsafe {
        center.postNotificationName_object_userInfo_deliverImmediately(
            &NSString::from_str(RELOAD_NOTIFICATION),
            None,
            None,
            true,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_drive_in_the_sidebar_turns_the_extension_on() {
        let ext = Path::new("/Applications/Cloudreve.app").join(EXTENSION_IN_APP);
        assert_eq!(
            registration(&ext, &[PathBuf::from("/Users/ana/Cloudreve")]),
            Registration::Enable(ext.clone())
        );
    }

    #[test]
    fn no_drive_left_in_the_sidebar_turns_the_extension_off() {
        let ext = Path::new("/Applications/Cloudreve.app").join(EXTENSION_IN_APP);
        assert_eq!(registration(&ext, &[]), Registration::Disable);
    }

    #[test]
    fn the_extension_is_found_inside_the_running_app_bundle() {
        let tmp = tempfile::tempdir().unwrap();
        let app = tmp.path().join("Cloudreve.app");
        let ext = app.join("Contents/Helpers/CloudreveSidebar.app/Contents/PlugIns/CloudreveFinderSync.appex");
        std::fs::create_dir_all(&ext).unwrap();
        assert_eq!(extension_of(&app.join("Contents/MacOS/cloudreve-desktop")), Some(ext));
    }

    #[test]
    fn a_binary_outside_an_app_bundle_has_no_extension() {
        // `cargo run`: target/debug/cloudreve-desktop.
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("target/debug/cloudreve-desktop");
        std::fs::create_dir_all(exe.parent().unwrap()).unwrap();
        assert_eq!(extension_of(&exe), None);
        // An app bundle built without the extension (e.g. an older one).
        let app = tmp.path().join("Cloudreve.app");
        std::fs::create_dir_all(app.join("Contents/MacOS")).unwrap();
        assert_eq!(extension_of(&app.join("Contents/MacOS/cloudreve-desktop")), None);
    }

    #[test]
    fn outside_an_app_bundle_nothing_is_written_or_registered() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join(".cloudreve/finder-sync/folders.json");
        let glyph = FinderSyncGlyph { extension: None, folders_file: Some(file.clone()), ..FinderSyncGlyph::for_this_app() };
        glyph.publish(&[PathBuf::from("/Users/ana/Cloudreve")]).unwrap();
        assert!(!file.exists());
    }

    static REGISTER_ATTEMPTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn register_fails(_: &Path) -> Result<()> {
        REGISTER_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        bail!("PlugInKit did not register the extension")
    }

    /// PlugInKit refusing the extension (an unsigned copy, a sandbox) is not
    /// going to change within one run: retrying it — five attempts a second
    /// apart — on every save of drives.json only stalled each save. The
    /// folder list is still kept current for when it does work.
    #[test]
    fn a_registration_that_failed_is_not_retried_on_every_save() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join(".cloudreve/finder-sync/folders.json");
        let glyph = FinderSyncGlyph {
            extension: Some(tmp.path().join("CloudreveFinderSync.appex")),
            folders_file: Some(file.clone()),
            register: register_fails,
            ..FinderSyncGlyph::for_this_app()
        };

        assert!(glyph.publish(&[PathBuf::from("/Users/ana/Cloudreve")]).is_err(), "the first failure is reported");
        glyph.publish(&[PathBuf::from("/Users/ana/Cloudreve"), PathBuf::from("/Users/ana/Work")]).unwrap();
        glyph.publish(&[PathBuf::from("/Users/ana/Work")]).unwrap();

        assert_eq!(REGISTER_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            encode_folders(&[PathBuf::from("/Users/ana/Work")]),
            "the list itself stays current"
        );
    }
}
