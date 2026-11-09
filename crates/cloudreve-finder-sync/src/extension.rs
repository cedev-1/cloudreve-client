//! The extension proper (macOS only).
//!
//! Lifecycle: Finder launches the process for an enabled extension and
//! `NSExtensionMain` instantiates [`PRINCIPAL_CLASS`] (the class defined
//! here, registered with the Objective-C runtime BEFORE `NSExtensionMain`
//! looks it up by name). `init` applies the folder list and starts
//! listening; everything then runs on the main run loop.
//!
//! Re-applying is "clear, then set": Finder ignores `directoryURLs` set to
//! the set it already holds, and after a volume is mounted or unmounted on a
//! monitored path the row's node is a new one, which Finder no longer
//! decorates until the set is cleared and given again (SidebarFavorites,
//! FINDERSYNC-CAPABILITY-MATRIX-26.6 §2, measured on an SMB mount). The
//! mount needs to settle first, hence the ladder of [`REAPPLY_DELAYS`].

use std::ffi::{c_char, c_int, CString};
use std::path::PathBuf;

use cloudreve_finder_sync::{decode_folders, folders_file, PRINCIPAL_CLASS, RELOAD_NOTIFICATION};
use objc2::rc::{Allocated, Retained};
use objc2::runtime::{AnyObject, NSObject};
use objc2::{define_class, msg_send, sel, ClassType};
use objc2_app_kit::{
    NSWorkspace, NSWorkspaceDidMountNotification, NSWorkspaceDidRenameVolumeNotification,
    NSWorkspaceDidUnmountNotification,
};
use objc2_finder_sync::{FIFinderSync, FIFinderSyncController};
use objc2_foundation::{NSDistributedNotificationCenter, NSNotification, NSSet, NSString, NSURL};

/// Seconds after a reload event at which the folder list is applied again
/// (SidebarFavorites' measured ladder for a mount to settle).
const REAPPLY_DELAYS: [f64; 3] = [0.4, 1.5, 3.0];

#[link(name = "Foundation", kind = "framework")]
unsafe extern "C" {
    fn NSExtensionMain(argc: c_int, argv: *mut *mut c_char) -> c_int;
}

unsafe extern "C" {
    // src/oslog.c
    fn cloudreve_finder_sync_log(message: *const c_char);
}

/// To the unified log, readable: CI checks the extension through it
/// (`log show --predicate 'process == "CloudreveFinderSync"'`).
fn log(message: &str) {
    let message = CString::new(message.replace('\0', " ")).unwrap_or_default();
    unsafe { cloudreve_finder_sync_log(message.as_ptr()) };
}

/// The user's real home: inside the sandbox `$HOME` is the container.
fn real_home() -> Option<PathBuf> {
    use std::os::unix::ffi::OsStrExt;
    let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut result: *mut libc::passwd = std::ptr::null_mut();
    let mut buf = vec![0 as c_char; 4096];
    let rc = unsafe { libc::getpwuid_r(libc::getuid(), &mut pwd, buf.as_mut_ptr(), buf.len(), &mut result) };
    if rc != 0 || result.is_null() || pwd.pw_dir.is_null() {
        return None;
    }
    let dir = unsafe { std::ffi::CStr::from_ptr(pwd.pw_dir) };
    Some(PathBuf::from(std::ffi::OsStr::from_bytes(dir.to_bytes())))
}

fn read_folders() -> Vec<PathBuf> {
    let Some(home) = real_home() else {
        log("CloudreveFinderSync: no home directory, decorating nothing");
        return Vec::new();
    };
    let file = folders_file(&home);
    match std::fs::read_to_string(&file) {
        Ok(content) => decode_folders(&content),
        Err(err) => {
            log(&format!("CloudreveFinderSync: cannot read {}: {err}", file.display()));
            Vec::new()
        }
    }
}

define_class!(
    // SAFETY: FIFinderSync has no subclassing requirement beyond calling
    // the designated initializer, which `init` does; no `Drop` impl.
    #[unsafe(super(FIFinderSync, NSObject))]
    #[name = "CloudreveFinderSync"]
    struct CloudreveFinderSync;

    impl CloudreveFinderSync {
        #[unsafe(method_id(init))]
        fn init(this: Allocated<Self>) -> Option<Retained<Self>> {
            let this = this.set_ivars(());
            let this: Option<Retained<Self>> = unsafe { msg_send![super(this), init] };
            if let Some(this) = &this {
                this.listen();
                apply_folders();
            }
            this
        }

        /// A mount, unmount or volume rename, or the app's
        /// [`RELOAD_NOTIFICATION`]: re-apply now and along the ladder.
        #[unsafe(method(cloudreveReload:))]
        fn reload(&self, _notification: Option<&NSNotification>) {
            apply_folders();
            unsafe {
                // A burst of events (several volumes) keeps one ladder.
                let _: () = msg_send![NSObject::class(), cancelPreviousPerformRequestsWithTarget: self];
                for delay in REAPPLY_DELAYS {
                    let _: () = msg_send![
                        self,
                        performSelector: sel!(cloudreveApply),
                        withObject: Option::<&AnyObject>::None,
                        afterDelay: delay
                    ];
                }
            }
        }

        #[unsafe(method(cloudreveApply))]
        fn apply(&self) {
            apply_folders();
        }
    }
);

/// Clear, then set (see the module doc).
fn apply_folders() {
    let folders = read_folders();
    let urls: Vec<Retained<NSURL>> = folders
        .iter()
        // `isDirectory:` given: building the URL never stats the folder (a
        // dead mount would hang it).
        .map(|f| NSURL::fileURLWithPath_isDirectory(&NSString::from_str(&f.to_string_lossy()), true))
        .collect();
    let controller = unsafe { FIFinderSyncController::defaultController() };
    unsafe {
        controller.setDirectoryURLs(Some(&NSSet::new()));
        controller.setDirectoryURLs(Some(&NSSet::from_retained_slice(&urls)));
    }
    let shown: Vec<String> = folders.iter().map(|f| f.display().to_string()).collect();
    log(&format!("CloudreveFinderSync: directoryURLs = {shown:?}"));
}

impl CloudreveFinderSync {
    fn listen(&self) {
        let observer: &AnyObject = self.as_ref();
        unsafe {
            NSDistributedNotificationCenter::defaultCenter().addObserver_selector_name_object(
                observer,
                sel!(cloudreveReload:),
                Some(&NSString::from_str(RELOAD_NOTIFICATION)),
                None,
            );
            let workspace = NSWorkspace::sharedWorkspace().notificationCenter();
            for name in [
                NSWorkspaceDidMountNotification,
                NSWorkspaceDidUnmountNotification,
                NSWorkspaceDidRenameVolumeNotification,
            ] {
                workspace.addObserver_selector_name_object(observer, sel!(cloudreveReload:), Some(name), None);
            }
        }
    }
}

pub fn main() {
    // Run as the helper app's executable (same binary): nothing to do.
    let exe = std::env::current_exe().unwrap_or_default();
    if !exe.to_string_lossy().contains(".appex/Contents/MacOS/") {
        return;
    }
    // Registered now: `NSExtensionMain` finds the principal class by name.
    let class = CloudreveFinderSync::class();
    if class.name().to_bytes() != PRINCIPAL_CLASS.as_bytes() {
        log("CloudreveFinderSync: principal class name mismatch");
        std::process::exit(1);
    }
    let args: Vec<CString> = std::env::args_os()
        .map(|a| CString::new(std::os::unix::ffi::OsStrExt::as_bytes(a.as_os_str())).unwrap_or_default())
        .collect();
    let mut argv: Vec<*mut c_char> = args.iter().map(|a| a.as_ptr() as *mut c_char).collect();
    argv.push(std::ptr::null_mut());
    // Never returns while Finder keeps the extension alive.
    let code = unsafe { NSExtensionMain(args.len() as c_int, argv.as_mut_ptr()) };
    std::process::exit(code);
}
