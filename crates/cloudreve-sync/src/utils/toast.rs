use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::UnboundedSender;

use crate::config::ConfigManager;
use crate::drive::manager::format_bytes;

static OS_NOTIFIER: OnceLock<UnboundedSender<(String, String)>> = OnceLock::new();

/// A sync short on space hits the guard on every single file, so the warning is
/// rate-limited per drive rather than repeated hundreds of times.
static LOW_DISK_THROTTLE: LazyLock<Throttle> =
    LazyLock::new(|| Throttle::new(Duration::from_secs(3600)));

/// An on-demand drive's effective cache cap (D3) is recomputed on every
/// mount/remount (pause+resume, app relaunch), which would otherwise repeat
/// the same warning every time — throttled per drive like the low-disk-space
/// warning above.
static SMALL_VFS_CACHE_THROTTLE: LazyLock<Throttle> =
    LazyLock::new(|| Throttle::new(Duration::from_secs(3600)));

/// Rate limiter keyed by an arbitrary string (a drive id, in practice).
/// Keeps a bulk operation from producing one notification per file.
struct Throttle {
    interval: Duration,
    last_fired: Mutex<HashMap<String, Instant>>,
}

impl Throttle {
    fn new(interval: Duration) -> Self {
        Self { interval, last_fired: Mutex::new(HashMap::new()) }
    }

    /// Whether the caller may fire for `key` now. Records the time when it
    /// returns true, so the next call within `interval` returns false.
    fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut last_fired = self
            .last_fired
            .lock()
            .expect("Throttle mutex poisoned — another thread panicked while holding the lock");
        if let Some(last) = last_fired.get(key) {
            if now.duration_since(*last) < self.interval {
                return false;
            }
        }
        last_fired.insert(key.to_string(), now);
        true
    }
}

/// Register the OS notification sender. Must be called once at app startup
/// from the Tauri context before any toast functions are used.
pub fn init_os_notifier(tx: UnboundedSender<(String, String)>) {
    let _ = OS_NOTIFIER.set(tx);
}

fn push_notification(title: impl Into<String>, body: impl Into<String>) {
    if let Some(tx) = OS_NOTIFIER.get() {
        let _ = tx.send((title.into(), body.into()));
    }
}

/// Send a general text notification.
pub fn send_general_text_toast(title: &str, message: &str) {
    tracing::info!(target: "toast", title = title, message = message, "Notification");
    push_notification(title, message);
}

/// Send a warning notification.
pub fn send_warning_toast(title: &str, message: &str) {
    tracing::warn!(target: "toast", title = title, message = message, "Warning notification");
    push_notification(title, message);
}

/// Send a token expiry notification.
/// Respects the notify_credential_expired config setting.
pub fn send_token_expiry_toast(drive_id: &str, title: &str, message: &str) {
    if let Some(config) = ConfigManager::try_get() {
        if !config.notify_credential_expired() {
            tracing::debug!(target: "toast", "Token expiry notification suppressed by config");
            return;
        }
    }
    tracing::warn!(
        target: "toast",
        drive_id = drive_id,
        title = title,
        message = message,
        "Token expiry notification"
    );
    push_notification(title, message);
}

/// Send a file conflict notification.
/// Respects the notify_file_conflict config setting.
pub fn send_conflict_toast(_drive_id: &str, path: &PathBuf, _inventory_id: i64) {
    if let Some(config) = ConfigManager::try_get() {
        if !config.notify_file_conflict() {
            tracing::debug!(target: "toast", "Conflict notification suppressed by config");
            return;
        }
    }
    tracing::warn!(
        target: "toast",
        path = %path.display(),
        "File conflict notification"
    );
    push_notification(
        "Sync Conflict",
        format!("File conflict: {}", path.display()),
    );
}

/// Warn that a sync needs more room than the volume has. Throttled to at most
/// one notification per hour per drive.
pub fn send_low_disk_space_toast(drive_id: &str, drive_name: &str, required: u64, available: u64) {
    if !LOW_DISK_THROTTLE.allow(drive_id) {
        return;
    }
    let clamp = |b: u64| b.min(i64::MAX as u64) as i64;
    let message = format!(
        "{drive_name} needs {} but only {} is available on this volume. Some files will not be downloaded.",
        format_bytes(clamp(required)),
        format_bytes(clamp(available)),
    );
    tracing::warn!(
        target: "toast",
        drive_id = drive_id,
        required = required,
        available = available,
        "Low disk space notification"
    );
    push_notification("Not enough disk space", message);
}

/// Warn that an on-demand drive's effective cache cap (D3) was clamped
/// below the 10 GiB default because of limited free space. Throttled to at
/// most one notification per hour per drive.
pub fn send_small_vfs_cache_toast(drive_id: &str, drive_name: &str, cap: u64) {
    if !SMALL_VFS_CACHE_THROTTLE.allow(drive_id) {
        return;
    }
    let clamp = |b: u64| b.min(i64::MAX as u64) as i64;
    let message = format!(
        "{drive_name}'s on-demand cache is limited to {} because of low disk space.",
        format_bytes(clamp(cap)),
    );
    tracing::warn!(
        target: "toast",
        drive_id = drive_id,
        cap = cap,
        "Small on-demand cache cap notification"
    );
    push_notification("Limited on-demand cache", message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_call_for_a_key_is_allowed() {
        let throttle = Throttle::new(Duration::from_secs(60));
        assert!(throttle.allow("drive-a"));
    }

    #[test]
    fn a_second_call_within_the_interval_is_suppressed() {
        let throttle = Throttle::new(Duration::from_secs(60));
        assert!(throttle.allow("drive-a"));
        assert!(!throttle.allow("drive-a"));
    }

    #[test]
    fn keys_are_throttled_independently() {
        let throttle = Throttle::new(Duration::from_secs(60));
        assert!(throttle.allow("drive-a"));
        assert!(throttle.allow("drive-b"));
    }

    #[test]
    fn a_call_after_the_interval_is_allowed_again() {
        let throttle = Throttle::new(Duration::from_millis(20));
        assert!(throttle.allow("drive-a"));
        std::thread::sleep(Duration::from_millis(30));
        assert!(throttle.allow("drive-a"));
    }
}
