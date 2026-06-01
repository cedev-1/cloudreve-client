use std::path::PathBuf;
use std::sync::OnceLock;

use tokio::sync::mpsc::UnboundedSender;

use crate::config::ConfigManager;

static OS_NOTIFIER: OnceLock<UnboundedSender<(String, String)>> = OnceLock::new();

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
