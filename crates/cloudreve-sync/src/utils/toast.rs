use std::path::PathBuf;

use crate::config::ConfigManager;

/// Send a general text notification (macOS/Linux: logs the message).
pub fn send_general_text_toast(title: &str, message: &str) {
    tracing::info!(target: "toast", title = title, message = message, "Notification");
}

/// Send a warning notification (macOS/Linux: logs the message).
pub fn send_warning_toast(title: &str, message: &str) {
    tracing::warn!(target: "toast", title = title, message = message, "Warning notification");
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
}
