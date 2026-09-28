use crate::AppStateHandle;
use chrono::{Duration, Utc};
use cloudreve_api::api::UserApi;
use cloudreve_sync::{
    config::LogLevel, ConfigManager, ConflictResolution, Credentials, DriveConfig, DriveInfo, DriveMode,
    StatusSummary,
};
#[cfg(target_os = "macos")]
use tauri::TitleBarStyle;
use tauri::{
    utils::{config::WindowEffectsConfig, WindowEffect},
    webview::WebviewWindowBuilder,
    AppHandle, Manager, State, WebviewUrl,
};
use tauri_plugin_autostart::ManagerExt;
use tauri_plugin_frame::WebviewWindowExt;
use tauri_plugin_positioner::{Position, WindowExt};
use uuid::Uuid;

type CommandResult<T> = Result<T, String>;

fn is_root_drive(_path: &str) -> bool {
    // On macOS/Linux, root drive is "/"
    _path.trim() == "/"
}

fn get_url_with_lang(base_path: &str) -> String {
    let locale = crate::get_effective_locale();
    if base_path.contains('?') {
        format!("{}&lng={}", base_path, locale)
    } else {
        format!("{}?lng={}", base_path, locale)
    }
}

#[tauri::command]
pub async fn is_dir_empty(path: String) -> CommandResult<bool> {
    let p = std::path::Path::new(&path);
    if !p.exists() || !p.is_dir() {
        return Ok(true);
    }
    match std::fs::read_dir(p) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(_) => Ok(true),
    }
}

#[tauri::command]
pub async fn list_drives(state: State<'_, AppStateHandle>) -> CommandResult<Vec<DriveConfig>> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    Ok(app_state.drive_manager.list_drives().await)
}

#[derive(serde::Deserialize)]
pub struct AddDriveArgs {
    pub site_url: String,
    pub access_token: String,
    pub refresh_token: String,
    pub access_token_expires: u64,
    pub refresh_token_expires: u64,
    pub drive_name: String,
    pub remote_path: String,
    pub local_path: String,
    pub user_id: String,
    pub drive_id: Option<String>,
    /// Full mirror vs. on-demand (D1/D9). Defaults to `FullMirror` so a
    /// caller that predates this field (or simply omits it) keeps today's
    /// behavior unchanged.
    #[serde(default)]
    pub mode: DriveMode,
}

/// Builds the `DriveConfig` for a brand-new drive from the command's args.
/// Factored out of `add_drive` so the mode-threading behavior is exercised
/// by a unit test through the exact same code the command runs.
fn build_drive_config(
    drive_id: String,
    sse_client_id: String,
    config: AddDriveArgs,
    credentials: Credentials,
) -> DriveConfig {
    DriveConfig {
        id: drive_id,
        name: config.drive_name,
        instance_url: config.site_url,
        remote_path: config.remote_path,
        credentials,
        sync_path: config.local_path.into(),
        icon_path: None,
        raw_icon_path: None,
        enabled: true,
        user_id: config.user_id,
        ignore_patterns: Vec::new(),
        max_file_size_mb: 3072,
        sse_client_id,
        mode: config.mode,
        extra: Default::default(),
    }
}

#[tauri::command]
pub async fn add_drive(
    state: State<'_, AppStateHandle>,
    config: AddDriveArgs,
) -> CommandResult<String> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;

    if config.drive_id.is_none() && is_root_drive(&config.local_path) {
        return Err(t!("localPathCannotBeRootDrive").to_string());
    }

    let now = Utc::now();
    let access_expires = (now + Duration::seconds(config.access_token_expires as i64)).to_rfc3339();
    let refresh_expires = (now + Duration::seconds(config.refresh_token_expires as i64)).to_rfc3339();

    // `build_drive_config` below takes `config` by whole-struct value, so no
    // field of it can be moved out beforehand — clone the few fields the
    // reauthorize branch needs instead of moving them.
    let credentials = Credentials {
        access_token: Some(config.access_token.clone()),
        refresh_token: config.refresh_token.clone(),
        access_expires: Some(access_expires),
        refresh_expires,
    };

    if let Some(drive_id) = config.drive_id.clone() {
        app_state.drive_manager
            .update_drive_credentials(&drive_id, config.drive_name.clone(), config.site_url.clone(), credentials, &config.user_id)
            .await
            .map_err(|e| e.to_string())?;
        app_state.drive_manager.persist().await.map_err(|e| e.to_string())?;
        return Ok(drive_id);
    }

    let drive_id = Uuid::new_v4().to_string();
    let sse_client_id = Uuid::new_v4().to_string();
    let drive_config = build_drive_config(drive_id, sse_client_id, config, credentials);

    let id = app_state.drive_manager.add_drive(drive_config).await.map_err(|e| e.to_string())?;
    app_state.drive_manager.persist().await.map_err(|e| e.to_string())?;
    Ok(id)
}

#[tauri::command]
pub async fn remove_drive(
    state: State<'_, AppStateHandle>,
    drive_id: String,
) -> CommandResult<Option<DriveConfig>> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    let result = app_state.drive_manager.remove_drive(&drive_id).await.map_err(|e| e.to_string())?;
    app_state.drive_manager.persist().await.map_err(|e| e.to_string())?;
    Ok(result)
}

#[tauri::command]
pub async fn get_ignore_patterns(
    state: State<'_, AppStateHandle>,
    drive_id: String,
) -> CommandResult<Vec<String>> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.get_ignore_patterns(&drive_id).await.map_err(|e| e.to_string())
}

/// The junk patterns applied to every drive on top of the user's own list.
/// They are not stored in the drive config, so the UI has no other way to show
/// them — and a file silently not syncing with no visible rule looks like a bug.
#[tauri::command]
pub fn get_default_ignore_patterns() -> Vec<String> {
    cloudreve_sync::drive::ignore::default_patterns()
}

#[tauri::command]
pub async fn set_ignore_patterns(
    state: State<'_, AppStateHandle>,
    drive_id: String,
    patterns: Vec<String>,
) -> CommandResult<()> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.update_ignore_patterns(&drive_id, patterns).await.map_err(|e| e.to_string())?;
    app_state.drive_manager.persist().await.map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
pub async fn get_drive_max_file_size(
    state: State<'_, AppStateHandle>,
    drive_id: String,
) -> CommandResult<u64> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.get_drive_max_file_size(&drive_id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_drive_max_file_size(
    state: State<'_, AppStateHandle>,
    drive_id: String,
    max_mb: u64,
) -> CommandResult<()> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.set_drive_max_file_size(&drive_id, max_mb).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_sync_status(
    state: State<'_, AppStateHandle>,
    drive_id: String,
) -> CommandResult<serde_json::Value> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.get_sync_status(&drive_id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_status_summary(
    state: State<'_, AppStateHandle>,
    drive_id: Option<String>,
) -> CommandResult<StatusSummary> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.get_status_summary(drive_id.as_deref()).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn resolve_conflict(
    state: State<'_, AppStateHandle>,
    drive_id: String,
    local_path: String,
    resolution: ConflictResolution,
) -> CommandResult<()> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state
        .drive_manager
        .resolve_conflict(&drive_id, &local_path, resolution)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_drives_info(state: State<'_, AppStateHandle>) -> CommandResult<Vec<DriveInfo>> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.get_drives_info().await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn pause_sync(
    state: State<'_, AppStateHandle>,
    drive_id: String,
) -> CommandResult<()> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.pause_drive(&drive_id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn resume_sync(
    state: State<'_, AppStateHandle>,
    drive_id: String,
) -> CommandResult<()> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    app_state.drive_manager.resume_drive(&drive_id).await.map_err(|e| e.to_string())
}

pub fn show_main_window(app: &AppHandle) {
    show_main_window_at_position(app, Position::TrayCenter);
}

pub fn show_main_window_center(app: &AppHandle) {
    show_main_window_at_position(app, Position::Center);
}

fn show_main_window_at_position(app: &AppHandle, position: Position) {
    if let Some(window) = app.get_webview_window("main_popup") {
        if window.is_visible().unwrap_or(false) {
            let _ = window.hide();
            return;
        }
        let _ = window.move_window(position);
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
        return;
    }

    match WebviewWindowBuilder::new(app, "main_popup", WebviewUrl::App(get_url_with_lang("index.html/#/popup").into()))
        .title("Cloudreve")
        .inner_size(370.0, 530.0)
        .resizable(false)
        .visible(false)
        .decorations(false)
        .skip_taskbar(true)
        .minimizable(false)
        .build()
    {
        Ok(window) => {
            let window_clone = window.clone();
            window.on_window_event(move |event| {
                if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                    if ConfigManager::get().fast_popup_launch() {
                        api.prevent_close();
                        let _ = window_clone.hide();
                    }
                }
            });
            let _ = window.move_window(position);
            let _ = window.show();
            let _ = window.set_focus();
        }
        Err(e) => {
            tracing::error!(target: "main_popup", error = %e, "Failed to create main window");
        }
    }
}

#[tauri::command]
pub async fn show_file_in_explorer(path: String) -> CommandResult<()> {
    showfile::show_path_in_file_manager(&path);
    Ok(())
}

#[tauri::command]
pub async fn show_add_drive_window(app: AppHandle) -> CommandResult<()> {
    show_add_drive_window_impl(&app);
    Ok(())
}

#[tauri::command]
pub async fn show_reauthorize_window(
    app: AppHandle,
    drive_id: String,
    site_url: String,
    drive_name: String,
) -> CommandResult<()> {
    show_reauthorize_window_impl(&app, &drive_id, &site_url, &drive_name);
    Ok(())
}

pub fn show_add_drive_window_impl(app: &AppHandle) {
    show_drive_window_internal(app, "Add Drive", &get_url_with_lang("index.html/#/add-drive"));
}

pub fn show_reauthorize_window_impl(app: &AppHandle, drive_id: &str, site_url: &str, drive_name: &str) {
    let encoded_site_url = urlencoding::encode(site_url);
    let encoded_drive_name = urlencoding::encode(drive_name);
    let url_path = format!("index.html/#/reauthorize/{}/{}/{}", drive_id, encoded_site_url, encoded_drive_name);
    show_drive_window_internal(app, "Reauthorize Drive", &get_url_with_lang(&url_path));
}

fn show_drive_window_internal(app: &AppHandle, title: &str, url_path: &str) {
    if let Some(window) = app.get_webview_window("add-drive") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
        return;
    }

    let effects = WindowEffectsConfig {
        effects: vec![WindowEffect::Blur],
        state: None,
        radius: None,
        color: None,
    };

    let builder = WebviewWindowBuilder::new(app, "add-drive", WebviewUrl::App(url_path.into()))
        .title(title)
        .inner_size(470.0, 630.0)
        .resizable(false)
        .visible(false)
        .effects(effects)
        .decorations(false)
        .minimizable(false);

    #[cfg(target_os = "macos")]
    let builder = builder
        .hidden_title(true)
        .title_bar_style(TitleBarStyle::Overlay);

    match builder.build() {
        Ok(window) => {
            let _ = window.move_window(Position::Center);
            let _ = window.create_overlay_titlebar();
            let _ = window.show();
            let _ = window.set_focus();
        }
        Err(e) => {
            tracing::error!(target: "main", error = %e, "Failed to create window: {}", title);
        }
    }
}

#[tauri::command]
pub async fn show_settings_window(app: AppHandle) -> CommandResult<()> {
    show_settings_window_impl(&app);
    Ok(())
}

pub fn show_settings_window_impl(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("settings") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
        return;
    }

    let builder = WebviewWindowBuilder::new(app, "settings", WebviewUrl::App(get_url_with_lang("index.html/#/settings").into()))
        .title("Settings")
        .inner_size(700.0, 500.0)
        .min_inner_size(600.0, 400.0)
        .visible(false)
        .resizable(true)
        .decorations(false)
        .minimizable(true);

    #[cfg(target_os = "macos")]
    let builder = builder
        .hidden_title(true)
        .title_bar_style(TitleBarStyle::Overlay);

    match builder.build() {
        Ok(window) => {
            let _ = window.move_window(Position::Center);
            let _ = window.create_overlay_titlebar();
            let _ = window.show();
            let _ = window.set_focus();
        }
        Err(e) => {
            tracing::error!(target: "main", error = %e, "Failed to create settings window");
        }
    }
}

/// Get whether auto-start is enabled using tauri-plugin-autostart
#[tauri::command]
pub async fn get_auto_start_enabled(app: AppHandle) -> CommandResult<bool> {
    app.autolaunch()
        .is_enabled()
        .map_err(|e| format!("Failed to get autostart state: {}", e))
}

/// Set auto-start using tauri-plugin-autostart
#[tauri::command]
pub async fn set_auto_start(app: AppHandle, enabled: bool) -> CommandResult<bool> {
    let autolaunch = app.autolaunch();
    if enabled {
        autolaunch.enable().map_err(|e| format!("Failed to enable autostart: {}", e))?;
    } else {
        autolaunch.disable().map_err(|e| format!("Failed to disable autostart: {}", e))?;
    }
    autolaunch.is_enabled().map_err(|e| format!("Failed to verify autostart: {}", e))
}

#[tauri::command]
pub async fn set_notify_credential_expired(enabled: bool) -> CommandResult<()> {
    ConfigManager::get().set_notify_credential_expired(enabled).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn close_window(app: AppHandle, label: String) {
    if let Some(window) = app.get_webview_window(&label) {
        let _ = window.close();
    }
}

#[tauri::command]
pub async fn open_folder_and_close_window(app: AppHandle, path: String, label: String) {
    showfile::show_path_in_file_manager(&path);
    if let Some(window) = app.get_webview_window(&label) {
        let _ = window.close();
    }
}

#[tauri::command]
pub async fn set_notify_file_conflict(enabled: bool) -> CommandResult<()> {
    ConfigManager::get().set_notify_file_conflict(enabled).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_fast_popup_launch(enabled: bool) -> CommandResult<()> {
    ConfigManager::get().set_fast_popup_launch(enabled).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_general_settings() -> CommandResult<GeneralSettings> {
    let config = ConfigManager::get().get_config();
    Ok(GeneralSettings {
        notify_credential_expired: config.notify_credential_expired,
        notify_file_conflict: config.notify_file_conflict,
        fast_popup_launch: config.fast_popup_launch,
        log_to_file: config.log_to_file,
        log_level: config.log_level.as_str().to_string(),
        log_max_files: config.log_max_files,
        log_dir: ConfigManager::get_log_dir().display().to_string(),
        language: config.language,
        heartbeat_interval: config.heartbeat_interval_secs,
        check_updates_on_startup: config.check_updates_on_startup,
        auto_install_updates: config.auto_install_updates,
    })
}

#[derive(serde::Serialize)]
pub struct GeneralSettings {
    pub notify_credential_expired: bool,
    pub notify_file_conflict: bool,
    pub fast_popup_launch: bool,
    pub log_to_file: bool,
    pub log_level: String,
    pub log_max_files: usize,
    pub log_dir: String,
    pub language: Option<String>,
    pub heartbeat_interval: u64,
    pub check_updates_on_startup: bool,
    pub auto_install_updates: bool,
}

#[tauri::command]
pub async fn set_heartbeat_interval(secs: u64) -> CommandResult<()> {
    ConfigManager::get().set_heartbeat_interval_secs(secs).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_log_to_file(enabled: bool) -> CommandResult<()> {
    ConfigManager::get().set_log_to_file(enabled).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_log_level(level: String) -> CommandResult<()> {
    let log_level = LogLevel::from_str(&level);
    ConfigManager::get().set_log_level(log_level).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_log_max_files(max_files: usize) -> CommandResult<()> {
    ConfigManager::get().set_log_max_files(max_files).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_language(app: AppHandle, language: Option<String>) -> CommandResult<()> {
    ConfigManager::get().set_language(language.clone()).map_err(|e| e.to_string())?;

    let locale = language.unwrap_or_else(|| sys_locale::get_locale().unwrap_or_else(|| String::from("en-US")));
    rust_i18n::set_locale(&locale);

    if let Some(window) = app.get_webview_window("main_popup") {
        let _ = window.close();
        let _ = window.destroy();
    }
    Ok(())
}

#[tauri::command]
pub async fn set_check_updates_on_startup(enabled: bool) -> CommandResult<()> {
    ConfigManager::get()
        .set_check_updates_on_startup(enabled)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn set_auto_install_updates(enabled: bool) -> CommandResult<()> {
    ConfigManager::get()
        .set_auto_install_updates(enabled)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn open_log_folder() -> CommandResult<()> {
    let log_dir = ConfigManager::get_log_dir();
    if !log_dir.exists() {
        std::fs::create_dir_all(&log_dir).map_err(|e| e.to_string())?;
    }
    showfile::show_path_in_file_manager(log_dir.display().to_string());
    Ok(())
}

#[derive(serde::Serialize)]
pub struct UserProfile {
    pub user: cloudreve_api::models::user::User,
    pub avatar_url: String,
    pub profile_url: String,
}

#[tauri::command]
pub async fn get_user_profile(state: State<'_, AppStateHandle>) -> CommandResult<Option<UserProfile>> {
    let app_state = state.get().ok_or_else(|| "App not yet initialized".to_string())?;
    let drives = app_state.drive_manager.get_drives_info().await.map_err(|e| e.to_string())?;
    let first = match drives.first() {
        Some(d) => d,
        None => return Ok(None),
    };
    let mount = app_state.drive_manager.get_drive(&first.id).await
        .ok_or_else(|| "Drive not found".to_string())?;
    let user = mount.cr_client.get_user_me().await.map_err(|e| e.to_string())?;
    let avatar_url = format!("{}/api/v4/user/avatar/{}", first.instance_url, user.id);
    let profile_url = format!("{}/profile/{}", first.instance_url, user.id);
    Ok(Some(UserProfile {
        user,
        avatar_url,
        profile_url,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `AddDriveArgs.mode` must default to `FullMirror` when the field is
    /// absent (old/naive callers, and today's actual `AddDrive.tsx` before
    /// this task wires the selector) and must carry `OnDemand` through when
    /// present. Exercises `build_drive_config`, the exact helper `add_drive`
    /// calls, so this cannot pass while the command still hardcodes the mode.
    #[test]
    fn add_drive_args_mode_defaults_to_full_mirror_and_threads_on_demand() {
        let with_mode: AddDriveArgs = serde_json::from_str(
            r#"{
                "site_url": "https://example.com",
                "access_token": "at",
                "refresh_token": "rt",
                "access_token_expires": 3600,
                "refresh_token_expires": 3600,
                "drive_name": "My Drive",
                "remote_path": "/",
                "local_path": "/tmp/drive",
                "user_id": "u1",
                "mode": "on_demand"
            }"#,
        )
        .expect("valid AddDriveArgs JSON with mode");
        assert_eq!(with_mode.mode, DriveMode::OnDemand);

        let without_mode: AddDriveArgs = serde_json::from_str(
            r#"{
                "site_url": "https://example.com",
                "access_token": "at",
                "refresh_token": "rt",
                "access_token_expires": 3600,
                "refresh_token_expires": 3600,
                "drive_name": "My Drive",
                "remote_path": "/",
                "local_path": "/tmp/drive",
                "user_id": "u1"
            }"#,
        )
        .expect("valid AddDriveArgs JSON without mode");
        assert_eq!(without_mode.mode, DriveMode::FullMirror);

        let credentials = Credentials {
            access_token: Some("at".into()),
            refresh_token: "rt".into(),
            access_expires: Some("2030-01-01T00:00:00Z".into()),
            refresh_expires: "2030-01-01T00:00:00Z".into(),
        };

        let on_demand_config = build_drive_config(
            "drive-1".into(),
            "sse-1".into(),
            with_mode,
            credentials.clone(),
        );
        assert_eq!(on_demand_config.mode, DriveMode::OnDemand);

        let mirror_config = build_drive_config(
            "drive-2".into(),
            "sse-2".into(),
            without_mode,
            credentials,
        );
        assert_eq!(mirror_config.mode, DriveMode::FullMirror);
    }

    /// The Settings dialog displays whatever this command returns, and nothing
    /// else describes the built-in rules. Wiring it to the drive's own list, or
    /// to an empty vector, would put the defaults back out of sight — a file
    /// that stops syncing then has no visible cause anywhere in the app.
    ///
    /// That the list is complete and faithful to what the engine applies is
    /// pinned in `cloudreve_sync::drive::ignore`; what is checked here is that
    /// this boundary hands that list over at all.
    #[test]
    fn the_settings_dialog_is_given_the_built_in_rules() {
        let shown = super::get_default_ignore_patterns();

        for expected in [".DS_Store", "Thumbs.db", ".Spotlight-V100"] {
            assert!(
                shown.iter().any(|p| p == expected),
                "the dialog is never told about `{expected}`: {shown:?}"
            );
        }
    }
}
