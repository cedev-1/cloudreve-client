use anyhow::Context;
use cloudreve_sync::{ConfigManager, DriveManager, EventBroadcaster, LogConfig, LogGuard};
use std::sync::Arc;
use tauri::{
    async_runtime::spawn,
    menu::{Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    AppHandle, Emitter, Manager, RunEvent,
};
use tauri_plugin_deep_link::DeepLinkExt;
use tokio::sync::OnceCell;

use crate::commands::{show_add_drive_window_impl, show_main_window, show_settings_window_impl};
mod commands;
mod event_handler;

#[macro_use]
extern crate rust_i18n;

i18n!("../locales");

fn init_i18n() {
    use rust_i18n::set_locale;
    use sys_locale::get_locale;

    let locale = ConfigManager::try_get()
        .and_then(|cm| cm.language())
        .unwrap_or_else(|| get_locale().unwrap_or_else(|| String::from("en-US")));
    set_locale(locale.as_str());
}

pub fn get_effective_locale() -> String {
    use sys_locale::get_locale;
    ConfigManager::try_get()
        .and_then(|cm| cm.language())
        .unwrap_or_else(|| get_locale().unwrap_or_else(|| String::from("en-US")))
}

pub struct AppState {
    pub drive_manager: Arc<DriveManager>,
    pub event_broadcaster: Arc<EventBroadcaster>,
    #[allow(dead_code)]
    log_guard: LogGuard,
}

static APP_STATE: OnceCell<AppState> = OnceCell::const_new();

async fn init_sync_service(app: AppHandle) -> anyhow::Result<()> {
    let log_guard = cloudreve_sync::logging::init_logging(LogConfig::from_config_manager())
        .context("Failed to initialize logging system")?;

    tracing::info!(target: "main", "Starting Cloudreve Sync Service...");

    let event_broadcaster = Arc::new(EventBroadcaster::new(100));
    spawn_event_bridge(app.clone(), &event_broadcaster);

    let drive_manager = Arc::new(
        DriveManager::new(event_broadcaster.clone()).context("Failed to create DriveManager")?,
    );

    drive_manager.spawn_command_processor().await;

    drive_manager
        .load()
        .await
        .context("Failed to load drive configurations")?;

    event_broadcaster.connection_status_changed(true);

    let state = AppState { drive_manager, event_broadcaster, log_guard };

    APP_STATE
        .set(state)
        .map_err(|_| anyhow::anyhow!("App state already initialized"))?;

    app.manage(AppStateHandle);

    tracing::info!(target: "main", "Sync service started");
    Ok(())
}

pub struct AppStateHandle;

impl AppStateHandle {
    pub fn get(&self) -> Option<&'static AppState> {
        APP_STATE.get()
    }
}

fn spawn_event_bridge(app_handle: AppHandle, event_broadcaster: &EventBroadcaster) {
    let mut receiver = event_broadcaster.subscribe();
    spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    event_handler::handle_event(&app_handle, &event);
                    event_handler::emit_event(&app_handle, &event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(target: "events", skipped = n, "Event receiver lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });
}

async fn shutdown() {
    tracing::info!(target: "main", "Initiating shutdown...");
    if let Some(state) = APP_STATE.get() {
        state.event_broadcaster.connection_status_changed(false);
        state.drive_manager.shutdown().await;
        if let Err(e) = state.drive_manager.persist().await {
            tracing::error!(target: "main", error = %e, "Failed to persist drive configurations");
        }
    }
    tracing::info!(target: "main", "Shutdown complete");
}

fn setup_tray(app: &tauri::App) -> anyhow::Result<()> {
    let show_i = MenuItem::with_id(app, "show", t!("show").as_ref(), true, None::<&str>)?;
    let add_drive_i = MenuItem::with_id(app, "add_drive", t!("addNewDrive").as_ref(), true, None::<&str>)?;
    let settings_i = MenuItem::with_id(app, "settings", t!("settings").as_ref(), true, None::<&str>)?;
    let quit_i = MenuItem::with_id(app, "quit", t!("quit").as_ref(), true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&show_i, &add_drive_i, &settings_i, &quit_i])?;

    TrayIconBuilder::new()
        .icon(app.default_window_icon().unwrap().clone())
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| match event.id.as_ref() {
            "show" => show_main_window(app),
            "add_drive" => show_add_drive_window_impl(app),
            "settings" => show_settings_window_impl(app),
            "quit" => app.exit(0),
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            tauri_plugin_positioner::on_tray_event(tray.app_handle(), &event);
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                show_main_window(tray.app_handle());
            }
        })
        .build(app)?;

    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    if let Err(e) = ConfigManager::init() {
        eprintln!("Failed to initialize config manager: {}", e);
    }

    init_i18n();

    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, argv, _cwd| {
            if argv.len() > 1 {
                let _ = app.emit("deeplink", argv[1].clone());
                show_add_drive_window_impl(app);
            }
        }))
        .plugin(tauri_plugin_autostart::init(tauri_plugin_autostart::MacosLauncher::LaunchAgent, Some(vec![])))
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_http::init())
        .plugin(tauri_plugin_frame::init())
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_os::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_prevent_default::debug())
        .setup(|app| {
            #[cfg(desktop)]
            let _ = app.handle().plugin(tauri_plugin_positioner::init());

            if let Err(e) = setup_tray(app) {
                tracing::error!(target: "main", error = %e, "Failed to setup tray (continuing)");
            }

            #[cfg(desktop)]
            {
                if let Err(e) = app.deep_link().register("cloudreve") {
                    tracing::warn!(target: "main", error = %e, "Failed to register deep link scheme via plugin, trying lsregister");
                    // On macOS in dev mode, try registering via lsregister directly
                    #[cfg(target_os = "macos")]
                    if let Ok(exe) = std::env::current_exe() {
                        let lsregister = "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister";
                        let _ = std::process::Command::new(lsregister)
                            .arg("-f")
                            .arg(&exe)
                            .output();
                        tracing::info!(target: "main", path = %exe.display(), "Ran lsregister for deep link");
                    }
                }

                let handle = app.handle().clone();
                app.deep_link().on_open_url(move |event| {
                    for url in event.urls() {
                        tracing::info!(target: "main", url = %url, "Deep link received");
                        let _ = handle.emit("deeplink", url.to_string());
                        show_add_drive_window_impl(&handle);
                    }
                });
            }

            let app_handle = app.handle().clone();
            spawn(async move {
                if let Err(e) = init_sync_service(app_handle).await {
                    tracing::error!(target: "main", error = %e, "Failed to initialize sync service");
                }
            });

            if let Some(window) = app.get_webview_window("main") {
                let _ = window.destroy();
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::is_dir_empty,
            commands::list_drives,
            commands::add_drive,
            commands::remove_drive,
            commands::get_ignore_patterns,
            commands::set_ignore_patterns,
            commands::get_sync_status,
            commands::get_status_summary,
            commands::get_drives_info,
            commands::show_file_in_explorer,
            commands::show_add_drive_window,
            commands::show_reauthorize_window,
            commands::show_settings_window,
            commands::get_auto_start_enabled,
            commands::set_auto_start,
            commands::set_notify_credential_expired,
            commands::close_window,
            commands::open_folder_and_close_window,
            commands::set_notify_file_conflict,
            commands::set_fast_popup_launch,
            commands::get_general_settings,
            commands::set_log_to_file,
            commands::set_log_level,
            commands::set_log_max_files,
            commands::set_language,
            commands::open_log_folder,
            commands::get_user_profile,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|_app_handle, event| match event {
            RunEvent::ExitRequested { api, code, .. } => {
                if code.is_none() {
                    api.prevent_exit();
                }
            }
            RunEvent::Exit => {
                tauri::async_runtime::block_on(shutdown());
            }
            _ => {}
        });
}
