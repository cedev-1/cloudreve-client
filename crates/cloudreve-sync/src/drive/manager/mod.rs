mod command_handlers;
pub(crate) mod favicon;
mod types;

pub use types::*;

use crate::drive::commands::{ManagerCommand, MountCommand};
use crate::drive::heartbeat::HeartbeatManager;
use crate::drive::mounts::{Credentials, DriveConfig, DriveMode, Mount};
use crate::{EventBroadcaster, SummaryNotifier};
use crate::inventory::InventoryDb;
use crate::tasks::TaskProgress;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};
use tokio::sync::{Mutex, RwLock, mpsc};

pub struct DriveManager {
    pub(super) drives: Arc<RwLock<HashMap<String, Arc<Mount>>>>,
    config_dir: PathBuf,
    pub(super) inventory: Arc<InventoryDb>,
    pub(super) command_tx: mpsc::UnboundedSender<ManagerCommand>,
    pub(super) command_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<ManagerCommand>>>>,
    pub(super) processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(super) event_broadcaster: Arc<EventBroadcaster>,
    pub(super) summary_notifier: Arc<SummaryNotifier>,
    heartbeat_manager: HeartbeatManager,
}

impl DriveManager {
    /// Create a new DriveManager instance
    pub fn new(event_broadcaster: Arc<EventBroadcaster>) -> Result<Self> {
        let config_dir = Self::get_config_dir()?;

        // Ensure config directory exists
        if !config_dir.exists() {
            fs::create_dir_all(&config_dir)
                .context("Failed to create .cloudreve config directory")?;
        }
        // The config dir holds drive credentials (OAuth tokens); keep it
        // owner-only so other local users can't read them.
        crate::utils::secure_fs::restrict_dir(&config_dir)?;

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let drives = Arc::new(RwLock::new(HashMap::new()));
        let heartbeat_manager = HeartbeatManager::new(drives.clone(), event_broadcaster.clone());
        let summary_notifier = Arc::new(SummaryNotifier::new(event_broadcaster.clone()));

        Ok(Self {
            config_dir,
            drives,
            inventory: Arc::new(InventoryDb::new().context("Failed to create inventory database")?),
            command_tx,
            command_rx: Arc::new(Mutex::new(Some(command_rx))),
            processor_handle: Arc::new(Mutex::new(None)),
            event_broadcaster: event_broadcaster,
            summary_notifier,
            heartbeat_manager,
        })
    }

    pub fn get_inventory(&self) -> Arc<InventoryDb> {
        self.inventory.clone()
    }

    /// Get the .cloudreve config directory path
    fn get_config_dir() -> Result<PathBuf> {
        let home_dir = dirs::home_dir().context("Failed to get user home directory")?;
        Ok(home_dir.join(".cloudreve"))
    }

    /// Get the config file path
    fn get_config_file(&self) -> PathBuf {
        self.config_dir.join("drives.json")
    }

    /// Load drive configurations from disk
    pub async fn load(&self) -> Result<()> {
        let config_file = self.get_config_file();

        if !config_file.exists() {
            tracing::info!(target: "drive", "No existing drive config found, starting fresh");
            self.event_broadcaster.no_drive();
            return Ok(());
        }

        tracing::debug!(target: "drive", path = %config_file.display(), "Loading drive configurations");

        let content =
            fs::read_to_string(&config_file).context("Failed to read drive config file")?;

        let state: DriveState =
            serde_json::from_str(&content).context("Failed to parse drive config")?;

        // Add drives to manager
        let mut count = 0;
        for config in state.drives.iter() {
            match self.add_drive(config.clone()).await {
                Ok(_) => {
                    count += 1;
                }
                Err(e) => {
                    tracing::error!(target: "drive", drive_id = %config.id, error = ?e, "Failed to add drive, skipping");
                    // crate::utils::toast::send_warning_toast(
                    //     &t!("driveLoadFailed"),
                    //     &format!("{}: {}", config.name, e),
                    // );
                }
            }
        }

        if count == 0 {
            self.event_broadcaster.no_drive();
        }

        tracing::info!(target: "drive", count = count, "Loaded drive(s) from config");

        // Start heartbeat monitoring after drives are loaded
        self.heartbeat_manager.start().await;

        Ok(())
    }

    /// Persist drive configurations to disk
    pub async fn persist(&self) -> Result<()> {
        let config_file = self.get_config_file();
        let write_guard = self.drives.write().await;

        tracing::debug!(target: "drive", path = %config_file.display(), count = write_guard.len(), "Persisting drive configurations");

        let mut new_state = DriveState::default();

        // Update drive states from underlying mounts
        for (_, mount) in write_guard.iter() {
            let config = mount.get_config().await;
            new_state.drives.push(config);
        }

        let content =
            serde_json::to_string_pretty(&new_state).context("Failed to serialize drive state")?;
        // drives.json contains OAuth access/refresh tokens: persist it owner-only.
        crate::utils::secure_fs::write_private(&config_file, content)
            .context("Failed to write drive config file")?;

        tracing::info!(target: "drive", count = new_state.drives.len(), "Persisted drive(s) to config");

        Ok(())
    }

    /// Register a callback to be invoked when status UI changes
    /// This is a dummy implementation that calls the callback every 30 seconds
    pub fn register_on_status_ui_changed<F>(&self, fnc: F) -> Result<()>
    where
        F: Fn() + Send + 'static,
    {
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(30));
                tracing::trace!(target: "drive::manager", "Register_on_status_ui_changed: Invoking status UI changed callback");
                fnc();
            }
        });
        Ok(())
    }

    /// Add a new drive
    pub async fn add_drive(&self, mut config: DriveConfig) -> Result<String> {
        // Fetch favicon if icon_path is not set or doesn't exist
        if config.icon_path.is_none()
            || !config
                .icon_path
                .as_ref()
                .map(|p| std::path::Path::new(p).exists())
                .unwrap_or(false)
        {
            match favicon::fetch_and_save_favicon(&config.instance_url).await {
                Ok(result) => {
                    tracing::info!(target: "drive", ico_path = %result.ico_path, raw_path = %result.raw_path, "Favicon fetched successfully");
                    config.icon_path = Some(result.ico_path);
                    config.raw_icon_path = Some(result.raw_path);
                }
                Err(e) => {
                    tracing::warn!(target: "drive", error = %e, "Failed to fetch favicon, continuing without icon");
                }
            }
        }

        // Ensure sse_client_id is set (for configs migrated from before this field existed)
        if config.sse_client_id.is_empty() {
            config.sse_client_id = uuid::Uuid::new_v4().to_string();
        }

        let mode = config.mode;

        // Review F3: build AND `start()` the mount BEFORE taking
        // `drives.write()`. On-demand `start()` now runs the phase-4 mount
        // lifecycle (pre-clean rungs, escalation, the mount call itself) —
        // north of 30s in the worst case — and holding the write lock
        // across that starved every `drives.read()` path (status polling,
        // `get_drive`, …), making the whole app appear hung while one drive
        // is merely slow to (re)mount. Mirrors the pattern `resume_drive`
        // already uses: do the heavy lifting on a standalone `Mount` with
        // no lock held, then take a lock only for the final map mutation.
        //
        // No uniqueness check is being relaxed by moving the lock this
        // late: the old code never checked for a colliding id either — it
        // went straight to `insert`, which silently overwrites on a
        // collision regardless of when the lock is taken. In practice a
        // collision can't happen: the Tauri `add_drive` command always
        // mints a fresh `Uuid::new_v4` for a new drive, and
        // `DriveManager::load()`'s startup replay calls `add_drive`
        // sequentially (one `.await` at a time, never concurrently) over
        // ids that were each minted uniquely when originally added.
        let mut mount = Mount::new(
            config.clone(),
            self.inventory.clone(),
            self.command_tx.clone(),
            self.summary_notifier.clone(),
        )
        .await;
        if let Err(e) = mount.start().await {
            tracing::error!(target: "drive", error = ?e, "Failed to start drive");
            return Err(e).context("Failed to start drive");
        }

        let mount_arc = Arc::new(mount);
        mount_arc.spawn_command_processor(mount_arc.clone()).await;
        mount_arc
            .spawn_remote_event_processor(mount_arc.clone())
            .await;
        mount_arc.spawn_props_refresh_task().await;
        // D2: an on-demand drive has no fs watcher and no periodic full sync
        // to catch up on — the periodic worker exists purely to paper over
        // what those two would otherwise miss for a `FullMirror` drive.
        if mode == DriveMode::FullMirror {
            mount_arc.spawn_periodic_sync().await;
        } else {
            // D6: start folding this on-demand drive's VfsEvents into its
            // counters/toasts right away — see `spawn_vfs_event_pump`'s doc
            // for why this is the only worker never re-spawned on resume.
            mount_arc.spawn_vfs_event_pump().await;
        }
        let id = mount_arc.id.clone();
        let command_tx = mount_arc.command_tx.clone();

        let mut write_guard = self.drives.write().await;
        write_guard.insert(id.clone(), mount_arc);
        drop(write_guard);

        // Start heartbeat monitoring if this is the first drive
        self.heartbeat_manager.start().await;

        // Trigger an initial full sync so existing remote/local files are
        // reconciled — skipped for on-demand (D2): there is no local mirror
        // to reconcile, and `full_sync` itself refuses to run for this mode
        // anyway (see its own unreachable guard).
        if mode == DriveMode::FullMirror {
            if let Err(e) = command_tx.send(MountCommand::FullSync) {
                tracing::warn!(target: "drive::manager", drive_id = %id, error = %e, "Failed to send initial FullSync command");
            } else {
                tracing::info!(target: "drive::manager", drive_id = %id, "Initial FullSync scheduled");
            }
        }

        Ok(id)
    }

    // Search drive by child file path.
    // Child path can be up to the sync root path.
    pub async fn search_drive_by_child_path(&self, path: &str) -> Option<Arc<Mount>> {
        let read_guard = self.drives.read().await;

        // Convert the input path to an absolute PathBuf for comparison
        let target_path = PathBuf::from(path);
        let target_path = match target_path.canonicalize() {
            Ok(p) => p,
            Err(_) => {
                // If canonicalize fails (e.g., path doesn't exist), try to work with the original path
                target_path
            }
        };

        // Iterate through all drives and check if the target path is under their sync root
        for (_, mount) in read_guard.iter() {
            let sync_path = mount.get_sync_path().await;

            // Normalize the sync path
            let sync_path = match sync_path.canonicalize() {
                Ok(p) => p,
                Err(_) => sync_path,
            };

            // Check if target_path starts with sync_path (is a child of sync_path)
            if target_path.starts_with(&sync_path) {
                return Some(mount.clone());
            }
        }

        None
    }

    /// Remove a drive by ID
    ///
    /// This will:
    /// 1. Stop and delete the mount (unregister sync root, cleanup inventory)
    /// 2. Remove the drive from the manager's drive map
    ///
    /// Note: The caller is responsible for calling `persist()` after this to save the config.
    pub async fn remove_drive(&self, id: &str) -> Result<Option<DriveConfig>> {
        let mut write_guard = self.drives.write().await;

        // Remove the mount from the map
        let mount = match write_guard.remove(id) {
            Some(m) => m,
            None => return Ok(None),
        };

        // Get the config before deleting the mount
        let config = mount.get_config().await;

        // Drop the write guard before calling delete to avoid potential deadlocks
        drop(write_guard);

        // Delete the mount (unregister sync root, cleanup, etc.)
        mount.delete().await.context("Failed to delete mount")?;

        // Broadcast no_drive event if no drives remain
        if self.drives.read().await.is_empty() {
            self.event_broadcaster.no_drive();
            // Stop heartbeat when there are no drives to monitor
            self.heartbeat_manager.stop().await;
        }

        tracing::info!(target: "drive::manager", drive_id = %id, "Drive removed successfully");

        Ok(Some(config))
    }

    /// Get a drive by ID
    pub async fn get_drive(&self, id: &str) -> Option<Arc<Mount>> {
        let read_guard = self.drives.read().await;
        read_guard.get(id).cloned()
    }

    /// List all drives
    pub async fn list_drives(&self) -> Vec<DriveConfig> {
        // let read_guard = self.drives.read().await;
        // read_guard
        //     .values()
        //     .map(|mount| mount.get_config())
        //     .collect()
        Vec::new()
    }

    /// Update drive configuration
    pub async fn update_drive(&self, _id: &str, _config: DriveConfig) -> Result<()> {
        // let mut write_guard = self.drives.write().await;
        // if write_guard.contains_key(id) {
        //     // write_guard.insert(id.to_string(), Mount::new(config.clone()));
        //     Ok(())
        // } else {
        //     anyhow::bail!("Drive not found: {}", id)
        // }
        Err(anyhow::anyhow!("Not implemented"))
    }

    /// Update drive credentials for reauthorization.
    ///
    /// This updates the name, instance_url, and credentials for an existing drive.
    /// It also clears and re-fetches the site icon.
    ///
    /// # Arguments
    /// * `id` - The drive ID to update
    /// * `name` - New drive name
    /// * `instance_url` - New instance URL
    /// * `credentials` - New credentials
    /// * `user_id` - The user ID from the new authorization (must match original)
    ///
    /// # Errors
    /// Returns an error if:
    /// - Drive is not found
    /// - The user_id doesn't match the original drive's user_id
    pub async fn update_drive_credentials(
        &self,
        id: &str,
        name: String,
        instance_url: String,
        credentials: Credentials,
        user_id: &str,
    ) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;

        // Check if user_id matches
        {
            let config = mount.config.read().await;
            if config.user_id != user_id {
                return Err(anyhow::anyhow!(t!("userIdMismatch")));
            }
        }

        // Update the config
        let mut config = mount.config.write().await;

        // Clear old icon files if they exist
        if let Some(ref ico_path) = config.icon_path {
            if std::path::Path::new(ico_path).exists() {
                if let Err(e) = std::fs::remove_file(ico_path) {
                    tracing::warn!(target: "drive::manager", drive_id = %id, error = %e, "Failed to remove old ICO file");
                }
            }
        }
        if let Some(ref raw_path) = config.raw_icon_path {
            if std::path::Path::new(raw_path).exists() {
                if let Err(e) = std::fs::remove_file(raw_path) {
                    tracing::warn!(target: "drive::manager", drive_id = %id, error = %e, "Failed to remove old raw icon file");
                }
            }
        }

        // Update fields
        config.name = name;
        config.instance_url = instance_url.clone();
        config.credentials = credentials.clone();

        // Clear icon paths - will be re-fetched
        config.icon_path = None;
        config.raw_icon_path = None;

        // Fetch new favicon
        match favicon::fetch_and_save_favicon(&instance_url).await {
            Ok(result) => {
                tracing::info!(target: "drive::manager", drive_id = %id, ico_path = %result.ico_path, raw_path = %result.raw_path, "Favicon re-fetched successfully");
                config.icon_path = Some(result.ico_path);
                config.raw_icon_path = Some(result.raw_path);
            }
            Err(e) => {
                tracing::warn!(target: "drive::manager", drive_id = %id, error = %e, "Failed to re-fetch favicon, continuing without icon");
            }
        }

        drop(config);

        // Update the client's tokens
        mount
            .cr_client
            .set_tokens_with_expiry(&cloudreve_api::models::user::Token {
                access_token: credentials.access_token.clone().unwrap_or_default(),
                refresh_token: credentials.refresh_token.clone(),
                access_expires: credentials.access_expires.clone().unwrap_or_default(),
                refresh_expires: credentials.refresh_expires.clone(),
            })
            .await?;

        // Clear the credential expired flag since we got new credentials
        mount.set_credential_expired(false).await;

        tracing::info!(target: "drive::manager", drive_id = %id, "Drive credentials updated successfully");

        Ok(())
    }

    /// Get the ignore patterns for a drive
    pub async fn get_ignore_patterns(&self, id: &str) -> Result<Vec<String>> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;
        let config = mount.config.read().await;
        Ok(config.ignore_patterns.clone())
    }

    /// Update the ignore patterns for a drive.
    ///
    /// Validates patterns, updates the config, and rebuilds the `IgnoreMatcher`.
    pub async fn update_ignore_patterns(&self, id: &str, patterns: Vec<String>) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;
        mount.update_ignore_patterns(patterns).await
    }

    /// Get the max file size limit (in MB) for a drive.
    pub async fn get_drive_max_file_size(&self, id: &str) -> Result<u64> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;
        let config = mount.config.read().await;
        Ok(config.max_file_size_mb)
    }

    /// Update the max file size limit (in MB) for a drive.
    pub async fn set_drive_max_file_size(&self, id: &str, max_mb: u64) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;
        let mut config = mount.config.write().await;
        config.max_file_size_mb = max_mb;
        drop(config);
        drop(read_guard);
        self.persist().await
    }

    /// Placeholder: Enable/disable a drive
    pub async fn set_drive_enabled(&self, _id: &str, _enabled: bool) -> Result<()> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    /// Start syncing a drive: send a FullSync command to the mount.
    pub async fn start_sync(&self, id: &str) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;

        if let Err(e) = mount.command_tx.send(MountCommand::FullSync) {
            anyhow::bail!("Failed to send FullSync command: {}", e);
        }

        tracing::info!(target: "drive::manager", drive_id = %id, "FullSync triggered");
        Ok(())
    }

    /// Stop syncing a drive: shut down its background tasks.
    pub async fn stop_sync(&self, id: &str) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;

        mount.shutdown().await;
        tracing::info!(target: "drive::manager", drive_id = %id, "Sync stopped");
        Ok(())
    }

    /// Pause a drive: stop background workers and cancel tasks, but keep the
    /// mount alive so it can be resumed.
    pub async fn pause_drive(&self, id: &str) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;

        mount.pause().await;
        tracing::info!(target: "drive::manager", drive_id = %id, "Drive paused");
        Ok(())
    }

    /// Resume a paused drive: restart background workers and trigger a full sync.
    pub async fn resume_drive(&self, id: &str) -> Result<()> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?
            .clone();
        drop(read_guard);

        mount.resume().await;

        let mode = mount.get_config().await.mode;
        match mode {
            DriveMode::FullMirror => {
                // Restart background workers
                let sync_path = mount.get_sync_path().await;
                mount.start_fs_watcher_public(&sync_path).await?;
                mount.spawn_remote_event_processor(mount.clone()).await;
                mount.spawn_periodic_sync().await;
                mount.spawn_props_refresh_task().await;

                // Trigger a full sync to catch up on missed changes
                let _ = mount.command_tx.send(MountCommand::FullSync);
            }
            DriveMode::OnDemand => {
                // D5: remount instead of restarting a fs watcher; no
                // periodic sync worker and no FullSync — same reasons as
                // `add_drive`'s on-demand branch. The vfs event pump is
                // NOT re-spawned here on purpose — see
                // `spawn_vfs_event_pump`'s doc: `pause()` never stopped it,
                // so it is still draining the same `Vfs`'s events right now.
                mount.remount_on_demand().await?;
                mount.spawn_remote_event_processor(mount.clone()).await;
                mount.spawn_props_refresh_task().await;
            }
        }

        tracing::info!(target: "drive::manager", drive_id = %id, "Drive resumed");
        Ok(())
    }

    /// Get sync status for a drive.
    pub async fn get_sync_status(&self, id: &str) -> Result<serde_json::Value> {
        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", id))?;

        let flags = mount.get_status_flags().await;
        let inflight = mount.task_queue.inflight_count();

        let status = if flags.is_credential_expired() {
            "credential_expired"
        } else if inflight > 0 {
            "syncing"
        } else if flags.is_event_push_subscribed() {
            "in_sync"
        } else {
            "idle"
        };

        Ok(serde_json::json!({
            "drive_id": id,
            "status": status,
            "event_push_subscribed": flags.is_event_push_subscribed(),
            "credential_expired": flags.is_credential_expired(),
            "inflight_tasks": inflight,
        }))
    }

    /// Get a summary of the current status including all drives and recent tasks.
    ///
    /// # Arguments
    /// * `drive_id` - Optional drive ID to filter tasks. If None, returns tasks from all drives.
    ///                Note: drives list always returns all drives regardless of this filter.
    pub async fn get_status_summary(&self, drive_id: Option<&str>) -> Result<StatusSummary> {
        // Get all drive configs (unfiltered) and check if any has completed initial sync
        let read_guard = self.drives.read().await;
        let mut drives = Vec::with_capacity(read_guard.len());
        let mut has_ever_synced = false;
        let mut paused_drives = Vec::new();
        let mut pending_uploads = HashMap::new();
        for mount in read_guard.values() {
            let config = mount.get_config().await;
            // D7: an on-demand drive never runs `FullSync`
            // (`is_initial_sync_completed` can never flip true for it — see
            // the guard in `mounts.rs`'s command processor), so gating on
            // that flag would leave a drive that has been mounted and
            // working the whole time misreported as "never synced" forever.
            // Being mounted (`vfs` populated) IS this mode's "synced/idle":
            // reads/writes go straight through the live volume, there is no
            // separate reconciliation pass to have "completed".
            let synced = if config.mode == DriveMode::OnDemand {
                mount.vfs.lock().await.is_some()
            } else {
                mount.get_status_flags().await.is_initial_sync_completed()
            };
            if synced {
                has_ever_synced = true;
            }
            if config.mode == DriveMode::OnDemand {
                pending_uploads.insert(
                    config.id.clone(),
                    mount.vfs_pending_uploads.load(std::sync::atomic::Ordering::Relaxed),
                );
            }
            if mount.is_paused() {
                paused_drives.push(mount.id.clone());
            }
            drives.push(config);
        }

        // Query recent tasks from inventory (filtered by drive_id if provided)
        let recent_tasks = self
            .inventory
            .query_recent_tasks(drive_id)
            .context("Failed to query recent tasks")?;

        // Collect running task progress from all task queues
        // Build a map of task_id -> TaskProgress for quick lookup
        let mut progress_map: HashMap<String, TaskProgress> = HashMap::new();

        if let Some(drive_filter) = drive_id {
            // If filtering by drive, only get progress from that drive's task queue
            if let Some(mount) = read_guard.get(drive_filter) {
                for progress in mount.task_queue.ongoing_progress().await {
                    progress_map.insert(progress.task_id.clone(), progress);
                }
            }
        } else {
            // Get progress from all drives
            for mount in read_guard.values() {
                for progress in mount.task_queue.ongoing_progress().await {
                    progress_map.insert(progress.task_id.clone(), progress);
                }
            }
        }

        // Merge progress info into active tasks
        let active_tasks: Vec<TaskWithProgress> = recent_tasks
            .active
            .into_iter()
            .map(|task| {
                let progress = progress_map.remove(&task.id);
                TaskWithProgress { task, live_progress: progress }
            })
            .collect();

        let finished_tasks = recent_tasks.finished;

        // Collect pending conflicts (filtered by drive_id if provided)
        let mut conflicts = Vec::new();
        let conflicted_files = self
            .inventory
            .query_conflicts(drive_id)
            .context("Failed to query conflicts")?;
        for meta in conflicted_files {
            let meta_drive_id = meta.drive_id.to_string();
            let drive_name = match read_guard.get(&meta_drive_id) {
                Some(mount) => mount.get_config().await.name,
                // Skip conflicts for drives that are no longer mounted
                None => continue,
            };
            let (local_size, local_modified_at) = match fs::metadata(&meta.local_path) {
                Ok(m) => (
                    Some(m.len() as i64),
                    m.modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64),
                ),
                Err(_) => (None, None),
            };
            conflicts.push(ConflictInfo {
                id: meta.id,
                drive_id: meta_drive_id,
                drive_name,
                local_path: meta.local_path,
                synced_size: meta.size,
                local_size,
                local_modified_at,
            });
        }

        Ok(StatusSummary {
            drives,
            active_tasks,
            finished_tasks,
            has_ever_synced,
            conflicts,
            paused_drives,
            pending_uploads,
        })
    }

    /// Resolve a pending file conflict.
    ///
    /// - `KeepLocal`: mark as override and re-upload, replacing the remote version.
    /// - `KeepRemote`: clear the conflict and download, replacing the local version.
    /// - `KeepBoth`: rename the local file to a "conflicted copy", upload the copy,
    ///   then download the remote version at the original path.
    pub async fn resolve_conflict(
        &self,
        drive_id: &str,
        local_path: &str,
        resolution: ConflictResolution,
    ) -> Result<()> {
        use crate::inventory::ConflictState;
        use crate::tasks::TaskPayload;

        let read_guard = self.drives.read().await;
        let mount = read_guard
            .get(drive_id)
            .ok_or_else(|| anyhow::anyhow!("Drive not found: {}", drive_id))?;

        let meta = self
            .inventory
            .query_by_path(local_path)
            .context("Failed to query conflicted file")?
            .ok_or_else(|| anyhow::anyhow!("File not found in inventory: {}", local_path))?;
        if !matches!(meta.conflict_state, Some(ConflictState::Pending)) {
            anyhow::bail!("File has no pending conflict: {}", local_path);
        }

        match resolution {
            ConflictResolution::KeepLocal => {
                // Override tells the upload task to skip the etag check
                self.inventory
                    .mark_as_conflicted(local_path, Some(ConflictState::Override))?;
                mount
                    .task_queue
                    .enqueue(TaskPayload::upload(local_path).with_force_override(true))
                    .await?;
            }
            ConflictResolution::KeepRemote => {
                self.inventory.mark_as_conflicted(local_path, None)?;
                // force_override: the user explicitly chose to overwrite local changes
                mount
                    .task_queue
                    .enqueue(TaskPayload::download(local_path).with_force_override(true))
                    .await?;
            }
            ConflictResolution::KeepBoth => {
                let path = PathBuf::from(local_path);
                if path.exists() {
                    let copy_path = conflicted_copy_path(&path);
                    fs::rename(&path, &copy_path).with_context(|| {
                        format!("Failed to rename conflicted file to {}", copy_path.display())
                    })?;
                    mount
                        .task_queue
                        .enqueue(TaskPayload::upload(copy_path))
                        .await?;
                }
                self.inventory.mark_as_conflicted(local_path, None)?;
                // The original path no longer exists locally (renamed above),
                // but force_override keeps this robust if a copy reappears.
                mount
                    .task_queue
                    .enqueue(TaskPayload::download(local_path).with_force_override(true))
                    .await?;
            }
        }

        tracing::info!(
            target: "drive::manager",
            drive_id = %drive_id,
            path = %local_path,
            resolution = ?resolution,
            "Conflict resolved"
        );
        Ok(())
    }

    /// Get all drives with their status information for the settings UI.
    pub async fn get_drives_info(&self) -> Result<Vec<DriveInfo>> {
        let read_guard = self.drives.read().await;
        let mut drives_info = Vec::with_capacity(read_guard.len());

        for mount in read_guard.values() {
            let config = mount.get_config().await;
            let drive_id = &config.id;

            let capacity = Self::get_capacity_summary(mount, drive_id, &config.remote_path);

            let drive_state = mount.get_status_flags().await;

            // Determine drive status
            let status = if drive_state.is_credential_expired() {
                DriveInfoStatus::CredentialExpired
            } else if !self.heartbeat_manager.is_online() {
                DriveInfoStatus::Offline
            } else if !drive_state.is_event_push_subscribed() {
                DriveInfoStatus::EventPushLost
            } else {
                DriveInfoStatus::Active
            };

            drives_info.push(DriveInfo {
                id: config.id.clone(),
                name: config.name.clone(),
                instance_url: config.instance_url.clone(),
                sync_path: config.sync_path.to_string_lossy().to_string(),
                icon_path: config.icon_path.clone(),
                remote_path: config.remote_path.clone(),
                raw_icon_path: config.raw_icon_path.clone(),
                enabled: config.enabled,
                paused: mount.is_paused(),
                user_id: config.user_id.clone(),
                status,
                capacity,
                mode: config.mode,
            });
        }

        Ok(drives_info)
    }

    /// Get a command sender for external code to send commands to the manager
    pub fn get_command_sender(&self) -> mpsc::UnboundedSender<ManagerCommand> {
        self.command_tx.clone()
    }

    pub async fn shutdown(&self) {
        tracing::info!(target: "drive::manager", "Shutting down DriveManager");

        // Stop heartbeat monitoring
        self.heartbeat_manager.stop().await;

        // Close the command channel to signal the processor task to stop
        drop(self.command_tx.clone());

        // Wait for the processor task to finish
        if let Some(handle) = self.processor_handle.lock().await.take() {
            tracing::debug!(target: "drive::manager", "Waiting for command processor to finish");
            handle.abort();
        }

        let write_guard = self.drives.write().await;
        for (_, mount) in write_guard.iter() {
            mount.shutdown().await;
        }
        tracing::info!(target: "drive", "All drives shutdown");
    }
}

/// Build a "conflicted copy" path next to the original file,
/// e.g. `report.txt` → `report (conflicted copy 2026-07-03 14-32-05).txt`.
fn conflicted_copy_path(path: &std::path::Path) -> PathBuf {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".to_string());
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H-%M-%S");
    let file_name = format!("{} (conflicted copy {}){}", stem, timestamp, ext);
    path.with_file_name(file_name)
}

impl DriveManager {
    /// Get capacity summary from a mount's drive props.
    /// Only returns capacity if the remote_path filesystem is "my".
    fn get_capacity_summary(mount: &Mount, drive_id: &str, remote_path: &str) -> Option<CapacitySummary> {
        // Only show capacity for "my" filesystem
        use cloudreve_api::models::uri::CrUri;
        let is_my_fs = CrUri::new(remote_path)
            .map(|uri| uri.fs() == "my")
            .unwrap_or(false);

        if !is_my_fs {
            return None;
        }

        match mount.get_drive_props() {
            Ok(Some(props)) => props.capacity.map(|cap| {
                let percentage = if cap.total > 0 {
                    (cap.used as f64 / cap.total as f64) * 100.0
                } else {
                    0.0
                };
                CapacitySummary {
                    total: cap.total,
                    used: cap.used,
                    label: format!(
                        "{} / {} ({:.1}%)",
                        format_bytes(cap.used),
                        format_bytes(cap.total),
                        percentage
                    ),
                }
            }),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(target: "drive::manager", drive_id = %drive_id, error = %e, "Failed to get drive props");
                None
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::conflicted_copy_path;
    use super::DriveManager;
    use crate::drive::heartbeat::HeartbeatManager;
    use crate::drive::mounts::{Credentials, DriveConfig, DriveMode, Mount};
    use crate::drive::vfs_mode::MountTestHook;
    use crate::inventory::InventoryDb;
    use crate::{EventBroadcaster, SummaryNotifier};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{mpsc, Mutex, RwLock};

    /// "Keep both" must produce a sibling file that keeps the original
    /// extension so the OS still opens it with the right application.
    #[test]
    fn conflicted_copy_keeps_extension_and_directory() {
        let copy = conflicted_copy_path(Path::new("/sync/docs/report.pdf"));
        let name = copy.file_name().unwrap().to_string_lossy().to_string();

        assert_eq!(copy.parent(), Some(Path::new("/sync/docs")));
        assert!(name.starts_with("report (conflicted copy "));
        assert!(name.ends_with(".pdf"));
        assert_ne!(copy, Path::new("/sync/docs/report.pdf"), "must not collide");
    }

    /// Files without an extension must still get a valid, distinct name.
    #[test]
    fn conflicted_copy_handles_files_without_extension() {
        let copy = conflicted_copy_path(Path::new("/sync/Makefile"));
        let name = copy.file_name().unwrap().to_string_lossy().to_string();

        assert!(name.starts_with("Makefile (conflicted copy "));
        assert!(!name.ends_with('.'), "no dangling dot");
    }

    // -----------------------------------------------------------------
    // Review finding 3: pin `resume_drive`'s `OnDemand` arm against the
    // REAL public entry point, not `Mount::remount_on_demand` called
    // directly — the exact "a test reaching into an internal while prod
    // goes through a wrapper" hazard this repo has been burned by before.
    //
    // `DriveManager::new` is unsafe to call from a test as-is: it opens
    // the REAL `~/.cloudreve` config dir and a REAL, shared inventory DB
    // on the machine running the suite. There is no injectable
    // constructor, and adding a public one for this alone felt like more
    // production surface than the fix warranted. Because every field
    // here is private or `pub(super)` to `crate::drive::manager`, and
    // this `tests` module is a DESCENDANT of that module, a direct
    // struct literal — using the same test-isolated `InventoryDb::
    // with_path` and `MountTestHook` seam every other test in this task
    // already relies on — reaches the real `resume_drive` method without
    // ever touching the real filesystem outside a tempdir. This is the
    // least invasive option found; a `DriveManager::new_for_tests`
    // constructor was considered and rejected as unnecessary production
    // surface once this was confirmed to work.
    // -----------------------------------------------------------------

    /// Builds a `DriveManager` with one on-demand drive already inserted
    /// and started (mount hook installed first, so nothing here ever
    /// touches the OS — see `vfs_mode`'s module doc). Every piece of
    /// storage is test-isolated: `config_dir`/`inventory` live under a
    /// fresh tempdir, never the real `~/.cloudreve`.
    ///
    /// `instance_url` is a parameter (not always the closed port below)
    /// because Task 5's `get_status_summary` test drives a real `Vfs`
    /// (`create`/`write`/`close`) through this same drive, which — unlike
    /// `resume_drive`'s on-demand arm — DOES make synchronous HTTP calls
    /// (`ensure_listed`'s EEXIST check), so it needs a real (wiremock)
    /// server behind the config rather than a connection that always
    /// refuses.
    async fn manager_with_one_on_demand_drive_at(instance_url: &str)
    -> (DriveManager, String, Arc<MountTestHook>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let inventory =
            Arc::new(InventoryDb::with_path(tmp.path().join("meta.db")).unwrap());
        let drive_id = uuid::Uuid::new_v4().to_string();
        let sync_path = tmp.path().join("sync");
        std::fs::create_dir_all(&sync_path).unwrap();

        let config = DriveConfig {
            id: drive_id.clone(),
            name: "Test Drive".to_string(),
            instance_url: instance_url.to_string(),
            remote_path: "cloudreve://my/sync".to_string(),
            credentials: Credentials {
                access_token: Some("test-access-token".to_string()),
                refresh_token: "test-refresh-token".to_string(),
                refresh_expires: "2099-01-01T00:00:00Z".to_string(),
                access_expires: Some("2099-01-01T00:00:00Z".to_string()),
            },
            sync_path,
            enabled: true,
            user_id: "test-user".to_string(),
            sse_client_id: uuid::Uuid::new_v4().to_string(),
            mode: DriveMode::OnDemand,
            ..Default::default()
        };

        let event_broadcaster = Arc::new(EventBroadcaster::new(16));
        let summary_notifier = Arc::new(SummaryNotifier::new(event_broadcaster.clone()));
        let (manager_tx, _manager_rx) = mpsc::unbounded_channel();

        let mut mount =
            Mount::new(config, inventory.clone(), manager_tx, summary_notifier.clone()).await;
        let hook = MountTestHook::new();
        mount.install_vfs_mount_hook_for_tests(hook.clone()).await;
        mount.start().await.expect("start the on-demand mount");
        let mount = Arc::new(mount);

        let drives: Arc<RwLock<HashMap<String, Arc<Mount>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        drives.write().await.insert(drive_id.clone(), mount);

        let heartbeat_manager = HeartbeatManager::new(drives.clone(), event_broadcaster.clone());
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let manager = DriveManager {
            drives,
            config_dir: tmp.path().join("config"),
            inventory,
            command_tx,
            command_rx: Arc::new(Mutex::new(Some(command_rx))),
            processor_handle: Arc::new(Mutex::new(None)),
            event_broadcaster,
            summary_notifier,
            heartbeat_manager,
        };

        (manager, drive_id, hook, tmp)
    }

    /// Deleting `mount.remount_on_demand().await?` from `resume_drive`'s
    /// `OnDemand` arm must fail this test — see this task's report for the
    /// mutation-testing log proving it does.
    #[tokio::test]
    async fn resume_drive_remounts_an_on_demand_drive_through_the_manager() {
        // Never actually dialed: `resume_drive`'s on-demand arm makes no
        // synchronous HTTP call, and the background SSE/props workers it
        // spawns fail harmlessly against a closed port.
        let (manager, drive_id, hook, _tmp) =
            manager_with_one_on_demand_drive_at("http://127.0.0.1:1").await;
        assert_eq!(hook.mount_count(), 1, "starting the mount already requested one mount");

        manager.pause_drive(&drive_id).await.expect("pause");
        assert_eq!(hook.unmount_count(), 1, "pausing must unmount");

        manager.resume_drive(&drive_id).await.expect("resume");

        assert_eq!(
            hook.mount_count(),
            2,
            "DriveManager::resume_drive must remount a paused on-demand drive"
        );
    }

    /// D7: a mounted on-demand drive must report itself as synced/idle
    /// (never "never synced" — `is_initial_sync_completed` can never flip
    /// true for this mode, see the `MountCommand::FullSync` guard) the
    /// moment it is mounted, with no upload queued yet; and once a draft
    /// IS queued (armed by `Vfs::close`, `VfsEvent::UploadQueued` already
    /// sent — see `WriteBackQueue::arm`'s doc), the pending-upload count
    /// reflects it. Goes through the REAL `DriveManager::get_status_summary`
    /// entry point, not a `Mount`-level proxy for it.
    #[tokio::test]
    async fn on_demand_status_summary_reports_synced_and_pending_count() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v4/file"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "msg": "",
                "data": {
                    "files": [],
                    "pagination": { "page": 1, "page_size": 500, "total_items": 0 },
                    "props": {
                        "max_page_size": 10000,
                        "order_by_options": ["name"],
                        "order_direction_options": ["asc"],
                    },
                },
            })))
            .mount(&server)
            .await;

        let (manager, drive_id, _hook, _tmp) =
            manager_with_one_on_demand_drive_at(&server.uri()).await;

        let summary = manager.get_status_summary(None).await.expect("status summary");
        assert!(
            summary.has_ever_synced,
            "a mounted on-demand drive must report synced/idle immediately, without ever \
             running FullSync"
        );
        assert_eq!(
            summary.pending_uploads.get(&drive_id).copied(),
            Some(0),
            "no draft queued yet"
        );

        let mount = manager.drives.read().await.get(&drive_id).unwrap().clone();
        let vfs = mount.vfs.lock().await.clone().expect("on-demand vfs");
        // Long enough that the debounce timer never fires during this test:
        // this pins the counter reflecting a QUEUED-but-not-yet-uploading
        // draft, not one that happened to already finish.
        vfs.set_debounce_for_tests(Duration::from_secs(600));
        let root = vfs.tree().root();
        let (_node, h) = vfs.create(root, "queued.txt").await.expect("create");
        vfs.write(h, 0, b"hello").await.expect("write");
        vfs.close(h).await.expect("close"); // Pending, UploadQueued sent, debounce armed

        mount.spawn_vfs_event_pump().await;
        for _ in 0..100 {
            if mount.vfs_pending_uploads.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let summary = manager.get_status_summary(None).await.expect("status summary");
        assert_eq!(
            summary.pending_uploads.get(&drive_id).copied(),
            Some(1),
            "a queued-but-not-yet-uploaded draft must count as one pending upload"
        );
    }
}
