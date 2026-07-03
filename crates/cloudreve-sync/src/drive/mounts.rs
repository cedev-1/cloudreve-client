use crate::drive::commands::{ManagerCommand, MountCommand};
use crate::drive::event_blocker::EventBlocker;
use crate::drive::ignore::IgnoreMatcher;
use crate::drive::sync::group_fs_events;
use crate::events::SummaryNotifier;
use crate::inventory::{DrivePropsUpdate, InventoryDb};
use crate::tasks::{TaskQueue, TaskQueueConfig};
use crate::utils::toast;

use ::serde::{Deserialize, Serialize};
use anyhow::{Context, Result};
use cloudreve_api::{Client, ClientConfig, models::user::Token};
use notify_debouncer_full::notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer};
use std::time::Duration;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
};
use tokio::spawn;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DriveConfig {
    pub id: String,
    pub name: String,
    pub instance_url: String,
    pub remote_path: String,
    pub credentials: Credentials,
    pub sync_path: PathBuf,
    pub icon_path: Option<String>,
    /// Path to the raw (non-ICO) favicon image
    pub raw_icon_path: Option<String>,
    pub enabled: bool,
    pub user_id: String,

    /// List of gitignore-style patterns for files/directories to ignore during sync
    #[serde(default)]
    pub ignore_patterns: Vec<String>,

    /// Maximum file size to sync in megabytes (0 = unlimited).
    #[serde(default = "default_max_file_size_mb")]
    pub max_file_size_mb: u64,

    /// Stable UUID used as SSE client identifier for event subscription.
    /// Persisted so the server can resume event buffering across reconnects.
    #[serde(default)]
    pub sse_client_id: String,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

fn default_max_file_size_mb() -> u64 {
    3072
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Credentials {
    pub access_token: Option<String>,
    pub refresh_token: String,
    pub refresh_expires: String,
    pub access_expires: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MountSyncStatus {
    InSync,
    Syncing,
    Paused,
    Error,
}

/// Bitflags for mount status
#[derive(Debug, Clone, Copy, Default)]
pub struct MountStatusFlags(u8);

impl MountStatusFlags {
    const CREDENTIAL_EXPIRED: u8 = 1 << 0;
    const EVENT_PUSH_SUBSCRIBED: u8 = 1 << 1;
    const INITIAL_SYNC_COMPLETED: u8 = 1 << 2;

    pub fn new() -> Self {
        Self(0)
    }

    pub fn is_credential_expired(&self) -> bool {
        self.0 & Self::CREDENTIAL_EXPIRED != 0
    }

    pub fn set_credential_expired(&mut self, expired: bool) {
        if expired {
            self.0 |= Self::CREDENTIAL_EXPIRED;
        } else {
            self.0 &= !Self::CREDENTIAL_EXPIRED;
        }
    }

    pub fn is_event_push_subscribed(&self) -> bool {
        self.0 & Self::EVENT_PUSH_SUBSCRIBED != 0
    }

    pub fn set_event_push_subscribed(&mut self, subscribed: bool) {
        if subscribed {
            self.0 |= Self::EVENT_PUSH_SUBSCRIBED;
        } else {
            self.0 &= !Self::EVENT_PUSH_SUBSCRIBED;
        }
    }

    pub fn bits(&self) -> u8 {
        self.0
    }

    pub fn is_initial_sync_completed(&self) -> bool {
        self.0 & Self::INITIAL_SYNC_COMPLETED != 0
    }

    pub fn set_initial_sync_completed(&mut self, completed: bool) {
        if completed {
            self.0 |= Self::INITIAL_SYNC_COMPLETED;
        } else {
            self.0 &= !Self::INITIAL_SYNC_COMPLETED;
        }
    }

    pub fn from_bits(bits: u8) -> Self {
        Self(bits)
    }
}

type FsWatcher = Debouncer<RecommendedWatcher, RecommendedCache>;

pub struct Mount {
    pub config: Arc<RwLock<DriveConfig>>,
    pub command_tx: mpsc::UnboundedSender<MountCommand>,
    command_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<MountCommand>>>>,
    processor_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    props_refresh_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    periodic_sync_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    remote_event_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    manager_command_tx: mpsc::UnboundedSender<ManagerCommand>,
    fs_watcher: Mutex<Option<FsWatcher>>,
    pub(crate) sync_lock: Mutex<()>,
    pub cr_client: Arc<Client>,
    pub inventory: Arc<InventoryDb>,
    pub task_queue: Arc<TaskQueue>,
    pub id: String,
    pub event_blocker: EventBlocker,

    pub ignore_matcher: RwLock<IgnoreMatcher>,
    pub(super) status_flags: Mutex<MountStatusFlags>,
}

impl Mount {
    pub async fn new(
        config: DriveConfig,
        inventory: Arc<InventoryDb>,
        manager_command_tx: mpsc::UnboundedSender<ManagerCommand>,
        summary_notifier: Arc<SummaryNotifier>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let client_config = ClientConfig::new(config.instance_url.clone())
            .with_client_id(config.sse_client_id.clone())
            .with_user_agent(crate::USER_AGENT);
        let mut cr_client = Client::new(client_config);
        cr_client
            .load_tokens(&Token {
                access_token: config.credentials.access_token.clone().unwrap_or_default(),
                refresh_token: config.credentials.refresh_token.clone(),
                access_expires: config.credentials.access_expires.clone().unwrap_or_default(),
                refresh_expires: config.credentials.refresh_expires.clone(),
            })
            .await;

        let command_tx_clone = command_tx.clone();
        cr_client.set_on_credential_refreshed(Arc::new(move |token| {
            let command_tx = command_tx_clone.clone();
            Box::pin(async move {
                let command = MountCommand::RefreshCredentials { credentials: token };
                if let Err(e) = command_tx.send(command) {
                    tracing::error!(target: "drive::mounts", error = %e, "Failed to send RefreshCredentials command");
                }
            })
        }));

        let command_tx_invalid = command_tx.clone();
        cr_client.set_on_credential_invalid(Arc::new(move || {
            let command_tx = command_tx_invalid.clone();
            Box::pin(async move {
                if let Err(e) = command_tx.send(MountCommand::CredentialInvalid) {
                    tracing::error!(target: "drive::mounts", error = %e, "Failed to send CredentialInvalid command");
                }
            })
        }));

        let cr_client = Arc::new(cr_client);
        let ignore_matcher = IgnoreMatcher::new(&config.ignore_patterns, config.sync_path.clone())
            .unwrap_or_else(|_| IgnoreMatcher::empty(config.sync_path.clone()));
        let event_blocker = EventBlocker::new();
        let task_queue = TaskQueue::new(
            config.id.clone(),
            cr_client.clone(),
            inventory.clone(),
            TaskQueueConfig::default(),
            config.sync_path.clone(),
            config.remote_path.clone(),
            event_blocker.clone(),
            config.max_file_size_mb,
            summary_notifier,
        ).await;
        let id = config.id.clone();

        Mount {
            id,
            config: Arc::new(RwLock::new(config)),
            command_tx,
            command_rx: Arc::new(Mutex::new(Some(command_rx))),
            processor_handle: Arc::new(Mutex::new(None)),
            props_refresh_handle: Arc::new(Mutex::new(None)),
            periodic_sync_handle: Arc::new(Mutex::new(None)),
            remote_event_handle: Arc::new(Mutex::new(None)),
            manager_command_tx,
            fs_watcher: Mutex::new(None),
            sync_lock: Mutex::new(()),
            cr_client,
            inventory,
            task_queue,
            event_blocker: event_blocker.clone(),
            ignore_matcher: RwLock::new(ignore_matcher),
            status_flags: Mutex::new(MountStatusFlags::new()),
        }
    }

    /// Start the mount: create local sync directory and start the fs watcher.
    pub async fn start(&mut self) -> Result<()> {
        let sync_path = {
            let config = self.config.read().await;
            config.sync_path.clone()
        };

        // Ensure the local sync directory exists
        if !sync_path.exists() {
            std::fs::create_dir_all(&sync_path)
                .with_context(|| format!("Failed to create sync directory: {}", sync_path.display()))?;
            tracing::info!(target: "drive::mounts", path = %sync_path.display(), "Created sync directory");
        }

        // Start filesystem watcher
        self.start_fs_watcher(&sync_path).await?;

        tracing::info!(target: "drive::mounts", id = %self.id, path = %sync_path.display(), "Mount started");
        Ok(())
    }

    async fn start_fs_watcher(&self, sync_path: &PathBuf) -> Result<()> {
        let command_tx = self.command_tx.clone();
        let event_blocker = self.event_blocker.clone();

        let watcher = new_debouncer(
            Duration::from_secs(2),
            None,
            move |result: DebounceEventResult| {
                match result {
                    Ok(events) => {
                        let events: Vec<_> = events
                            .into_iter()
                            .filter(|e| !event_blocker.should_block(&e.kind, e.paths.first().unwrap_or(&PathBuf::new())))
                            .collect();
                        if events.is_empty() {
                            return;
                        }
                        let grouped = group_fs_events(events);
                        let _ = command_tx.send(MountCommand::Sync {
                            local_paths: grouped.all_paths(),
                            mode: crate::drive::sync::SyncMode::LocalChanged,
                            user_initiated: false,
                        });
                    }
                    Err(errors) => {
                        for e in errors {
                            tracing::error!(target: "drive::mounts", error = ?e, "Filesystem watcher error");
                        }
                    }
                }
            },
        )?;

        let mut watcher = watcher;
        watcher.watch(sync_path, RecursiveMode::Recursive)?;

        *self.fs_watcher.lock().await = Some(watcher);
        tracing::info!(target: "drive::mounts", path = %sync_path.display(), "Filesystem watcher started");
        Ok(())
    }

    /// Spawn the mount command processor
    pub async fn spawn_command_processor(self: &Arc<Self>, mount: Arc<Self>) {
        let mut guard = self.command_rx.lock().await;
        if let Some(rx) = guard.take() {
            let handle = tokio::spawn(async move {
                mount.process_commands(rx).await;
            });
            *self.processor_handle.lock().await = Some(handle);
        }
    }

    async fn process_commands(
        self: &Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<MountCommand>,
    ) {
        tracing::info!(target: "drive::mounts", id = %self.id, "Command processor started");

        while let Some(command) = rx.recv().await {
            tracing::trace!(target: "drive::mounts", command = ?command, "Processing command");
            let mount = self.clone();
            match command {
                MountCommand::Sync { local_paths, mode, user_initiated } => {
                    spawn(async move {
                        let _lock = mount.sync_lock.lock().await;
                        if let Err(e) = mount.perform_sync(local_paths, mode, user_initiated).await {
                            tracing::error!(target: "drive::mounts", error = %e, "Sync failed");
                        }
                    });
                }
                MountCommand::RefreshCredentials { credentials } => {
                    mount.handle_refresh_credentials(credentials).await;
                }
                MountCommand::CredentialInvalid => {
                    mount.set_credential_expired(true).await;
                    let config = mount.config.read().await;
                    toast::send_token_expiry_toast(
                        &config.id,
                        &t!("credentialExpiredTitle"),
                        &t!("credentialExpiredMessage", name = config.name.as_str()),
                    );
                }
                MountCommand::FullSync => {
                    let mount_clone = mount.clone();
                    spawn(async move {
                        if let Err(e) = mount_clone.task_queue.re_enqueue_offline_tasks() {
                            tracing::warn!(target: "drive::mounts", error = %e, "Failed to re-enqueue offline tasks");
                        }
                        let _lock = mount_clone.sync_lock.lock().await;
                        match mount_clone.perform_full_sync().await {
                            Ok(()) => {
                                mount_clone.set_initial_sync_completed(true).await;
                            }
                            Err(e) => {
                                tracing::error!(target: "drive::mounts", error = %e, "Full sync failed");
                            }
                        }
                    });
                }
            }
        }
    }

    async fn handle_refresh_credentials(&self, token: Token) {
        let mut config = self.config.write().await;
        config.credentials.access_token = Some(token.access_token.clone());
        config.credentials.refresh_token = token.refresh_token.clone();
        config.credentials.access_expires = Some(token.access_expires.clone());
        config.credentials.refresh_expires = token.refresh_expires.clone();
        drop(config);
        let _ = self.manager_command_tx.send(ManagerCommand::PersistConfig);
    }

    async fn perform_sync(
        &self,
        local_paths: Vec<PathBuf>,
        mode: crate::drive::sync::SyncMode,
        _user_initiated: bool,
    ) -> Result<()> {
        use crate::drive::sync::SyncMode;
        use crate::tasks::TaskPayload;

        tracing::debug!(target: "drive::mounts", id = %self.id, mode = ?mode, paths = local_paths.len(), "Incremental sync triggered");

        match mode {
            SyncMode::LocalChanged => {
                for path in local_paths {
                    if !path.exists() || path.is_dir() {
                        continue;
                    }
                    let ignored = {
                        let matcher = self.ignore_matcher.read().await;
                        matcher.is_match(&path)
                    };
                    if ignored {
                        continue;
                    }
                    // Check file size against drive limit before uploading
                    if let Ok(local_meta) = std::fs::metadata(&path) {
                        if !self.is_file_size_allowed(local_meta.len()).await {
                            tracing::debug!(
                                target: "drive::mounts",
                                path = %path.display(),
                                size = local_meta.len(),
                                "Skipping upload: file exceeds size limit"
                            );
                            continue;
                        }
                    }
                    // Check inventory DB to avoid re-uploading files we just downloaded.
                    // If the local file still matches the last synced state (size and
                    // recorded mtime), it was likely written by a download task.
                    if let Some(path_str) = path.to_str() {
                        if let Ok(Some(db_entry)) = self.inventory.query_by_path(path_str) {
                            if !db_entry.is_locally_modified(&path) {
                                tracing::debug!(
                                    target: "drive::mounts",
                                    path = %path.display(),
                                    "Skipping upload: file matches inventory (likely just downloaded)"
                                );
                                continue;
                            }
                        }
                    }
                    tracing::debug!(target: "drive::mounts", path = %path.display(), "Enqueuing upload for local change");
                    self.task_queue.enqueue(TaskPayload::upload(path)).await?;
                }
            }
            SyncMode::RemoteChanged => {
                for path in local_paths {
                    if path.is_dir() {
                        continue;
                    }
                    tracing::debug!(target: "drive::mounts", path = %path.display(), "Enqueuing download for remote change");
                    self.task_queue.enqueue(TaskPayload::download(path)).await?;
                }
            }
            SyncMode::Full => {
                let (sync_path, remote_path) = {
                    let config = self.config.read().await;
                    (config.sync_path.clone(), config.remote_path.clone())
                };
                crate::drive::sync::full_sync(self, &sync_path, &remote_path).await?;
            }
        }

        Ok(())
    }

    async fn perform_full_sync(&self) -> Result<()> {
        tracing::info!(target: "drive::mounts", id = %self.id, "Starting full sync");
        let (sync_path, remote_path) = {
            let config = self.config.read().await;
            (config.sync_path.clone(), config.remote_path.clone())
        };
        crate::drive::sync::full_sync(self, &sync_path, &remote_path).await
    }

    /// Spawn the remote event processor (SSE)
    pub async fn spawn_remote_event_processor(self: &Arc<Self>, mount: Arc<Self>) {
        let s = self.clone();
        let handle = tokio::spawn(async move {
            s.process_remote_events(mount).await;
        });
        *self.remote_event_handle.lock().await = Some(handle);
    }

    /// Spawn a task to periodically refresh drive properties (quota, etc.)
    pub async fn spawn_props_refresh_task(self: &Arc<Self>) {
        let mount = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                if let Err(e) = mount.refresh_drive_props().await {
                    tracing::warn!(target: "drive::mounts", id = %mount.id, error = %e, "Failed to refresh drive props");
                }
            }
        });
        *self.props_refresh_handle.lock().await = Some(handle);
    }

    /// Spawn a periodic full sync every 5 minutes to catch changes
    /// missed by the event stream (remote) or fs watcher (local).
    pub async fn spawn_periodic_sync(self: &Arc<Self>) {
        let command_tx = self.command_tx.clone();
        let id = self.id.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                tracing::debug!(target: "drive::mounts", id = %id, "Periodic full sync triggered");
                let _ = command_tx.send(MountCommand::FullSync);
            }
        });
        *self.periodic_sync_handle.lock().await = Some(handle);
    }

    async fn refresh_drive_props(&self) -> Result<()> {
        use cloudreve_api::api::user::UserApi;
        let config = self.config.read().await;
        let remote_path = config.remote_path.clone();
        drop(config);

        use cloudreve_api::models::uri::CrUri;
        let uri = CrUri::new(&remote_path)?;
        if uri.fs() != "my" {
            return Ok(());
        }

        let capacity = self.cr_client.get_user_capacity().await?;
        let update = DrivePropsUpdate::default().with_capacity(capacity);
        self.inventory.upsert_drive_props(&self.id, update)?;
        Ok(())
    }

    pub async fn get_config(&self) -> DriveConfig {
        self.config.read().await.clone()
    }

    /// Check if a file size (in bytes) is within the drive's configured limit.
    /// Returns true if the file should be synced (within limit or limit is 0).
    pub async fn is_file_size_allowed(&self, size_bytes: u64) -> bool {
        let max_mb = self.config.read().await.max_file_size_mb;
        if max_mb == 0 {
            return true; // unlimited
        }
        let max_bytes = max_mb * 1024 * 1024;
        size_bytes <= max_bytes
    }

    pub async fn get_sync_path(&self) -> PathBuf {
        self.config.read().await.sync_path.clone()
    }

    pub async fn get_status_flags(&self) -> MountStatusFlags {
        *self.status_flags.lock().await
    }

    pub async fn set_credential_expired(&self, expired: bool) {
        let mut flags = self.status_flags.lock().await;
        flags.set_credential_expired(expired);
    }

    pub async fn set_initial_sync_completed(&self, completed: bool) {
        let mut flags = self.status_flags.lock().await;
        flags.set_initial_sync_completed(completed);
    }

    pub fn get_drive_props(&self) -> Result<Option<crate::inventory::DriveProps>> {
        self.inventory.get_drive_props(&self.id)
    }

    pub async fn update_ignore_patterns(&self, patterns: Vec<String>) -> Result<()> {
        let sync_path = self.config.read().await.sync_path.clone();
        let new_matcher = IgnoreMatcher::new(&patterns, sync_path.clone())
            .unwrap_or_else(|_| IgnoreMatcher::empty(sync_path));
        let mut config = self.config.write().await;
        config.ignore_patterns = patterns;
        drop(config);
        *self.ignore_matcher.write().await = new_matcher;
        Ok(())
    }

    pub async fn shutdown(&self) {
        tracing::info!(target: "drive::mounts", id = %self.id, "Shutting down mount");

        // Stop fs watcher
        *self.fs_watcher.lock().await = None;

        // Abort background tasks
        if let Some(h) = self.processor_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.remote_event_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.props_refresh_handle.lock().await.take() {
            h.abort();
        }

        // Shutdown task queue
        self.task_queue.shutdown().await;
    }

    pub async fn delete(&self) -> Result<()> {
        self.shutdown().await;
        self.inventory.nuke_drive(&self.id)?;
        self.inventory.delete_drive_props(&self.id)?;
        Ok(())
    }

    /// Generate a thumbnail for the given file.
    /// Returns None on platforms where thumbnail generation is not supported.
    pub async fn generate_thumbnail(&self, _path: PathBuf) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

