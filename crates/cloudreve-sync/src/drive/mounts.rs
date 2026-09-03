use crate::drive::commands::{ManagerCommand, MountCommand};
use crate::drive::event_blocker::EventBlocker;
use crate::drive::ignore::IgnoreMatcher;
use crate::drive::sync::group_fs_events;
use crate::drive::vfs_mode::{self, MountTestHook};
use crate::events::SummaryNotifier;
use crate::inventory::{DrivePropsUpdate, InventoryDb};
use crate::tasks::{TaskQueue, TaskQueueConfig};
use crate::utils::toast;

use ::serde::{Deserialize, Serialize};
use anyhow::{Context, Result};
use cloudreve_api::{Client, ClientConfig, models::user::Token};
use cloudreve_vfs::mount::MountedVfs;
use cloudreve_vfs::vfs::{Vfs, VfsEvent};
use notify_debouncer_full::notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer};
use std::time::Duration;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::spawn;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

/// Per-drive sync strategy (D1). `FullMirror` is today's behavior — a real,
/// byte-for-byte local mirror kept in sync by a fs watcher, the task queue,
/// and periodic/initial full syncs. `OnDemand` instead mounts `sync_path`
/// as a virtual volume backed by `cloudreve-vfs`: no fs watcher, no task
/// queue replay, no full sync — reads/writes go straight through the
/// mounted filesystem. See `Mount`'s lifecycle methods for exactly what
/// each mode skips (D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriveMode {
    #[default]
    FullMirror,
    OnDemand,
}

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

    /// Full mirror vs. on-demand (D1). Declared BEFORE `extra`: `extra`'s
    /// `#[serde(flatten)]` would otherwise swallow an unrecognized `mode`
    /// key instead of deserializing it. `#[serde(default)]` means an old
    /// `drives.json` written before this field existed parses unchanged,
    /// defaulting every existing drive to `FullMirror`.
    #[serde(default)]
    pub mode: DriveMode,

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
    /// On-demand mode only (D2/D5): the OS-level mount handle. `None`
    /// whenever unmounted — paused, never started, `start()` failed before
    /// attaching, or (always, under test) intercepted by
    /// `vfs_mount_test_hook`. Mirrors `fs_watcher`'s `Mutex<Option<...>>`
    /// shape.
    mounted_vfs: Mutex<Option<MountedVfs>>,
    /// On-demand mode only: the `Vfs` facade built by `start_on_demand`,
    /// kept alive across a pause/resume cycle (`remount_on_demand`
    /// re-attaches the SAME instance rather than rebuilding it) — `None`
    /// for a `FullMirror` drive or before `start()` has run. Task 5 reads
    /// this for VFS-backed SSE invalidation and status.
    pub vfs: Mutex<Option<Arc<Vfs>>>,
    /// On-demand mode only: the receiving half of `vfs`'s `VfsEvent`
    /// channel, `None` under the same conditions as `vfs`. Task 5 consumes
    /// this.
    pub vfs_events: Mutex<Option<mpsc::UnboundedReceiver<VfsEvent>>>,
    /// Test-only: see `vfs_mode::MountTestHook`'s doc. `None` on every
    /// production path.
    vfs_mount_test_hook: Mutex<Option<Arc<MountTestHook>>>,
    pub(crate) sync_lock: Mutex<()>,
    pub cr_client: Arc<Client>,
    pub inventory: Arc<InventoryDb>,
    pub task_queue: Arc<TaskQueue>,
    pub id: String,
    pub event_blocker: EventBlocker,

    pub ignore_matcher: RwLock<IgnoreMatcher>,
    pub(super) status_flags: Mutex<MountStatusFlags>,
    /// Maximum time (seconds) to wait for any SSE data (event or keep-alive)
    /// before treating the connection as dead and reconnecting.
    /// Defaults to 120 s; tests override with a short value.
    pub sse_idle_timeout_secs: std::sync::atomic::AtomicU64,
    /// Runtime-only pause flag (not persisted). When true, sync operations
    /// are skipped and background workers are stopped.
    pub paused: std::sync::atomic::AtomicBool,
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
        // On-demand drives never replay parked/interrupted tasks at startup
        // (D2): there is no local mirror for a stale upload/download task to
        // apply to, and the queue itself stays inert (no fs watcher, no full
        // sync ever enqueues anything into it either). `resume_on_start` is
        // the least-invasive seam for this — `TaskQueue::new` has exactly
        // one caller (here), so threading a bool straight through avoids a
        // second constructor or a wrapper type just to skip one internal
        // call. See `TaskQueue::new`'s own doc for the disclosure.
        let resume_on_start = config.mode == DriveMode::FullMirror;
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
            &ignore_matcher,
            resume_on_start,
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
            mounted_vfs: Mutex::new(None),
            vfs: Mutex::new(None),
            vfs_events: Mutex::new(None),
            vfs_mount_test_hook: Mutex::new(None),
            sync_lock: Mutex::new(()),
            cr_client,
            inventory,
            task_queue,
            event_blocker: event_blocker.clone(),
            ignore_matcher: RwLock::new(ignore_matcher),
            status_flags: Mutex::new(MountStatusFlags::new()),
            sse_idle_timeout_secs: std::sync::atomic::AtomicU64::new(120),
            paused: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Start the mount. `FullMirror`: create the local sync directory and
    /// start the fs watcher (unchanged behavior). `OnDemand` (D2): mount
    /// `sync_path` as a virtual volume instead — no fs watcher.
    pub async fn start(&mut self) -> Result<()> {
        let (sync_path, mode) = {
            let config = self.config.read().await;
            (config.sync_path.clone(), config.mode)
        };

        match mode {
            DriveMode::FullMirror => {
                // Ensure the local sync directory exists
                if !sync_path.exists() {
                    std::fs::create_dir_all(&sync_path).with_context(|| {
                        format!("Failed to create sync directory: {}", sync_path.display())
                    })?;
                    tracing::info!(target: "drive::mounts", path = %sync_path.display(), "Created sync directory");
                }

                // Start filesystem watcher
                self.start_fs_watcher(&sync_path).await?;
            }
            DriveMode::OnDemand => {
                self.start_on_demand(&sync_path).await?;
            }
        }

        tracing::info!(target: "drive::mounts", id = %self.id, path = %sync_path.display(), "Mount started");
        Ok(())
    }

    /// D2/D5: mounts `mountpoint` as an on-demand virtual volume — computes
    /// the effective cache cap (D3, warning once if it was clamped below
    /// the default), builds the `Vfs` facade, and attaches it (which itself
    /// pre-cleans a stale leftover mount, THEN ensures the target is an
    /// empty directory — see `vfs_mode::attach`'s doc for why that order
    /// matters). Stores the resulting `Vfs`, event receiver, and OS mount
    /// handle on `self`.
    async fn start_on_demand(&self, mountpoint: &Path) -> Result<()> {
        let cache_dir = vfs_mode::cache_dir_for(&self.id)?;
        let cap = vfs_mode::effective_cache_cap(&cache_dir);

        let (remote_path, name) = {
            let config = self.config.read().await;
            (config.remote_path.clone(), config.name.clone())
        };

        if vfs_mode::is_clamped(cap) {
            toast::send_small_vfs_cache_toast(&self.id, &name, cap);
        }

        let (vfs, events) =
            vfs_mode::build_vfs(self.cr_client.clone(), remote_path, &cache_dir, cap)?;

        let hook = self.vfs_mount_test_hook.lock().await.clone();
        let mounted =
            vfs_mode::attach(vfs.clone(), mountpoint, &name, &cache_dir, cap, hook.as_ref()).await?;

        *self.vfs.lock().await = Some(vfs);
        *self.vfs_events.lock().await = Some(events);
        *self.mounted_vfs.lock().await = mounted;

        Ok(())
    }

    /// D5: re-attaches the on-demand vfs after a pause, reusing the SAME
    /// `Vfs` instance `start_on_demand` built — its block cache, draft
    /// store, and write-back queue keep running across a pause; only the
    /// OS-level attachment was ever torn down (`unmount_on_demand`), so
    /// this never touches `Vfs::new`/`build_vfs` again.
    pub async fn remount_on_demand(&self) -> Result<()> {
        let vfs = self.vfs.lock().await.clone().context(
            "on-demand remount requested but no vfs was ever built for this drive — was start() called?",
        )?;

        let (sync_path, name) = {
            let config = self.config.read().await;
            (config.sync_path.clone(), config.name.clone())
        };

        let cache_dir = vfs_mode::cache_dir_for(&self.id)?;
        let cap = vfs_mode::effective_cache_cap(&cache_dir);

        // `attach` itself pre-cleans a stale leftover mount before checking
        // the mountpoint is empty — see its doc for why that order matters
        // (review finding 4).
        let hook = self.vfs_mount_test_hook.lock().await.clone();
        let mounted =
            vfs_mode::attach(vfs, &sync_path, &name, &cache_dir, cap, hook.as_ref()).await?;

        *self.mounted_vfs.lock().await = mounted;
        Ok(())
    }

    /// D5: unmounts the on-demand vfs (pause/shutdown/delete). The `Vfs`
    /// facade and its event receiver are left in place — only the OS-level
    /// attachment goes away — so `remount_on_demand` can re-attach the same
    /// instance.
    async fn unmount_on_demand(&self) {
        let mountpoint = self.get_sync_path().await;
        let hook = self.vfs_mount_test_hook.lock().await.clone();
        let mounted = self.mounted_vfs.lock().await.take();
        if let Err(err) = vfs_mode::detach(mounted, &mountpoint, hook.as_ref()).await {
            tracing::warn!(target: "drive::mounts", id = %self.id, ?err, "failed to unmount the on-demand vfs");
        }
    }

    /// Test-only: install a substitute for the real OS mount/unmount calls
    /// this on-demand `Mount` would otherwise make — see
    /// `vfs_mode::MountTestHook`'s doc for the seam's design. Must be
    /// installed before `start()`/`pause()`/a resume for it to take effect
    /// on that call.
    #[doc(hidden)]
    pub async fn install_vfs_mount_hook_for_tests(&self, hook: Arc<MountTestHook>) {
        *self.vfs_mount_test_hook.lock().await = Some(hook);
    }

    /// Public wrapper for restarting the FS watcher (used by resume).
    pub async fn start_fs_watcher_public(&self, sync_path: &PathBuf) -> Result<()> {
        self.start_fs_watcher(sync_path).await
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
                    tracing::warn!(target: "drive::mounts", id = %mount.id, "Credential invalid, marking drive as expired");
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
                        // Review finding 2: `FullSync` is sent unconditionally
                        // by several callers (SSE `Subscribed`, `DriveManager::
                        // start_sync`, this mount's own periodic-sync worker —
                        // though that one is never spawned for on-demand) —
                        // guard the WHOLE handler here rather than letting an
                        // on-demand drive reach `full_sync`'s own D2 guard two
                        // calls deep, which would still no-op correctly but
                        // only after firing a should-be-unreachable warning on
                        // every routine SSE subscription and marking
                        // `initial_sync_completed` true off a sync that never
                        // ran. This makes the FullMirror-era machinery inert
                        // for on-demand WITHOUT deleting the command variant or
                        // the SSE worker that sends it — Task 5 repurposes that
                        // branch for on-demand SSE handling.
                        if mount_clone.config.read().await.mode == DriveMode::OnDemand {
                            tracing::debug!(
                                target: "drive::mounts",
                                id = %mount_clone.id,
                                "FullSync command ignored: drive is on-demand"
                            );
                            return;
                        }
                        if let Err(e) = mount_clone.re_enqueue_offline_tasks().await {
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

        // A successful refresh proves credentials are valid again: clear any
        // stale "credentials expired" state without requiring re-authorization.
        self.set_credential_expired(false).await;

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

        if self.is_paused() {
            tracing::info!(target: "drive::mounts", id = %self.id, "Incremental sync skipped: drive is paused");
            return Ok(());
        }

        tracing::debug!(target: "drive::mounts", id = %self.id, mode = ?mode, paths = local_paths.len(), "Incremental sync triggered");

        match mode {
            SyncMode::LocalChanged => {
                for path in local_paths {
                    if !path.exists() || path.is_dir() {
                        continue;
                    }
                    if self.is_ignored(&path).await {
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
                    if path.is_dir() || self.is_ignored(&path).await {
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

    /// Whether this path is excluded from syncing, in either direction.
    pub async fn is_ignored(&self, path: &std::path::Path) -> bool {
        self.ignore_matcher.read().await.is_match(path)
    }

    /// Replay the tasks parked while offline, dropping the ones now ignored.
    ///
    /// D2 ("on-demand skips: TaskQueue replay") applies here too, not just
    /// at launch (`resume_on_start` in `TaskQueue::new`) — this is the ONE
    /// choke point every reconnect/SSE path shares (`heartbeat.rs`'s
    /// offline→online transition, `remote_events.rs`'s `Resumed`/
    /// `Subscribed` handlers, and the `MountCommand::FullSync` handler
    /// below), so guarding here fixes all three at once rather than
    /// needing a mode check duplicated at every call site (review finding
    /// 1). There is no local mirror for a stale on-demand task to apply
    /// to, same reasoning as the launch-time guard.
    pub async fn re_enqueue_offline_tasks(&self) -> Result<usize> {
        if self.config.read().await.mode == DriveMode::OnDemand {
            return Ok(0);
        }
        let matcher = self.ignore_matcher.read().await.clone();
        self.task_queue.re_enqueue_offline_tasks(&matcher)
    }

    async fn perform_full_sync(&self) -> Result<()> {
        tracing::info!(target: "drive::mounts", id = %self.id, "Starting full sync");
        let (sync_path, remote_path) = {
            let config = self.config.read().await;
            (config.sync_path.clone(), config.remote_path.clone())
        };
        crate::drive::sync::full_sync(self, &sync_path, &remote_path).await
    }

    /// Install a freshly spawned worker, stopping the one it replaces.
    ///
    /// Dropping a `JoinHandle` DETACHES its task rather than aborting it, so
    /// overwriting the slot on its own would leave the previous worker running
    /// forever and unreachable: `pause()` and `shutdown()` can only abort the
    /// handle still stored here. `resume_drive` re-spawns these workers
    /// unconditionally, so this is reached whenever a drive is resumed twice.
    async fn replace_worker(slot: &Mutex<Option<JoinHandle<()>>>, new: JoinHandle<()>) {
        if let Some(previous) = slot.lock().await.replace(new) {
            previous.abort();
        }
    }

    /// Spawn the remote event processor (SSE)
    pub async fn spawn_remote_event_processor(self: &Arc<Self>, mount: Arc<Self>) {
        let s = self.clone();
        let handle = tokio::spawn(async move {
            s.process_remote_events(mount).await;
        });
        Self::replace_worker(&self.remote_event_handle, handle).await;
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
        Self::replace_worker(&self.props_refresh_handle, handle).await;
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
        Self::replace_worker(&self.periodic_sync_handle, handle).await;
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
        // Refuse the whole list rather than half-apply it: this is the
        // Settings save path, the one place the user can fix the typo. The
        // startup path (`Mount::new`) stays tolerant instead — a bad line in
        // the persisted config must never switch the defaults off.
        crate::drive::ignore::validate_patterns(&patterns)?;
        let sync_path = self.config.read().await.sync_path.clone();
        let new_matcher = IgnoreMatcher::new(&patterns, sync_path.clone())
            .unwrap_or_else(|_| IgnoreMatcher::empty(sync_path));
        let mut config = self.config.write().await;
        config.ignore_patterns = patterns;
        drop(config);
        *self.ignore_matcher.write().await = new_matcher;
        Ok(())
    }

    /// Check if this mount is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Pause sync: sets the flag, stops background workers (SSE, periodic
    /// sync, props refresh, FS watcher), and cancels running tasks.
    /// The command processor stays alive so it can process Resume.
    pub async fn pause(&self) {
        self.paused.store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(target: "drive::mounts", id = %self.id, "Drive paused");

        // D5: unmount an on-demand drive (the volume disappears — no
        // half-alive mount) rather than stop a fs watcher it never had.
        match self.config.read().await.mode {
            DriveMode::FullMirror => {
                *self.fs_watcher.lock().await = None;
            }
            DriveMode::OnDemand => {
                self.unmount_on_demand().await;
            }
        }

        // Abort background handles (but NOT the command processor)
        if let Some(h) = self.remote_event_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.periodic_sync_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.props_refresh_handle.lock().await.take() {
            h.abort();
        }

        // Cancel all running/pending tasks
        self.task_queue.cancel_all().await;
    }

    /// Resume sync: clears the flag. Callers (DriveManager) are responsible
    /// for restarting background workers and triggering a full sync.
    pub async fn resume(&self) {
        self.paused.store(false, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(target: "drive::mounts", id = %self.id, "Drive resumed");
    }

    pub async fn shutdown(&self) {
        tracing::info!(target: "drive::mounts", id = %self.id, "Shutting down mount");

        // D5: shutdown unmounts an on-demand drive the same way pause does
        // (Drop is only the crash net, not the primary teardown path).
        match self.config.read().await.mode {
            DriveMode::FullMirror => {
                *self.fs_watcher.lock().await = None;
            }
            DriveMode::OnDemand => {
                self.unmount_on_demand().await;
            }
        }

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

        // D5: an on-demand drive's cache dir is this crate's own, not
        // anything the user put there — remove it on delete the same way a
        // FullMirror drive's inventory rows are wiped below. Best-effort:
        // `inventory.nuke_drive` below is a harmless no-op for a drive that
        // never wrote any rows, but a leftover cache dir would otherwise
        // accumulate forever across add/remove cycles.
        if self.config.read().await.mode == DriveMode::OnDemand {
            match vfs_mode::cache_dir_for(&self.id) {
                Ok(cache_dir) => {
                    if let Err(err) = std::fs::remove_dir_all(&cache_dir)
                        && err.kind() != std::io::ErrorKind::NotFound
                    {
                        tracing::warn!(
                            target: "drive::mounts",
                            id = %self.id,
                            path = %cache_dir.display(),
                            ?err,
                            "failed to remove the on-demand vfs cache directory"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(target: "drive::mounts", id = %self.id, ?err, "failed to resolve the on-demand vfs cache directory for cleanup");
                }
            }
        }

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

