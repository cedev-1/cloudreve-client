use crate::drive::commands::{ManagerCommand, MountCommand};
use crate::drive::event_blocker::EventBlocker;
use crate::drive::ignore::IgnoreMatcher;
use crate::drive::sync::group_fs_events;
use crate::drive::upload_health::{self, UploadHealth};
use crate::drive::vfs_mode::{self, MountTestHook};
use crate::events::SummaryNotifier;
use crate::inventory::{DrivePropsUpdate, InventoryDb};
use crate::sidebar::{self, DriveFolder, NoSidebar, SidebarBackend, SidebarSettings};
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

    /// "Show in sidebar" (see `crate::sidebar`). Declared before `extra`
    /// for the same reason as `mode`; an old `drives.json` without it gets
    /// the default: on, not added yet — so an existing drive appears in the
    /// sidebar once, at the first launch of a version that has it.
    #[serde(default)]
    pub sidebar: SidebarSettings,

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
    /// D6: background worker draining `vfs_events` into
    /// `vfs_pending_uploads`, `upload_health` and toasts — see
    /// `spawn_vfs_event_pump`'s doc. Unlike every other `*_handle` slot
    /// below, `pause()` deliberately never aborts this one: see
    /// `spawn_vfs_event_pump`'s doc for why.
    vfs_event_pump_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// On-demand mode only (D6): count of upload cycles currently between
    /// their `VfsEvent::UploadQueued` and one of the five terminal events
    /// documented on `VfsEvent` (`UploadSucceeded`/`UploadFailed`/
    /// `UploadCancelled`/`UploadRenamed`/`ConflictSaved`). Folded by
    /// `fold_vfs_event`; read by `DriveManager::get_status_summary` (D7).
    /// Reconciles against on-disk `DraftStore` state for free on every
    /// (re)start: `Vfs::new` itself re-arms every still-`Pending` draft for
    /// immediate upload before returning
    /// (`WriteBackQueue::enqueue_immediate`), synchronously sending a
    /// fresh `UploadQueued` for each one into the very channel this pump
    /// drains — see `spawn_vfs_event_pump`'s doc for why none of those
    /// startup events are ever lost.
    pub vfs_pending_uploads: std::sync::atomic::AtomicU64,
    /// On-demand mode only: consecutive upload failures, and the reason
    /// shown to the user once they stop looking transient (see
    /// `drive::upload_health`). Folded by `fold_vfs_event`.
    upload_health: std::sync::Mutex<UploadHealth>,
    /// The plain-language message for `upload_health`'s current problem,
    /// `None` while uploads go through. Read by `get_status_summary`.
    upload_problem: std::sync::Mutex<Option<String>>,
    /// Test-only: see `vfs_mode::MountTestHook`'s doc. `None` on every
    /// production path.
    vfs_mount_test_hook: Mutex<Option<Arc<MountTestHook>>>,
    /// On-demand mode only: why the drive is not mounted right now, in
    /// plain language for the user (see `vfs_mode::mount_problem_message`),
    /// or `None` when it is mounted (or paused on purpose). Set when
    /// mounting fails at launch or on "Retry"; the drive stays listed and
    /// saved instead of vanishing — see `DriveManager::add_drive_inner`.
    mount_problem: Arc<std::sync::Mutex<Option<String>>>,
    /// On-demand only: the server behind the current mount died on its own
    /// (`MountedVfs::server_stopped`). "Retry" must then drop that dead
    /// mount before attaching again.
    vfs_server_stopped: Arc<std::sync::atomic::AtomicBool>,
    /// On-demand only: held across an attach or a detach, so two of them
    /// never interleave (two resumes racing used to stack two mounts on
    /// one folder).
    attach_lock: Mutex<()>,
    /// On-demand only: whether the volume is attached right now. Not
    /// `mounted_vfs.is_some()`: under `MountTestHook` an attach succeeds
    /// with no OS handle.
    vfs_attached: std::sync::atomic::AtomicBool,
    /// Where "show in sidebar" lands. [`NoSidebar`] until the `DriveManager`
    /// installs the platform one (`set_sidebar_backend`) before `start()`:
    /// a `Mount` built anywhere else (tests) never edits the real sidebar.
    sidebar_backend: std::sync::RwLock<Arc<dyn SidebarBackend>>,
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
                    tracing::error!(target: "drive::mounts", error = %format_args!("{e:#}"), "Failed to send RefreshCredentials command");
                }
            })
        }));

        let command_tx_invalid = command_tx.clone();
        cr_client.set_on_credential_invalid(Arc::new(move || {
            let command_tx = command_tx_invalid.clone();
            Box::pin(async move {
                if let Err(e) = command_tx.send(MountCommand::CredentialInvalid) {
                    tracing::error!(target: "drive::mounts", error = %format_args!("{e:#}"), "Failed to send CredentialInvalid command");
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
            vfs_event_pump_handle: Arc::new(Mutex::new(None)),
            vfs_pending_uploads: std::sync::atomic::AtomicU64::new(0),
            upload_health: std::sync::Mutex::new(UploadHealth::default()),
            upload_problem: std::sync::Mutex::new(None),
            vfs_mount_test_hook: Mutex::new(None),
            mount_problem: Arc::new(std::sync::Mutex::new(None)),
            vfs_server_stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            attach_lock: Mutex::new(()),
            vfs_attached: std::sync::atomic::AtomicBool::new(false),
            sidebar_backend: std::sync::RwLock::new(Arc::new(NoSidebar)),
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
                    crate::utils::secure_fs::create_private_dir_all(&sync_path).with_context(|| {
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

        // After the mount: an on-demand folder only takes its icon once its
        // volume is attached (see `crate::sidebar`).
        self.apply_sidebar_at_start().await;

        tracing::info!(target: "drive::mounts", id = %self.id, path = %sync_path.display(), "Mount started");
        Ok(())
    }

    /// Installs the backend "show in sidebar" edits go through. Called by
    /// the `DriveManager` before `start()`.
    pub fn set_sidebar_backend(&self, backend: Arc<dyn SidebarBackend>) {
        *self.sidebar_backend.write().unwrap() = backend;
    }

    fn sidebar_backend(&self) -> Arc<dyn SidebarBackend> {
        self.sidebar_backend.read().unwrap().clone()
    }

    /// `(folder, label, icon_writable, settings)` for `crate::sidebar`.
    /// Writable: a mirror folder always; an on-demand one only while its
    /// volume is attached — never the bare mountpoint.
    async fn sidebar_state(&self) -> (PathBuf, String, bool, SidebarSettings) {
        let config = self.config.read().await.clone();
        // Config guard released first: never held across another lock.
        let writable = match config.mode {
            DriveMode::FullMirror => true,
            DriveMode::OnDemand => self.mounted_vfs.lock().await.is_some(),
        };
        (config.sync_path, config.name, writable, config.sidebar)
    }

    /// The `start()` half of "show in sidebar": adds the folder the first
    /// time, re-applies its icon every time. Never fails the start.
    async fn apply_sidebar_at_start(&self) {
        let (path, label, icon_writable, settings) = self.sidebar_state().await;
        let backend = self.sidebar_backend();
        let result = tokio::task::spawn_blocking(move || {
            let folder = DriveFolder { path: &path, label: &label, icon_writable };
            sidebar::at_start(backend.as_ref(), &folder, settings)
        })
        .await;
        match result {
            Ok(updated) => self.config.write().await.sidebar = updated,
            Err(err) => tracing::warn!(target: "drive::mounts", id = %self.id, ?err, "sidebar update panicked"),
        }
    }

    /// The drive settings toggle. The caller persists the config.
    pub async fn set_show_in_sidebar(&self, enabled: bool) -> Result<()> {
        let (path, label, icon_writable, _) = self.sidebar_state().await;
        let backend = self.sidebar_backend();
        let updated = tokio::task::spawn_blocking(move || {
            let folder = DriveFolder { path: &path, label: &label, icon_writable };
            sidebar::set_enabled(backend.as_ref(), &folder, enabled)
        })
        .await
        .context("sidebar update panicked")??;
        self.config.write().await.sidebar = updated;
        Ok(())
    }

    /// What the drive settings toggle shows (see `sidebar::is_shown`).
    pub async fn is_shown_in_sidebar(&self) -> bool {
        let (path, _, _, settings) = self.sidebar_state().await;
        let backend = self.sidebar_backend();
        tokio::task::spawn_blocking(move || sidebar::is_shown(backend.as_ref(), &path, settings))
            .await
            .unwrap_or(settings.enabled)
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
        // Its log lines then name the drive by id, like every other line here.
        vfs.set_drive_label(&self.id);

        // Kept BEFORE attaching, so a failed mount still leaves the facade
        // running: drafts left by the last session keep uploading (`Vfs::new`
        // already re-queued them), and "Retry" re-attaches this same
        // instance through `remount_on_demand`.
        *self.vfs.lock().await = Some(vfs.clone());
        *self.vfs_events.lock().await = Some(events);

        // Before attaching: the OS reads the volume root (and Finder lists
        // it) as soon as it is mounted, before `apply_sidebar_at_start`
        // runs (see `SidebarBackend::volume_root_files`). Kept by a pause:
        // `remount_on_demand` reuses this facade and its overlay.
        let show_icon = self.config.read().await.sidebar.enabled;
        let root = vfs.tree().root();
        for (name, data) in self.sidebar_backend().volume_root_files(show_icon) {
            if let Err(err) = vfs.put_local_only(root, name, data).await {
                tracing::warn!(target: "drive::mounts", id = %self.id, name, ?err, "failed to seed the volume root");
            }
        }

        let hook = self.vfs_mount_test_hook.lock().await.clone();
        let mounted = vfs_mode::attach(vfs, mountpoint, &name, &cache_dir, cap, hook.as_ref()).await?;
        self.attached(mounted, mountpoint).await;

        Ok(())
    }

    /// D5: re-attaches the on-demand vfs after a pause, reusing the SAME
    /// `Vfs` instance `start_on_demand` built — its block cache, draft
    /// store, and write-back queue keep running across a pause; only the
    /// OS-level attachment was ever torn down (`unmount_on_demand`), so
    /// this never touches `Vfs::new`/`build_vfs` again.
    pub async fn remount_on_demand(&self) -> Result<()> {
        let _attaching = self.attach_lock.lock().await;
        if self.vfs_attached.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        // Never built (the very first `start` failed before the facade
        // existed): build and attach it now, like `start` would have.
        let Some(vfs) = self.vfs.lock().await.clone() else {
            let sync_path = self.get_sync_path().await;
            return self.start_on_demand(&sync_path).await;
        };

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
        self.attached(mounted, &sync_path).await;
        Ok(())
    }

    /// Records a successful attach: keeps the OS handle, and watches its
    /// server for a death of its own (`on_vfs_server_stopped`).
    async fn attached(&self, mounted: Option<MountedVfs>, mountpoint: &Path) {
        if let Some(m) = &mounted {
            let mut stopped = m.server_stopped();
            let (problem, flag) = (self.mount_problem.clone(), self.vfs_server_stopped.clone());
            let (id, message) = (self.id.clone(), server_stopped_message(mountpoint));
            tokio::spawn(async move {
                // `Err`: the mount was torn down on purpose — nothing to say.
                while stopped.changed().await.is_ok() {
                    if let Some(reason) = stopped.borrow().clone() {
                        record_server_stopped(&id, &reason, &message, &problem, &flag);
                        break;
                    }
                }
            });
        }
        *self.mounted_vfs.lock().await = mounted;
        self.vfs_server_stopped.store(false, std::sync::atomic::Ordering::SeqCst);
        self.vfs_attached.store(true, std::sync::atomic::Ordering::SeqCst);
        tracing::info!(target: "drive::mounts", id = %self.id, mountpoint = %mountpoint.display(), "On-demand drive mounted");
        // A new node behind the folder: its sidebar glyph must be re-attached.
        self.sidebar_backend().glyph_folder_remounted();
    }

    /// The server behind this drive's mount died on its own: the folder
    /// now fails every access. Reported like a mount failure, with a Retry
    /// (`retry_mount` drops the dead mount first). Called by the watcher
    /// `attached` spawns.
    pub(crate) fn on_vfs_server_stopped(&self, reason: &str) {
        let mountpoint = self.config.try_read().map(|c| c.sync_path.clone()).unwrap_or_default();
        record_server_stopped(&self.id, reason, &server_stopped_message(&mountpoint), &self.mount_problem, &self.vfs_server_stopped);
    }

    /// D5: unmounts the on-demand vfs (pause/shutdown/delete). The `Vfs`
    /// facade and its event receiver are left in place — only the OS-level
    /// attachment goes away — so `remount_on_demand` can re-attach the same
    /// instance.
    async fn unmount_on_demand(&self) {
        let _detaching = self.attach_lock.lock().await;
        self.vfs_attached.store(false, std::sync::atomic::Ordering::SeqCst);
        let mountpoint = self.get_sync_path().await;
        let hook = self.vfs_mount_test_hook.lock().await.clone();
        let mounted = self.mounted_vfs.lock().await.take();
        if let Err(err) = vfs_mode::detach(mounted, &mountpoint, hook.as_ref()).await {
            tracing::warn!(target: "drive::mounts", id = %self.id, ?err, "failed to unmount the on-demand vfs");
        } else {
            tracing::info!(target: "drive::mounts", id = %self.id, mountpoint = %mountpoint.display(), "On-demand drive unmounted");
        }
        // The bare mountpoint again: another node for the sidebar glyph.
        self.sidebar_backend().glyph_folder_remounted();
    }

    /// "Retry" on an on-demand drive that failed to mount: attach it, then
    /// do what `start()` does once a drive is up (the sidebar entry).
    pub async fn retry_mount(&self) -> Result<()> {
        // A mount whose server died is still attached, but dead: drop it.
        if self.vfs_server_stopped.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.unmount_on_demand().await;
        }
        self.remount_on_demand().await?;
        self.apply_sidebar_at_start().await;
        Ok(())
    }

    /// On-demand only: files with changes not on the server yet (see
    /// `Vfs::unsynced_file_count`); 0 for a mirror drive.
    pub async fn unsynced_file_count(&self) -> u64 {
        let vfs = self.vfs.lock().await.clone();
        match vfs {
            Some(vfs) => vfs.unsynced_file_count().await as u64,
            None => 0,
        }
    }

    /// Why this drive's uploads keep failing, in plain language, or `None`
    /// while they go through (see `upload_health`).
    pub fn upload_problem(&self) -> Option<String> {
        self.upload_problem.lock().unwrap().clone()
    }

    /// See `mount_problem`'s field doc.
    pub fn mount_problem(&self) -> Option<String> {
        self.mount_problem.lock().unwrap().clone()
    }

    pub fn set_mount_problem(&self, message: Option<String>) {
        let previous = std::mem::replace(&mut *self.mount_problem.lock().unwrap(), message.clone());
        match (previous.is_some(), message.is_some()) {
            (_, true) => tracing::warn!(target: "drive::mounts", id = %self.id, "Mount problem recorded"),
            (true, false) => tracing::info!(target: "drive::mounts", id = %self.id, "Mount problem cleared"),
            (false, false) => {}
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
                            tracing::error!(target: "drive::mounts", error = %format_args!("{e:#}"), "Sync failed");
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
                            tracing::warn!(target: "drive::mounts", error = %format_args!("{e:#}"), "Failed to re-enqueue offline tasks");
                        }
                        let _lock = mount_clone.sync_lock.lock().await;
                        match mount_clone.perform_full_sync().await {
                            Ok(()) => {
                                mount_clone.set_initial_sync_completed(true).await;
                            }
                            Err(e) => {
                                tracing::error!(target: "drive::mounts", error = %format_args!("{e:#}"), "Full sync failed");
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

    /// D6: spawn the `VfsEvent` pump — takes ownership of `vfs_events` (the
    /// receiver `start_on_demand` populated) and folds every event it sees
    /// into `vfs_pending_uploads` and `upload_health`, plus a toast for a
    /// conflict copy and for uploads that keep failing. A no-op for a `FullMirror` drive, or if this
    /// is called a second time for the same on-demand drive: `vfs_events`
    /// is `Mutex<Option<...>>` and `.take()`n exactly once, mirroring
    /// `mounted_vfs`/`vfs`'s own "only ever set once per lifetime" shape —
    /// see the field's own doc.
    ///
    /// DELIBERATELY never re-spawned or torn down by `pause()`/
    /// `resume_drive` (unlike `remote_event_handle`/`periodic_sync_handle`/
    /// `props_refresh_handle`, all stopped while paused and respawned on
    /// resume): `remount_on_demand` (D5) reuses the SAME `Vfs` instance —
    /// and therefore the SAME `VfsEvent` channel — across a pause/resume
    /// cycle rather than rebuilding it, so the receiver this pump owns is
    /// the only one that will ever exist for this `Mount`'s whole
    /// lifetime. Aborting the task that owns it (as `pause()` does to the
    /// other workers) would drop that receiver for good — `vfs_events`
    /// only ever gets set once, by `start_on_demand` — permanently
    /// freezing the counters with no way to recover on resume. There is
    /// also nothing network-related here to stop: unlike the SSE
    /// worker/periodic sync/props refresh, this pump makes no requests of
    /// its own — it only observes activity the `Vfs`'s own write-back queue
    /// keeps running regardless of pause (only the OS-level mount attachment
    /// goes away on pause, per `unmount_on_demand`'s doc). `shutdown()`
    /// aborts it like every other worker, since that IS real teardown with
    /// no resume to come back from.
    pub async fn spawn_vfs_event_pump(self: &Arc<Self>) {
        let Some(mut events) = self.vfs_events.lock().await.take() else {
            return;
        };
        let mount = self.clone();
        let handle = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                mount.fold_vfs_event(event).await;
            }
        });
        Self::replace_worker(&self.vfs_event_pump_handle, handle).await;
    }

    /// D6: folds one `VfsEvent` into the counters/toasts — see
    /// `spawn_vfs_event_pump`'s doc and the `vfs_pending_uploads` and
    /// `upload_health` field docs for the accounting rules. Every
    /// terminal variant (all but `UploadQueued`) decrements
    /// `vfs_pending_uploads` exactly once, matching the "one `UploadQueued`,
    /// exactly one terminal" contract documented on `VfsEvent` itself —
    /// including `UploadRenamed`, whose immediately-following fresh
    /// `UploadQueued` (per that same contract) re-increments it right back,
    /// net zero, correctly reflecting that the cycle merely continued under
    /// a new name rather than actually finishing.
    pub(crate) async fn fold_vfs_event(&self, event: VfsEvent) {
        use std::sync::atomic::Ordering;

        let dec_pending = || {
            let _ = self.vfs_pending_uploads.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        };

        match event {
            VfsEvent::UploadQueued { .. } => {
                self.vfs_pending_uploads.fetch_add(1, Ordering::Relaxed);
            }
            VfsEvent::UploadSucceeded { .. } => {
                dec_pending();
                self.upload_health.lock().unwrap().on_success();
                if self.upload_problem.lock().unwrap().take().is_some() {
                    tracing::info!(target: "drive::mounts", id = %self.id, "Uploads go through again");
                }
            }
            VfsEvent::UploadCancelled { .. } => {
                dec_pending();
            }
            VfsEvent::UploadRenamed { .. } => {
                dec_pending();
            }
            VfsEvent::UploadFailed { error, will_retry, .. } => {
                dec_pending();
                let newly = self.upload_health.lock().unwrap().on_failure(&error, !will_retry);
                if let Some(problem) = newly {
                    let name = self.config.read().await.name.clone();
                    let message = upload_health::problem_message(problem, &name);
                    tracing::warn!(target: "drive::mounts", id = %self.id, ?problem, error = %format_args!("{error:#}"), "Uploads keep failing");
                    *self.upload_problem.lock().unwrap() = Some(message.clone());
                    toast::send_upload_problem_toast(&self.id, &message);
                }
            }
            VfsEvent::ConflictSaved { original, conflict_copy } => {
                dec_pending();
                // Reviewer fix (Task 5): `original` is a facade-internal
                // `cloudreve://…` remote uri (`VfsEvent`'s own doc; traced
                // to `ConflictSaved`'s emission site in `writeback.rs`,
                // which passes `remote_path.to_string()` straight through —
                // `NodeAttr::remote_path` for a child is built as
                // `format!("{parent_path}/{name}")` off the tree's
                // `remote_base`, so it is NEVER a local path). Every OTHER
                // `send_conflict_toast` call site (`tasks/upload.rs`,
                // `tasks/download.rs`, `drive/sync.rs`) passes a genuine
                // local filesystem path — `toast.rs` renders it with
                // `path.display()` on that assumption. Passing the raw uri
                // straight through leaked "cloudreve://my/sync/…" to the
                // user instead of a path they'd recognize; map it onto the
                // drive's local mounted path first, the exact inverse of
                // D8's `handle_view_online` mapping.
                let display_path = self.remote_uri_to_display_path(&original).await;
                let copy_path = self.remote_uri_to_display_path(&conflict_copy).await;
                toast::send_conflict_toast(&self.id, &display_path, Some(&copy_path));
            }
        }
    }

    /// Maps a facade-internal `cloudreve://…` remote uri (as carried by
    /// `VfsEvent::ConflictSaved`/`UploadFailed`) onto the local path a user
    /// would recognize under this drive's mounted `sync_path` — the exact
    /// inverse of `local_path_to_cr_uri` (D8's `handle_view_online`
    /// mapping), reusing the existing `remote_path_to_local_relative_path`
    /// helper rather than duplicating its uri-relativization logic.
    ///
    /// Falls back to just the uri's last path segment (its file/dir name)
    /// if the inversion is ever unsound — `remote_path` failing to parse as
    /// a `CrUri`, or not actually living under this drive's `remote_path`
    /// (shouldn't happen: every uri this is called with was built from THIS
    /// drive's own tree) — an honest best-effort display, never the raw uri.
    async fn remote_uri_to_display_path(&self, remote_path: &str) -> PathBuf {
        let (remote_base, sync_path) = {
            let config = self.config.read().await;
            (config.remote_path.clone(), config.sync_path.clone())
        };
        let mapped = (|| -> Result<PathBuf> {
            use cloudreve_api::models::uri::CrUri;
            let remote = CrUri::new(remote_path)?;
            let base = CrUri::new(&remote_base)?;
            let relative = crate::drive::utils::remote_path_to_local_relative_path(&remote, &base)?;
            Ok(sync_path.join(relative))
        })();
        mapped.unwrap_or_else(|_| {
            let name = remote_path.rsplit('/').next().unwrap_or(remote_path);
            sync_path.join(name)
        })
    }

    /// Spawn a task to periodically refresh drive properties (quota, etc.)
    pub async fn spawn_props_refresh_task(self: &Arc<Self>) {
        let mount = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                if let Err(e) = mount.refresh_drive_props().await {
                    tracing::warn!(target: "drive::mounts", id = %mount.id, error = %format_args!("{e:#}"), "Failed to refresh drive props");
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
    /// Returns whether the drive WAS paused: of two resumes racing, only one
    /// sees `true`, so only one restarts the workers and remounts.
    pub async fn resume(&self) -> bool {
        let was_paused = self.paused.swap(false, std::sync::atomic::Ordering::SeqCst);
        if was_paused {
            tracing::info!(target: "drive::mounts", id = %self.id, "Drive resumed");
        }
        was_paused
    }

    /// Real teardown (app quit, a drive that failed to start): unmounts,
    /// stops every worker, and cuts the on-demand vfs off the server
    /// (`Vfs::shutdown`) — its drafts stay on disk for the next launch.
    pub async fn shutdown(&self) {
        self.stop(true).await;
    }

    /// `stop_vfs`: whether to also cut the vfs off the server. `delete`
    /// passes false, to give uploads already under way a last chance to
    /// land, and cuts it itself afterwards.
    async fn stop(&self, stop_vfs: bool) {
        tracing::info!(target: "drive::mounts", id = %self.id, "Shutting down mount");

        // D5: shutdown unmounts an on-demand drive the same way pause does
        // (Drop is only the crash net, not the primary teardown path).
        match self.config.read().await.mode {
            DriveMode::FullMirror => {
                *self.fs_watcher.lock().await = None;
            }
            DriveMode::OnDemand => {
                self.unmount_on_demand().await;
                // Its cache no longer takes a share of the volume.
                if let Ok(cache_dir) = vfs_mode::cache_dir_for(&self.id) {
                    vfs_mode::forget_running_cache(&cache_dir);
                }
            }
        }

        // Abort background tasks
        if let Some(h) = self.processor_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.remote_event_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.periodic_sync_handle.lock().await.take() {
            h.abort();
        }
        if let Some(h) = self.props_refresh_handle.lock().await.take() {
            h.abort();
        }
        // Unlike pause() (see `vfs_event_pump_handle`'s field doc), real
        // teardown DOES abort the vfs event pump — there is no resume to
        // come back from here.
        if let Some(h) = self.vfs_event_pump_handle.lock().await.take() {
            h.abort();
        }

        if stop_vfs {
            self.shutdown_vfs().await;
        }

        // Shutdown task queue
        self.task_queue.shutdown().await;
    }

    /// See `Vfs::shutdown`. A no-op for a mirror drive.
    async fn shutdown_vfs(&self) {
        if let Some(vfs) = self.vfs.lock().await.clone() {
            vfs.shutdown();
        }
    }

    pub async fn delete(&self) -> Result<()> {
        self.stop(false).await;

        // After `shutdown()`: an on-demand volume is detached by now, so
        // `sidebar_state` reports its bare mountpoint as not writable and
        // no icon is cleared through (or written into) it.
        let (path, label, icon_writable, settings) = self.sidebar_state().await;
        let backend = self.sidebar_backend();
        let _ = tokio::task::spawn_blocking(move || {
            let folder = DriveFolder { path: &path, label: &label, icon_writable };
            sidebar::on_drive_removed(backend.as_ref(), &folder, settings);
        })
        .await;

        // D5: an on-demand drive's cache dir is this crate's own, not
        // anything the user put there — remove it on delete the same way a
        // FullMirror drive's inventory rows are wiped below. Best-effort:
        // `inventory.nuke_drive` below is a harmless no-op for a drive that
        // never wrote any rows, but a leftover cache dir would otherwise
        // accumulate forever across add/remove cycles.
        if self.config.read().await.mode == DriveMode::OnDemand {
            // The cache holds the drafts — changes not on the server yet.
            // Give uploads already under way a chance to land first; the
            // settings window warned the user about whatever is left
            // (`DriveInfo::unsynced_files`).
            let vfs = self.vfs.lock().await.clone();
            if let Some(vfs) = vfs {
                let _ = tokio::time::timeout(Duration::from_secs(20), vfs.wait_for_writeback_idle()).await;
                let left = vfs.unsynced_file_count().await;
                if left > 0 {
                    tracing::warn!(target: "drive::mounts", id = %self.id, left, "removing an on-demand drive with changes that were never uploaded");
                }
            }
            // Whatever did not land in time never will: no retry may run
            // later with the tokens of a drive that no longer exists.
            self.shutdown_vfs().await;
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


/// What the user is told when the server behind an on-demand mount died.
fn server_stopped_message(mountpoint: &Path) -> String {
    t!("mountErrorServerStopped", folder = mountpoint.display().to_string()).into_owned()
}

/// Shared by the server watcher (which has no `Mount` to call into) and
/// `Mount::on_vfs_server_stopped`.
fn record_server_stopped(
    id: &str,
    reason: &str,
    message: &str,
    problem: &std::sync::Mutex<Option<String>>,
    flag: &std::sync::atomic::AtomicBool,
) {
    tracing::error!(target: "drive::mounts", id, reason, "The server behind the on-demand mount stopped; the folder is unusable until Retry");
    flag.store(true, std::sync::atomic::Ordering::SeqCst);
    *problem.lock().unwrap() = Some(message.to_string());
}
