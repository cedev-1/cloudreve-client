//! A drive added the way the app adds one — `DriveManager::add_drive`,
//! what the "add drive" command runs — over a fresh folder of the real e2e
//! server (see `cloudreve-vfs/tests/real_server/mod.rs`). Shared by
//! `real_server_sync.rs` and `real_server_mounted.rs`.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cloudreve_sync::EventBroadcaster;
use cloudreve_sync::drive::manager::DriveManager;
use cloudreve_sync::drive::mounts::{Credentials, DriveConfig, DriveMode, Mount};
use cloudreve_sync::drive::vfs_mode::MountTestHook;
use cloudreve_sync::sidebar::SidebarSettings;
use cloudreve_vfs::vfs::Vfs;
use tempfile::TempDir;

use crate::real_server::{E2e, Proxy, Remote, e2e_or_skip, eventually};

pub const MIB: usize = 1024 * 1024;
/// How long a change may take to cross between the server and the drive.
pub const PROPAGATION: Duration = Duration::from_secs(20);

fn redirect_app_data() {
    // RUST_LOG=debug shows what the drive did when a test fails.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let root = std::env::temp_dir().join(format!("cloudreve-e2e-home-{}", std::process::id()));
    cloudreve_sync::utils::app_dir::redirect_for_tests(&root);
}

/// One runtime and ONE `DriveManager` for the whole test binary, like the
/// app: every drive of every test is added to the same manager (sharing
/// its inventory database, heartbeat, command processor). Separate managers
/// would each open their own connection to the one inventory database the
/// process gets, and fight over its lock; a per-test runtime would kill the
/// drive's workers whenever a test ends.
struct Shared {
    rt: tokio::runtime::Runtime,
    manager: Arc<DriveManager>,
}

static SHARED: std::sync::OnceLock<Shared> = std::sync::OnceLock::new();

/// Whether this binary's on-demand drives mount through the OS. Fixed by
/// the binary (the mount seam stub is manager-wide): `real_server_sync`
/// stubs it, `real_server_mounted` does not.
pub static REAL_MOUNTS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn shared() -> &'static Shared {
    SHARED.get_or_init(|| {
        redirect_app_data();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let manager = rt.block_on(async {
            let manager = Arc::new(DriveManager::new(Arc::new(EventBroadcaster::new(100))).unwrap());
            manager.spawn_command_processor().await;
            if !REAL_MOUNTS.load(std::sync::atomic::Ordering::SeqCst) {
                manager.install_vfs_mount_hook_for_tests(MountTestHook::new());
            }
            manager
        });
        Shared { rt, manager }
    })
}

/// Runs a test body on the shared runtime, re-raising its panic here.
pub fn run<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    let rt = &shared().rt;
    if let Err(e) = rt.handle().block_on(rt.spawn(body)) {
        if e.is_panic() {
            std::panic::resume_unwind(e.into_panic());
        }
        panic!("test body cancelled: {e}");
    }
}

pub struct App {
    pub e2e: E2e,
    pub manager: Arc<DriveManager>,
    pub id: String,
    pub remote: Remote,
    pub sync: PathBuf,
    pub proxy: Option<Proxy>,
    pub tmp: TempDir,
}

pub struct Opts {
    pub mode: DriveMode,
    /// Behind a [`Proxy`] the test can cut.
    pub proxied: bool,
    pub max_file_size_mb: u64,
    /// On-demand only: mount through the OS for real (NFS on macOS, FUSE on
    /// Linux) instead of stubbing the mount seam.
    pub real_mount: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts { mode: DriveMode::FullMirror, proxied: false, max_file_size_mb: 3072, real_mount: false }
    }
}

impl App {
    pub async fn mirror(label: &str) -> Option<App> {
        Self::add(label, Opts::default(), |_| async {}).await
    }

    pub async fn on_demand(label: &str) -> Option<App> {
        Self::add(label, Opts { mode: DriveMode::OnDemand, ..Opts::default() }, |_| async {}).await
    }

    /// `seed` fills the remote folder before the drive is added.
    pub async fn add<F, Fut>(label: &str, opts: Opts, seed: F) -> Option<App>
    where
        F: FnOnce(Remote) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let e2e = e2e_or_skip(label)?;
        assert_eq!(
            opts.real_mount,
            REAL_MOUNTS.load(std::sync::atomic::Ordering::SeqCst),
            "real_mount is fixed per test binary (see REAL_MOUNTS)"
        );
        let remote = e2e.remote(label).await;
        seed(remote.clone_handle()).await;
        let proxy = if opts.proxied { Some(Proxy::start(&e2e.url).await) } else { None };
        let base = proxy.as_ref().map(|p| p.url.clone()).unwrap_or_else(|| e2e.url.clone());
        let session = e2e.oauth_session().await;
        let manager = shared().manager.clone();
        let tmp = TempDir::new().unwrap();
        let sync = tmp.path().join("Cloudreve");
        let config = DriveConfig {
            id: uuid::Uuid::new_v4().to_string(),
            name: format!("e2e {label}"),
            instance_url: base,
            remote_path: remote.root.clone(),
            credentials: Credentials {
                access_token: Some(session.access_token.clone()),
                refresh_token: session.refresh_token.clone(),
                access_expires: Some(session.access_expires.clone()),
                refresh_expires: session.refresh_expires.clone(),
            },
            sync_path: sync.clone(),
            enabled: true,
            user_id: "e2e".to_string(),
            max_file_size_mb: opts.max_file_size_mb,
            sse_client_id: uuid::Uuid::new_v4().to_string(),
            mode: opts.mode,
            // Never touch the Finder sidebar of the machine running this.
            sidebar: SidebarSettings { enabled: false, added: false },
            ..Default::default()
        };
        let id = manager.add_drive(config).await.expect("add_drive");
        Some(App { e2e, manager, id, remote, sync, proxy, tmp })
    }

    pub async fn mount(&self) -> Arc<Mount> {
        self.manager.get_drive(&self.id).await.expect("drive is registered")
    }

    pub async fn vfs(&self) -> Arc<Vfs> {
        self.mount().await.vfs.lock().await.clone().expect("on-demand drive has a vfs")
    }

    pub fn local(&self, rel: &str) -> PathBuf {
        self.sync.join(rel)
    }

    pub fn proxy(&self) -> &Proxy {
        self.proxy.as_ref().expect("built with proxied: true")
    }

    pub async fn cleanup(self) {
        let _ = self.manager.remove_drive(&self.id).await;
        self.remote.cleanup().await;
    }
}

/// Runs a blocking filesystem closure off the runtime with a deadline, so
/// a hung mount fails the test instead of hanging the suite.
pub async fn fs<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::time::timeout(Duration::from_secs(120), tokio::task::spawn_blocking(f))
        .await
        .expect("filesystem call hung")
        .expect("filesystem call panicked")
}

pub async fn wait_local(app: &App, rel: &str, want: &[u8]) {
    let path = app.local(rel);
    let want = want.to_vec();
    eventually(&format!("{rel} to show up locally"), PROPAGATION, || {
        let path = path.clone();
        let want = want.clone();
        async move {
            match fs(move || std::fs::read(&path)).await {
                Ok(b) if b == want => Ok(()),
                Ok(b) => Err(format!("{} bytes locally", b.len())),
                Err(e) => Err(e.to_string()),
            }
        }
    })
    .await;
}

pub async fn wait_local_gone(app: &App, rel: &str) {
    let path = app.local(rel);
    eventually(&format!("{rel} to disappear locally"), PROPAGATION, || {
        let path = path.clone();
        async move { if fs(move || path.exists()).await { Err("still there".into()) } else { Ok(()) } }
    })
    .await;
}

pub async fn wait_remote(remote: &Remote, rel: &str, want: &[u8]) {
    eventually(&format!("{rel} to reach the server"), PROPAGATION * 3, || async {
        match remote.read(rel).await {
            Some(b) if b == want => Ok(()),
            Some(b) => Err(format!("{} bytes on the server", b.len())),
            None => Err("absent".into()),
        }
    })
    .await;
}

pub async fn wait_remote_gone(remote: &Remote, rel: &str) {
    eventually(&format!("{rel} to leave the server"), PROPAGATION, || async {
        if remote.info(rel).await.is_some() { Err("still there".into()) } else { Ok(()) }
    })
    .await;
}

pub fn write(path: &Path, data: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, data).unwrap();
}
