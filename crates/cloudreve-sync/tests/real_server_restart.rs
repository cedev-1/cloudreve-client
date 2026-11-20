//! Quitting the app with on-demand changes not uploaded yet (the server
//! unreachable), then launching it again: the next `DriveManager::load()`
//! — what app startup runs — must bring the drive back over the same cache
//! and upload them, against a REAL Cloudreve server (see
//! `cloudreve-vfs/tests/real_server/mod.rs`; skipped unless
//! `CLOUDREVE_E2E_URL` is set).
//!
//! Its own binary: it needs a `drives.json` of its own, and the app data
//! root is process-wide.

#[path = "../../cloudreve-vfs/tests/real_server/mod.rs"]
mod real_server;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_sync::EventBroadcaster;
use cloudreve_sync::drive::manager::DriveManager;
use cloudreve_sync::drive::mounts::{Credentials, DriveConfig, DriveMode};
use cloudreve_sync::drive::vfs_mode::MountTestHook;
use cloudreve_sync::sidebar::SidebarSettings;
use real_server::{Proxy, e2e_or_skip, eventually, pattern};

async fn manager() -> Arc<DriveManager> {
    let manager = Arc::new(DriveManager::new(Arc::new(EventBroadcaster::new(100))).unwrap());
    manager.spawn_command_processor().await;
    manager.install_vfs_mount_hook_for_tests(MountTestHook::new());
    manager
}

#[tokio::test(flavor = "multi_thread")]
async fn on_demand_changes_left_unsent_at_quit_upload_after_the_next_launch() {
    let Some(e2e) = e2e_or_skip("restart") else { return };
    let home = tempfile::TempDir::new().unwrap();
    cloudreve_sync::utils::app_dir::redirect_for_tests(home.path());
    let remote = e2e.remote("restart").await;
    let proxy = Proxy::start(&e2e.url).await;
    let session = e2e.oauth_session().await;
    let sync = tempfile::TempDir::new().unwrap();

    let first = manager().await;
    let id = first
        .add_drive(DriveConfig {
            id: uuid::Uuid::new_v4().to_string(),
            name: "e2e restart".to_string(),
            instance_url: proxy.url.clone(),
            remote_path: remote.root.clone(),
            credentials: Credentials {
                access_token: Some(session.access_token.clone()),
                refresh_token: session.refresh_token.clone(),
                access_expires: Some(session.access_expires.clone()),
                refresh_expires: session.refresh_expires.clone(),
            },
            sync_path: sync.path().join("Cloudreve"),
            enabled: true,
            user_id: "e2e".to_string(),
            max_file_size_mb: 3072,
            sse_client_id: uuid::Uuid::new_v4().to_string(),
            mode: DriveMode::OnDemand,
            sidebar: SidebarSettings { enabled: false, added: false },
            ..Default::default()
        })
        .await
        .unwrap();
    first.persist().await.unwrap();

    let mount = first.get_drive(&id).await.unwrap();
    let vfs = mount.vfs.lock().await.clone().unwrap();
    let root = vfs.tree().root();
    assert!(vfs.readdir(root).await.unwrap().is_empty());
    proxy.set_down(true);
    let data = pattern(3 * 1024 * 1024 + 7, 80);
    let (_, h) = vfs.create(root, "unsent.bin").await.unwrap();
    for (i, c) in data.chunks(1024 * 1024).enumerate() {
        vfs.write(h, (i * 1024 * 1024) as u64, c).await.unwrap();
    }
    vfs.close(h).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(mount.unsynced_file_count().await, 1);

    // Quit.
    mount.shutdown().await;
    drop((vfs, mount, first));
    assert!(remote.info("unsent.bin").await.is_none());

    // Next launch, network back.
    proxy.set_down(false);
    let second = manager().await;
    second.load().await.unwrap();
    assert!(second.get_drive(&id).await.is_some(), "the drive did not come back");
    eventually("the unsent file to upload after relaunch", Duration::from_secs(60), || async {
        match remote.read("unsent.bin").await {
            Some(b) if b == data => Ok(()),
            other => Err(format!("{:?} bytes on the server", other.map(|b| b.len()))),
        }
    })
    .await;
    let _ = second.remove_drive(&id).await;
    remote.cleanup().await;
}
