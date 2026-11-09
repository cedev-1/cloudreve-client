//! "Show in sidebar" through the real `Mount` lifecycle: `start()` (every
//! launch), the settings toggle, and `delete()` (drive removed). The OS
//! sidebar is an in-memory fake here; the real macOS and Linux backends
//! have their own tests (`finder_sidebar_macos.rs`, `sidebar::linux`).

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cloudreve_sync::drive::mounts::DriveMode;
use cloudreve_sync::sidebar::{SidebarBackend, SidebarSettings};
use common::TestEnv;

#[derive(Default)]
struct FakeSidebar {
    entries: Mutex<BTreeSet<PathBuf>>,
    icons: Mutex<BTreeSet<PathBuf>>,
    adds: Mutex<usize>,
}

impl FakeSidebar {
    fn lists(&self, p: &Path) -> bool {
        self.entries.lock().unwrap().contains(p)
    }
    fn has_icon(&self, p: &Path) -> bool {
        self.icons.lock().unwrap().contains(p)
    }
}

impl SidebarBackend for FakeSidebar {
    fn contains(&self, folder: &Path) -> anyhow::Result<bool> {
        Ok(self.lists(folder))
    }
    fn add(&self, folder: &Path, _label: &str) -> anyhow::Result<()> {
        *self.adds.lock().unwrap() += 1;
        self.entries.lock().unwrap().insert(folder.to_path_buf());
        Ok(())
    }
    fn remove(&self, folder: &Path) -> anyhow::Result<()> {
        self.entries.lock().unwrap().remove(folder);
        Ok(())
    }
    fn set_icon(&self, folder: &Path) -> anyhow::Result<()> {
        self.icons.lock().unwrap().insert(folder.to_path_buf());
        Ok(())
    }
    fn clear_icon(&self, folder: &Path) -> anyhow::Result<()> {
        self.icons.lock().unwrap().remove(folder);
        Ok(())
    }
}

#[tokio::test]
async fn a_started_drive_is_in_the_sidebar_with_its_logo_and_remembers_it() {
    let fake = Arc::new(FakeSidebar::default());
    let env = TestEnv::with_sidebar(DriveMode::FullMirror, fake.clone()).await;

    assert!(fake.lists(&env.sync_dir));
    assert!(fake.has_icon(&env.sync_dir));
    assert_eq!(env.mount.get_config().await.sidebar, SidebarSettings { enabled: true, added: true });
    assert!(env.mount.is_shown_in_sidebar().await);
}

#[tokio::test]
async fn an_entry_the_user_removed_stays_removed_after_a_relaunch() {
    let fake = Arc::new(FakeSidebar::default());
    let mut env = TestEnv::with_sidebar(DriveMode::FullMirror, fake.clone()).await;
    fake.remove(&env.sync_dir).unwrap();

    env.restart_mount().await;

    assert!(!fake.lists(&env.sync_dir), "the app re-added an entry the user removed");
    assert_eq!(*fake.adds.lock().unwrap(), 1);
    assert!(!env.mount.is_shown_in_sidebar().await, "the toggle must reflect the removal");
}

#[tokio::test]
async fn the_logo_comes_back_at_every_launch() {
    let fake = Arc::new(FakeSidebar::default());
    let mut env = TestEnv::with_sidebar(DriveMode::FullMirror, fake.clone()).await;
    fake.icons.lock().unwrap().clear();

    env.restart_mount().await;

    assert!(fake.has_icon(&env.sync_dir));
}

#[tokio::test]
async fn an_on_demand_drive_that_is_not_mounted_gets_nothing_written_into_its_mountpoint() {
    // Under the mount test hook nothing is attached at the OS level: the
    // sync folder is the bare mountpoint, which must stay empty or the
    // next real mount refuses it.
    let fake = Arc::new(FakeSidebar::default());
    let env = TestEnv::with_sidebar(DriveMode::OnDemand, fake.clone()).await;

    assert!(fake.lists(&env.sync_dir), "the entry does not depend on the mount");
    assert!(!fake.has_icon(&env.sync_dir));
}

#[tokio::test]
async fn turning_the_toggle_off_removes_the_entry_for_good() {
    let fake = Arc::new(FakeSidebar::default());
    let mut env = TestEnv::with_sidebar(DriveMode::FullMirror, fake.clone()).await;

    env.mount.set_show_in_sidebar(false).await.unwrap();
    assert!(!fake.lists(&env.sync_dir));
    assert!(!fake.has_icon(&env.sync_dir));
    assert!(!env.mount.is_shown_in_sidebar().await);

    env.restart_mount().await;
    assert!(!fake.lists(&env.sync_dir), "a drive switched off came back at the next launch");
    assert!(!fake.has_icon(&env.sync_dir), "a drive switched off got its logo back at the next launch");
    assert!(!env.mount.get_config().await.sidebar.enabled, "the choice was not kept in the config");

    env.mount.set_show_in_sidebar(true).await.unwrap();
    assert!(fake.lists(&env.sync_dir));
    assert!(env.mount.is_shown_in_sidebar().await);
}

#[tokio::test]
async fn removing_the_drive_removes_its_sidebar_entry() {
    let fake = Arc::new(FakeSidebar::default());
    let env = TestEnv::with_sidebar(DriveMode::FullMirror, fake.clone()).await;

    env.mount.delete().await.unwrap();

    assert!(!fake.lists(&env.sync_dir));
    assert!(!fake.has_icon(&env.sync_dir));
}

#[tokio::test]
async fn an_old_drives_json_without_the_setting_turns_the_sidebar_on() {
    let config: cloudreve_sync::DriveConfig = serde_json::from_value(serde_json::json!({
        "id": "d1", "name": "Work", "instance_url": "https://cloud.example.com",
        "remote_path": "cloudreve://my", "sync_path": "/Users/ana/Work",
        "credentials": { "access_token": null, "refresh_token": "r", "refresh_expires": "2099-01-01T00:00:00Z", "access_expires": null },
        "icon_path": null, "raw_icon_path": null, "enabled": true, "user_id": "u1"
    }))
    .unwrap();
    assert_eq!(config.sidebar, SidebarSettings { enabled: true, added: false });
    assert!(!config.extra.contains_key("sidebar"));
}
