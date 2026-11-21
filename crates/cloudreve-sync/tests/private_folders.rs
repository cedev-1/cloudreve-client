//! A drive folder the app creates holds the user's files (a mirror) or is
//! the mountpoint they are served through (on-demand): other local users
//! have no business listing it. A folder the user picked and already owns
//! keeps whatever permissions they gave it.
#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use cloudreve_sync::drive::mounts::DriveMode;
use common::TestEnv;

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).expect("stat the drive folder").permissions().mode() & 0o777
}

#[tokio::test]
async fn a_mirror_folder_the_app_creates_is_private() {
    let parent = tempfile::tempdir().unwrap();
    let folder = parent.path().join("Cloudreve");

    let _env = TestEnv::started_on(folder.clone(), DriveMode::FullMirror).await;

    assert_eq!(mode_of(&folder), 0o700);
}

#[tokio::test]
async fn an_on_demand_mountpoint_the_app_creates_is_private() {
    let parent = tempfile::tempdir().unwrap();
    let folder = parent.path().join("Cloudreve");

    let _env = TestEnv::started_on(folder.clone(), DriveMode::OnDemand).await;

    assert_eq!(mode_of(&folder), 0o700);
}

#[tokio::test]
async fn an_existing_folder_keeps_the_permissions_its_owner_gave_it() {
    for mode in [DriveMode::FullMirror, DriveMode::OnDemand] {
        let parent = tempfile::tempdir().unwrap();
        let folder = parent.path().join("Shared with family");
        std::fs::create_dir(&folder).unwrap();
        std::fs::set_permissions(&folder, std::fs::Permissions::from_mode(0o750)).unwrap();

        let _env = TestEnv::started_on(folder.clone(), mode).await;

        assert_eq!(mode_of(&folder), 0o750, "{mode:?}");
    }
}
