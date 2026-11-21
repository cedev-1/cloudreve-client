//! A folder moved into a mirror drive (dragged from the Desktop, extracted
//! elsewhere then moved, `mv ~/Downloads/photos .`) must upload what it
//! holds right away.
//!
//! A move is ONE filesystem event, for the folder; there is none for the
//! files inside. The watcher's batch used to skip folders, so nothing was
//! uploaded until the periodic full sync, up to five minutes later. Found
//! by the real-server suite (`real_server_sync.rs`).

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use common::TestEnv;

#[tokio::test(flavor = "multi_thread")]
async fn every_file_of_a_folder_moved_into_the_drive_is_uploaded_without_waiting_for_a_full_sync() {
    let outer = tempfile::TempDir::new().unwrap();
    let base = std::fs::canonicalize(outer.path()).unwrap();
    let sync = base.join("Cloudreve");
    std::fs::create_dir_all(&sync).unwrap();
    let env = TestEnv::started_on(sync.clone(), DriveMode::FullMirror).await;
    env.set_remote_files(vec![]).await;
    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Built outside the drive, then moved in with one rename.
    let staging = base.join("holiday");
    std::fs::create_dir_all(staging.join("day2")).unwrap();
    std::fs::write(staging.join("beach.jpg"), b"beach").unwrap();
    std::fs::write(staging.join("notes.txt"), b"notes").unwrap();
    std::fs::write(staging.join("day2/hike.jpg"), b"hike").unwrap();
    std::fs::rename(&staging, sync.join("holiday")).unwrap();

    let wanted = ["holiday/beach.jpg", "holiday/notes.txt", "holiday/day2/hike.jpg"];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let sessions: Vec<String> = env
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|r| r.method.as_str() == "PUT" && r.url.path() == "/api/v4/file/upload")
            .map(|r| String::from_utf8_lossy(&r.body).to_string())
            .collect();
        let missing: Vec<&str> =
            wanted.iter().copied().filter(|w| !sessions.iter().any(|b| b.contains(w))).collect();
        if missing.is_empty() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "never uploaded: {missing:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
