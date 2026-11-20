//! A sync folder whose path goes through a symlink (`/tmp`, `/var/folders`
//! on macOS, `/home` -> `/var/home` on Fedora Atomic, a folder the user
//! linked elsewhere) must sync like any other.
//!
//! macOS FSEvents reports the RESOLVED path of a change (`/private/var/…`
//! for a folder configured as `/var/…`). Every path the drive works with —
//! the remote uri computation, the inventory, the event blocker — is keyed
//! by the configured path, so an unresolved event matched none of them: the
//! upload failed with "Path is not under root" and nothing ever reached the
//! server. Found by the real-server suite (`real_server_sync.rs`), whose
//! temp folders all live under the `/var` -> `/private/var` link.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use common::TestEnv;

#[tokio::test(flavor = "multi_thread")]
async fn a_file_saved_in_a_sync_folder_reached_through_a_symlink_is_uploaded() {
    let outer = tempfile::TempDir::new().unwrap();
    // Resolved first, so the link is the ONLY indirection under test.
    let real = std::fs::canonicalize(outer.path()).unwrap().join("real-folder");
    std::fs::create_dir_all(&real).unwrap();
    let link = std::fs::canonicalize(outer.path()).unwrap().join("Cloudreve");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    let env = TestEnv::started_on(link.clone(), DriveMode::FullMirror).await;
    env.set_remote_files(vec![]).await;
    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    // FSEvents may replay a few events from before the watch started.
    tokio::time::sleep(Duration::from_millis(500)).await;

    std::fs::write(link.join("report.txt"), b"saved through the link").unwrap();

    // The upload starts by asking the server for an upload session for the
    // file's remote uri: only possible once the local path was understood.
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
        if sessions.iter().any(|b| b.contains("report.txt")) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no upload session was ever requested for report.txt; upload tasks: {:?}",
            env.tasks_of_type("upload").iter().map(|t| (&t.local_path, &t.status, &t.error)).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // And the task is recorded under the folder the user configured.
    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.starts_with(link.to_str().unwrap())),
        "upload tasks recorded outside the configured folder: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
}
