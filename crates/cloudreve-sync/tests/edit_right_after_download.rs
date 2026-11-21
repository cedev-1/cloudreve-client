//! A file saved a moment after the drive downloaded it must be uploaded.
//!
//! Every download used to tell the fs watcher to drop ALL create/modify
//! events on its path for the next 5 seconds, so the drive would not take
//! its own write for a user edit. A user (or an editor's autosave, or a
//! script) saving within those seconds was dropped with it: nothing was
//! uploaded until the periodic full sync, minutes later. Found by the
//! real-server suite (`real_server_sync.rs`, an editor's safe save right
//! after the first sync, on Linux). The drive's own write is already told
//! apart by the inventory check before every upload.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::commands::MountCommand;
use cloudreve_sync::drive::mounts::DriveMode;
use cloudreve_sync::drive::sync::SyncMode;
use common::{TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

async fn upload_sessions_for(env: &TestEnv, name: &str) -> usize {
    env.server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path() == "/api/v4/file/upload")
        .filter(|r| String::from_utf8_lossy(&r.body).contains(name))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_save_right_after_a_download_is_uploaded_and_the_download_itself_is_not() {
    let outer = tempfile::TempDir::new().unwrap();
    let sync = std::fs::canonicalize(outer.path()).unwrap().join("Cloudreve");
    std::fs::create_dir_all(&sync).unwrap();
    let env = TestEnv::started_on(sync.clone(), DriveMode::FullMirror).await;

    let file = remote_file("doc.txt", 9, "etag-1");
    env.set_remote_files(vec![file.clone()]).await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "", "data": file })))
        .mount(&env.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0, "msg": "",
            "data": { "urls": [{ "url": format!("{}/blob/doc.txt", env.server.uri()) }], "expires": "2099-01-01T00:00:00Z" },
        })))
        .mount(&env.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/blob/doc.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"version 1".to_vec()))
        .mount(&env.server)
        .await;

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = env.mount.command_tx.send(MountCommand::Sync {
        local_paths: vec![sync.join("doc.txt")],
        mode: SyncMode::RemoteChanged,
        user_initiated: false,
    });
    let local = sync.join("doc.txt");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while std::fs::read(&local).ok().as_deref() != Some(&b"version 1"[..]) {
        assert!(tokio::time::Instant::now() < deadline, "the download never landed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The drive's own write settles first: it must not come back as an upload.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(upload_sessions_for(&env, "doc.txt").await, 0, "the download was uploaded back");

    // Then the user saves, still within seconds of the download.
    std::fs::write(&local, b"version 2, saved by the user").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while upload_sessions_for(&env, "doc.txt").await == 0 {
        assert!(tokio::time::Instant::now() < deadline, "the user's save was never uploaded");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
