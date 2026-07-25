//! Downloads must work when the sync folder is on its own volume.
//!
//! Syncing to an external drive, a second SSD or a separate /home partition is
//! ordinary usage. Downloads staged through the system temp directory cannot be
//! moved into place across a filesystem boundary, so this has to be exercised
//! on a genuinely separate volume — a shared temp dir would hide the problem.
//!
//! macOS only: creating a filesystem on Linux needs root, which a test suite
//! must not require.
#![cfg(target_os = "macos")]

mod common;

use std::time::Duration;

use cloudreve_sync::inventory::TaskStatus;
use common::{RamDisk, TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// A file downloaded onto a volume other than the one holding the system temp
/// directory must land intact at its destination.
#[tokio::test]
async fn a_file_downloads_onto_a_volume_of_its_own() {
    let Some(disk) = RamDisk::new(2048, "CloudreveXDevTest") else {
        eprintln!("skipping: could not create a RAM disk on this machine");
        return;
    };
    assert_ne!(
        std::env::temp_dir().to_string_lossy().starts_with("/Volumes/CloudreveXDevTest"),
        true,
        "the test volume must differ from the system temp volume"
    );

    let env = TestEnv::with_sync_dir(disk.mount_point.join("sync"), 0).await;

    let file = remote_file("report.txt", 5, "etag-1");
    env.set_remote_files(vec![file.clone()]).await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0, "msg": "", "data": file,
        })))
        .mount(&env.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": {
                "urls": [{ "url": format!("{}/blob/report.txt", env.server.uri()) }],
                "expires": "2099-01-01T00:00:00Z",
            },
        })))
        .mount(&env.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/blob/report.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    let downloaded = env.local_path("report.txt");
    for _ in 0..100 {
        if downloaded.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let error = env
        .tasks_of_type("download")
        .into_iter()
        .find(|t| t.status == TaskStatus::Failed)
        .and_then(|t| t.error);
    assert!(error.is_none(), "download should not have failed: {error:?}");
    assert_eq!(
        std::fs::read(&downloaded).expect("file should exist on the sync volume"),
        b"hello"
    );
}
