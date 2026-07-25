//! Behavior tests for the disk space guard.
//!
//! The client mirrors the whole remote drive locally. Without a guard, a remote
//! bigger than the local volume fills the disk until the machine breaks. A
//! download that cannot fit must fail loudly instead of being attempted.

mod common;

use std::time::Duration;

use cloudreve_sync::inventory::TaskStatus;
use common::{TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

async fn wait_for_failed_download(env: &TestEnv) -> String {
    for _ in 0..100 {
        if let Some(task) = env
            .tasks_of_type("download")
            .into_iter()
            .find(|t| t.status == TaskStatus::Failed)
        {
            return task.error.unwrap_or_default();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("expected a failed download, got: {:?}", env.tasks_of_type("download"));
}

/// A remote file larger than the free space on the sync volume must never be
/// downloaded: the task fails with a disk-space error and nothing is written.
#[tokio::test]
async fn download_is_refused_when_the_file_cannot_fit_on_the_volume() {
    // max_file_size_mb = 0 disables the size limit, so only the disk space
    // guard can stop this download.
    let env = TestEnv::with_max_file_size(0).await;

    let huge = remote_file("huge.bin", i64::MAX, "etag-huge");
    env.set_remote_files(vec![huge.clone()]).await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": huge,
        })))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    let error = wait_for_failed_download(&env).await;
    assert!(
        error.to_lowercase().contains("disk space"),
        "download should fail with a disk space error, got: {error}"
    );
    assert!(
        !env.local_path("huge.bin").exists(),
        "no file must be written when there is not enough space"
    );
}

/// The guard must not get in the way of ordinary files that comfortably fit.
#[tokio::test]
async fn download_proceeds_when_the_file_fits_on_the_volume() {
    let env = TestEnv::with_max_file_size(0).await;

    let small = remote_file("small.txt", 5, "etag-small");
    env.set_remote_files(vec![small.clone()]).await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": small,
        })))
        .mount(&env.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": {
                "urls": [{ "url": format!("{}/blob/small.txt", env.server.uri()) }],
                "expires": "2099-01-01T00:00:00Z",
            },
        })))
        .mount(&env.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/blob/small.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    for _ in 0..100 {
        if env.local_path("small.txt").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        std::fs::read(env.local_path("small.txt")).expect("downloaded file"),
        b"hello",
        "a file that fits must download normally"
    );
}
