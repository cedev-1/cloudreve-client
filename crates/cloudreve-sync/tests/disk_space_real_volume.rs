//! Disk space guard exercised against a *real* volume.
//!
//! The other disk space tests use absurd file sizes, which no volume can
//! satisfy — they would still pass if the guard queried the wrong volume, or
//! kept no reserve at all. This one mounts a genuine 2 GB RAM disk and checks
//! the guard reads the free space of the *sync* volume specifically: the file
//! it refuses would fit on the boot volume without trouble.
//!
//! macOS only: creating a filesystem on Linux needs root, which a test suite
//! must not require.
#![cfg(target_os = "macos")]

mod common;

use std::time::Duration;

use cloudreve_sync::inventory::TaskStatus;
use common::{REMOTE_BASE, RamDisk, TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

async fn mock_file_info(env: &TestEnv, name: &str, file: &serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .and(query_param("uri", format!("{REMOTE_BASE}/{name}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": file,
        })))
        .mount(&env.server)
        .await;
}

/// On a real 2 GB volume, a 1.5 GB file must be refused while a small one goes
/// through. The 1.5 GB file would fit on the boot volume, so this only passes
/// if the guard looks at the volume the sync folder actually lives on.
#[tokio::test]
async fn only_files_that_fit_the_sync_volume_are_downloaded() {
    let Some(disk) = RamDisk::new(2048, "CloudreveSyncTest") else {
        eprintln!("skipping: could not create a RAM disk on this machine");
        return;
    };

    let free = cloudreve_sync::utils::disk_space::available_space_for(&disk.mount_point)
        .expect("query test volume");
    assert!(
        (1..4 * 1024 * 1024 * 1024).contains(&free),
        "test volume should report a couple of GB free, got {free} bytes"
    );

    // max_file_size_mb = 0 disables the size limit, so only the disk space
    // guard can refuse anything here.
    let env = TestEnv::with_sync_dir(disk.mount_point.join("sync"), 0).await;

    let big = remote_file("big.bin", 1_500_000_000, "etag-big");
    let small = remote_file("small.txt", 5, "etag-small");
    env.set_remote_files(vec![big.clone(), small.clone()]).await;
    mock_file_info(&env, "big.bin", &big).await;
    mock_file_info(&env, "small.txt", &small).await;

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

    // The 1.5 GB file is refused for lack of room, and nothing is written.
    let mut error = None;
    for _ in 0..100 {
        if let Some(task) = env
            .tasks_of_type("download")
            .into_iter()
            .find(|t| t.local_path.ends_with("big.bin") && t.status == TaskStatus::Failed)
        {
            error = task.error;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let error = error.expect("the oversized download should have failed");
    assert!(
        error.to_lowercase().contains("disk space"),
        "expected a disk space error, got: {error}"
    );
    assert!(
        error.contains(&free.to_string()),
        "the error should quote the sync volume's free space ({free}), got: {error}"
    );
    assert!(
        !env.local_path("big.bin").exists(),
        "nothing must be written for a file that does not fit"
    );

    // Positive control: a small file on the same volume is not blocked by the
    // guard, proving it discriminates on size rather than refusing everything.
    let small_error = env
        .tasks_of_type("download")
        .into_iter()
        .find(|t| t.local_path.ends_with("small.txt"))
        .and_then(|t| t.error)
        .unwrap_or_default();
    assert!(
        !small_error.to_lowercase().contains("disk space"),
        "a small file must not be refused for disk space, got: {small_error}"
    );
}
