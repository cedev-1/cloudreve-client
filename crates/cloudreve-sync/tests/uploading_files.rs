//! Behavior tests for remote files still being uploaded.
//!
//! While an upload is in progress (or was abandoned), the server already
//! lists the file but no downloadable entity exists yet: the listing entry
//! carries the `sys:upload_session_id` metadata. Trying to download such a
//! file can only fail ("Entity not exist") — the sync must skip it.

mod common;

use common::{TestEnv, remote_file};
use serde_json::json;

/// A remote file with an active upload session must not be downloaded;
/// a regular remote file next to it must still be.
#[tokio::test]
async fn files_still_uploading_are_not_downloaded() {
    let env = TestEnv::new().await;

    let mut uploading = remote_file("uploading.bin", 10, "etag-up");
    uploading["metadata"] = json!({ "sys:upload_session_id": "session-1" });

    env.set_remote_files(vec![uploading, remote_file("ready.bin", 10, "etag-ok")])
        .await;

    env.full_sync().await.expect("full sync");

    let downloads = env.tasks_of_type("download");
    assert_eq!(
        downloads.len(),
        1,
        "only the fully uploaded file must be downloaded, got: {downloads:?}"
    );
    assert!(
        downloads[0].local_path.ends_with("ready.bin"),
        "the enqueued download must be ready.bin, got: {}",
        downloads[0].local_path
    );
}
