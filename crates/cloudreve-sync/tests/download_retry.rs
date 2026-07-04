//! Behavior tests for download retry policy.
//!
//! When a download fails permanently (e.g. the server lists a file whose blob
//! no longer exists — "Entity not exist"), re-running a full sync must not
//! enqueue the same doomed download again: it would fail again and spam the
//! task list. A retry only makes sense once the remote file changed (new etag).

mod common;

use std::time::Duration;

use cloudreve_sync::inventory::TaskStatus;
use common::{TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mock the endpoints hit by a download task for a broken remote file:
/// file info resolves fine, but the download URL fails with 40081
/// ("Batch operation not fully completed: Entity not exist").
async fn mock_broken_entity(server: &MockServer, name: &str, size: i64, etag: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": remote_file(name, size, etag),
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 40081,
            "msg": "Batch operation not fully completed: Entity not exist",
        })))
        .mount(server)
        .await;
}

async fn wait_for_failed_downloads(env: &TestEnv, expected: usize) {
    for _ in 0..100 {
        let failed = env
            .tasks_of_type("download")
            .into_iter()
            .filter(|t| t.status == TaskStatus::Failed)
            .count();
        if failed == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "expected {expected} failed download task(s), got: {:?}",
        env.tasks_of_type("download")
    );
}

/// A download that failed permanently for a given remote version (etag) must
/// not be re-enqueued by subsequent full syncs — only a *changed* remote file
/// (new etag) justifies a fresh attempt.
#[tokio::test]
async fn permanently_failed_download_is_not_retried_until_remote_changes() {
    let env = TestEnv::new().await;

    // Remote lists a file whose blob is gone server-side.
    env.set_remote_files(vec![remote_file("broken.png", 5, "etag-1")]).await;
    mock_broken_entity(&env.server, "broken.png", 5, "etag-1").await;

    env.full_sync().await.expect("first full sync");
    wait_for_failed_downloads(&env, 1).await;

    // Nothing changed remotely: a second full sync must not enqueue the same
    // doomed download again.
    env.full_sync().await.expect("second full sync");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        env.tasks_of_type("download").len(),
        1,
        "an unchanged permanently-failed download must not be re-enqueued"
    );

    // The remote file changed (new etag): now a retry is justified.
    env.set_remote_files(vec![remote_file("broken.png", 5, "etag-2")]).await;
    mock_broken_entity(&env.server, "broken.png", 5, "etag-2").await;

    env.full_sync().await.expect("third full sync");
    wait_for_failed_downloads(&env, 2).await;
    assert_eq!(
        env.tasks_of_type("download").len(),
        2,
        "a changed remote file must be retried after a permanent failure"
    );
}
