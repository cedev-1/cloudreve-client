//! Behavior test for the user-facing low-disk-space warning.
//!
//! Lives in its own test binary because the OS notifier is a process-wide
//! singleton and the warning is throttled per drive: another test firing it
//! first would make the assertions here meaningless.

mod common;

use std::time::Duration;

use common::{TestEnv, remote_file};
use serde_json::json;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// A sync that cannot fit on the volume must notify the user up front instead
/// of letting files fail one by one with no explanation — and only once, not
/// on every periodic sync.
#[tokio::test]
async fn a_sync_that_cannot_fit_warns_the_user_once() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    cloudreve_sync::utils::toast::init_os_notifier(tx);

    // max_file_size_mb = 0 disables the size limit, so nothing else filters
    // these files out before the disk space checks run.
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

    let (title, body) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for a low disk space notification")
        .expect("notifier channel closed");
    assert!(
        title.to_lowercase().contains("disk space"),
        "notification should be about disk space, got title: {title}"
    );
    assert!(
        body.contains("Test Drive"),
        "notification should name the drive, got body: {body}"
    );

    // Syncs run on a timer; the warning must not fire again on every pass.
    env.full_sync().await.expect("second full sync");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        rx.try_recv().is_err(),
        "the low disk space warning must be throttled, not repeated every sync"
    );
}
