//! A save on an on-demand drive that the server keeps refusing must not
//! wait silently forever as "waiting to upload": the drive reports why, in
//! words the user can act on, and notifies once (rate-limited per drive).
//!
//! Own binary: asserts on notifications, which go through a process-wide
//! notifier.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use common::TestEnv;
use serde_json::json;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

#[tokio::test]
async fn a_full_account_is_explained_and_notified_once() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    cloudreve_sync::utils::toast::init_os_notifier(tx);

    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    env.set_remote_files(vec![]).await;
    // What Cloudreve answers when the account has no room left.
    Mock::given(method("PUT"))
        .and(path("/api/v4/file/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 40050, "msg": "Insufficient capacity",
        })))
        .mount(&env.server)
        .await;

    let vfs = env.mount.vfs.lock().await.clone().expect("on-demand vfs");
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    vfs.set_retry_backoff_for_tests([Duration::from_millis(5), Duration::from_millis(5)]);
    vfs.set_parked_retry_delay_for_tests(Duration::from_millis(100));
    env.mount.spawn_vfs_event_pump().await;

    let root = vfs.tree().root();
    let (_node, h) = vfs.create(root, "report.pdf").await.unwrap();
    vfs.write(h, 0, b"quarterly numbers").await.unwrap();
    vfs.close(h).await.unwrap();

    let mut problem = None;
    for _ in 0..100 {
        problem = env.mount.upload_problem();
        if problem.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let problem = problem.expect("the drive never reported why its upload keeps failing");
    assert!(problem.to_lowercase().contains("full"), "the reason is the full account: {problem}");
    assert!(problem.contains("Test Drive"), "names the drive: {problem}");

    // Other notifications (a small cache on a crowded test machine) may
    // come first: look for the upload one.
    let (title, body) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (title, body) = rx.recv().await.expect("notifier closed");
            if title.to_lowercase().contains("upload") {
                return (title, body);
            }
        }
    })
    .await
    .expect("no upload notification");
    assert!(title.to_lowercase().contains("upload"), "{title}");
    assert_eq!(body, problem, "the notification says the same thing as the popup");
    assert!(!body.contains("40050") && !body.contains("API error"), "no raw error text: {body}");

    // Parked retries keep failing the same way: no second notification.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut extra = Vec::new();
    while let Ok(n) = rx.try_recv() {
        extra.push(n);
    }
    assert!(extra.iter().all(|(t, _)| !t.to_lowercase().contains("upload")), "notified again: {extra:?}");
    vfs.shutdown();
}
