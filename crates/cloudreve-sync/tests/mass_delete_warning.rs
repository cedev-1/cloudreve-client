//! The notification for a mass deletion held back (`drive::propagate`).
//!
//! Its own test binary: the OS notifier is a process-wide singleton and the
//! notification is throttled per drive.

mod common;

use std::time::Duration;

use common::{TestEnv, remote_file};
use tokio::sync::mpsc;

/// Held back silently, the user would never learn why the server still
/// has the files: they are told, with the drive and the number of files,
/// once — not at every sync that holds them back again.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_mass_deletion_tells_the_user_once() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    cloudreve_sync::utils::toast::init_os_notifier(tx);

    let env = TestEnv::new().await;
    let mut remote = Vec::new();
    for i in 0..40 {
        let rel = format!("f{i:02}.txt");
        env.write_local(&rel, b"data");
        env.track_synced(&rel, &format!("e{i}"));
        remote.push(remote_file(&rel, 4, &format!("e{i}")));
    }
    for i in 0..27 {
        std::fs::remove_file(env.local_path(&format!("f{i:02}.txt"))).unwrap();
    }
    env.set_remote_files(remote).await;

    env.full_sync().await.unwrap();

    let (title, body) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("no notification for a held mass deletion")
        .expect("notifier channel closed");
    assert!(title.to_lowercase().contains("delet"), "title: {title}");
    assert!(body.contains("Test Drive"), "must name the drive: {body}");
    assert!(body.contains("27"), "must say how many files: {body}");

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(rx.try_recv().is_err(), "notified again at the next sync");
}
