//! Behavioral tests for pause/resume: a paused drive must not sync,
//! and resuming must restore normal operation.

mod common;

use common::{remote_file, TestEnv};

/// A paused drive must ignore full sync commands — no downloads enqueued.
#[tokio::test]
async fn paused_drive_ignores_full_sync() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 5, "etag-1")]).await;

    // Pause the drive
    env.mount.pause().await;

    // Full sync should be a no-op while paused
    env.full_sync().await.unwrap();

    assert!(
        env.tasks_of_type("download").is_empty(),
        "a paused drive must not enqueue downloads"
    );
}

/// Resuming a paused drive must allow syncing again.
#[tokio::test]
async fn resumed_drive_syncs_normally() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 5, "etag-1")]).await;

    // Pause then resume
    env.mount.pause().await;
    env.mount.resume().await;

    // Full sync should work after resume
    env.full_sync().await.unwrap();

    assert!(
        !env.tasks_of_type("download").is_empty(),
        "a resumed drive must enqueue downloads"
    );
}

/// Pausing must be idempotent — pausing twice must not panic or break state.
#[tokio::test]
async fn pause_is_idempotent() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![remote_file("hello.txt", 5, "etag-1")]).await;

    env.mount.pause().await;
    env.mount.pause().await; // second pause must not panic

    env.full_sync().await.unwrap();
    assert!(
        env.tasks_of_type("download").is_empty(),
        "double-paused drive must not sync"
    );
}

/// When paused, FullSync commands sent through the command channel must be
/// ignored by the command processor.
#[tokio::test]
async fn paused_drive_ignores_full_sync_command() {
    use std::time::Duration;
    use cloudreve_sync::drive::commands::MountCommand;

    let env = TestEnv::new().await;
    env.set_remote_files(vec![remote_file("doc.txt", 10, "etag-cmd")]).await;

    // Start the command processor
    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;

    // Pause, then send FullSync via the command channel
    env.mount.pause().await;
    let _ = env.mount.command_tx.send(MountCommand::FullSync);

    // Give the command processor time to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        env.tasks_of_type("download").is_empty(),
        "FullSync command must be ignored when paused"
    );
}

/// The `is_paused()` accessor must reflect current state.
#[tokio::test]
async fn is_paused_reflects_state() {
    let env = TestEnv::new().await;

    assert!(!env.mount.is_paused(), "new mount should not be paused");

    env.mount.pause().await;
    assert!(env.mount.is_paused(), "mount should be paused after pause()");

    env.mount.resume().await;
    assert!(!env.mount.is_paused(), "mount should not be paused after resume()");
}
