//! `Vfs::shutdown`: once a drive is deleted (or the app quits), its vfs
//! must stop talking to the server for good — no debounced upload firing
//! later, no parked-retry loop re-running with the deleted drive's tokens,
//! no upload in flight left running.

mod common;

use std::time::Duration;

use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use cloudreve_vfs::writeback::{DraftState, UPLOAD_RETRIES};
use common::{remote_file, VfsTestEnv};

async fn request_count(env: &VfsTestEnv) -> usize {
    env.server.received_requests().await.map(|r| r.len()).unwrap_or(0)
}

/// A save closed just before the drive is deleted: its debounce timer is
/// still armed. After shutdown it must never fire.
#[tokio::test]
async fn an_armed_upload_never_fires_after_shutdown() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(200));

    let root = vfs.tree().root();
    let (_node, h) = vfs.create(root, "notes.txt").await.unwrap();
    vfs.write(h, 0, b"written just before the drive was removed").await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.shutdown();
    let before = request_count(&env).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(env.upload_session_count(), 0, "the armed upload fired after shutdown");
    assert_eq!(request_count(&env).await, before, "no request may reach the server after shutdown");
    tokio::time::timeout(Duration::from_secs(2), vfs.wait_for_writeback_idle())
        .await
        .expect("shutdown must leave the queue idle");
}

/// The deleted-drive bug: a draft parked after failed attempts used to be
/// retried every 30 s ... 10 min forever with the deleted drive's tokens.
#[tokio::test]
async fn a_parked_retry_never_runs_after_shutdown() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("report.txt", 5, "e1")]).await;
    env.serve_file_content("report.txt", b"abcde").await;
    env.fail_next_upload_sessions(UPLOAD_RETRIES as usize);

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    vfs.set_retry_backoff_for_tests([Duration::from_millis(5), Duration::from_millis(5)]);
    vfs.set_parked_retry_delay_for_tests(Duration::from_millis(300));

    let root = vfs.tree().root();
    let node = vfs.tree().lookup(root, "report.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.write(h, 0, b"final version").await.unwrap();
    vfs.close(h).await.unwrap();
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.upload_session_count(), UPLOAD_RETRIES as usize, "precondition: every attempt failed");

    vfs.shutdown();
    let before = request_count(&env).await;
    tokio::time::sleep(Duration::from_millis(900)).await;

    assert_eq!(env.upload_session_count(), UPLOAD_RETRIES as usize, "the parked retry ran after shutdown");
    assert_eq!(request_count(&env).await, before, "no request may reach the server after shutdown");
    assert_eq!(vfs.retry_pending_uploads().await, 0, "a reconnect after shutdown must queue nothing");
}

/// An upload under way when the drive is deleted stops; its draft stays on
/// disk (re-uploaded at the next launch if the drive still exists).
#[tokio::test]
async fn an_upload_in_flight_stops_at_shutdown() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![]).await;
    env.slow_down_chunk_uploads(Duration::from_millis(150));

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(10));

    let root = vfs.tree().root();
    let (_node, h) = vfs.create(root, "big.txt").await.unwrap();
    // 60 bytes over the mock's 5-byte chunks: 12 chunks, ~1.8 s of upload.
    vfs.write(h, 0, &[b'x'; 60]).await.unwrap();
    vfs.close(h).await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while vfs.draft_state_for_tests(&common::uri_of("big.txt")).await != Some(DraftState::Uploading) {
        assert!(std::time::Instant::now() < deadline, "the upload never started");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    vfs.shutdown();
    // Let a chunk POST already on the wire land.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = request_count(&env).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(request_count(&env).await, before, "the upload kept sending chunks after shutdown");
    assert_ne!(
        env.uploaded_content("big.txt").map(|c| c.len()),
        Some(60),
        "precondition: the upload was cut short"
    );
    assert_eq!(vfs.unsynced_file_count().await, 1, "the draft must survive for the next launch");
    tokio::time::timeout(Duration::from_secs(2), vfs.wait_for_writeback_idle())
        .await
        .expect("shutdown must leave the queue idle");
}

/// Shutdown twice (Mount::delete runs Mount::shutdown first) is harmless.
#[tokio::test]
async fn shutdown_is_idempotent() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.shutdown();
    vfs.shutdown();
    vfs.wait_for_writeback_idle().await;
}
