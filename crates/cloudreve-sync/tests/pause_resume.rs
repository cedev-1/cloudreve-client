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

/// Starting the SSE worker while one is already running must not leave two of
/// them alive.
///
/// `resume_drive` re-spawns the workers unconditionally, so a double resume
/// (two clicks, or a resume on an already-running drive) goes through
/// `spawn_remote_event_processor` twice. The mount only keeps one `JoinHandle`,
/// and dropping the previous one DETACHES the task instead of aborting it — so
/// the first worker survives, invisible and unstoppable: every remote event is
/// then handled twice, and `pause()` no longer stops the traffic because it can
/// only abort the handle it still holds.
///
/// The observable here is exactly that harm: after pausing, the SSE endpoint
/// must stop being polled.
#[tokio::test]
async fn starting_the_sse_worker_twice_leaves_only_one_running() {
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    // An SSE stream that ends as soon as it is read: the event loop treats it
    // as a dropped connection and reconnects immediately, so a live worker
    // keeps hitting the endpoint. The delay just keeps the loop from spinning
    // hard enough to drown the mock server.
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_raw("", "text/event-stream")
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&env.server)
        .await;

    let sse_requests = || async {
        env.server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.url.path() == "/api/v4/file/events")
            .count()
    };

    let mount = env.mount.clone();
    mount.spawn_remote_event_processor(mount.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    mount.spawn_remote_event_processor(mount.clone()).await;

    // Pause aborts the handle the mount is holding. Anything still polling the
    // endpoint after this is a worker nobody can reach anymore.
    tokio::time::sleep(Duration::from_millis(200)).await;
    env.mount.pause().await;

    // Let any request already in flight when the abort landed finish.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_pause = sse_requests().await;
    // Without this the assertion below would also hold for a worker that never
    // started: nothing polled, nothing to stop, counts equal at zero.
    assert!(
        after_pause > 0,
        "the SSE worker never reached the endpoint: the test proves nothing"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let later = sse_requests().await;

    assert_eq!(
        later, after_pause,
        "an orphaned SSE worker survived: the endpoint was polled {} more times \
         after the drive was paused",
        later - after_pause
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
