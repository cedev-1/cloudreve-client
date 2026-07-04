//! Behavior tests for SSE reconnection: when the server *resumes* a
//! subscription (buffered events replayed via Client-Id), nothing was missed
//! and no full sync is needed. Only a *new* subscription (server-side buffer
//! lost) justifies a full sync.
//!
//! This matters behind proxies like Cloudflare that cut idle SSE connections
//! every couple of minutes: each cut must not trigger a full drive re-listing.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::remote_events::run_remote_event_loop;
use common::TestEnv;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Serve the SSE endpoint: the first connection gets `event_name`, then the
/// stream ends; later reconnections stall for a long time so the retry loop
/// doesn't spin during the test.
async fn mock_sse(server: &MockServer, event_name: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_raw(
                    format!("event: {event_name}\ndata: <nil>\n\n"),
                    "text/event-stream",
                ),
        )
        .up_to_n_times(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_raw("event: keep-alive\n\n", "text/event-stream")
                .set_delay(Duration::from_secs(30)),
        )
        .mount(server)
        .await;
}

/// Count full-sync listing requests (`GET /api/v4/file`) seen by the server.
async fn listing_requests(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.method.as_str() == "GET" && r.url.path() == "/api/v4/file")
        .count()
}

/// Build the env, wire the SSE mock and start the command processor and the
/// remote event loop, as `Mount::start` would in production.
async fn start_with_sse(event_name: &str) -> TestEnv {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;
    mock_sse(&env.server, event_name).await;

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::spawn(run_remote_event_loop(mount));
    env
}

/// A resumed subscription means the server replayed every missed event:
/// re-listing the whole drive would be pure waste (and happens on every
/// proxy-induced reconnect).
#[tokio::test]
async fn resumed_subscription_does_not_trigger_full_sync() {
    let env = start_with_sse("resumed").await;

    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        listing_requests(&env.server).await,
        0,
        "a resumed SSE subscription must not trigger a full sync"
    );
}

/// A brand-new subscription means the server-side event buffer is gone:
/// events may have been missed, so a full sync is required.
#[tokio::test]
async fn new_subscription_triggers_full_sync() {
    let env = start_with_sse("subscribed").await;

    for _ in 0..40 {
        if listing_requests(&env.server).await >= 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("a new SSE subscription must trigger a full sync");
}
