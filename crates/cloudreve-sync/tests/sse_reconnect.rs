//! Behavior tests for SSE reconnection: when the server *resumes* a
//! subscription (buffered events replayed via Client-Id), nothing was missed
//! and no full sync is needed. Only a *new* subscription (server-side buffer
//! lost) justifies a full sync.
//!
//! This matters behind proxies like Cloudflare that cut idle SSE connections
//! every couple of minutes: each cut must not trigger a full drive re-listing.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

/// When a proxy silently drops the connection (half-open socket, no FIN/RST),
/// the SSE stream stalls — `chunk().await` blocks indefinitely. An idle
/// timeout must detect this and trigger a reconnection.
#[tokio::test]
async fn stalled_sse_connection_triggers_reconnect() {
    use cloudreve_sync::drive::mounts::{Credentials, DriveConfig, Mount};
    use cloudreve_sync::{EventBroadcaster, SummaryNotifier};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let sse_connections = Arc::new(AtomicUsize::new(0));
    let sse_count = sse_connections.clone();

    // Raw TCP server: sends SSE headers + initial event, then stalls.
    // Wiremock can't do partial responses, so we need this to simulate a
    // half-open socket where data stops arriving mid-stream.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let count = sse_count.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);

                if request.contains("/file/events") {
                    count.fetch_add(1, Ordering::SeqCst);
                    let resp = "HTTP/1.1 200 OK\r\n\
                                Content-Type: text/event-stream\r\n\
                                \r\n\
                                event: resumed\ndata: <nil>\n\n";
                    let _ = socket.write_all(resp.as_bytes()).await;
                    let _ = socket.flush().await;
                    // Simulate half-open socket: connection stays open, no more data
                    tokio::time::sleep(Duration::from_secs(300)).await;
                } else if request.contains("/file") {
                    let body = r#"{"code":0,"msg":"","data":{"files":[],"pagination":{"page":1,"page_size":500,"total_items":0},"props":{"max_page_size":10000}}}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(), body
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                } else {
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await;
                }
            });
        }
    });

    // Build a Mount pointing to the raw TCP server with a short idle timeout.
    let tmp = tempfile::TempDir::new().unwrap();
    let sync_dir = tmp.path().join("sync");
    std::fs::create_dir_all(&sync_dir).unwrap();
    let inventory = Arc::new(
        cloudreve_sync::inventory::InventoryDb::with_path(tmp.path().join("meta.db")).unwrap(),
    );
    let (manager_tx, _manager_rx) = tokio::sync::mpsc::unbounded_channel();
    let notifier = Arc::new(SummaryNotifier::new(Arc::new(EventBroadcaster::new(16))));
    let config = DriveConfig {
        id: uuid::Uuid::new_v4().to_string(),
        name: "Test".into(),
        instance_url: format!("http://127.0.0.1:{port}"),
        remote_path: "cloudreve://my/sync".into(),
        credentials: Credentials {
            access_token: Some("test".into()),
            refresh_token: "test".into(),
            refresh_expires: "2099-01-01T00:00:00Z".into(),
            access_expires: Some("2099-01-01T00:00:00Z".into()),
        },
        sync_path: sync_dir,
        enabled: true,
        user_id: "test".into(),
        sse_client_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    };

    let mount = Arc::new(Mount::new(config, inventory, manager_tx, notifier).await);
    mount
        .sse_idle_timeout_secs
        .store(2, Ordering::Relaxed);
    mount.spawn_command_processor(mount.clone()).await;
    tokio::spawn(run_remote_event_loop(mount));

    // With a 2s idle timeout, the first connection stalls after the initial
    // event, the timeout fires, and the loop reconnects. We expect ≥2
    // SSE connections within 8 seconds.
    for _ in 0..80 {
        if sse_connections.load(Ordering::SeqCst) >= 2 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "expected ≥2 SSE connection attempts (stall detection), got {}",
        sse_connections.load(Ordering::SeqCst)
    );
}
