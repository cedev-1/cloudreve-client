//! A server (or anyone able to answer in its place) must never be able to make
//! the client write, overwrite or delete a file outside the sync folder.
//!
//! Paths reach the client from the server in two places: the `from`/`to` of
//! SSE file events, and the `path` of every entry in a folder listing. Both
//! are turned into local paths. A `..` segment, or a name hiding one behind an
//! encoded `/`, used to survive that translation, so `sync/../x` resolved
//! next to the sync folder — e.g. `~/Library/LaunchAgents` for a sync folder
//! in the home directory.
//!
//! Each test plants (or targets) a file NEXT TO the sync folder and drives the
//! production entry points: the SSE event loop and `full_sync`.

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use cloudreve_sync::drive::remote_events::run_remote_event_loop;
use common::{REMOTE_BASE, TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Serve `body` once on the SSE endpoint, then stall on reconnection.
async fn mock_sse(server: &MockServer, body: String) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_raw(body, "text/event-stream"),
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

/// Every file the server is asked about exists, and downloads as `payload`.
async fn mock_downloadable(server: &MockServer, payload: &'static [u8]) {
    let info = remote_file("payload.txt", payload.len() as i64, "etag-evil");
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0, "msg": "", "data": info,
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": {
                "urls": [{ "url": format!("{}/blob/payload", server.uri()) }],
                "expires": "2099-01-01T00:00:00Z",
            },
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/blob/payload"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(server)
        .await;
}

/// Every file the server is asked about is gone (the "remote deletion" answer
/// that makes the download task remove the local copy).
async fn mock_everything_gone(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 40016, "msg": "not found", "data": null,
        })))
        .mount(server)
        .await;
}

/// The folder holding the sync folder: where `sync/..` lands.
fn outside(env: &TestEnv) -> PathBuf {
    env.sync_dir.parent().unwrap().to_path_buf()
}

async fn start_event_loop(env: &TestEnv) {
    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::spawn(run_remote_event_loop(mount));
}

/// Waits long enough for an event to go through the command processor, the
/// task queue and a download, polling `done` to return early.
async fn settle(done: impl Fn() -> bool) {
    for _ in 0..40 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn read(path: &Path) -> Option<Vec<u8>> {
    std::fs::read(path).ok()
}

/// Tasks queued for a path that leaves the sync folder. Asserting none were
/// queued pins the first line of defense (the path translation) on its own,
/// independently of the download task's own guard.
fn escaping_tasks(env: &TestEnv) -> Vec<String> {
    env.all_tasks().into_iter().map(|t| t.local_path).filter(|p| p.contains("..")).collect()
}

#[tokio::test]
async fn a_create_event_climbing_out_of_the_sync_folder_writes_nothing_outside() {
    let env = TestEnv::new().await;
    mock_downloadable(&env.server, b"planted").await;
    mock_sse(
        &env.server,
        r#"event: event
data: [{"type":"create","file_id":"f1","from":"/../planted.txt"}]

"#
        .into(),
    )
    .await;
    let target = outside(&env).join("planted.txt");

    start_event_loop(&env).await;
    settle(|| target.exists()).await;
    // Leave time for a download that would land late.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(!target.exists(), "a server event planted {}", target.display());
    assert!(env.tasks_of_type("download").is_empty(), "no download may even be queued for it");
}

#[tokio::test]
async fn a_create_event_with_a_deep_climb_writes_nothing_outside() {
    let env = TestEnv::new().await;
    mock_downloadable(&env.server, b"planted").await;
    // Climbs out, then back down into a sibling folder: the shape of
    // `/../../Library/LaunchAgents/x.plist`.
    std::fs::create_dir_all(outside(&env).join("LaunchAgents")).unwrap();
    mock_sse(
        &env.server,
        r#"event: event
data: [{"type":"modify","file_id":"f1","from":"/a/../../LaunchAgents/evil.plist"}]

"#
        .into(),
    )
    .await;
    let target = outside(&env).join("LaunchAgents/evil.plist");

    start_event_loop(&env).await;
    settle(|| target.exists()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(!target.exists(), "a server event planted {}", target.display());
    assert!(escaping_tasks(&env).is_empty(), "queued: {:?}", escaping_tasks(&env));
}

#[tokio::test]
async fn a_delete_event_climbing_out_of_the_sync_folder_deletes_nothing_outside() {
    let env = TestEnv::new().await;
    mock_everything_gone(&env.server).await;
    let victim = outside(&env).join("notes.txt");
    std::fs::write(&victim, b"my own notes").unwrap();
    mock_sse(
        &env.server,
        r#"event: event
data: [{"type":"delete","file_id":"f1","from":"/../notes.txt"}]

"#
        .into(),
    )
    .await;

    start_event_loop(&env).await;
    settle(|| !victim.exists()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(read(&victim).as_deref(), Some(&b"my own notes"[..]), "a server event deleted a file outside the sync folder");
    assert!(escaping_tasks(&env).is_empty(), "queued: {:?}", escaping_tasks(&env));
}

#[tokio::test]
async fn a_rename_event_from_outside_the_sync_folder_deletes_nothing_outside() {
    let env = TestEnv::new().await;
    mock_everything_gone(&env.server).await;
    let victim = outside(&env).join("notes.txt");
    std::fs::write(&victim, b"my own notes").unwrap();
    mock_sse(
        &env.server,
        r#"event: event
data: [{"type":"rename","file_id":"f1","from":"/../notes.txt","to":"/notes.txt"}]

"#
        .into(),
    )
    .await;

    start_event_loop(&env).await;
    settle(|| !victim.exists()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(read(&victim).as_deref(), Some(&b"my own notes"[..]), "a server event deleted a file outside the sync folder");
    assert!(escaping_tasks(&env).is_empty(), "queued: {:?}", escaping_tasks(&env));
}

#[tokio::test]
async fn a_rename_event_to_outside_the_sync_folder_writes_nothing_outside() {
    let env = TestEnv::new().await;
    mock_downloadable(&env.server, b"planted").await;
    mock_sse(
        &env.server,
        r#"event: event
data: [{"type":"rename","file_id":"f1","from":"/report.txt","to":"/../planted.txt"}]

"#
        .into(),
    )
    .await;
    let target = outside(&env).join("planted.txt");

    start_event_loop(&env).await;
    settle(|| target.exists()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(!target.exists(), "a server event planted {}", target.display());
    assert!(escaping_tasks(&env).is_empty(), "queued: {:?}", escaping_tasks(&env));
}

/// A legitimate event still goes through: the guard must not just drop
/// everything.
#[tokio::test]
async fn an_ordinary_create_event_still_downloads_the_file() {
    let env = TestEnv::new().await;
    mock_downloadable(&env.server, b"hello").await;
    mock_sse(
        &env.server,
        r#"event: event
data: [{"type":"create","file_id":"f1","from":"/docs/report.txt"}]

"#
        .into(),
    )
    .await;
    let target = env.local_path("docs/report.txt");

    start_event_loop(&env).await;
    settle(|| target.exists()).await;

    assert_eq!(read(&target).as_deref(), Some(&b"hello"[..]));
}

/// A listing entry whose name hides `..` behind encoded slashes — one path
/// segment for the server, three for the local filesystem.
#[tokio::test]
async fn a_listed_name_hiding_a_climb_writes_nothing_outside() {
    let env = TestEnv::new().await;
    let mut evil = remote_file("x", 7, "etag-evil");
    evil["name"] = json!("../planted.txt");
    evil["path"] = json!(format!("{REMOTE_BASE}/..%2Fplanted.txt"));
    let fine = remote_file("fine.txt", 7, "etag-fine");
    env.set_remote_files(vec![evil, fine]).await;
    mock_downloadable(&env.server, b"planted").await;
    let target = outside(&env).join("planted.txt");

    env.full_sync().await.expect("full sync");
    settle(|| target.exists() && env.local_path("fine.txt").exists()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(!target.exists(), "a listed name planted {}", target.display());
    assert!(escaping_tasks(&env).is_empty(), "queued: {:?}", escaping_tasks(&env));
    assert!(env.local_path("fine.txt").exists(), "the legitimate file must still sync");
}

/// The last line of defense: whatever produced it, a download task whose
/// path is outside the sync folder never touches the disk.
#[tokio::test]
async fn a_download_task_outside_the_sync_folder_is_refused() {
    let env = TestEnv::new().await;
    mock_everything_gone(&env.server).await;
    let victim = outside(&env).join("notes.txt");
    std::fs::write(&victim, b"my own notes").unwrap();

    env.mount
        .task_queue
        .enqueue(cloudreve_sync::tasks::TaskPayload::download(env.sync_dir.join("../notes.txt")))
        .await
        .unwrap();
    settle(|| !victim.exists()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(read(&victim).as_deref(), Some(&b"my own notes"[..]));
}
