//! A folder deleted, renamed or moved on the server (web interface, another
//! computer) must follow in a mirror drive, like a single file does.
//!
//! The server sends ONE event for the folder, never one per file inside.
//! Every remote event used to become a download of its path, and a
//! download of a path that is a local folder is skipped: the folder, and
//! everything in it, stayed on this computer for good (the next full sync
//! only forgets what the server lost). Found by the real-server suite
//! (`real_server_sync.rs`).

mod common;

use std::time::Duration;

use cloudreve_sync::drive::remote_events::run_remote_event_loop;
use common::{TestEnv, remote_file};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Resumed subscription (no full sync of its own), then `event`, then a
/// stalled stream so the loop does not spin.
async fn mock_sse_event(server: &MockServer, event: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!("event: resumed\ndata: <nil>\n\nevent: event\ndata: {event}\n\n"),
            "text/event-stream",
        ))
        .up_to_n_times(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("event: keep-alive\n\n", "text/event-stream")
                .set_delay(Duration::from_secs(30)),
        )
        .mount(server)
        .await;
}

/// The server's answer for anything under `prefix`: gone.
async fn mock_gone_under(server: &MockServer, uri: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .and(query_param("uri", uri))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 40016, "msg": "Path not exist", "data": null
        })))
        .mount(server)
        .await;
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folder_deleted_on_the_server_disappears_locally_with_its_content() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;
    env.write_local("photos/2024/beach.jpg", b"beach");
    env.write_local("photos/notes.txt", b"notes");
    env.write_local("keep/other.txt", b"untouched");
    env.track_synced("photos/2024/beach.jpg", "e-beach");
    env.track_synced("photos/notes.txt", "e-notes");
    env.track_synced("keep/other.txt", "e-other");
    mock_gone_under(&env.server, "cloudreve://my/sync/photos/2024/beach.jpg").await;
    mock_gone_under(&env.server, "cloudreve://my/sync/photos/notes.txt").await;
    mock_sse_event(&env.server, json!({ "type": "delete", "file_id": "f1", "from": "/photos", "to": "" })).await;

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::spawn(run_remote_event_loop(mount));

    let photos = env.local_path("photos");
    wait_until("the deleted folder is still here", || !photos.exists()).await;
    assert_eq!(std::fs::read(env.local_path("keep/other.txt")).unwrap(), b"untouched");
}

/// What the server never had (a file added here, not uploaded yet) or what
/// changed here since the last sync is not the server's to delete.
#[tokio::test(flavor = "multi_thread")]
async fn a_folder_deleted_on_the_server_keeps_local_work_the_server_never_saw() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;
    env.write_local("project/synced.txt", b"synced");
    env.track_synced("project/synced.txt", "e-synced");
    env.write_local("project/edited.txt", b"v1");
    env.track_synced("project/edited.txt", "e-edited");
    env.set_local_mtime("project/edited.txt", 1_700_000_000);
    env.write_local("project/edited.txt", b"v2, edited here after the sync");
    env.write_local("project/brand-new.txt", b"not uploaded yet");
    for name in ["synced.txt", "edited.txt", "brand-new.txt"] {
        mock_gone_under(&env.server, &format!("cloudreve://my/sync/project/{name}")).await;
    }
    mock_sse_event(&env.server, json!({ "type": "delete", "file_id": "f3", "from": "/project", "to": "" })).await;

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::spawn(run_remote_event_loop(mount));

    let synced = env.local_path("project/synced.txt");
    wait_until("the synced file was not removed", || !synced.exists()).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(std::fs::read(env.local_path("project/brand-new.txt")).unwrap(), b"not uploaded yet");
    assert_eq!(std::fs::read(env.local_path("project/edited.txt")).unwrap(), b"v2, edited here after the sync");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folder_renamed_on_the_server_leaves_its_old_name_and_is_fetched_under_the_new_one() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![remote_file("report.txt", 6, "e-report")]).await;
    env.write_local("old-name/report.txt", b"report");
    env.track_synced("old-name/report.txt", "e-report");
    env.write_local("old-name/draft.txt", b"local draft");
    mock_gone_under(&env.server, "cloudreve://my/sync/old-name/report.txt").await;
    mock_sse_event(
        &env.server,
        json!({ "type": "rename", "file_id": "f2", "from": "/old-name", "to": "/new-name" }),
    )
    .await;
    let listings_before = env.server.received_requests().await.unwrap_or_default().len();

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::spawn(run_remote_event_loop(mount));

    let old = env.local_path("old-name/report.txt");
    wait_until("the old folder's synced file is still here", || !old.exists()).await;
    // Work the server never saw stays where it was.
    assert_eq!(std::fs::read(env.local_path("old-name/draft.txt")).unwrap(), b"local draft");
    // The new name's content comes from a listing of the drive.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listings = env
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .skip(listings_before)
            .filter(|r| r.method.as_str() == "GET" && r.url.path() == "/api/v4/file")
            .count();
        if listings > 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "the new folder was never listed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
