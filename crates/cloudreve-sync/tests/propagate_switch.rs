//! "Delete and rename in the cloud too" is a per-drive choice
//! (`DriveConfig::propagate_deletes`). Off, a mirror drive never touches the
//! server for a local delete or rename — what every drive did before the
//! switch existed, and what an existing drive keeps after an upgrade. On,
//! `drive::propagate` applies. The switch is read live: no restart.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::{DriveConfig, DriveMode};
use cloudreve_sync::inventory::NewTaskRecord;
use common::{REMOTE_BASE, TestEnv, remote_file};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ok(data: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "", "data": data }))
}

async fn mock_info(server: &MockServer, rel: &str, file: Value) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .and(query_param("uri", format!("{REMOTE_BASE}/{rel}").as_str()))
        .respond_with(ok(file))
        .mount(server)
        .await;
}

async fn accept_writes(server: &MockServer) {
    Mock::given(method("DELETE")).and(path("/api/v4/file")).respond_with(ok(Value::Null)).mount(server).await;
    Mock::given(method("POST")).and(path("/api/v4/file/move")).respond_with(ok(Value::Null)).mount(server).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/rename"))
        .respond_with(ok(remote_file("renamed", 0, "e")))
        .mount(server)
        .await;
}

/// Every write request the server got: deletes, moves, renames.
async fn server_writes(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| {
            (r.method.as_str() == "DELETE" && r.url.path() == "/api/v4/file")
                || (r.method.as_str() == "POST" && ["/api/v4/file/move", "/api/v4/file/rename"].contains(&r.url.path()))
        })
        .map(|r| format!("{} {}", r.method, r.url.path()))
        .collect()
}

async fn wait_for<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !cond().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A drive saved before the switch existed has no such key: it loads with
/// the switch off, so an upgrade never starts deleting on the server.
#[test]
fn a_drive_saved_before_the_switch_existed_keeps_the_server_untouched() {
    let saved = json!({
        "id": "d1", "name": "Work", "instance_url": "https://cloud.example.com",
        "remote_path": "cloudreve://my", "sync_path": "/Users/me/Work",
        "icon_path": null, "raw_icon_path": null, "enabled": true, "user_id": "u1",
        "credentials": { "access_token": null, "refresh_token": "r", "refresh_expires": "2099-01-01T00:00:00Z", "access_expires": null },
        "ignore_patterns": [], "max_file_size_mb": 3072, "sse_client_id": "c", "mode": "full_mirror"
    });
    let config: DriveConfig = serde_json::from_value(saved).unwrap();
    assert!(!config.propagate_deletes);
    assert!(!config.extra.contains_key("propagate_deletes"));

    let written = serde_json::to_value(DriveConfig { propagate_deletes: true, ..config }).unwrap();
    let reread: DriveConfig = serde_json::from_value(written).unwrap();
    assert!(reread.propagate_deletes, "the choice must survive a save and a reload");
}

#[tokio::test(flavor = "multi_thread")]
async fn switched_off_a_file_deleted_offline_stays_on_the_server() {
    let env = TestEnv::new().await;
    env.mount.set_propagate_deletes(false).await;
    env.write_local("report.pdf", b"pdf");
    env.track_synced("report.pdf", "e-r");
    std::fs::remove_file(env.local_path("report.pdf")).unwrap();
    env.set_remote_files(vec![remote_file("report.pdf", 3, "e-r")]).await;
    mock_info(&env.server, "report.pdf", remote_file("report.pdf", 3, "e-r")).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(server_writes(&env.server).await.is_empty());
    assert!(env.tasks_of_type("delete").is_empty());
    assert!(env.tasks_of_type("download").is_empty(), "nor downloaded back");
    assert!(env.db_entry("report.pdf").is_none(), "forgotten, as before the switch");
}

#[tokio::test(flavor = "multi_thread")]
async fn switched_on_the_same_delete_reaches_the_server() {
    let env = TestEnv::new().await;
    env.mount.set_propagate_deletes(true).await;
    env.write_local("report.pdf", b"pdf");
    env.track_synced("report.pdf", "e-r");
    std::fs::remove_file(env.local_path("report.pdf")).unwrap();
    env.set_remote_files(vec![remote_file("report.pdf", 3, "e-r")]).await;
    mock_info(&env.server, "report.pdf", remote_file("report.pdf", 3, "e-r")).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();

    wait_for("the delete", || async { server_writes(&env.server).await == vec!["DELETE /api/v4/file".to_string()] }).await;
}

/// A delete queued while the switch was on, run after it was turned off —
/// at reconnection or at the next launch alike: not sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_queued_before_switching_off_is_not_sent_at_the_next_launch() {
    let mut env = TestEnv::new().await;
    env.mount.set_propagate_deletes(false).await;
    env.write_local("gone.txt", b"gone");
    env.track_synced("gone.txt", "e-g");
    std::fs::remove_file(env.local_path("gone.txt")).unwrap();
    env.set_remote_files(vec![remote_file("gone.txt", 4, "e-g")]).await;
    mock_info(&env.server, "gone.txt", remote_file("gone.txt", 4, "e-g")).await;
    accept_writes(&env.server).await;
    let record = NewTaskRecord::new(
        uuid::Uuid::new_v4().to_string(),
        env.drive_id.clone(),
        "delete",
        env.local_path("gone.txt").to_string_lossy().to_string(),
    );
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.restart_mount().await;
    wait_for("the task to settle", || async {
        env.tasks_of_type("delete").iter().all(|t| !t.status.is_active())
    })
    .await;

    assert!(server_writes(&env.server).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_parked_before_switching_off_is_not_replayed_as_a_rename_on_reconnect() {
    let env = TestEnv::new().await;
    env.mount.set_propagate_deletes(false).await;
    env.write_local("after.txt", b"content");
    env.track_synced("after.txt", "e-1");
    env.inventory
        .rename_path(env.local_path("after.txt").to_str().unwrap(), env.local_path("before.txt").to_str().unwrap())
        .unwrap();
    env.set_remote_files(vec![remote_file("before.txt", 7, "e-1")]).await;
    mock_info(&env.server, "before.txt", remote_file("before.txt", 7, "e-1")).await;
    accept_writes(&env.server).await;
    let record = NewTaskRecord::new(
        uuid::Uuid::new_v4().to_string(),
        env.drive_id.clone(),
        "move",
        env.local_path("after.txt").to_string_lossy().to_string(),
    )
    .with_custom_state(json!({ "from": env.local_path("before.txt").to_string_lossy(), "offline_waiting": true }));
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.mount.re_enqueue_offline_tasks().await.unwrap();
    wait_for("the task to settle", || async {
        env.tasks_of_type("move").iter().all(|t| !t.status.is_active())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(server_writes(&env.server).await.is_empty());
    // Off, a rename is an upload of the new name, as before the switch.
    let uploads = env.tasks_of_type("upload");
    assert!(uploads.iter().any(|t| t.local_path.ends_with("after.txt")), "{uploads:?}");
}

/// The real watcher, the switch flipped while the app runs.
#[tokio::test(flavor = "multi_thread")]
async fn flipping_the_switch_applies_to_the_next_local_change_without_a_restart() {
    let outer = tempfile::TempDir::new().unwrap();
    let sync = std::fs::canonicalize(outer.path()).unwrap().join("Cloudreve");
    std::fs::create_dir_all(&sync).unwrap();
    for name in ["first.txt", "second.txt", "keep.txt"] {
        std::fs::write(sync.join(name), name).unwrap();
    }
    let env = TestEnv::started_on(sync, DriveMode::FullMirror).await;
    for name in ["first.txt", "second.txt", "keep.txt"] {
        env.track_synced(name, &format!("e-{name}"));
    }
    env.set_remote_files(vec![]).await;
    for name in ["first.txt", "second.txt"] {
        mock_info(&env.server, name, remote_file(name, name.len() as i64, &format!("e-{name}"))).await;
    }
    accept_writes(&env.server).await;
    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    env.mount.set_propagate_deletes(false).await;
    std::fs::remove_file(env.local_path("first.txt")).unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(server_writes(&env.server).await.is_empty(), "switched off, yet the server was written to");

    env.mount.set_propagate_deletes(true).await;
    std::fs::remove_file(env.local_path("second.txt")).unwrap();
    wait_for("the delete of second.txt", || async { !server_writes(&env.server).await.is_empty() }).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let deletes: Vec<Value> = env
        .server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == "DELETE")
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect();
    assert_eq!(deletes, vec![json!({ "uris": [format!("{REMOTE_BASE}/second.txt")] })]);
}

/// Turned off with deletes still queued (offline, say): they are dropped,
/// not sent once the connection is back.
#[tokio::test(flavor = "multi_thread")]
async fn switching_off_drops_the_deletes_still_queued() {
    let env = TestEnv::new().await;
    env.mount.set_propagate_deletes(true).await;
    let record = NewTaskRecord::new(
        uuid::Uuid::new_v4().to_string(),
        env.drive_id.clone(),
        "delete",
        env.local_path("queued.txt").to_string_lossy().to_string(),
    )
    .with_custom_state(json!({ "offline_waiting": true }));
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.mount.set_propagate_deletes(false).await;

    assert!(env.tasks_of_type("delete").iter().all(|t| !t.status.is_active()), "{:?}", env.tasks_of_type("delete"));
}
