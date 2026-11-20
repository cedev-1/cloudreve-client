//! A file or folder deleted, renamed or moved in a mirror drive must follow
//! on the server, like Dropbox or Nextcloud do — deletes into the server's
//! trash, renames as renames (never a second copy under the new name).
//!
//! The mirror used to only forget a locally deleted file: the server kept
//! it, and a local rename left the old name on the server next to the new
//! one. See `drive::propagate` for the design and its safety guards.
//!
//! Offline cases go through `full_sync` (what runs at launch, on reconnect
//! and every five minutes); live cases through the real fs watcher of a
//! started mount.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use cloudreve_sync::inventory::NewTaskRecord;
use common::{REMOTE_BASE, TestEnv, remote_file};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn ok(data: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "", "data": data }))
}

fn remote_folder(rel: &str) -> Value {
    let name = rel.rsplit('/').next().unwrap();
    json!({
        "type": 1,
        "id": format!("folder-{rel}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": 0,
        "path": format!("{REMOTE_BASE}/{rel}"),
    })
}

/// Serves `files` as the listing of the folder at `rel` ("" = the root).
async fn mock_folder(server: &MockServer, rel: &str, files: Vec<Value>) {
    let uri = if rel.is_empty() { REMOTE_BASE.to_string() } else { format!("{REMOTE_BASE}/{rel}") };
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("uri", uri.as_str()))
        .respond_with(ok(json!({
            "files": files,
            "pagination": { "page": 0, "page_size": 500, "total_items": 0 },
            "props": { "max_page_size": 10000, "order_by_options": ["name"], "order_direction_options": ["asc"] },
        })))
        .mount(server)
        .await;
}

/// What `GET /file/info` answers for `rel`.
async fn mock_info(server: &MockServer, rel: &str, file: Value) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .and(query_param("uri", format!("{REMOTE_BASE}/{rel}").as_str()))
        .respond_with(ok(file))
        .mount(server)
        .await;
}

/// Every write the server accepts.
async fn accept_writes(server: &MockServer) {
    Mock::given(method("DELETE")).and(path("/api/v4/file")).respond_with(ok(Value::Null)).mount(server).await;
    Mock::given(method("POST")).and(path("/api/v4/file/move")).respond_with(ok(Value::Null)).mount(server).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/create"))
        .respond_with(ok(remote_folder("created")))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/rename"))
        .respond_with(ok(remote_file("renamed", 0, "e")))
        .mount(server)
        .await;
}

async fn requests(server: &MockServer, verb: &str, route: &str) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r: &Request| r.method.as_str() == verb && r.url.path() == route)
        .map(|r| serde_json::from_slice(&r.body).unwrap_or(Value::Null))
        .collect()
}

async fn deletes(server: &MockServer) -> Vec<Value> {
    requests(server, "DELETE", "/api/v4/file").await
}

/// Every uri sent to `DELETE /file`, in order.
async fn deleted_uris(server: &MockServer) -> Vec<String> {
    let mut out = Vec::new();
    for body in deletes(server).await {
        for uri in body["uris"].as_array().cloned().unwrap_or_default() {
            out.push(uri.as_str().unwrap().to_string());
        }
    }
    out
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

/// Long enough for the watcher (2 s debounce) and the queue to have acted.
async fn settle() {
    tokio::time::sleep(Duration::from_secs(6)).await;
}

fn uri(rel: &str) -> String {
    format!("{REMOTE_BASE}/{rel}")
}

// ------------------------------------------------------------- offline path

/// Deleted while the app was not running: the next full sync deletes it on
/// the server, into the trash (no `skip_soft_delete`, no `unlink`), and
/// forgets it.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_deleted_while_offline_goes_to_the_server_trash_at_the_next_sync() {
    let env = TestEnv::new().await;
    env.write_local("keep.txt", b"keep");
    env.track_synced("keep.txt", "etag-keep");
    env.write_local("important.zip", b"data");
    env.track_synced("important.zip", "etag-v1");
    std::fs::remove_file(env.local_path("important.zip")).unwrap();
    env.set_remote_files(vec![remote_file("keep.txt", 4, "etag-keep"), remote_file("important.zip", 4, "etag-v1")]).await;
    mock_info(&env.server, "important.zip", remote_file("important.zip", 4, "etag-v1")).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();

    wait_for("the delete request", || async { !deletes(&env.server).await.is_empty() }).await;
    let bodies = deletes(&env.server).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    assert_eq!(bodies[0]["uris"], json!([uri("important.zip")]));
    assert!(bodies[0].get("skip_soft_delete").is_none(), "must go to the trash: {}", bodies[0]);
    assert!(bodies[0].get("unlink").is_none(), "must go to the trash: {}", bodies[0]);
    wait_for("the row to be forgotten", || async { env.db_entry("important.zip").is_none() }).await;
    assert!(env.db_entry("keep.txt").is_some());
}

/// Someone edited the file on the server after this computer last synced
/// it, then it was deleted here: the server's edit wins — it comes back,
/// nothing is deleted.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_edit_made_after_the_last_sync_wins_over_a_local_delete() {
    let env = TestEnv::new().await;
    env.write_local("plan.docx", b"v1");
    env.track_synced("plan.docx", "etag-v1");
    std::fs::remove_file(env.local_path("plan.docx")).unwrap();
    env.set_remote_files(vec![remote_file("plan.docx", 9, "etag-v2")]).await;
    mock_info(&env.server, "plan.docx", remote_file("plan.docx", 9, "etag-v2")).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(deletes(&env.server).await.is_empty(), "the server's newer version was deleted");
    assert!(env.tasks_of_type("delete").is_empty());
    assert_eq!(env.tasks_of_type("download").len(), 1, "the server's version must come back");
}

/// A folder deleted while offline goes to the trash as one item, so the
/// user restores it in one click — and no empty folder is left behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_folder_deleted_while_offline_is_deleted_as_one_folder() {
    let env = TestEnv::new().await;
    env.write_local("Photos/a.jpg", b"aaaa");
    env.write_local("Photos/2024/b.jpg", b"bbbbb");
    env.write_local("notes.txt", b"notes");
    env.track_synced("Photos/a.jpg", "e-a");
    env.track_synced("Photos/2024/b.jpg", "e-b");
    env.track_synced("notes.txt", "e-n");
    std::fs::remove_dir_all(env.local_path("Photos")).unwrap();
    let a = remote_file("Photos/a.jpg", 4, "e-a");
    let b = remote_file("Photos/2024/b.jpg", 5, "e-b");
    mock_folder(&env.server, "", vec![remote_folder("Photos"), remote_file("notes.txt", 5, "e-n")]).await;
    mock_folder(&env.server, "Photos", vec![remote_folder("Photos/2024"), a]).await;
    mock_folder(&env.server, "Photos/2024", vec![b]).await;
    mock_info(&env.server, "Photos", remote_folder("Photos")).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();

    wait_for("the delete request", || async { !deletes(&env.server).await.is_empty() }).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(deleted_uris(&env.server).await, vec![uri("Photos")]);
    wait_for("the rows to be forgotten", || async {
        env.db_entry("Photos/a.jpg").is_none() && env.db_entry("Photos/2024/b.jpg").is_none()
    })
    .await;
    assert!(env.db_entry("notes.txt").is_some());
}

/// A file added to that folder on the server meanwhile was never seen
/// here: deleting the whole folder would delete it too. Only what this
/// computer had is deleted; the new file comes down.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_folder_never_takes_a_file_this_computer_never_had() {
    let env = TestEnv::new().await;
    env.write_local("Shared/old.txt", b"old");
    env.write_local("other.txt", b"x");
    env.track_synced("Shared/old.txt", "e-old");
    env.track_synced("other.txt", "e-x");
    std::fs::remove_dir_all(env.local_path("Shared")).unwrap();
    let old = remote_file("Shared/old.txt", 3, "e-old");
    let added = remote_file("Shared/added-on-web.txt", 7, "e-added");
    mock_folder(&env.server, "", vec![remote_folder("Shared"), remote_file("other.txt", 1, "e-x")]).await;
    mock_folder(&env.server, "Shared", vec![old.clone(), added]).await;
    mock_info(&env.server, "Shared/old.txt", old).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();

    wait_for("the delete request", || async { !deletes(&env.server).await.is_empty() }).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(deleted_uris(&env.server).await, vec![uri("Shared/old.txt")]);
    let downloads = env.tasks_of_type("download");
    assert!(downloads.iter().any(|t| t.local_path.ends_with("Shared/added-on-web.txt")), "{downloads:?}");
}

/// More than half of a drive gone at once looks like an accident (a
/// folder dragged out by mistake, a script gone wrong), not a cleanup:
/// nothing is deleted on the server, and the files stay tracked so the
/// next syncs keep holding them back.
#[tokio::test(flavor = "multi_thread")]
async fn most_of_a_drive_deleted_at_once_is_held_back() {
    let env = TestEnv::new().await;
    let mut remote = Vec::new();
    for i in 0..40 {
        let rel = format!("f{i:02}.txt");
        env.write_local(&rel, b"data");
        env.track_synced(&rel, &format!("e{i}"));
        remote.push(remote_file(&rel, 4, &format!("e{i}")));
    }
    for i in 0..30 {
        std::fs::remove_file(env.local_path(&format!("f{i:02}.txt"))).unwrap();
    }
    env.set_remote_files(remote).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(deletes(&env.server).await.is_empty());
    assert!(env.tasks_of_type("delete").is_empty());
    assert!(env.db_entry("f00.txt").is_some(), "a held file must stay tracked");
    assert!(env.tasks_of_type("download").is_empty(), "held files must not be downloaded back either");

    // Held at every later sync too.
    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(deletes(&env.server).await.is_empty());
}

/// In a big drive, a few hundred files is still a lot to undo, even far
/// from half of it.
#[tokio::test(flavor = "multi_thread")]
async fn hundreds_of_files_deleted_at_once_are_held_back_even_in_a_big_drive() {
    let env = TestEnv::new().await;
    let mut remote = Vec::new();
    for i in 0..1000 {
        let rel = format!("f{i:04}.txt");
        env.write_local(&rel, b"d");
        env.track_synced(&rel, &format!("e{i}"));
        remote.push(remote_file(&rel, 1, &format!("e{i}")));
    }
    for i in 0..250 {
        std::fs::remove_file(env.local_path(&format!("f{i:04}.txt"))).unwrap();
    }
    env.set_remote_files(remote).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(deletes(&env.server).await.is_empty());
    assert!(env.tasks_of_type("delete").is_empty());
}

/// An ordinary cleanup in a drive of that size goes through.
#[tokio::test(flavor = "multi_thread")]
async fn a_few_files_deleted_in_a_big_drive_are_deleted() {
    let env = TestEnv::new().await;
    let mut remote = Vec::new();
    for i in 0..300 {
        let rel = format!("f{i:03}.txt");
        env.write_local(&rel, b"d");
        env.track_synced(&rel, &format!("e{i}"));
        remote.push(remote_file(&rel, 1, &format!("e{i}")));
    }
    for i in 0..5 {
        let rel = format!("f{i:03}.txt");
        std::fs::remove_file(env.local_path(&rel)).unwrap();
    }
    env.set_remote_files(remote.clone()).await;
    for i in 0..5 {
        let rel = format!("f{i:03}.txt");
        mock_info(&env.server, &rel, remote_file(&rel, 1, &format!("e{i}"))).await;
    }
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();

    wait_for("five deletes", || async { deleted_uris(&env.server).await.len() == 5 }).await;
}

/// Junk files (ignore patterns) are never synced: removing one locally is
/// no reason to touch the server.
#[tokio::test(flavor = "multi_thread")]
async fn an_ignored_file_deleted_locally_is_never_deleted_on_the_server() {
    let env = TestEnv::new().await;
    env.write_local("doc.txt", b"doc");
    env.track_synced("doc.txt", "e-doc");
    // A row left by an older version without ignore rules.
    env.write_local(".DS_Store", b"junk");
    env.track_synced(".DS_Store", "e-junk");
    std::fs::remove_file(env.local_path(".DS_Store")).unwrap();
    env.set_remote_files(vec![remote_file("doc.txt", 3, "e-doc"), remote_file(".DS_Store", 4, "e-junk")]).await;
    mock_info(&env.server, ".DS_Store", remote_file(".DS_Store", 4, "e-junk")).await;
    accept_writes(&env.server).await;

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(deletes(&env.server).await.is_empty());
}

/// A delete still queued when the app quit is sent at the next launch.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_left_queued_at_quit_is_sent_at_the_next_launch() {
    let mut env = TestEnv::new().await;
    env.write_local("gone.txt", b"gone");
    env.track_synced("gone.txt", "e-gone");
    std::fs::remove_file(env.local_path("gone.txt")).unwrap();
    env.set_remote_files(vec![remote_file("gone.txt", 4, "e-gone")]).await;
    mock_info(&env.server, "gone.txt", remote_file("gone.txt", 4, "e-gone")).await;
    accept_writes(&env.server).await;
    let record = NewTaskRecord::new(
        uuid::Uuid::new_v4().to_string(),
        env.drive_id.clone(),
        "delete",
        env.local_path("gone.txt").to_string_lossy().to_string(),
    );
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.restart_mount().await;

    wait_for("the delete request", || async { deleted_uris(&env.server).await == vec![uri("gone.txt")] }).await;
}

/// A rename parked while offline keeps where it came from: replaying it at
/// reconnection renames on the server instead of losing the old name.
#[tokio::test(flavor = "multi_thread")]
async fn a_rename_parked_offline_is_replayed_as_a_rename_on_reconnect() {
    let env = TestEnv::new().await;
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
    .with_custom_state(json!({
        "from": env.local_path("before.txt").to_string_lossy(),
        "offline_waiting": true,
        "network_retries": 1,
    }));
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.mount.re_enqueue_offline_tasks().await.unwrap();

    wait_for("the rename request", || async {
        !requests(&env.server, "POST", "/api/v4/file/rename").await.is_empty()
    })
    .await;
    let renames = requests(&env.server, "POST", "/api/v4/file/rename").await;
    assert_eq!(renames[0], json!({ "uri": uri("before.txt"), "new_name": "after.txt" }));
    assert!(deletes(&env.server).await.is_empty());
}

/// A full sync running while a rename waits in the queue (offline, say)
/// must not also see it as a delete of the old name and a new file.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_sync_leaves_a_queued_rename_alone() {
    let env = TestEnv::new().await;
    env.write_local("after.txt", b"content");
    env.track_synced("after.txt", "e-1");
    env.inventory
        .rename_path(env.local_path("after.txt").to_str().unwrap(), env.local_path("before.txt").to_str().unwrap())
        .unwrap();
    env.set_remote_files(vec![remote_file("before.txt", 7, "e-1")]).await;
    accept_writes(&env.server).await;
    let record = NewTaskRecord::new(
        uuid::Uuid::new_v4().to_string(),
        env.drive_id.clone(),
        "move",
        env.local_path("after.txt").to_string_lossy().to_string(),
    )
    .with_custom_state(json!({ "from": env.local_path("before.txt").to_string_lossy(), "offline_waiting": true }));
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.full_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(env.tasks_of_type("delete").is_empty(), "{:?}", env.tasks_of_type("delete"));
    assert!(env.tasks_of_type("upload").is_empty(), "{:?}", env.tasks_of_type("upload"));
    assert!(deletes(&env.server).await.is_empty());
}

/// Replayed at reconnection, then the app quits while the rename is still
/// in flight: the next launch resumes it — still as a rename, so the task
/// must still know its source after the reconnection replay.
#[tokio::test(flavor = "multi_thread")]
async fn a_replayed_rename_cut_off_by_quitting_is_resumed_as_a_rename_at_launch() {
    let mut env = TestEnv::new().await;
    env.write_local("after.txt", b"content");
    env.track_synced("after.txt", "e-1");
    env.inventory
        .rename_path(env.local_path("after.txt").to_str().unwrap(), env.local_path("before.txt").to_str().unwrap())
        .unwrap();
    env.set_remote_files(vec![remote_file("before.txt", 7, "e-1")]).await;
    mock_info(&env.server, "before.txt", remote_file("before.txt", 7, "e-1")).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/file/rename"))
        .respond_with(ok(remote_file("after.txt", 7, "e-1")).set_delay(Duration::from_secs(2)))
        .mount(&env.server)
        .await;
    let record = NewTaskRecord::new(
        uuid::Uuid::new_v4().to_string(),
        env.drive_id.clone(),
        "move",
        env.local_path("after.txt").to_string_lossy().to_string(),
    )
    .with_custom_state(json!({ "from": env.local_path("before.txt").to_string_lossy(), "offline_waiting": true }));
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    env.mount.re_enqueue_offline_tasks().await.unwrap();
    wait_for("the rename to be in flight", || async {
        !requests(&env.server, "POST", "/api/v4/file/rename").await.is_empty()
    })
    .await;
    env.mount.shutdown().await;
    env.restart_mount().await;

    wait_for("the rename to be sent again", || async {
        requests(&env.server, "POST", "/api/v4/file/rename").await.len() == 2
    })
    .await;
    let renames = requests(&env.server, "POST", "/api/v4/file/rename").await;
    assert_eq!(renames[1], json!({ "uri": uri("before.txt"), "new_name": "after.txt" }));
}

// ---------------------------------------------------------------- live path

/// A mirror drive started on its own folder, watcher and command
/// processor running, with `files` already synced.
async fn live_env(files: &[(&str, &[u8], &str)]) -> (tempfile::TempDir, TestEnv) {
    live_env_aged(files, Duration::ZERO).await
}

/// macOS reports a rename of a file created in the last ~30 s as a
/// creation of the new name only (see `drive::propagate`): files that
/// existed long before, as in real use, need that long before the watcher
/// starts for their rename to be seen as one.
const OLD_ENOUGH: Duration = Duration::from_secs(32);

async fn live_env_aged(files: &[(&str, &[u8], &str)], age: Duration) -> (tempfile::TempDir, TestEnv) {
    let outer = tempfile::TempDir::new().unwrap();
    let sync = std::fs::canonicalize(outer.path()).unwrap().join("Cloudreve");
    std::fs::create_dir_all(&sync).unwrap();
    for (rel, data, _) in files {
        let p = sync.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, data).unwrap();
    }
    tokio::time::sleep(age).await;
    let env = TestEnv::started_on(sync, DriveMode::FullMirror).await;
    for (rel, _, etag) in files {
        env.track_synced(rel, etag);
    }
    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    (outer, env)
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_synced_file_deletes_it_on_the_server() {
    let (_t, env) = live_env(&[("report.pdf", b"pdf", "e-r"), ("keep.txt", b"k", "e-k")]).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "report.pdf", remote_file("report.pdf", 3, "e-r")).await;
    accept_writes(&env.server).await;

    std::fs::remove_file(env.local_path("report.pdf")).unwrap();

    wait_for("the delete request", || async { deleted_uris(&env.server).await == vec![uri("report.pdf")] }).await;
    wait_for("the row to be forgotten", || async { env.db_entry("report.pdf").is_none() }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn renaming_a_synced_file_renames_it_on_the_server_without_a_second_copy() {
    let (_t, env) = live_env_aged(&[("draft.txt", b"text", "e-d"), ("keep.txt", b"k", "e-k")], OLD_ENOUGH).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "draft.txt", remote_file("draft.txt", 4, "e-d")).await;
    accept_writes(&env.server).await;

    std::fs::rename(env.local_path("draft.txt"), env.local_path("final.txt")).unwrap();

    wait_for("the rename request", || async {
        !requests(&env.server, "POST", "/api/v4/file/rename").await.is_empty()
    })
    .await;
    settle().await;
    let renames = requests(&env.server, "POST", "/api/v4/file/rename").await;
    assert_eq!(renames, vec![json!({ "uri": uri("draft.txt"), "new_name": "final.txt" })]);
    assert!(deletes(&env.server).await.is_empty(), "a rename must not delete");
    assert!(requests(&env.server, "PUT", "/api/v4/file/upload").await.is_empty(), "a rename must not upload a copy");
    assert!(env.db_entry("draft.txt").is_none());
    assert_eq!(env.db_entry("final.txt").map(|e| e.etag), Some("e-d".to_string()));
}

/// A file renamed seconds after it was synced ("Untitled" saved, then
/// named): whatever the watcher makes of it, the server must not keep the
/// old name next to the new one.
#[tokio::test(flavor = "multi_thread")]
async fn renaming_a_file_just_synced_leaves_no_second_copy_on_the_server() {
    let (_t, env) = live_env(&[("Untitled.txt", b"notes", "e-u"), ("keep.txt", b"k", "e-k")]).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "Untitled.txt", remote_file("Untitled.txt", 5, "e-u")).await;
    accept_writes(&env.server).await;

    std::fs::rename(env.local_path("Untitled.txt"), env.local_path("Meeting notes.txt")).unwrap();

    wait_for("the old name to leave the server", || async {
        let renamed = requests(&env.server, "POST", "/api/v4/file/rename")
            .await
            .iter()
            .any(|r| r["uri"] == json!(uri("Untitled.txt")));
        renamed || deleted_uris(&env.server).await.contains(&uri("Untitled.txt"))
    })
    .await;
    assert!(env.db_entry("Untitled.txt").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_file_into_another_folder_moves_it_on_the_server() {
    let (_t, env) = live_env_aged(&[("a.txt", b"aaa", "e-a"), ("Archive/old.txt", b"o", "e-o")], OLD_ENOUGH).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "a.txt", remote_file("a.txt", 3, "e-a")).await;
    accept_writes(&env.server).await;

    std::fs::rename(env.local_path("a.txt"), env.local_path("Archive/a.txt")).unwrap();

    wait_for("the move request", || async { !requests(&env.server, "POST", "/api/v4/file/move").await.is_empty() }).await;
    settle().await;
    let moves = requests(&env.server, "POST", "/api/v4/file/move").await;
    assert_eq!(moves.len(), 1, "{moves:?}");
    assert_eq!(moves[0]["uris"], json!([uri("a.txt")]));
    assert_eq!(moves[0]["dst"], json!(uri("Archive")));
    assert!(requests(&env.server, "POST", "/api/v4/file/rename").await.is_empty(), "same name: no rename");
    assert!(deletes(&env.server).await.is_empty());
    assert!(requests(&env.server, "PUT", "/api/v4/file/upload").await.is_empty());
    assert!(env.db_entry("Archive/a.txt").is_some());
}

/// Into a folder made just for it, in the same watcher batch: the new
/// folder's own event must not upload the file the move is taking there —
/// on a real server, that upload locks the destination and the move is
/// refused (found by the real-server suite).
///
/// macOS only: Linux starts watching a new folder only once inotify has
/// reported it, so a move into it within microseconds is seen as the file
/// leaving (a delete plus an upload — still safe). A move made later, in
/// its own batch, is covered on both by the next test.
#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread")]
async fn moving_a_file_into_a_new_folder_is_one_move_and_no_upload() {
    let (_t, env) = live_env_aged(&[("a.txt", b"aaa", "e-a"), ("k.txt", b"k", "e-k")], OLD_ENOUGH).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "a.txt", remote_file("a.txt", 3, "e-a")).await;
    accept_writes(&env.server).await;

    std::fs::create_dir_all(env.local_path("New")).unwrap();
    std::fs::rename(env.local_path("a.txt"), env.local_path("New/a.txt")).unwrap();

    wait_for("the move request", || async { !requests(&env.server, "POST", "/api/v4/file/move").await.is_empty() }).await;
    settle().await;
    assert!(requests(&env.server, "PUT", "/api/v4/file/upload").await.is_empty(), "the moved file was also uploaded");
    assert_eq!(requests(&env.server, "POST", "/api/v4/file/move").await[0]["dst"], json!(uri("New")));
}

/// A folder made first, a file moved into it a moment later: a move into
/// a folder the server does not have yet (created there first).
#[tokio::test(flavor = "multi_thread")]
async fn moving_a_file_into_a_folder_made_a_moment_before_is_one_move() {
    let (_t, env) = live_env_aged(&[("b.txt", b"bbb", "e-b"), ("k.txt", b"k", "e-k")], OLD_ENOUGH).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "b.txt", remote_file("b.txt", 3, "e-b")).await;
    accept_writes(&env.server).await;

    std::fs::create_dir_all(env.local_path("Later")).unwrap();
    settle().await;
    std::fs::rename(env.local_path("b.txt"), env.local_path("Later/b.txt")).unwrap();

    wait_for("the move request", || async { !requests(&env.server, "POST", "/api/v4/file/move").await.is_empty() }).await;
    settle().await;
    let created = requests(&env.server, "POST", "/api/v4/file/create").await;
    assert!(created.iter().any(|c| c["uri"] == json!(uri("Later")) && c["type"] == json!("folder")), "{created:?}");
    assert_eq!(requests(&env.server, "POST", "/api/v4/file/move").await[0]["dst"], json!(uri("Later")));
    assert!(requests(&env.server, "PUT", "/api/v4/file/upload").await.is_empty());
    assert!(deletes(&env.server).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn renaming_a_folder_renames_it_once_on_the_server() {
    let (_t, env) = live_env_aged(&[("Docs/a.txt", b"a", "e-a"), ("Docs/sub/b.txt", b"b", "e-b"), ("x.txt", b"x", "e-x")], OLD_ENOUGH).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "Docs", remote_folder("Docs")).await;
    accept_writes(&env.server).await;

    std::fs::rename(env.local_path("Docs"), env.local_path("Papers")).unwrap();

    wait_for("the rename request", || async {
        !requests(&env.server, "POST", "/api/v4/file/rename").await.is_empty()
    })
    .await;
    settle().await;
    let renames = requests(&env.server, "POST", "/api/v4/file/rename").await;
    assert_eq!(renames, vec![json!({ "uri": uri("Docs"), "new_name": "Papers" })]);
    assert!(deletes(&env.server).await.is_empty());
    assert!(requests(&env.server, "PUT", "/api/v4/file/upload").await.is_empty());
    assert_eq!(env.db_entry("Papers/sub/b.txt").map(|e| e.etag), Some("e-b".to_string()));
    assert!(env.db_entry("Docs/a.txt").is_none());
}

/// The folder deleted here holds, on the server, a file added there since
/// the last sync: deleting the folder would take it. Only the file this
/// computer had goes; the other one comes down.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_folder_delete_never_takes_a_file_added_on_the_server_meanwhile() {
    let (_t, env) = live_env(&[
        ("Team/mine.txt", b"m", "e-m"),
        ("k1.txt", b"1", "e-1"),
        ("k2.txt", b"2", "e-2"),
        ("k3.txt", b"3", "e-3"),
    ])
    .await;
    env.server.reset().await;
    mock_folder(&env.server, "Team", vec![remote_file("Team/mine.txt", 1, "e-m"), remote_file("Team/theirs.txt", 6, "e-t")]).await;
    mock_info(&env.server, "Team", remote_folder("Team")).await;
    accept_writes(&env.server).await;

    std::fs::remove_dir_all(env.local_path("Team")).unwrap();

    wait_for("the delete request", || async { !deletes(&env.server).await.is_empty() }).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(deleted_uris(&env.server).await, vec![uri("Team/mine.txt")]);
    assert!(env.tasks_of_type("download").iter().any(|t| t.local_path.ends_with("Team/theirs.txt")));
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_synced_folder_deletes_it_on_the_server() {
    let (_t, env) = live_env(&[
        ("Old/a.txt", b"a", "e-a"),
        ("Old/deep/b.txt", b"b", "e-b"),
        ("k1.txt", b"1", "e-1"),
        ("k2.txt", b"2", "e-2"),
        ("k3.txt", b"3", "e-3"),
    ])
    .await;
    env.server.reset().await;
    mock_folder(&env.server, "Old", vec![remote_file("Old/a.txt", 1, "e-a"), remote_folder("Old/deep")]).await;
    mock_folder(&env.server, "Old/deep", vec![remote_file("Old/deep/b.txt", 1, "e-b")]).await;
    mock_info(&env.server, "Old", remote_folder("Old")).await;
    accept_writes(&env.server).await;

    std::fs::remove_dir_all(env.local_path("Old")).unwrap();

    wait_for("the delete request", || async { deleted_uris(&env.server).await == vec![uri("Old")] }).await;
    wait_for("the rows to be forgotten", || async { env.db_entry("Old/deep/b.txt").is_none() }).await;
}

/// The server's newer version wins over a live delete too.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_delete_of_a_file_changed_on_the_server_brings_the_server_version_back() {
    let (_t, env) = live_env(&[("shared.txt", b"v1", "e-v1"), ("keep.txt", b"k", "e-k")]).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "shared.txt", remote_file("shared.txt", 12, "e-v2")).await;
    accept_writes(&env.server).await;

    std::fs::remove_file(env.local_path("shared.txt")).unwrap();

    wait_for("the download of the server version", || async {
        env.tasks_of_type("download").iter().any(|t| t.local_path.ends_with("shared.txt"))
    })
    .await;
    assert!(deletes(&env.server).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn most_of_a_drive_deleted_live_is_held_back() {
    let mut files: Vec<(String, String)> = Vec::new();
    for i in 0..30 {
        files.push((format!("Big/f{i:02}.txt"), format!("e{i}")));
    }
    for i in 0..6 {
        files.push((format!("k{i}.txt"), format!("k{i}")));
    }
    let refs: Vec<(&str, &[u8], &str)> = files.iter().map(|(r, e)| (r.as_str(), &b"d"[..], e.as_str())).collect();
    let (_t, env) = live_env(&refs).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "Big", remote_folder("Big")).await;
    accept_writes(&env.server).await;

    std::fs::remove_dir_all(env.local_path("Big")).unwrap();
    settle().await;

    assert!(deletes(&env.server).await.is_empty());
    assert!(env.tasks_of_type("delete").is_empty());
    assert!(env.db_entry("Big/f00.txt").is_some());
}

/// The sync folder itself gone (deleted, an unmounted volume) is never the
/// user deleting every file.
#[tokio::test(flavor = "multi_thread")]
async fn the_sync_folder_disappearing_deletes_nothing_on_the_server() {
    let (_t, env) = live_env(&[("a.txt", b"a", "e-a")]).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "a.txt", remote_file("a.txt", 1, "e-a")).await;
    accept_writes(&env.server).await;

    std::fs::remove_dir_all(&env.sync_dir).unwrap();
    settle().await;

    assert!(deletes(&env.server).await.is_empty());
    assert!(env.db_entry("a.txt").is_some());
}

/// What the watcher reports once the sync folder is gone depends on the
/// platform (macOS mostly reports the folder itself). Whatever it is, a
/// deletion reported while the folder is unreachable deletes nothing: the
/// batch goes in through the same command the watcher sends.
#[tokio::test(flavor = "multi_thread")]
async fn a_deletion_reported_while_the_sync_folder_is_gone_deletes_nothing() {
    let (_t, env) = live_env(&[("a.txt", b"a", "e-a"), ("b.txt", b"b", "e-b")]).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "a.txt", remote_file("a.txt", 1, "e-a")).await;
    accept_writes(&env.server).await;

    std::fs::remove_dir_all(&env.sync_dir).unwrap();
    env.mount
        .command_tx
        .send(cloudreve_sync::drive::commands::MountCommand::LocalChanges {
            changed: vec![],
            deleted: vec![env.local_path("a.txt")],
            renamed: vec![],
        })
        .unwrap();
    settle().await;

    assert!(env.tasks_of_type("delete").is_empty());
    assert!(deletes(&env.server).await.is_empty());
    assert!(env.db_entry("a.txt").is_some());
}

/// A delete queued while the folder was there, run once it is gone (the
/// volume ejected meanwhile, replayed at the next launch): not sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_delete_is_not_sent_once_the_sync_folder_is_gone() {
    let env = TestEnv::new().await;
    env.write_local("a.txt", b"a");
    env.track_synced("a.txt", "e-a");
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "a.txt", remote_file("a.txt", 1, "e-a")).await;
    accept_writes(&env.server).await;
    std::fs::remove_dir_all(&env.sync_dir).unwrap();

    env.mount.task_queue.enqueue(cloudreve_sync::tasks::TaskPayload::delete(env.local_path("a.txt"))).await.unwrap();
    wait_for("the task to finish", || async {
        env.tasks_of_type("delete").iter().all(|t| !t.status.is_active())
    })
    .await;

    assert!(deletes(&env.server).await.is_empty());
    assert!(env.db_entry("a.txt").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ignored_file_deleted_live_is_never_deleted_on_the_server() {
    let (_t, env) = live_env(&[("doc.txt", b"d", "e-d"), ("Thumbs.db", b"t", "e-t")]).await;
    env.set_remote_files(vec![]).await;
    mock_info(&env.server, "Thumbs.db", remote_file("Thumbs.db", 1, "e-t")).await;
    accept_writes(&env.server).await;

    std::fs::remove_file(env.local_path("Thumbs.db")).unwrap();
    settle().await;

    assert!(deletes(&env.server).await.is_empty());
}

/// A file deleted on the server is removed here by the client itself: that
/// removal is not the user deleting it, and must not come back to the
/// server as a delete of its own (it would hit whatever the server has at
/// that path by then).
#[tokio::test(flavor = "multi_thread")]
async fn a_file_the_client_removes_for_a_server_delete_is_not_sent_back_as_a_delete() {
    let (_t, env) = live_env(&[("gone-on-web.txt", b"g", "e-g"), ("keep.txt", b"k", "e-k")]).await;
    env.set_remote_files(vec![]).await;
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .and(query_param("uri", uri("gone-on-web.txt").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "code": 40016, "msg": "Path not exist", "data": null })))
        .mount(&env.server)
        .await;
    accept_writes(&env.server).await;

    env.mount
        .task_queue
        .enqueue(cloudreve_sync::tasks::TaskPayload::download(env.local_path("gone-on-web.txt")))
        .await
        .unwrap();
    wait_for("the local copy to be removed", || async { !env.local_path("gone-on-web.txt").exists() }).await;
    settle().await;

    let lookups = requests(&env.server, "GET", "/api/v4/file/info").await.len();
    assert_eq!(lookups, 1, "the client's own removal was handled as a local delete");
    assert!(env.tasks_of_type("delete").is_empty());
    assert!(deletes(&env.server).await.is_empty());
}
