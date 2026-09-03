//! Behavioral tests for phase-4 task 5: SSE invalidation, the `VfsEvent`
//! pump (counters + toasts), and mode-aware status for on-demand drives.
//!
//! Its own binary (not folded into another suite) for the same reason
//! `disk_space_warning.rs`/`on_demand_mode.rs` are: the OS notifier is a
//! process-wide `OnceLock` singleton, so a test asserting on toast content
//! needs isolated statics — see `TestEnv`'s harness doc.
//!
//! `TestEnv::with_mode(DriveMode::OnDemand)` never performs a real OS mount
//! (see `vfs_mode`'s module doc) — but the drive's `Vfs` IS real, backed by
//! a real cache dir and a real (wiremock) server, so readdir/listing/
//! invalidation/uploads below are all real and observable via listing hit
//! counts and mocked upload requests, not internals.
//!
//! `cloudreve-sync`'s own `TestEnv` (`tests/common/mod.rs`) has no uri-aware
//! listing mock or upload-session mock — those live in `cloudreve-vfs`'s
//! OWN `tests/common/mod.rs`, a different crate's test-only harness this
//! crate cannot import. The small subset needed for these four tests is
//! reimplemented below rather than pulled in.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use cloudreve_sync::drive::remote_events::run_remote_event_loop;
use cloudreve_vfs::vfs::VfsEvent;
use common::{REMOTE_BASE, TestEnv};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

// ---------------------------------------------------------------------
// Minimal, uri-aware wiremock harness for this file only.
// ---------------------------------------------------------------------

type Files = Arc<Mutex<HashMap<String, Vec<Value>>>>;
type Hits = Arc<Mutex<HashMap<String, u32>>>;

/// Listing endpoint, keyed by the `uri` query param exactly as
/// `ExplorerApiExt::list_files_all` sends it — unlike `TestEnv::
/// set_remote_files` (which answers every directory identically), this
/// lets a test tell "listed the root" and "listed a subdirectory" apart,
/// and count each independently (`hits_for`).
async fn mount_listing_mock(server: &MockServer) -> (Files, Hits) {
    let files: Files = Arc::new(Mutex::new(HashMap::new()));
    let hits: Hits = Arc::new(Mutex::new(HashMap::new()));
    let files_captured = files.clone();
    let hits_captured = hits.clone();
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(move |req: &Request| {
            let uri = req
                .url
                .query_pairs()
                .find(|(k, _)| k == "uri")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default();
            *hits_captured.lock().unwrap().entry(uri.clone()).or_default() += 1;
            let dir_files = files_captured.lock().unwrap().get(&uri).cloned().unwrap_or_default();
            ResponseTemplate::new(200).set_body_json(json!({
                "code": 0,
                "msg": "",
                "data": {
                    "files": dir_files,
                    "pagination": { "page": 1, "page_size": 500, "total_items": 0 },
                    "props": {
                        "max_page_size": 10000,
                        "order_by_options": ["name"],
                        "order_direction_options": ["asc"],
                    },
                },
            }))
        })
        .mount(server)
        .await;
    (files, hits)
}

fn set_files(files: &Files, uri: &str, list: Vec<Value>) {
    files.lock().unwrap().insert(uri.to_string(), list);
}

fn hits_for(hits: &Hits, uri: &str) -> u32 {
    hits.lock().unwrap().get(uri).copied().unwrap_or(0)
}

fn remote_dir_at(name: &str, parent: &str) -> Value {
    json!({
        "type": 1,
        "id": format!("dir-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": 0,
        "path": format!("{parent}/{name}"),
    })
}

fn remote_file_at(name: &str, parent: &str, size: i64, etag: &str) -> Value {
    json!({
        "type": 0,
        "id": format!("file-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": size,
        "path": format!("{parent}/{name}"),
        "primary_entity": etag,
    })
}

/// Mounts the SSE endpoint: the FIRST connection replies with `body`
/// (one or more `event: ...\ndata: ...\n\n` blocks, then the stream ends);
/// every later reconnection stalls for a long time so the retry loop
/// doesn't spin (or re-send the event) during the test — same pattern as
/// `sse_reconnect.rs`'s `mock_sse`.
async fn mount_sse(server: &MockServer, body: impl Into<String>) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_raw(body.into(), "text/event-stream"),
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

/// Minimal upload mocks (session create + one chunk, unconditional
/// success) — a trimmed port of `cloudreve-vfs/tests/common`'s
/// `expect_uploads` (a different crate's test-only harness).
async fn mount_upload_mocks(server: &MockServer) -> Arc<AtomicU32> {
    let session_count = Arc::new(AtomicU32::new(0));
    let session_count_captured = session_count.clone();
    Mock::given(method("PUT"))
        .and(path("/api/v4/file/upload"))
        .respond_with(move |req: &Request| {
            session_count_captured.fetch_add(1, Ordering::SeqCst);
            let uri = req
                .body_json::<Value>()
                .ok()
                .and_then(|b| b.get("uri").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_default();
            let session_id = format!("mock-session-{}", uuid::Uuid::new_v4());
            ResponseTemplate::new(200).set_body_json(json!({
                "code": 0,
                "msg": "",
                "data": {
                    "session_id": session_id,
                    "expires": 9_999_999_999i64,
                    "chunk_size": 4_194_304i64,
                    "callback_secret": "",
                    "uri": uri,
                },
            }))
        })
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/v4/file/upload/.+/[0-9]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "" })))
        .mount(server)
        .await;
    session_count
}

/// Download-URL + range-aware content mocks for one file, needed whenever a
/// test opens/writes an EXISTING (non-drafted) file: `Vfs::open` resolves a
/// signed download url, and `Vfs::write`'s materialize step pulls the
/// file's current bytes through it — mirrors (trimmed) `cloudreve-vfs`'s
/// own harness.
async fn mount_download_mocks(server: &MockServer, name: &str, content: &[u8]) {
    let base = server.uri();
    let download_url = format!("{base}/vfs-download/{name}");
    Mock::given(method("POST"))
        .and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "",
            "data": { "urls": [{ "url": download_url }], "expires": "2099-01-01T00:00:00Z" },
        })))
        .mount(server)
        .await;

    let content = content.to_vec();
    Mock::given(method("GET"))
        .and(path(format!("/vfs-download/{name}")))
        .respond_with(move |req: &Request| {
            let len = content.len();
            let range_header =
                req.headers.get("Range").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            if let Some((start, end)) = range_header.as_deref().and_then(|h| parse_range(h, len)) {
                let body = content[start..=end].to_vec();
                return ResponseTemplate::new(206)
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Content-Range", format!("bytes {start}-{end}/{len}"))
                    .set_body_bytes(body);
            }
            ResponseTemplate::new(200)
                .insert_header("Accept-Ranges", "bytes")
                .set_body_bytes(content.clone())
        })
        .mount(server)
        .await;
}

fn parse_range(header: &str, content_len: usize) -> Option<(usize, usize)> {
    let spec = header.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (start_str, end_str) = spec.split_once('-')?;
    let start: usize = start_str.parse().ok()?;
    let end: usize = if end_str.is_empty() { content_len.saturating_sub(1) } else { end_str.parse().ok()? };
    if start > end || end >= content_len {
        return None;
    }
    Some((start, end))
}

// ---------------------------------------------------------------------
// Test (a): SSE modify event invalidates the vfs tree.
// ---------------------------------------------------------------------

/// D4: an SSE `modify` event for `.../a/b.txt` forgets the cached listing
/// of `.../a` (its parent), so the next `readdir` refetches even though
/// `LISTING_TTL` (5s) has not naturally expired — observed purely via the
/// mocked listing endpoint's hit count, never internals.
#[tokio::test]
async fn an_sse_modify_event_invalidates_its_parent_directory_listing() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let (files, hits) = mount_listing_mock(&env.server).await;
    set_files(&files, REMOTE_BASE, vec![remote_dir_at("a", REMOTE_BASE)]);
    let a_uri = format!("{REMOTE_BASE}/a");
    set_files(&files, &a_uri, vec![remote_file_at("b.txt", &a_uri, 5, "e1")]);

    let vfs = env.mount.vfs.lock().await.clone().expect("on-demand vfs");
    let root = vfs.tree().root();
    let (a_id, _) = vfs.tree().lookup(root, "a").await.unwrap().expect("dir a");
    vfs.tree().readdir(a_id).await.unwrap();
    assert_eq!(hits_for(&hits, &a_uri), 1, "first readdir of a/ must list it exactly once");

    mount_sse(
        &env.server,
        r#"event: event
data: [{"type":"modify","file_id":"f1","from":"/a/b.txt"}]

"#,
    )
    .await;
    tokio::spawn(run_remote_event_loop(env.mount.clone()));

    // Give the SSE loop a moment to receive and process the event.
    let mut invalidated = false;
    for _ in 0..50 {
        vfs.tree().readdir(a_id).await.unwrap();
        if hits_for(&hits, &a_uri) >= 2 {
            invalidated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        invalidated,
        "an SSE modify event for a/b.txt must invalidate a/'s cached listing so the next \
         readdir refetches"
    );
}

// ---------------------------------------------------------------------
// Test (b): Subscribed invalidates the root and retries pending uploads.
// ---------------------------------------------------------------------

/// D4: `FileEvent::Subscribed` forgets the root's cached listing (so the
/// next readdir anywhere refetches) and calls `Vfs::retry_pending_uploads`
/// — bypassing the (here, very long) debounce so an already-queued draft
/// settles right away, exactly once.
#[tokio::test]
async fn subscribed_invalidates_root_and_retries_pending_uploads_once() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let (files, hits) = mount_listing_mock(&env.server).await;
    set_files(&files, REMOTE_BASE, vec![]);
    let session_count = mount_upload_mocks(&env.server).await;

    let vfs = env.mount.vfs.lock().await.clone().expect("on-demand vfs");
    // Long enough that the normal debounce path never fires on its own
    // during this test — only `retry_pending_uploads` (triggered by
    // Subscribed) should cause the upload below to actually happen.
    vfs.set_debounce_for_tests(Duration::from_secs(600));

    let root = vfs.tree().root();
    let (_node, h) = vfs.create(root, "new.txt").await.unwrap();
    vfs.write(h, 0, b"hello").await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, debounce armed but parked for 600s.
    assert_eq!(session_count.load(Ordering::SeqCst), 0, "nothing uploaded yet");

    let hits_before_root = hits_for(&hits, REMOTE_BASE);

    mount_sse(&env.server, "event: subscribed\ndata: <nil>\n\n").await;
    tokio::spawn(run_remote_event_loop(env.mount.clone()));

    let mut settled = false;
    for _ in 0..100 {
        if session_count.load(Ordering::SeqCst) >= 1 {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(settled, "Subscribed must retry the pending upload without waiting out the debounce");

    // Give any (incorrect) extra retry a moment to show up before asserting
    // exactly one session was ever created.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        session_count.load(Ordering::SeqCst),
        1,
        "Subscribed must retry pending uploads exactly once, not repeatedly"
    );

    // Root invalidation: readdir(root) again must refetch despite still
    // being within LISTING_TTL.
    vfs.tree().readdir(root).await.unwrap();
    assert!(
        hits_for(&hits, REMOTE_BASE) > hits_before_root,
        "Subscribed must invalidate the root's cached listing"
    );
}

// ---------------------------------------------------------------------
// Test (c): ConflictSaved is folded into a conflict toast.
// ---------------------------------------------------------------------

/// D6: a real remote-changed-since-draft-began conflict (mirroring
/// `cloudreve-vfs/tests/write_back.rs`'s own pinning test for the
/// underlying mechanism) produces `VfsEvent::ConflictSaved`, and the pump
/// worker folds that into a conflict toast via the reused
/// `toast::send_conflict_toast`.
#[tokio::test]
async fn a_conflict_saved_event_produces_a_conflict_toast() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    cloudreve_sync::utils::toast::init_os_notifier(tx);

    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let (files, _hits) = mount_listing_mock(&env.server).await;
    set_files(&files, REMOTE_BASE, vec![remote_file_at("shared.txt", REMOTE_BASE, 5, "e1")]);
    mount_download_mocks(&env.server, "shared.txt", b"abcde").await;
    mount_upload_mocks(&env.server).await;

    let vfs = env.mount.vfs.lock().await.clone().expect("on-demand vfs");
    vfs.set_debounce_for_tests(Duration::from_millis(20));

    let root = vfs.tree().root();
    let (node, _) = vfs.tree().lookup(root, "shared.txt").await.unwrap().expect("shared.txt");
    let h = vfs.open(node).await.unwrap(); // observes etag "e1".
    vfs.write(h, 0, b"local edit").await.unwrap(); // materializes with base_etag "e1".

    // Someone else's edit lands on the server before this draft uploads.
    set_files(&files, REMOTE_BASE, vec![remote_file_at("shared.txt", REMOTE_BASE, 5, "e2")]);

    vfs.close(h).await.unwrap(); // Pending, short debounce armed.

    env.mount.spawn_vfs_event_pump().await;

    let (title, body) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for a conflict toast")
        .expect("notifier channel closed");
    assert!(
        title.to_lowercase().contains("conflict"),
        "notification should be about a conflict, got title: {title}"
    );
    assert!(
        !body.contains("cloudreve://"),
        "notification must never leak the raw facade-internal remote uri to the user, got \
         body: {body}"
    );
    let expected_local_path = env.local_path("shared.txt").display().to_string();
    assert!(
        body.contains(&expected_local_path),
        "notification should name the conflicted file by its LOCAL mounted path ({expected_local_path}), got body: {body}"
    );
}

// ---------------------------------------------------------------------
// Test (d): a succeeded upload decrements the pending count back to 0.
// ---------------------------------------------------------------------

/// M4: drives a real queued -> succeeded upload cycle through the pump and
/// asserts `vfs_pending_uploads` returns to 0. `UploadQueued` increments
/// the counter (already covered by the D7 status-summary test in
/// `drive/manager/mod.rs`'s own test module) but nothing previously pinned
/// the OTHER half of the contract: that a terminal `UploadSucceeded`
/// actually decrements it back down rather than leaving it to drift
/// upward forever. Deleting `dec_pending()` from the `UploadSucceeded` arm
/// of `Mount::fold_vfs_event` must fail this test — see this task's report
/// for the mutation-testing log proving it does.
#[tokio::test]
async fn a_succeeded_upload_returns_the_pending_count_to_zero() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let (files, _hits) = mount_listing_mock(&env.server).await;
    set_files(&files, REMOTE_BASE, vec![]);
    let session_count = mount_upload_mocks(&env.server).await;

    let vfs = env.mount.vfs.lock().await.clone().expect("on-demand vfs");
    // Short enough that the draft settles on its own during this test,
    // without needing an SSE Subscribed event to force an early retry.
    vfs.set_debounce_for_tests(Duration::from_millis(20));

    let root = vfs.tree().root();
    let (_node, h) = vfs.create(root, "new.txt").await.unwrap();
    vfs.write(h, 0, b"hello").await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, UploadQueued sent, debounce armed.

    env.mount.spawn_vfs_event_pump().await;

    // First confirm the counter actually registers the queued draft, so a
    // later "back to 0" read can't pass by having never left 0 at all.
    let mut saw_pending = false;
    for _ in 0..100 {
        if env.mount.vfs_pending_uploads.load(Ordering::Relaxed) == 1 {
            saw_pending = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(saw_pending, "UploadQueued must increment vfs_pending_uploads to 1");

    let mut settled = false;
    for _ in 0..100 {
        if session_count.load(Ordering::SeqCst) >= 1 {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(settled, "the debounced draft must actually upload during this test");

    let mut back_to_zero = false;
    for _ in 0..100 {
        if env.mount.vfs_pending_uploads.load(Ordering::Relaxed) == 0 {
            back_to_zero = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        back_to_zero,
        "UploadSucceeded must decrement vfs_pending_uploads back to 0 once the queued draft \
         finishes uploading"
    );
}

// ---------------------------------------------------------------------
// Tracked obligation (Task-4 review): on-demand SSE file events must never
// reach the full-mirror download/delete/task machinery.
// ---------------------------------------------------------------------

/// Before this task, `FileEvent::Event` for an on-demand drive ran the
/// SAME `handle_file_events` full-mirror handlers a `FullMirror` drive
/// uses — which would enqueue a download/delete TASK against `sync_path`,
/// a real (never actually OS-mounted, but real-on-disk-in-tests) directory.
/// Pins the negative alongside test (a)'s positive: an on-demand SSE event
/// enqueues no task and writes no local file.
#[tokio::test]
async fn on_demand_sse_events_never_reach_the_full_mirror_task_machinery() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let (files, _hits) = mount_listing_mock(&env.server).await;
    set_files(&files, REMOTE_BASE, vec![remote_file_at("existing.txt", REMOTE_BASE, 5, "e1")]);

    mount_sse(
        &env.server,
        r#"event: event
data: [{"type":"modify","file_id":"f1","from":"/existing.txt"}]

"#,
    )
    .await;

    env.mount.spawn_command_processor(env.mount.clone()).await;
    tokio::spawn(run_remote_event_loop(env.mount.clone()));

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        env.all_tasks().is_empty(),
        "an on-demand drive's SSE file events must never enqueue a full-mirror task"
    );
    assert!(
        !env.sync_dir.join("existing.txt").exists(),
        "an on-demand drive's SSE file events must never write into the mount's local path"
    );
}

// ---------------------------------------------------------------------
// D6 mutation-testing anchor: touching the pump's ConflictSaved arm (see
// Step 3 of this task's brief) is checked by hand against test (c) above,
// not duplicated here.
// ---------------------------------------------------------------------
#[allow(dead_code)]
fn assert_vfs_event_variants_are_exhaustively_handled(e: VfsEvent) {
    // Compile-time reminder only: if `VfsEvent` ever grows a new variant,
    // `Mount::fold_vfs_event`'s `match` (no wildcard arm) fails to compile
    // until it's handled there too.
    match e {
        VfsEvent::UploadQueued { .. }
        | VfsEvent::UploadSucceeded { .. }
        | VfsEvent::UploadFailed { .. }
        | VfsEvent::UploadCancelled { .. }
        | VfsEvent::UploadRenamed { .. }
        | VfsEvent::ConflictSaved { .. } => {}
    }
}
