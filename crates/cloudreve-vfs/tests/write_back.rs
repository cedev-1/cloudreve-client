//! Smoke test for the harness's upload mocks: drives `cloudreve_uploader::Uploader`
//! directly (the vfs facade does not write yet — see Task 8+) against a temp
//! file, proving the mocks speak the uploader's real "local" storage-policy
//! protocol end to end — session creation, per-chunk POSTs keyed by index,
//! and reassembly in the mock matching what was actually written to disk.

mod common;

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use cloudreve_uploader::{
    NoSessionStore, ProgressCallback, ProgressUpdate, UploadParams, Uploader, UploaderConfig,
};
use cloudreve_vfs::vfs::{Vfs, VfsEvent, DEFAULT_CACHE_MAX_BYTES};
use cloudreve_vfs::writeback::UPLOAD_RETRIES;
use common::{remote_file, uri_of, VfsTestEnv};
use tempfile::NamedTempFile;
use tokio::sync::mpsc::UnboundedReceiver;

/// Waits (bounded) for the next event matching `pred`, panicking if none
/// arrives in time — every Task 8 test needs this to observe the
/// write-back queue's outcome events without racing the background worker.
async fn expect_event(
    rx: &mut UnboundedReceiver<VfsEvent>,
    pred: impl Fn(&VfsEvent) -> bool,
) -> VfsEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(remaining > Duration::ZERO, "timed out waiting for a matching VfsEvent");
        let event = tokio::time::timeout(remaining, rx.recv())
            .await
            .expect("timed out waiting for a VfsEvent")
            .expect("event channel closed unexpectedly");
        if pred(&event) {
            return event;
        }
    }
}

/// Collects exactly `n` events in arrival order, bounded by an overall
/// deadline. Unlike `expect_event`, which skips anything that doesn't
/// match a predicate, this pins the exact SEQUENCE — a dropped or
/// reordered event shows up as a timeout or a mismatched `Vec`, not as a
/// silent pass.
async fn collect_events(rx: &mut UnboundedReceiver<VfsEvent>, n: usize) -> Vec<VfsEvent> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut events = Vec::with_capacity(n);
    while events.len() < n {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            remaining > Duration::ZERO,
            "timed out waiting for {n} events, only got {}: {events:?}",
            events.len()
        );
        let event = tokio::time::timeout(remaining, rx.recv())
            .await
            .expect("timed out waiting for a VfsEvent")
            .expect("event channel closed unexpectedly");
        events.push(event);
    }
    events
}

/// A progress callback that does nothing — this smoke test only cares about
/// the bytes the mock received, not progress reporting.
struct NoOpProgress;

impl ProgressCallback for NoOpProgress {
    fn on_progress(&self, _update: ProgressUpdate) {}
}

#[tokio::test]
async fn uploader_speaks_the_mocked_protocol_across_multiple_chunks() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;

    // 13 bytes over the mock's 5-byte chunk size splits into three chunks
    // (5, 5, 3) — enough to prove chunk-index-keyed reassembly, not just a
    // single-shot upload.
    let content = b"hello world!!".to_vec();
    let mut file = NamedTempFile::new().expect("create temp file");
    file.write_all(&content).expect("write temp file");

    let params = UploadParams {
        local_path: file.path().to_path_buf(),
        remote_uri: uri_of("greeting.txt"),
        file_size: content.len() as u64,
        mime_type: None,
        last_modified: None,
        overwrite: false,
        previous_version: String::new(),
        task_id: "task-1".to_string(),
        drive_id: "drive-1".to_string(),
    };

    let uploader = Uploader::new(env.client(), Arc::new(NoSessionStore), UploaderConfig::default());
    uploader
        .upload(params, NoOpProgress)
        .await
        .expect("upload should succeed against the mock");

    assert_eq!(env.upload_session_count(), 1);
    assert_eq!(env.uploaded_content("greeting.txt"), Some(content));
}

/// Editors' real save pattern: truncate + rewrite. No download may occur —
/// the `O_TRUNC` fast path (D2) begins the draft `Empty` rather than
/// materializing the original content it's about to discard anyway.
#[tokio::test]
async fn a_truncate_then_rewrite_downloads_nothing() {
    let env = VfsTestEnv::new().await;
    let original = vec![7u8; 2 * 1024 * 1024];
    env.set_remote_files(vec![remote_file("doc.txt", original.len() as i64, "e1")]).await;
    env.serve_file_content("doc.txt", &original).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "doc.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    vfs.truncate(h, 0).await.unwrap();
    let new_content = b"brand new content, much shorter than the original".to_vec();
    vfs.write(h, 0, &new_content).await.unwrap();

    let back = vfs.read(h, 0, new_content.len() as u32).await.unwrap();
    assert_eq!(back.as_ref(), &new_content[..]);
    assert!(
        env.download_requests("doc.txt").is_empty(),
        "truncate-then-rewrite must never download the file it's about to overwrite"
    );
}

/// A partial in-place write must first materialize the original (D2): the
/// bytes surrounding the patch have to come from somewhere, and the only
/// place they can come from is the file as it stood before this write.
#[tokio::test]
async fn a_partial_write_keeps_the_untouched_bytes() {
    let env = VfsTestEnv::new().await;
    let original: Vec<u8> = (0..=255u8).cycle().take(3 * 1024 * 1024).collect();
    env.set_remote_files(vec![remote_file("big.bin", original.len() as i64, "e1")]).await;
    env.serve_file_content("big.bin", &original).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "big.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let patch = b"NEWBYTES!!".to_vec();
    let patch_offset = 1_500_000usize;
    vfs.write(h, patch_offset as u64, &patch).await.unwrap();

    assert!(
        !env.download_requests("big.bin").is_empty(),
        "a partial in-place write must materialize the original first"
    );

    let whole = vfs.read(h, 0, original.len() as u32).await.unwrap();
    assert_eq!(&whole[..patch_offset], &original[..patch_offset], "prefix must survive");
    assert_eq!(&whole[patch_offset..patch_offset + patch.len()], &patch[..], "the patch lands intact");
    let suffix_start = patch_offset + patch.len();
    assert_eq!(&whole[suffix_start..], &original[suffix_start..], "suffix must survive");
}

/// A created file exists for the frontends before any upload happens.
#[tokio::test]
async fn a_created_file_is_visible_before_any_upload() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("existing.txt", 1, "e1")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let (new_id, h) = vfs.create(root, "new.txt").await.unwrap();
    let content = b"freshly created, never uploaded".to_vec();
    vfs.write(h, 0, &content).await.unwrap();

    let listing = vfs.readdir(root).await.unwrap();
    let names: Vec<&str> = listing.iter().map(|(_, a)| a.name.as_str()).collect();
    assert!(names.contains(&"existing.txt"), "the real remote listing must still be there too");
    let (_, attr) = listing
        .into_iter()
        .find(|(id, _)| *id == new_id)
        .expect("the newly created file must be visible in readdir before any upload");
    assert_eq!(attr.size, content.len() as u64);
    assert_eq!(
        env.list_request_count(),
        1,
        "the local overlay must be client-side only — no extra listing round-trip"
    );
}

/// Drafted reads bypass the block cache entirely: once a draft exists, its
/// bytes are authoritative even for a file whose original content was
/// already sitting in the cache from an earlier, unrelated read.
#[tokio::test]
async fn reads_of_a_drafted_file_see_the_draft_not_the_cache() {
    let env = VfsTestEnv::new().await;
    let original = vec![1u8; 4096];
    env.set_remote_files(vec![remote_file("warm.bin", original.len() as i64, "e1")]).await;
    env.serve_file_content("warm.bin", &original).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "warm.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let warmed = vfs.read(h, 0, 4096).await.unwrap();
    assert_eq!(warmed.as_ref(), &original[..]);

    vfs.truncate(h, 0).await.unwrap();
    let draft_content = vec![9u8; 100];
    vfs.write(h, 0, &draft_content).await.unwrap();

    let after = vfs.read(h, 0, 100).await.unwrap();
    assert_eq!(
        after.as_ref(),
        &draft_content[..],
        "read must serve the draft, not the still-cached original"
    );
}

/// Ledger debt from phase 1: a transient (5xx) failure followed by an
/// expired-url (403) must both be recovered from within one `read`, and the
/// 403 must not itself consume a slot from the transient-error retry
/// budget — it's handled by its own one-time URL refresh, orthogonal to
/// `FETCH_RETRIES`.
///
/// The harness's `/api/v4/file/url` mock hands back a genuinely different
/// url every call (`?v={n}`, see `common::VfsTestEnv::new`'s comment), so
/// this test can tell "the original url" (v1) and "the refreshed url" (v2)
/// apart — an earlier version of this test used a mock that always
/// reissued the identical url, which meant a mutation that made the 403
/// arm consume a backoff attempt on the SAME url (instead of forcing a
/// real refresh) still passed: the byte-count assertion alone could not
/// distinguish "retried the old url" from "correctly fetched a new one".
#[tokio::test]
async fn a_transient_error_then_an_expired_url_still_serves_the_read() {
    let env = VfsTestEnv::new().await;
    let body = vec![9u8; 4096];
    env.set_remote_files(vec![remote_file("recovering.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("recovering.bin", &body).await;
    // First attempt: transient server error. Second attempt: the signed url
    // has "expired" (403). Both land on v1 — the url `open()` fetched.
    env.fail_downloads_with_sequence("recovering.bin", vec![500, 403]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "recovering.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap(); // 1st `/file/url` call -> v1

    let bytes = vfs.read(h, 0, 4096).await.unwrap();
    assert_eq!(bytes.as_ref(), &body[..]);

    assert_eq!(
        env.file_url_request_count("recovering.bin"),
        2,
        "open() issues v1; the 403 must force exactly one REAL refresh to v2 — if 403 \
         were treated as just another retryable transport error (retried against the \
         same url instead), no second /file/url call would ever happen"
    );
    assert_eq!(
        env.download_hits_for_version("recovering.bin", 1),
        2,
        "the 500 and the 403 must both land on the original url (v1), not spill onto v2"
    );
    assert_eq!(
        env.download_hits_for_version("recovering.bin", 2),
        1,
        "exactly one successful attempt on the refreshed url (v2)"
    );
}

/// Two writers racing the first write on the same undrafted file (phase 3's
/// NFS frontend dispatches concurrent WRITEs to the same handle routinely)
/// must never lose either one's already-acknowledged bytes. Without a
/// per-path guard, both writers can observe "no draft yet", both
/// materialize, and the second `DraftStore::begin` — which unconditionally
/// overwrites — silently discards the first writer's already-applied write.
///
/// An artificial delay on the download endpoint widens the race window so
/// this doesn't depend on scheduler luck; looping several iterations is
/// extra insurance on top of that.
#[tokio::test]
async fn concurrent_first_writes_never_lose_acknowledged_bytes() {
    for iteration in 0..20 {
        let env = VfsTestEnv::new().await;
        let original = vec![0u8; 4096];
        env.set_remote_files(vec![remote_file("shared.bin", original.len() as i64, "e1")]).await;
        env.serve_file_content("shared.bin", &original).await;
        env.slow_down_downloads("shared.bin", std::time::Duration::from_millis(20));

        let (vfs, _rx) =
            Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
                .unwrap();
        let node = vfs.tree().lookup(vfs.tree().root(), "shared.bin").await.unwrap().unwrap().0;
        let h = vfs.open(node).await.unwrap();

        let x = b"XXXXXXXXXX".to_vec();
        let y = b"YYYYYYYYYY".to_vec();

        // Both writers race `ensure_drafted` on the same undrafted handle:
        // neither has begun a draft yet when this starts.
        let (ra, rb) = tokio::join!(vfs.write(h, 0, &x), vfs.write(h, 1000, &y));
        ra.unwrap_or_else(|e| panic!("iteration {iteration}: writer A failed: {e}"));
        rb.unwrap_or_else(|e| panic!("iteration {iteration}: writer B failed: {e}"));

        let whole = vfs.read(h, 0, original.len() as u32).await.unwrap();
        assert_eq!(
            &whole[0..10],
            &x[..],
            "iteration {iteration}: writer A's acknowledged bytes were lost"
        );
        assert_eq!(
            &whole[1000..1010],
            &y[..],
            "iteration {iteration}: writer B's acknowledged bytes were lost"
        );
    }
}

/// `create` must refuse a name that already resolves to a real remote file:
/// without a guard, it reuses the existing NodeId, clobbers its attrs with
/// a size-0 placeholder, and shadows the real content with an empty draft
/// whose blank `base_etag` would later bypass the conflict check entirely
/// (D5) — a silent overwrite of a file the caller never touched.
#[tokio::test]
async fn creating_over_an_existing_file_is_refused() {
    let env = VfsTestEnv::new().await;
    let original = b"do not touch me".to_vec();
    env.set_remote_files(vec![remote_file("keep.txt", original.len() as i64, "e1")]).await;
    env.serve_file_content("keep.txt", &original).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    let result = vfs.create(root, "keep.txt").await;
    assert!(result.is_err(), "creating over an existing remote file must be refused");

    // The original must be completely unharmed: same size, same content —
    // not clobbered by the refused create's placeholder attrs/draft.
    let (node, attr) = vfs.lookup(root, "keep.txt").await.unwrap().unwrap();
    assert_eq!(attr.size, original.len() as u64, "the original's attrs must be untouched");
    let h = vfs.open(node).await.unwrap();
    let content = vfs.read(h, 0, original.len() as u32).await.unwrap();
    assert_eq!(content.as_ref(), &original[..], "the original's content must be untouched");
}

// ---------------------------------------------------------------------
// Task 8: write-back queue (debounce, upload, conflicts, retry).
// ---------------------------------------------------------------------

/// A closed, dirty draft uploads by itself, and the exact saved bytes reach
/// the (mocked) server. Once it's done, the draft is gone and the block
/// cache was left empty for the file (D6) — a fresh open+read must
/// genuinely refetch from the server rather than replay anything local.
#[tokio::test]
async fn a_closed_draft_uploads_and_the_server_receives_the_exact_bytes() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    let original = b"original server content".to_vec();
    env.set_remote_files(vec![remote_file("doc.txt", original.len() as i64, "e1")]).await;
    env.serve_file_content("doc.txt", &original).await;

    let (vfs, mut rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));

    let node = vfs.tree().lookup(vfs.tree().root(), "doc.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.truncate(h, 0).await.unwrap();
    let saved = b"freshly saved bytes".to_vec();
    vfs.write(h, 0, &saved).await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.wait_for_writeback_idle().await;

    assert_eq!(
        env.uploaded_content("doc.txt"),
        Some(saved.clone()),
        "the server must receive the exact saved bytes"
    );

    let event =
        expect_event(&mut rx, |e| matches!(e, VfsEvent::UploadSucceeded { .. })).await;
    assert!(
        matches!(&event, VfsEvent::UploadSucceeded { remote_path, .. } if remote_path.ends_with("doc.txt")),
        "unexpected event: {event:?}"
    );

    // D6: draft removed, cache left empty for the file.
    assert!(
        env.download_requests("doc.txt").is_empty(),
        "no ranged download should have happened before this point"
    );
    let h2 = vfs.open(node).await.unwrap();
    let back = vfs.read(h2, 0, original.len() as u32).await.unwrap();
    assert_eq!(
        back.as_ref(),
        &original[..],
        "the draft is gone: a fresh read must come from the server, not the removed draft"
    );
    assert!(
        !env.download_requests("doc.txt").is_empty(),
        "the fresh read must have actually hit the network"
    );
}

/// Save, close (arms the debounce), reopen well within the window (which
/// must cancel it), save again, close again: only ONE upload session is
/// ever created, and it carries the SECOND save's bytes.
#[tokio::test]
async fn save_close_reopen_save_uploads_once() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("notes.txt", 5, "e1")]).await;
    env.serve_file_content("notes.txt", b"abcde").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(300));

    let node = vfs.tree().lookup(vfs.tree().root(), "notes.txt").await.unwrap().unwrap().0;

    let h1 = vfs.open(node).await.unwrap();
    vfs.truncate(h1, 0).await.unwrap();
    vfs.write(h1, 0, b"first save").await.unwrap();
    vfs.close(h1).await.unwrap(); // Pending, debounce armed.

    // Reopen immediately, well inside the 300ms debounce window: this must
    // cancel the timer and put the draft back into Editing.
    let h2 = vfs.open(node).await.unwrap();
    let second_save = b"second save, overwrites".to_vec();
    vfs.write(h2, 0, &second_save).await.unwrap();
    vfs.close(h2).await.unwrap(); // Pending again, a fresh debounce armed.

    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.upload_session_count(), 1, "only one upload session should ever be created");
    assert_eq!(env.uploaded_content("notes.txt"), Some(second_save));
}

/// The file's remote etag changed since the draft began (someone else's
/// concurrent edit): the write-back queue must never overwrite it. It
/// uploads to a conflict-copy name instead, leaves the original untouched,
/// and reports `ConflictSaved`.
#[tokio::test]
async fn a_remote_change_since_the_draft_began_becomes_a_conflict_copy() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("shared.txt", 5, "e1")]).await;
    env.serve_file_content("shared.txt", b"abcde").await;

    let (vfs, mut rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));

    let node = vfs.tree().lookup(vfs.tree().root(), "shared.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap(); // observes etag "e1" at draft-begin time.
    vfs.write(h, 0, b"local edit").await.unwrap(); // materializes with base_etag "e1".

    // Someone else's edit lands on the server before this draft uploads.
    env.set_remote_etag("shared.txt", "e2").await;

    vfs.close(h).await.unwrap();
    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.upload_session_count(), 1, "exactly one upload — the conflict copy");
    assert_eq!(
        env.uploaded_content("shared.txt"),
        None,
        "the original must never be overwritten by a conflicting draft"
    );

    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let conflict_name = format!("shared (conflict {today}).txt");
    assert!(conflict_name.contains("(conflict "));
    assert_eq!(
        env.uploaded_content(&conflict_name),
        Some(b"local edit".to_vec()),
        "the draft's content must land under the conflict-copy name instead"
    );

    let event = expect_event(&mut rx, |e| matches!(e, VfsEvent::ConflictSaved { .. })).await;
    match event {
        VfsEvent::ConflictSaved { original, conflict_copy } => {
            assert!(original.ends_with("shared.txt"));
            assert!(conflict_copy.contains("(conflict "));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

/// Every upload attempt fails (session creation itself errors): after
/// exhausting `UPLOAD_RETRIES`, the draft is parked back `Pending` — never
/// dropped — and `UploadFailed{will_retry: true}` is reported. Once the
/// server heals, `retry_pending_uploads` re-arms it on demand and the save
/// finally reaches the server.
#[tokio::test]
async fn a_failed_upload_keeps_the_draft_and_retries_on_demand() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("flaky.txt", 5, "e1")]).await;
    env.serve_file_content("flaky.txt", b"abcde").await;
    env.fail_next_upload_sessions(UPLOAD_RETRIES as usize);

    let (vfs, mut rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    vfs.set_retry_backoff_for_tests([Duration::from_millis(20), Duration::from_millis(20)]);

    let node = vfs.tree().lookup(vfs.tree().root(), "flaky.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    let saved = b"new content".to_vec();
    vfs.write(h, 0, &saved).await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.wait_for_writeback_idle().await;

    let event = expect_event(&mut rx, |e| matches!(e, VfsEvent::UploadFailed { .. })).await;
    assert!(
        matches!(&event, VfsEvent::UploadFailed { will_retry: true, .. }),
        "unexpected event: {event:?}"
    );
    assert_eq!(env.upload_session_count(), UPLOAD_RETRIES as usize);
    assert_eq!(env.uploaded_content("flaky.txt"), None, "nothing must have actually uploaded");

    // The draft's bytes must survive the failure — reopening still serves
    // them (D3), proving nothing was dropped when the upload gave up.
    let h2 = vfs.open(node).await.unwrap();
    let back = vfs.read(h2, 0, saved.len() as u32).await.unwrap();
    assert_eq!(back.as_ref(), &saved[..], "the draft's bytes must survive a failed upload");
    vfs.close(h2).await.unwrap();

    // The mock's injected-failure budget is exhausted: every session
    // creation from here on succeeds normally.
    let requeued = vfs.retry_pending_uploads().await;
    assert_eq!(requeued, 1);

    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("flaky.txt"), Some(saved));
}

/// A file that is only ever read, never written, must never trigger an
/// upload — there is no dirty draft to write back.
#[tokio::test]
async fn nothing_is_uploaded_for_a_file_only_read() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("readonly.txt", 5, "e1")]).await;
    env.serve_file_content("readonly.txt", b"abcde").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();

    let node = vfs.tree().lookup(vfs.tree().root(), "readonly.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    let back = vfs.read(h, 0, 5).await.unwrap();
    assert_eq!(back.as_ref(), b"abcde");
    vfs.close(h).await.unwrap();

    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.upload_session_count(), 0);
}

/// Review finding 2 (pinning): a normal save must report its outcome
/// events in the right ORDER — `UploadQueued` when the draft is armed,
/// then `UploadSucceeded` once it lands — not just "both showed up
/// somewhere in the channel eventually". `expect_event` (used by the other
/// tests) skips anything that doesn't match its predicate and so cannot
/// tell a dropped or reordered event from a correct sequence; this test
/// uses `collect_events` specifically to close that gap.
#[tokio::test]
async fn a_successful_save_emits_queued_then_succeeded_in_order() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("ordered.txt", 5, "e1")]).await;
    env.serve_file_content("ordered.txt", b"abcde").await;

    let (vfs, mut rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));

    let node = vfs.tree().lookup(vfs.tree().root(), "ordered.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.truncate(h, 0).await.unwrap();
    vfs.write(h, 0, b"ordered save").await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.wait_for_writeback_idle().await;

    let events = collect_events(&mut rx, 2).await;
    match &events[..] {
        [VfsEvent::UploadQueued { remote_path: queued }, VfsEvent::UploadSucceeded { remote_path: succeeded, .. }] =>
        {
            assert!(queued.ends_with("ordered.txt"));
            assert!(succeeded.ends_with("ordered.txt"));
        }
        other => panic!("expected [UploadQueued, UploadSucceeded] in order, got {other:?}"),
    }
}

/// Review finding 3 (pinning): the write-back queue drains sequentially,
/// one upload at a time (YAGNI on parallelism per the plan) — never two
/// sessions "in flight" together. Two files are saved in the same debounce
/// window so both their timers fire close together; an artificial per-chunk
/// delay widens the window in which a broken `upload_gate` would actually
/// let both sessions overlap (real loopback HTTP is otherwise fast enough
/// that even a broken gate could get lucky and still look sequential).
/// `max_concurrent_uploads` is the cheapest honest signal available here:
/// the local storage policy has no completion/callback call to hook (see
/// `expect_uploads`'s doc), so the mock infers "session no longer in
/// flight" from its last EXPECTED chunk (by declared size) having arrived —
/// exactly the same event that ends `Uploader::upload`'s chunk phase.
#[tokio::test]
async fn uploads_drain_one_at_a_time() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![
        remote_file("alpha.bin", 20, "e1"),
        remote_file("beta.bin", 20, "e1"),
    ])
    .await;
    env.serve_file_content("alpha.bin", &[1u8; 20]).await;
    env.serve_file_content("beta.bin", &[2u8; 20]).await;
    env.slow_down_chunk_uploads(Duration::from_millis(50));

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));

    let root = vfs.tree().root();
    let alpha = vfs.tree().lookup(root, "alpha.bin").await.unwrap().unwrap().0;
    let beta = vfs.tree().lookup(root, "beta.bin").await.unwrap().unwrap().0;

    let alpha_bytes = vec![9u8; 20];
    let beta_bytes = vec![8u8; 20];

    let ha = vfs.open(alpha).await.unwrap();
    vfs.truncate(ha, 0).await.unwrap();
    vfs.write(ha, 0, &alpha_bytes).await.unwrap();
    vfs.close(ha).await.unwrap(); // arms alpha's debounce timer.

    let hb = vfs.open(beta).await.unwrap();
    vfs.truncate(hb, 0).await.unwrap();
    vfs.write(hb, 0, &beta_bytes).await.unwrap();
    vfs.close(hb).await.unwrap(); // arms beta's debounce timer, moments later.

    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.upload_session_count(), 2, "both files must have uploaded");
    assert_eq!(env.uploaded_content("alpha.bin"), Some(alpha_bytes));
    assert_eq!(env.uploaded_content("beta.bin"), Some(beta_bytes));
    assert_eq!(
        env.max_concurrent_uploads(),
        1,
        "the write-back queue must drain sequentially — two sessions were in flight at once"
    );
}

/// Review finding 1 (durability), superseded by Task 9's promoted fix: a
/// reopen of a file that already has an active draft must never strand an
/// acknowledged save. Originally this was guaranteed by ORDERING — `open()`
/// only cancelled the write-back debounce timer / flipped the draft back to
/// `Editing` after its fallible download-URL fetch had actually succeeded,
/// so a failed fetch (e.g. offline) could never leave the draft `Editing`
/// with no timer and no handle to close it again. Task 9 removes the
/// fallible step itself: a drafted reopen never needs a download URL at
/// all (drafted reads are served from the draft), so the fetch below is
/// never even attempted, and the injected failure is never consumed —
/// stranding is now impossible by construction, not merely by position.
#[tokio::test]
async fn a_reopen_of_a_pending_draft_never_strands_it_even_if_offline() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("offline.txt", 5, "e1")]).await;
    env.serve_file_content("offline.txt", b"abcde").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    // Wide enough that the debounce timer armed by `close` below is still
    // very much alive (and thus still cancellable) at the moment the
    // reopen is attempted.
    vfs.set_debounce_for_tests(Duration::from_millis(300));

    let node = vfs.tree().lookup(vfs.tree().root(), "offline.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap(); // 1st (and, per the assertion below, ONLY) file/url fetch.
    let saved = b"saved offline".to_vec();
    vfs.write(h, 0, &saved).await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, debounce armed.

    // Simulate being offline for exactly the fetch a reopen used to
    // perform — the point of this test is that it no longer matters: a
    // draft already exists, so `open()` skips the fetch and never even
    // sees this injected failure.
    env.fail_next_file_url_requests(1);
    let reopen = vfs.open(node).await;
    assert!(reopen.is_ok(), "reopening a drafted file must never need the network at all");
    assert_eq!(
        env.file_url_request_count("offline.txt"),
        1,
        "the reopen must not have attempted a second download-url fetch"
    );
    vfs.close(reopen.unwrap()).await.unwrap(); // Pending again, a fresh debounce armed.

    vfs.wait_for_writeback_idle().await;

    assert_eq!(
        env.uploaded_content("offline.txt"),
        Some(saved),
        "an acknowledged save must always eventually upload"
    );
}

// ---------------------------------------------------------------------
// Task 9: drafts survive a restart.
// ---------------------------------------------------------------------

/// Quit mid-upload, relaunch: the edit still reaches the server. A draft
/// left `Pending` at shutdown (every upload attempt exhausted before the
/// process died) must be re-enqueued at the very next `Vfs::new`, not left
/// waiting for a manual `retry_pending_uploads` call nobody would think to
/// make.
#[tokio::test]
async fn pending_drafts_upload_after_a_restart() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("resume.txt", 5, "e1")]).await;
    env.serve_file_content("resume.txt", b"abcde").await;
    // Every attempt across the whole first close's retry cycle fails, so
    // the draft is genuinely parked `Pending` — nothing uploaded yet —
    // when the app "quits" below.
    env.fail_next_upload_sessions(UPLOAD_RETRIES as usize);

    let saved = b"saved before quitting".to_vec();
    {
        let (vfs, mut rx) = Vfs::new(
            env.client(),
            common::REMOTE_BASE.into(),
            env.cache_dir(),
            DEFAULT_CACHE_MAX_BYTES,
        )
        .unwrap();
        vfs.set_debounce_for_tests(Duration::from_millis(20));
        vfs.set_retry_backoff_for_tests([Duration::from_millis(20), Duration::from_millis(20)]);

        let node = vfs.tree().lookup(vfs.tree().root(), "resume.txt").await.unwrap().unwrap().0;
        let h = vfs.open(node).await.unwrap();
        vfs.write(h, 0, &saved).await.unwrap();
        vfs.close(h).await.unwrap();

        vfs.wait_for_writeback_idle().await;

        let event = expect_event(&mut rx, |e| matches!(e, VfsEvent::UploadFailed { .. })).await;
        assert!(
            matches!(&event, VfsEvent::UploadFailed { will_retry: true, .. }),
            "unexpected event: {event:?}"
        );
        assert_eq!(env.upload_session_count(), UPLOAD_RETRIES as usize);
        assert_eq!(
            env.uploaded_content("resume.txt"),
            None,
            "nothing must have uploaded before the (simulated) quit"
        );
        // `vfs` and `rx` drop here: simulates quitting the app entirely
        // while the draft is still parked `Pending` on disk.
    }

    // The mock's injected-failure budget is exhausted: every upload-session
    // creation from here on succeeds normally, simulating the network (or
    // server) healing while the app was closed.
    let (vfs2, _rx2) = Vfs::new(
        env.client(),
        common::REMOTE_BASE.into(),
        env.cache_dir(),
        DEFAULT_CACHE_MAX_BYTES,
    )
    .unwrap();

    // Bounded rather than a bare `wait_for_writeback_idle`: if the restart
    // never re-enqueues the pending draft, `busy` is 0 from the very start
    // and an unbounded wait would return immediately without proving
    // anything either way — the deadline instead gives the (possibly
    // absent) re-enqueue a real chance to run and finish before the
    // assertion below is checked.
    let _ = tokio::time::timeout(Duration::from_secs(5), vfs2.wait_for_writeback_idle()).await;

    assert_eq!(
        env.uploaded_content("resume.txt"),
        Some(saved),
        "a draft still Pending at the last quit must upload after the next launch"
    );
}

// ---------------------------------------------------------------------
// Promoted fix from Task 8's review: `open()` of a Pending LOCAL-ONLY
// created draft must not fail trying to fetch a download URL for a path
// the server has never heard of.
// ---------------------------------------------------------------------

/// A brand new file (`create`, no remote counterpart) can be reopened
/// before its very first upload lands. Before this fix, `open()`
/// unconditionally fetched a download URL, and a locally-created file has
/// nothing on the server yet for that fetch to resolve — every
/// save-close-reopen on a brand new file errored until the pending upload
/// finally drained.
#[tokio::test]
async fn a_new_file_can_be_reopened_before_its_upload_lands() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(300));

    let root = vfs.tree().root();
    let (node, h1) = vfs.create(root, "brand-new.txt").await.unwrap();
    let first_version = b"first version".to_vec();
    vfs.write(h1, 0, &first_version).await.unwrap();
    vfs.close(h1).await.unwrap(); // Pending, debounce armed; the upload has not landed yet.

    // A real server has no `file/url` answer for a uri it has never heard
    // of — simulated here the same way `a_reopen_of_a_pending_draft_never_
    // strands_it_even_if_offline` simulates being offline for that same
    // endpoint. Reopen well within the debounce window, exactly the
    // save-close-reopen pattern a real editor performs.
    env.fail_next_file_url_requests(1);
    let (looked_up_node, _attr) = vfs.lookup(root, "brand-new.txt").await.unwrap().unwrap();
    assert_eq!(looked_up_node, node);
    let h2 = vfs
        .open(looked_up_node)
        .await
        .expect("reopening a not-yet-uploaded new file must succeed");

    let back = vfs.read(h2, 0, first_version.len() as u32).await.unwrap();
    assert_eq!(
        back.as_ref(),
        &first_version[..],
        "the reopened handle must read the draft's own bytes"
    );

    let second_version = b"second version, overwrites".to_vec();
    vfs.truncate(h2, 0).await.unwrap();
    vfs.write(h2, 0, &second_version).await.unwrap();
    vfs.close(h2).await.unwrap(); // Pending again, a fresh debounce armed.

    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.upload_session_count(), 1, "only one upload session should ever be created");
    assert_eq!(
        env.uploaded_content("brand-new.txt"),
        Some(second_version),
        "the eventual upload must carry the SECOND version's bytes"
    );
}
