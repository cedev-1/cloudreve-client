//! Smoke test for the harness's upload mocks: drives `cloudreve_uploader::Uploader`
//! directly (the vfs facade does not write yet — see Task 8+) against a temp
//! file, proving the mocks speak the uploader's real "local" storage-policy
//! protocol end to end — session creation, per-chunk POSTs keyed by index,
//! and reassembly in the mock matching what was actually written to disk.

mod common;

use std::io::Write;
use std::sync::Arc;

use cloudreve_uploader::{
    NoSessionStore, ProgressCallback, ProgressUpdate, UploadParams, Uploader, UploaderConfig,
};
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, uri_of, VfsTestEnv};
use tempfile::NamedTempFile;

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
