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
use common::{uri_of, VfsTestEnv};
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
