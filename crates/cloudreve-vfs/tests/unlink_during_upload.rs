//! A new file deleted while its upload is finishing must not come back on
//! the server. `process` checked "deleted mid-upload?" once, right after
//! the upload, then refreshed the etag (a listing request) and settled the
//! draft: an unlink landing in between found the upload already past its
//! check, removed the draft, and the uploaded copy stayed on the server.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::vfs::{CreatePauseHook, Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{uri_of, VfsTestEnv};

#[tokio::test]
async fn a_file_deleted_while_its_upload_settles_is_removed_from_the_server() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(10));
    let hook = Arc::new(CreatePauseHook::new());
    vfs.pause_upload_before_settle_for_tests(hook.clone());

    let root = vfs.tree().root();
    let (_n, h) = vfs.create(root, "draft.txt").await.unwrap();
    vfs.write(h, 0, b"short-lived").await.unwrap();
    vfs.close(h).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), hook.parked.notified()).await.expect("the upload never landed");
    assert_eq!(env.uploaded_content("draft.txt").as_deref(), Some(&b"short-lived"[..]));

    vfs.unlink(root, "draft.txt").await.expect("unlink");
    hook.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), vfs.wait_for_writeback_idle()).await.unwrap();

    assert!(
        env.last_deleted_uris().contains(&uri_of("draft.txt")),
        "the uploaded copy was left on the server: deletes {:?}",
        env.last_deleted_uris()
    );
}
