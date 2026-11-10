//! Two handles on one file (FUSE keeps one per open(2): an editor and a
//! preview, or an app that opens its document twice). Closing one used to
//! arm the upload while the other still wrote: the upload removed the
//! draft, and the next write through the other handle started a fresh
//! draft from the server's copy — losing the first handle's bytes, or
//! zero-filling them for a never-uploaded file.

mod common;

use std::time::Duration;

use cloudreve_vfs::vfs::{StaleHandleError, Vfs, DEFAULT_CACHE_MAX_BYTES};
use cloudreve_vfs::writeback::DraftState;
use common::{remote_file, uri_of, VfsTestEnv};

#[tokio::test]
async fn closing_one_of_two_handles_does_not_upload_under_the_other() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_file("a.txt", 10, "e1")]).await;
    env.serve_file_content("a.txt", b"0123456789").await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    let root = vfs.tree().root();
    let node = vfs.lookup(root, "a.txt").await.unwrap().unwrap().0;

    let h1 = vfs.open(node).await.unwrap();
    let h2 = vfs.open(node).await.unwrap();
    vfs.write(h1, 0, b"AAAA").await.unwrap();
    vfs.close(h1).await.unwrap();
    // Well past the debounce: an upload armed by that close would land now.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(env.upload_session_count(), 0, "uploaded while another handle was still open");

    vfs.write(h2, 4, b"BBBB").await.unwrap();
    vfs.close(h2).await.unwrap();
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("a.txt").as_deref(), Some(&b"AAAABBBB89"[..]));
}

/// A handle opened on a draft whose draft is gone (uploaded meanwhile):
/// writing through it must fail loudly instead of starting a new, empty
/// draft that would then overwrite the server copy with zeros.
#[tokio::test]
async fn a_write_through_a_handle_that_outlived_its_draft_is_stale() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let (node, h1) = vfs.create(root, "new.txt").await.unwrap();
    vfs.write(h1, 0, b"hello").await.unwrap();
    let h2 = vfs.open(node).await.unwrap();

    // The draft gets uploaded and removed while h2 stays open.
    vfs.set_draft_state_for_tests(&uri_of("new.txt"), DraftState::Pending).await.unwrap();
    vfs.retry_pending_uploads().await;
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("new.txt").as_deref(), Some(&b"hello"[..]));

    let err = vfs.write(h2, 5, b" world").await.expect_err("the handle's draft is gone");
    assert!(err.downcast_ref::<StaleHandleError>().is_some(), "{err:#}");
    let err = vfs.truncate(h2, 2).await.expect_err("the handle's draft is gone");
    assert!(err.downcast_ref::<StaleHandleError>().is_some(), "{err:#}");
    let _ = vfs.close(h1).await;
    let _ = vfs.close(h2).await;
}
