//! Attribute stability (NFS hardening, item 3). Apple's NFS client can
//! panic (`nfs_vinvalbuf2: ubc_msync failed`, fuse-t #109) when the server
//! serves unstable attributes for one file: a fileid that changes without a
//! rename/recreate, or size/mtime that flap or go backwards. These tests
//! drive the facade (and the NFS adapter, for WRITE) the way the frontends
//! do — no mount needed.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::nfs::VfsNfs;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, uri_of, VfsTestEnv};
use nfs3_server::nfs3_types::nfs3::stable_how;
use nfs3_server::vfs::{FileHandleU64, NfsFileSystem, NfsReadFileSystem};

fn new_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    Arc::new(vfs)
}

/// Re-listing a directory (TTL expiry, or any invalidation of the
/// directory itself) must hand an unchanged file the same NodeId — the
/// NodeId IS the NFS fileid.
#[tokio::test]
async fn re_listing_a_directory_keeps_the_node_id_of_an_unchanged_file() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 11, "e1"), remote_file("b.txt", 5, "e2")]).await;
    let vfs = new_vfs(&env);
    let root = vfs.tree().root();
    let (a_before, _) = vfs.lookup(root, "a.txt").await.unwrap().unwrap();

    // A new sibling appears remotely; the directory is re-listed.
    env.set_remote_files(vec![
        remote_file("new.txt", 3, "e3"),
        remote_file("a.txt", 11, "e1"),
        remote_file("b.txt", 5, "e2"),
    ])
    .await;
    let lists_before = env.list_request_count();
    vfs.tree().invalidate_path(common::REMOTE_BASE).await;
    let listing = vfs.readdir(root).await.unwrap();
    assert!(env.list_request_count() > lists_before, "the directory was not re-listed");

    let a_after = listing.iter().find(|(_, attr)| attr.name == "a.txt").unwrap().0;
    assert_eq!(a_after, a_before);
}

/// An SSE event for a file (any remote change — including the echo of our
/// own upload) invalidates its path. The file's NodeId must stay valid:
/// the kernel keeps using the handle it already holds (GETATTR, READ,
/// WRITE by fileid, no fresh LOOKUP), so the node must still resolve — to
/// the server's current attributes — not vanish until something happens
/// to re-list its parent.
#[tokio::test]
async fn sse_invalidation_of_a_file_keeps_it_resolvable_under_the_same_node_id() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("doc.txt", 11, "e1")]).await;
    let vfs = new_vfs(&env);
    let (id, _) = vfs.lookup(vfs.tree().root(), "doc.txt").await.unwrap().unwrap();

    // Changed remotely (new version, 20 bytes), then the SSE event lands.
    env.set_remote_files(vec![remote_file("doc.txt", 20, "e2")]).await;
    vfs.tree().invalidate_path(&uri_of("doc.txt")).await;

    let attr = vfs
        .getattr(id)
        .await
        .unwrap()
        .expect("an invalidated file that still exists must keep resolving by its NodeId");
    assert_eq!(attr.name, "doc.txt");
    assert_eq!(attr.size, 20, "getattr must report the server's new size, not the stale one");
    assert_eq!(attr.etag, "e2");

    let (relooked, _) = vfs.lookup(vfs.tree().root(), "doc.txt").await.unwrap().unwrap();
    assert_eq!(relooked, id, "the NodeId must not change across an SSE invalidation");
}

/// The counterpart: an invalidated file that really was deleted remotely
/// must stop resolving — keeping its handle alive must not resurrect it.
#[tokio::test]
async fn sse_invalidation_of_a_deleted_file_stops_resolving_it() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1"), remote_file("gone.txt", 1, "e2")]).await;
    let vfs = new_vfs(&env);
    let root = vfs.tree().root();
    let (gone, _) = vfs.lookup(root, "gone.txt").await.unwrap().unwrap();

    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1")]).await;
    vfs.tree().invalidate_path(&uri_of("gone.txt")).await;

    assert!(vfs.getattr(gone).await.unwrap().is_none());
    let names: Vec<String> = vfs.readdir(root).await.unwrap().into_iter().map(|(_, a)| a.name).collect();
    assert_eq!(names, vec!["keep.txt".to_string()]);
}

/// A WRITE by fileid arriving right after an SSE invalidation of the same
/// file must land. A failed WRITE is a lost write: macOS drops the
/// buffered data rather than surfacing it to the app (hf-mount saw this
/// with STALE), so neither STALE nor any other error is acceptable here.
#[tokio::test]
async fn a_write_right_after_an_sse_invalidation_of_the_file_succeeds() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("doc.txt", 11, "e1")]).await;
    env.serve_file_content("doc.txt", b"hello world").await;
    env.expect_uploads().await;
    let vfs = new_vfs(&env);
    let fs = VfsNfs::new(vfs.clone());
    let root = fs.root_dir();
    let fh: FileHandleU64 = fs.lookup(&root, &b"doc.txt".to_vec().into()).await.unwrap();

    vfs.tree().invalidate_path(&uri_of("doc.txt")).await;

    let (attr, _) = fs
        .write(&fh, 0, b"HELLO", stable_how::UNSTABLE)
        .await
        .expect("a WRITE after an invalidation of its file must not fail");
    assert_eq!(attr.size, 11);
    assert_eq!(attr.fileid, fh.as_u64());
}

/// Write-back: while the draft exists getattr reports the draft's size and
/// mtime; once its upload completes and the draft is dropped, the file must
/// keep reporting the size that was written and an mtime no older than the
/// draft's. The mock server's listing keeps reporting the pre-upload
/// attributes (11 bytes, 2026-01-02) — exactly what a server whose listing
/// lags, or which stamps the file with its own (older, skewed) clock,
/// serves — and the client must not flip back to them.
#[tokio::test]
async fn after_a_write_back_upload_size_stays_the_written_size_and_mtime_does_not_go_backwards() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("doc.txt", 11, "e1")]).await;
    env.serve_file_content("doc.txt", b"hello world").await;
    env.expect_uploads().await;
    let vfs = new_vfs(&env);
    let (id, _) = vfs.lookup(vfs.tree().root(), "doc.txt").await.unwrap().unwrap();

    let h = vfs.open(id).await.unwrap();
    vfs.write(h, 11, b" and more").await.unwrap();
    let during = vfs.getattr(id).await.unwrap().unwrap();
    assert_eq!(during.size, 20);
    vfs.close(h).await.unwrap();

    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("doc.txt").as_deref(), Some(&b"hello world and more"[..]));

    let after = vfs.getattr(id).await.unwrap().unwrap();
    assert_eq!(after.size, 20, "size flipped back after the upload completed");
    assert!(
        after.mtime_secs >= during.mtime_secs,
        "mtime went backwards after the upload: {} -> {}",
        during.mtime_secs,
        after.mtime_secs
    );

    // And it stays that way across the directory's next re-list.
    vfs.tree().invalidate_path(common::REMOTE_BASE).await;
    let (relisted_id, relisted) = vfs.lookup(vfs.tree().root(), "doc.txt").await.unwrap().unwrap();
    assert_eq!(relisted_id, id);
    assert_eq!(relisted.size, 20, "size flipped back on the next re-list");
    assert!(relisted.mtime_secs >= during.mtime_secs, "mtime went backwards on the next re-list");
}
