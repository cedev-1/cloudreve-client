//! A file the kernel is still writing must keep answering by its id while
//! its first upload lands. Copying a folder in through the NFS mount
//! (`cp -R`, Finder) does CREATE, then WRITE and SETATTR on the returned
//! fileid; with the write-back debounce shorter than that gap, the empty
//! draft CREATE left behind uploads first. The upload's etag refresh then
//! marked the file stale and re-listed its folder: a listing that did not
//! show the file yet (one fetched before the upload landed, or a server
//! still catching up) dropped the node, and every later WRITE/SETATTR on
//! it failed with ENOENT ("fcopyfile failed: No such file or directory").

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::tree::NodeId;
use cloudreve_vfs::vfs::{CreatePauseHook, Vfs, VfsEvent, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, VfsTestEnv, REMOTE_BASE};
use serde_json::{json, Value};
use tokio::sync::mpsc::UnboundedReceiver;

fn photo_in_album(name: &str, size: i64, etag: &str) -> Value {
    json!({
        "type": 0,
        "id": format!("file-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": size,
        "path": format!("{REMOTE_BASE}/Album/{name}"),
        "primary_entity": etag,
    })
}

async fn vfs_with_album(env: &VfsTestEnv) -> (Arc<Vfs>, NodeId, UnboundedReceiver<VfsEvent>) {
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("Album")]).await;
    env.set_remote_files_at("Album", vec![]).await;
    let (vfs, events) =
        Vfs::new(env.client(), REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES).unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(10));
    let vfs = Arc::new(vfs);
    let (album, _) = vfs.lookup(vfs.tree().root(), "Album").await.unwrap().expect("Album is listed");
    (vfs, album, events)
}

/// Waits for the first upload to settle (its draft removed or kept).
async fn upload_succeeded(events: &mut UnboundedReceiver<VfsEvent>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = events.recv().await {
            if let VfsEvent::UploadSucceeded { .. } = event {
                return;
            }
        }
    })
    .await
    .expect("the upload never succeeded");
}

/// One NFS WRITE: open by id, write, close (see `nfs.rs`'s handle model).
async fn nfs_write(vfs: &Vfs, node: NodeId, offset: u64, data: &[u8]) -> anyhow::Result<()> {
    let h = vfs.open(node).await?;
    let written = vfs.write(h, offset, data).await;
    vfs.close(h).await?;
    written.map(|_| ())
}

#[tokio::test]
async fn a_file_written_while_its_empty_first_upload_lands_stays_writable_by_its_id() {
    let env = VfsTestEnv::new().await;
    let (vfs, album, mut events) = vfs_with_album(&env).await;
    let hook = Arc::new(CreatePauseHook::new());
    vfs.pause_upload_before_settle_for_tests(hook.clone());

    // CREATE: the handle closes at once, the empty draft is queued.
    let (node, h) = vfs.create(album, "IMG_0750.jpg").await.unwrap();
    vfs.close(h).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), hook.parked.notified())
        .await
        .expect("the empty draft never uploaded");

    // The kernel flushes the copied bytes while that upload settles.
    nfs_write(&vfs, node, 0, b"photo number 750").await.expect("WRITE during the upload");
    hook.resume.notify_one();
    upload_succeeded(&mut events).await;

    // SETATTR (fchmod) and any later WRITE still address the same fileid.
    let attr = vfs.getattr(node).await.unwrap().expect("the file vanished under its open fileid");
    assert_eq!(attr.size, 16);
    nfs_write(&vfs, node, 16, b"!").await.expect("a WRITE after the first upload settled");
    let (found, _) = vfs.lookup(album, "IMG_0750.jpg").await.unwrap().expect("no longer found by name");
    assert_eq!(found, node, "the file came back under another id");
}

#[tokio::test]
async fn a_new_file_whose_listing_lags_its_upload_stays_writable_by_its_id() {
    let env = VfsTestEnv::new().await;
    let (vfs, album, mut events) = vfs_with_album(&env).await;

    let (node, h) = vfs.create(album, "IMG_0750.jpg").await.unwrap();
    vfs.close(h).await.unwrap();
    upload_succeeded(&mut events).await;

    // The server took the upload, its listing does not show it yet: the
    // kernel's late WRITE must still reach the file.
    nfs_write(&vfs, node, 0, b"photo number 750").await.expect("a WRITE after the upload landed");
    let attr = vfs.getattr(node).await.unwrap().expect("the file vanished under its open fileid");
    assert_eq!(attr.size, 16);
}

#[tokio::test]
async fn bytes_written_after_the_empty_first_upload_reach_the_server() {
    let env = VfsTestEnv::new().await;
    let (vfs, album, mut events) = vfs_with_album(&env).await;
    env.list_uploaded_files();

    let (node, h) = vfs.create(album, "IMG_0750.jpg").await.unwrap();
    vfs.close(h).await.unwrap();
    upload_succeeded(&mut events).await;
    assert_eq!(env.uploaded_content("IMG_0750.jpg").as_deref(), Some(&b""[..]));

    nfs_write(&vfs, node, 0, b"photo number 750").await.expect("a WRITE after the upload landed");
    upload_succeeded(&mut events).await;
    assert_eq!(env.uploaded_content("IMG_0750.jpg").as_deref(), Some(&b"photo number 750"[..]));
}

#[tokio::test]
async fn a_listing_fetched_before_an_upload_landed_does_not_drop_the_uploaded_file() {
    let env = VfsTestEnv::new().await;
    let (vfs, album, mut events) = vfs_with_album(&env).await;
    let hook = Arc::new(CreatePauseHook::new());
    vfs.pause_upload_before_settle_for_tests(hook.clone());

    let (node, h) = vfs.create(album, "IMG_0750.jpg").await.unwrap();
    vfs.write(h, 0, b"photo number 750").await.unwrap();
    vfs.close(h).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), hook.parked.notified())
        .await
        .expect("the draft never uploaded");

    // A slow listing of the folder (Finder, a LOOKUP past the TTL) leaves
    // before the server has the file...
    env.slow_down_listing("Album", Duration::from_millis(600));
    vfs.remote_changed(&format!("{REMOTE_BASE}/Album/IMG_0751.jpg")).await;
    let before = env.list_request_count();
    let early = {
        let vfs = vfs.clone();
        tokio::spawn(async move { vfs.lookup(album, "IMG_0751.jpg").await })
    };
    while env.list_request_count() == before {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // ...and answers after the upload's own refresh asked again, by which
    // time the server lists the file.
    env.set_remote_files_at("Album", vec![photo_in_album("IMG_0750.jpg", 16, "e-uploaded")]).await;
    hook.resume.notify_one();
    upload_succeeded(&mut events).await;
    early.await.unwrap().unwrap();

    let attr = vfs.getattr(node).await.unwrap().expect("the uploaded file vanished under its fileid");
    assert_eq!(attr.size, 16);
    let (found, _) = vfs.lookup(album, "IMG_0750.jpg").await.unwrap().expect("no longer found by name");
    assert_eq!(found, node, "the file came back under another id");
}
