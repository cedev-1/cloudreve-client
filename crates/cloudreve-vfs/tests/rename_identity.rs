//! A rename keeps the entry's identity: the `NodeId` the kernel holds
//! (NFS fileid / FUSE inode) follows the file to its new name, so an app
//! that renames a file it is still writing — a downloader's `x.part`, an
//! editor's save — keeps reaching the same file instead of a dead id.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::nfs::VfsNfs;
use cloudreve_vfs::tree::NodeId;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, remote_file, VfsTestEnv};
use nfs3_server::nfs3_types::nfs3::{filename3, sattr3, stable_how};
use nfs3_server::vfs::{NfsFileSystem, NfsReadFileSystem};
use serde_json::json;

/// Uploads are held back (5s debounce) until the test is done renaming: the
/// mock upload endpoint does not add what it receives to the listing, so a
/// file uploaded mid-test would vanish from the next re-list.
async fn new_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_secs(5));
    Arc::new(vfs)
}

async fn read_by_id(vfs: &Vfs, id: NodeId) -> Vec<u8> {
    let attr = vfs.getattr(id).await.unwrap().expect("the node must resolve");
    let h = vfs.open(id).await.unwrap();
    let data = vfs.read(h, 0, attr.size as u32 + 16).await.unwrap();
    vfs.close(h).await.unwrap();
    data.to_vec()
}

async fn write_by_id(vfs: &Vfs, id: NodeId, offset: u64, data: &[u8]) {
    let h = vfs.open(id).await.unwrap();
    vfs.write(h, offset, data).await.unwrap();
    vfs.close(h).await.unwrap();
}

#[tokio::test]
async fn a_renamed_file_keeps_its_node_id_and_resolves_under_the_new_name() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("a.txt", b"hello".to_vec(), "e1").await;
    env.serve_file_content("b.txt", b"hello").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (id, _) = vfs.lookup(root, "a.txt").await.unwrap().unwrap();

    vfs.rename(root, "a.txt", root, "b.txt").await.unwrap();

    let attr = vfs.getattr(id).await.unwrap().expect("the old id must still resolve");
    assert_eq!(attr.name, "b.txt");
    assert_eq!(attr.remote_path, common::uri_of("b.txt"));
    assert_eq!(vfs.lookup(root, "b.txt").await.unwrap().unwrap().0, id);
    assert!(vfs.lookup(root, "a.txt").await.unwrap().is_none());
    assert_eq!(read_by_id(&vfs, id).await, b"hello");
}

/// A write that reaches the old id after the rename (the kernel keeps
/// writing by fileid) lands in the renamed file and uploads there.
#[tokio::test]
async fn a_write_through_the_old_node_id_after_a_rename_uploads_to_the_new_path() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("a.txt", b"hello".to_vec(), "e1").await;
    env.serve_file_content("b.txt", b"hello").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (id, _) = vfs.lookup(root, "a.txt").await.unwrap().unwrap();

    vfs.rename(root, "a.txt", root, "b.txt").await.unwrap();
    write_by_id(&vfs, id, 5, b" world").await;
    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.uploaded_content("b.txt").as_deref(), Some(&b"hello world"[..]));
    assert_eq!(env.uploaded_content("a.txt"), None);
}

/// The downloader idiom on a file that never reached the server yet:
/// write `x.part`, rename it to `x` while still writing.
#[tokio::test]
async fn a_part_file_renamed_mid_write_uploads_every_byte_under_its_final_name() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (id, h) = vfs.create(root, "movie.mkv.part").await.unwrap();
    vfs.write(h, 0, b"first half,").await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.rename(root, "movie.mkv.part", root, "movie.mkv").await.unwrap();
    write_by_id(&vfs, id, 11, b" second half").await;

    assert_eq!(vfs.getattr(id).await.unwrap().unwrap().size, 23);
    assert_eq!(vfs.lookup(root, "movie.mkv").await.unwrap().unwrap().0, id);
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("movie.mkv").as_deref(), Some(&b"first half, second half"[..]));
    assert_eq!(env.uploaded_content("movie.mkv.part"), None);
}

#[tokio::test]
async fn renaming_a_folder_keeps_the_node_ids_of_the_folder_and_its_children() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    let mut a = remote_file("a.txt", 1, "e1");
    a["path"] = json!(format!("{}/docs/a.txt", common::REMOTE_BASE));
    env.set_remote_files_at("docs", vec![a]).await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (docs, _) = vfs.lookup(root, "docs").await.unwrap().unwrap();
    let (a_id, _) = vfs.lookup(docs, "a.txt").await.unwrap().unwrap();

    vfs.rename(root, "docs", root, "papers").await.unwrap();
    // What the server lists under the folder's new name.
    let mut a = remote_file("a.txt", 1, "e1");
    a["path"] = json!(format!("{}/papers/a.txt", common::REMOTE_BASE));
    env.set_remote_files_at("papers", vec![a]).await;

    assert_eq!(vfs.getattr(docs).await.unwrap().unwrap().remote_path, common::uri_of("papers"));
    assert_eq!(vfs.getattr(a_id).await.unwrap().unwrap().remote_path, common::uri_of("papers/a.txt"));
    assert_eq!(vfs.lookup(root, "papers").await.unwrap().unwrap().0, docs);
    assert_eq!(vfs.lookup(docs, "a.txt").await.unwrap().unwrap().0, a_id);
    let listed: Vec<NodeId> = vfs.readdir(docs).await.unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(listed, vec![a_id]);
}

/// Rename over an existing file: the moved file keeps its id, and the
/// replaced file's old id stops resolving — it must never alias the file
/// that replaced it.
#[tokio::test]
async fn renaming_over_an_existing_file_retires_the_destination_id() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("new.txt", b"fresh".to_vec(), "e1").await;
    env.add_remote_file("old.txt", b"stale".to_vec(), "e2").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (moved, _) = vfs.lookup(root, "new.txt").await.unwrap().unwrap();
    let (replaced, _) = vfs.lookup(root, "old.txt").await.unwrap().unwrap();

    vfs.rename(root, "new.txt", root, "old.txt").await.unwrap();
    env.serve_file_content("old.txt", b"fresh").await;

    assert!(vfs.getattr(replaced).await.unwrap().is_none(), "the replaced file's id must retire");
    assert_eq!(vfs.lookup(root, "old.txt").await.unwrap().unwrap().0, moved);
    assert_eq!(read_by_id(&vfs, moved).await, b"fresh");
}

/// The editor atomic save: write a temp file, rename it over the target.
/// The target name now resolves to the temp file's node with the new
/// content and size, right away — no stale size from the replaced file.
#[tokio::test]
async fn an_editor_atomic_save_shows_the_new_content_under_the_target_name() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("doc.txt", b"v1".to_vec(), "e1").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (target, _) = vfs.lookup(root, "doc.txt").await.unwrap().unwrap();
    let (tmp, h) = vfs.create(root, ".doc.txt.swp").await.unwrap();
    vfs.write(h, 0, b"version two").await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.rename(root, ".doc.txt.swp", root, "doc.txt").await.unwrap();

    let (id, attr) = vfs.lookup(root, "doc.txt").await.unwrap().unwrap();
    assert_eq!(id, tmp);
    assert_eq!(attr.size, 11);
    assert_eq!(read_by_id(&vfs, id).await, b"version two");
    assert!(vfs.getattr(target).await.unwrap().is_none(), "the replaced target's id must retire");
    assert!(vfs.lookup(root, ".doc.txt.swp").await.unwrap().is_none());
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("doc.txt").as_deref(), Some(&b"version two"[..]));
}

#[tokio::test]
async fn a_renamed_sidecar_keeps_its_node_id() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (id, h) = vfs.create(root, "._a.txt").await.unwrap();
    vfs.write(h, 0, b"tags").await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.rename(root, "._a.txt", root, "._b.txt").await.unwrap();

    assert_eq!(vfs.lookup(root, "._b.txt").await.unwrap().unwrap().0, id);
    assert_eq!(read_by_id(&vfs, id).await, b"tags");
}

/// The same downloader idiom driven through the NFS adapter: the kernel
/// renames by name, then keeps writing by the fileid it already holds.
#[tokio::test]
async fn nfs_writes_by_the_old_fileid_after_a_rename_reach_the_renamed_file() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let fs = VfsNfs::new(vfs.clone());
    let root = fs.root_dir();
    let name = |s: &str| -> filename3<'static> { s.as_bytes().to_vec().into() };

    let (fid, _) = fs.create(&root, &name("dl.part"), sattr3::default()).await.unwrap();
    fs.write(&fid, 0, b"0123", stable_how::UNSTABLE).await.unwrap();
    fs.rename(&root, &name("dl.part"), &root, &name("dl.bin")).await.unwrap();
    let (attr, _) = fs.write(&fid, 4, b"4567", stable_how::UNSTABLE).await.expect("write by the old fileid");

    assert_eq!(attr.size, 8);
    assert_eq!(fs.lookup(&root, &name("dl.bin")).await.unwrap().as_u64(), fid.as_u64());
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("dl.bin").as_deref(), Some(&b"01234567"[..]));
    assert_eq!(env.uploaded_content("dl.part"), None);
}

/// NFS dispatches every RPC on its own task: a WRITE by fileid can arrive
/// while the RENAME of the same file is still talking to the server. The
/// open behind that WRITE resolved the old path, then waited on the
/// rename's lock; it must follow the file to its new path, never register
/// a handle under the old one (whose upload would resurrect `a.txt`).
#[tokio::test(flavor = "multi_thread")]
async fn an_open_racing_a_rename_of_the_same_file_follows_it_to_the_new_path() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("a.txt", b"hello".to_vec(), "e1").await;
    env.serve_file_content("b.txt", b"hello").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (id, _) = vfs.lookup(root, "a.txt").await.unwrap().unwrap();
    env.slow_down_renames(Duration::from_millis(500));

    let renaming = {
        let vfs = vfs.clone();
        tokio::spawn(async move { vfs.rename(root, "a.txt", root, "b.txt").await })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while env.rename_call_count() == 0 {
        assert!(std::time::Instant::now() < deadline, "the rename never reached the server");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    write_by_id(&vfs, id, 5, b" world").await;
    renaming.await.unwrap().unwrap();
    vfs.wait_for_writeback_idle().await;

    assert_eq!(env.uploaded_content("b.txt").as_deref(), Some(&b"hello world"[..]));
    assert_eq!(env.uploaded_content("a.txt"), None, "nothing may be uploaded under the old name");
}

/// A move into another folder: the old folder's next re-list must not
/// prune the moved node as a ghost.
#[tokio::test]
async fn moving_a_file_to_another_folder_keeps_its_node_id() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("a.txt", b"hello".to_vec(), "e1").await;
    env.set_remote_files(vec![remote_file("a.txt", 5, "e1"), remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![]).await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (id, _) = vfs.lookup(root, "a.txt").await.unwrap().unwrap();
    let (docs, _) = vfs.lookup(root, "docs").await.unwrap().unwrap();
    vfs.readdir(docs).await.unwrap();

    vfs.rename(root, "a.txt", docs, "a.txt").await.unwrap();
    let mut moved = remote_file("a.txt", 5, "e1");
    moved["path"] = json!(format!("{}/docs/a.txt", common::REMOTE_BASE));
    env.set_remote_files_at("docs", vec![moved]).await;
    vfs.readdir(root).await.unwrap();

    assert_eq!(vfs.getattr(id).await.unwrap().unwrap().remote_path, common::uri_of("docs/a.txt"));
    assert_eq!(vfs.lookup(docs, "a.txt").await.unwrap().unwrap().0, id);
}
