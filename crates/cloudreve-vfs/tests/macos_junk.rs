//! macOS metadata junk (`._*` AppleDouble sidecars, `.DS_Store`, volume
//! folders) through the `Vfs` facade: local-only names work but never reach
//! the server, reserved names are refused, and junk already on the server
//! stays invisible.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::tree::NodeId;
use cloudreve_vfs::vfs::{CrossOverlayRenameError, JunkNameError, Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, remote_file, VfsTestEnv};
use serde_json::json;

async fn new_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    Arc::new(vfs)
}

/// Every request that would change something on the server.
fn server_writes(env: &VfsTestEnv) -> usize {
    env.upload_session_count()
        + env.create_file_call_count()
        + env.delete_call_count()
        + env.rename_call_count()
        + env.move_call_count()
}

async fn write_new(vfs: &Vfs, parent: NodeId, name: &str, data: &[u8]) {
    let (_, h) = vfs.create(parent, name).await.expect("create should succeed");
    vfs.write(h, 0, data).await.unwrap();
    vfs.close(h).await.unwrap();
}

async fn read_all(vfs: &Vfs, parent: NodeId, name: &str) -> Vec<u8> {
    let (id, attr) = vfs.lookup(parent, name).await.unwrap().expect("entry should resolve");
    let h = vfs.open(id).await.unwrap();
    let data = vfs.read(h, 0, attr.size as u32 + 16).await.unwrap();
    vfs.close(h).await.unwrap();
    data.to_vec()
}

fn listed_names(listing: &[(NodeId, cloudreve_vfs::tree::NodeAttr)]) -> Vec<String> {
    listing.iter().map(|(_, a)| a.name.clone()).collect()
}

#[tokio::test]
async fn an_appledouble_sidecar_is_written_and_read_back_without_reaching_the_server() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("report.pdf", b"%PDF".to_vec(), "e1").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    write_new(&vfs, root, "._report.pdf", b"com.apple.quarantine blob").await;
    vfs.wait_for_writeback_idle().await;

    assert_eq!(read_all(&vfs, root, "._report.pdf").await, b"com.apple.quarantine blob");
    let (_, attr) = vfs.lookup(root, "._report.pdf").await.unwrap().unwrap();
    assert_eq!(attr.size, 25);
    assert_eq!(server_writes(&env), 0, "a sidecar must never be uploaded");
    assert_eq!(env.file_url_request_count("._report.pdf"), 0);
    assert_eq!(vfs.retry_pending_uploads().await, 0, "nothing junk may sit in the upload queue");
}

#[tokio::test]
async fn a_finder_ds_store_is_hidden_from_listings_but_resolves_by_name() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("a.txt", b"a".to_vec(), "e1").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    write_new(&vfs, root, ".DS_Store", b"Bud1").await;

    let names = listed_names(&vfs.readdir(root).await.unwrap());
    assert_eq!(names, vec!["a.txt".to_string()]);
    assert_eq!(read_all(&vfs, root, ".DS_Store").await, b"Bud1");
    vfs.wait_for_writeback_idle().await;
    assert_eq!(server_writes(&env), 0);
}

#[tokio::test]
async fn junk_already_on_the_server_is_neither_listed_nor_resolved() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![
        remote_file("a.txt", 1, "e1"),
        remote_file(".DS_Store", 6148, "e2"),
        remote_file("._a.txt", 4096, "e3"),
        remote_file(".ds_store", 10, "e4"),
        remote_dir("__MACOSX"),
        remote_dir(".Trashes"),
    ])
    .await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    let names = listed_names(&vfs.readdir(root).await.unwrap());
    assert_eq!(names, vec!["a.txt".to_string()]);
    for name in [".DS_Store", "._a.txt", ".ds_store", "__MACOSX", ".Trashes"] {
        assert!(vfs.lookup(root, name).await.unwrap().is_none(), "{name} must not resolve");
    }
}

#[tokio::test]
async fn a_local_ds_store_never_reads_or_replaces_the_server_copy() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file(".DS_Store", b"server bytes".to_vec(), "e1").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    write_new(&vfs, root, ".DS_Store", b"local").await;

    assert_eq!(read_all(&vfs, root, ".DS_Store").await, b"local");
    vfs.wait_for_writeback_idle().await;
    assert_eq!(server_writes(&env), 0);
    assert!(env.download_requests(".DS_Store").is_empty(), "the server copy must never be fetched");
}

#[tokio::test]
async fn reserved_volume_folders_are_refused() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    for name in [".Trashes", ".fseventsd", ".Spotlight-V100", ".TemporaryItems", "__MACOSX"] {
        let err = vfs.mkdir(root, name).await.unwrap_err();
        assert!(err.downcast_ref::<JunkNameError>().is_some(), "mkdir {name}: {err:#}");
        let err = vfs.create(root, name).await.unwrap_err();
        assert!(err.downcast_ref::<JunkNameError>().is_some(), "create {name}: {err:#}");
    }
    // The overlay holds plain files only: a sidecar-named folder is refused.
    let err = vfs.mkdir(root, "._photos").await.unwrap_err();
    assert!(err.downcast_ref::<JunkNameError>().is_some(), "{err:#}");
    assert_eq!(server_writes(&env), 0);
}

#[tokio::test]
async fn renaming_a_sidecar_to_another_sidecar_moves_it_locally() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    write_new(&vfs, root, "._draft.txt", b"tags").await;
    // The destination may already exist: rename replaces it.
    write_new(&vfs, root, "._final.txt", b"old").await;

    vfs.rename(root, "._draft.txt", root, "._final.txt").await.expect("rename should succeed");

    assert!(vfs.lookup(root, "._draft.txt").await.unwrap().is_none());
    assert_eq!(read_all(&vfs, root, "._final.txt").await, b"tags");
    vfs.wait_for_writeback_idle().await;
    assert_eq!(server_writes(&env), 0);
}

#[tokio::test]
async fn renaming_a_sidecar_onto_a_real_name_is_a_cross_device_rename() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    write_new(&vfs, root, "._notes.txt", b"tags").await;

    let err = vfs.rename(root, "._notes.txt", root, "notes.txt").await.unwrap_err();
    assert!(err.downcast_ref::<CrossOverlayRenameError>().is_some(), "{err:#}");
    assert_eq!(read_all(&vfs, root, "._notes.txt").await, b"tags", "the sidecar must be untouched");
    vfs.wait_for_writeback_idle().await;
    assert_eq!(server_writes(&env), 0);
}

#[tokio::test]
async fn renaming_a_real_file_onto_a_junk_name_is_refused() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("a.txt", b"real".to_vec(), "e1").await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    for target in [".DS_Store", "._a.txt", ".Trashes"] {
        let err = vfs.rename(root, "a.txt", root, target).await.unwrap_err();
        assert!(err.downcast_ref::<JunkNameError>().is_some(), "{target}: {err:#}");
    }
    assert_eq!(server_writes(&env), 0);
    assert!(vfs.lookup(root, "a.txt").await.unwrap().is_some());
}

#[tokio::test]
async fn deleting_a_sidecar_forgets_it_without_a_server_call() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    write_new(&vfs, root, "._a.txt", b"tags").await;
    let (id, _) = vfs.lookup(root, "._a.txt").await.unwrap().unwrap();

    vfs.unlink(root, "._a.txt").await.expect("unlink should succeed");

    assert!(vfs.lookup(root, "._a.txt").await.unwrap().is_none());
    assert!(vfs.getattr(id).await.unwrap().is_none(), "the old node must not resolve either");
    assert_eq!(server_writes(&env), 0);
}

#[tokio::test]
async fn truncating_a_sidecar_through_its_handle_resizes_it() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    write_new(&vfs, root, "._a.txt", b"0123456789").await;

    let (id, _) = vfs.lookup(root, "._a.txt").await.unwrap().unwrap();
    let h = vfs.open(id).await.unwrap();
    vfs.truncate(h, 4).await.unwrap();
    vfs.write(h, 6, b"xy").await.unwrap();
    vfs.close(h).await.unwrap();

    assert_eq!(read_all(&vfs, root, "._a.txt").await, b"0123\0\0xy");
}

/// The rmdir trap: a folder whose only content is junk (Finder's
/// `.DS_Store` on the server, a sidecar in the overlay) lists as empty to
/// every tool, so `rm -rf` ends with an rmdir that must succeed.
#[tokio::test]
async fn a_folder_holding_only_junk_can_be_removed() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    let mut ds = remote_file(".DS_Store", 6148, "e1");
    ds["path"] = json!(format!("{}/docs/.DS_Store", common::REMOTE_BASE));
    env.set_remote_files_at("docs", vec![ds]).await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (docs, _) = vfs.lookup(root, "docs").await.unwrap().unwrap();
    write_new(&vfs, docs, "._gone.txt", b"tags").await;

    vfs.unlink(root, "docs").await.expect("rmdir of a junk-only folder must succeed");
    assert_eq!(env.delete_call_count(), 1);
    assert_eq!(env.last_deleted_uris(), vec![common::uri_of("docs")]);

    // A new folder of the same name starts empty: the old sidecar went
    // with the folder it belonged to.
    let docs = vfs.mkdir(root, "docs").await.unwrap();
    assert!(vfs.lookup(docs, "._gone.txt").await.unwrap().is_none());
}

/// Finder writes a `.DS_Store` into every folder it shows and the kernel
/// writes sidecars next to files: renaming the folder must keep them, and
/// must not be refused as busy because of them.
#[tokio::test]
async fn renaming_a_folder_carries_its_sidecars_along() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    let mut a = remote_file("a.txt", 1, "e1");
    a["path"] = json!(format!("{}/docs/a.txt", common::REMOTE_BASE));
    env.set_remote_files_at("docs", vec![a]).await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (docs, _) = vfs.lookup(root, "docs").await.unwrap().unwrap();
    write_new(&vfs, docs, "._a.txt", b"tags").await;
    write_new(&vfs, docs, ".DS_Store", b"Bud1").await;

    vfs.rename(root, "docs", root, "papers").await.expect("folder rename should succeed");

    let (papers, _) = vfs.lookup(root, "papers").await.unwrap().unwrap();
    assert_eq!(read_all(&vfs, papers, "._a.txt").await, b"tags");
    assert_eq!(read_all(&vfs, papers, ".DS_Store").await, b"Bud1");
    assert_eq!(env.rename_call_count(), 1);
    assert_eq!(env.upload_session_count(), 0);
}

/// A lock file deleted while some process still holds it open — on Linux
/// a FUSE `release` can arrive after the app's next request, and a helper
/// process that inherited the descriptor keeps it open longer still. The
/// "busy" guard exists so a deleted file's pending upload cannot bring it
/// back; local-only junk never uploads, so the delete must just succeed.
#[tokio::test]
async fn an_open_lock_file_can_be_deleted() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    write_new(&vfs, root, ".~lock.Budget.ods#", b",ana,laptop;").await;
    let (id, _) = vfs.lookup(root, ".~lock.Budget.ods#").await.unwrap().unwrap();
    let held = vfs.open(id).await.unwrap();

    tokio::time::timeout(Duration::from_millis(500), vfs.unlink(root, ".~lock.Budget.ods#"))
        .await
        .expect("deleting an open lock file must not wait for it to close")
        .expect("deleting an open lock file must succeed");
    assert!(vfs.lookup(root, ".~lock.Budget.ods#").await.unwrap().is_none());

    vfs.close(held).await.unwrap();
    assert_eq!(server_writes(&env), 0);
}
