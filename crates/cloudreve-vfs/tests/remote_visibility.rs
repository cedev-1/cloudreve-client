//! Remote changes must become visible through a mount that caches. Apple's
//! NFS client only drops a directory's cached names, negative entries and
//! READDIR buffers when that directory's mtime changes
//! (`kext/nfs_vnops.c`, `nfs_getattr`: `NFS_CHANGED_NC` -> `cache_purge`,
//! `NFS_CHANGED` -> `nfs_invaldir`; for NFSv3 both compare the mtime). So
//! when a directory's listing changes on the server, the mtime the adapter
//! reports for it must move — once, forward, and never on a re-list that
//! found nothing new (that would purge the kernel's caches for nothing,
//! and a value that flaps is what fuse-t #109's panic is made of). No
//! mount needed: these drive the NFS adapter the way the kernel does.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::nfs::VfsNfs;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, VfsTestEnv, REMOTE_BASE};
use nfs3_server::nfs3_types::nfs3::nfstime3;
use nfs3_server::vfs::{FileHandleU64, NfsReadFileSystem};
use serde_json::{json, Value};

fn new_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    let (vfs, _rx) =
        Vfs::new(env.client(), REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES).unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    Arc::new(vfs)
}

fn file_in(dir: &str, name: &str, size: i64, etag: &str) -> Value {
    json!({
        "type": 0,
        "id": format!("file-{dir}-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": size,
        "path": format!("{REMOTE_BASE}/{dir}/{name}"),
        "primary_entity": etag,
    })
}

fn uri(rel: &str) -> String {
    format!("{REMOTE_BASE}/{rel}")
}

/// `docs/` (listed, as the kernel would after a READDIRPLUS) under a root
/// holding only it. Returns the adapter and `docs/`'s file handle.
async fn browsed_docs(env: &VfsTestEnv, files: Vec<Value>) -> (Arc<Vfs>, VfsNfs, FileHandleU64) {
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", files).await;
    let vfs = new_vfs(env);
    let (docs, _) = vfs.lookup(vfs.tree().root(), "docs").await.unwrap().expect("docs/");
    vfs.readdir(docs).await.unwrap();
    (vfs.clone(), VfsNfs::new(vfs), FileHandleU64::new(docs.0))
}

async fn mtime(nfs: &VfsNfs, h: &FileHandleU64) -> (u32, u32) {
    let nfstime3 { seconds, nseconds } = nfs.getattr(h).await.expect("getattr").mtime;
    (seconds, nseconds)
}

#[tokio::test]
async fn a_folder_mtime_moves_forward_when_a_file_is_added_remotely() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1")]).await;
    let before = mtime(&nfs, &docs).await;

    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 1, "e1"), file_in("docs", "b.txt", 2, "e2")])
        .await;
    vfs.remote_changed(&uri("docs/b.txt")).await;

    assert!(mtime(&nfs, &docs).await > before, "the kernel would keep serving its cached listing of docs/");
}

#[tokio::test]
async fn a_folder_mtime_moves_forward_when_a_file_is_deleted_remotely() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) =
        browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1"), file_in("docs", "b.txt", 2, "e2")]).await;
    let before = mtime(&nfs, &docs).await;

    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 1, "e1")]).await;
    vfs.remote_changed(&uri("docs/b.txt")).await;

    assert!(mtime(&nfs, &docs).await > before);
}

#[tokio::test]
async fn a_folder_mtime_moves_forward_when_a_file_is_renamed_remotely() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "draft.txt", 3, "e1")]).await;
    let before = mtime(&nfs, &docs).await;

    env.set_remote_files_at("docs", vec![file_in("docs", "final.txt", 3, "e1")]).await;
    vfs.remote_changed(&uri("docs/draft.txt")).await;
    vfs.remote_changed(&uri("docs/final.txt")).await;

    assert!(mtime(&nfs, &docs).await > before);
}

/// READDIRPLUS hands the kernel every child's attributes along with the
/// names; a child edited remotely makes that cached copy stale too.
#[tokio::test]
async fn a_folder_mtime_moves_forward_when_a_file_in_it_is_modified_remotely() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 5, "e1")]).await;
    let before = mtime(&nfs, &docs).await;

    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 9, "e2")]).await;
    vfs.remote_changed(&uri("docs/a.txt")).await;

    assert!(mtime(&nfs, &docs).await > before);
}

/// Invalidated, re-listed, nothing changed: the mtime must hold still, or
/// every SSE echo of our own upload would purge the kernel's caches.
#[tokio::test]
async fn a_folder_mtime_holds_still_when_a_re_list_finds_nothing_new() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1")]).await;
    let before = mtime(&nfs, &docs).await;
    let lists = env.list_request_count();

    vfs.remote_changed(&uri("docs/a.txt")).await;
    let after = mtime(&nfs, &docs).await;

    assert!(env.list_request_count() > lists, "the invalidated folder was not re-listed");
    assert_eq!(after, before);
}

/// Re-listing the PARENT reports the folder with the server's own
/// `updated_at`, which Cloudreve does not move for a child change. That
/// must not walk the folder's mtime back to the old value (two changes in
/// a row would then look like none to the kernel).
#[tokio::test]
async fn re_listing_the_parent_never_moves_a_folder_mtime_backwards() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1")]).await;
    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 1, "e1"), file_in("docs", "b.txt", 2, "e2")])
        .await;
    vfs.remote_changed(&uri("docs/b.txt")).await;
    let bumped = mtime(&nfs, &docs).await;

    vfs.remote_changed(&uri("docs")).await; // the root is re-listed
    vfs.readdir(vfs.tree().root()).await.unwrap();

    assert!(mtime(&nfs, &docs).await >= bumped);
}

/// The root folder has no parent listing to come from, but its own
/// listing changes like any other.
#[tokio::test]
async fn the_root_mtime_moves_forward_when_a_file_is_added_at_the_root() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![common::remote_file("a.txt", 1, "e1")]).await;
    let vfs = new_vfs(&env);
    let root = vfs.tree().root();
    vfs.readdir(root).await.unwrap();
    let nfs = VfsNfs::new(vfs.clone());
    let h = FileHandleU64::new(root.0);
    let before = mtime(&nfs, &h).await;

    env.set_remote_files(vec![common::remote_file("a.txt", 1, "e1"), common::remote_file("b.txt", 1, "e2")])
        .await;
    vfs.remote_changed(&common::uri_of("b.txt")).await;

    assert!(mtime(&nfs, &h).await > before);
}

/// With a short attribute cache the kernel GETATTRs a folder about once a
/// second while anything touches it. That must stay in memory: a fresh
/// listing costs no request, an invalidated one costs exactly one.
#[tokio::test]
async fn getattr_on_a_folder_lists_it_only_once_after_an_invalidation() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1")]).await;
    let lists = env.list_request_count();
    for _ in 0..20 {
        mtime(&nfs, &docs).await;
    }
    assert_eq!(env.list_request_count(), lists, "a fresh folder must be served from memory");

    vfs.remote_changed(&uri("docs/a.txt")).await;
    for _ in 0..20 {
        mtime(&nfs, &docs).await;
    }
    assert_eq!(env.list_request_count(), lists + 1);
}

/// A folder nobody ever opened has nothing cached in the kernel to
/// refresh: its GETATTR (Finder stats every subfolder it shows) must not
/// cost a listing.
#[tokio::test]
async fn getattr_on_a_folder_never_opened_does_not_list_it() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 1, "e1")]).await;
    let vfs = new_vfs(&env);
    let (docs, _) = vfs.lookup(vfs.tree().root(), "docs").await.unwrap().unwrap();
    let nfs = VfsNfs::new(vfs.clone());
    let lists = env.list_request_count();

    vfs.remote_changed(&uri("docs/a.txt")).await;
    mtime(&nfs, &FileHandleU64::new(docs.0)).await;

    assert_eq!(env.list_request_count(), lists);
}

/// Two remote changes within the same second are two changes: the second
/// must move the mtime again (sub-second part included on the wire), or
/// the kernel keeps the listing it fetched after the first.
#[tokio::test]
async fn two_remote_changes_in_the_same_second_both_move_the_folder_mtime() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1")]).await;

    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 1, "e1"), file_in("docs", "b.txt", 2, "e2")])
        .await;
    vfs.remote_changed(&uri("docs/b.txt")).await;
    let first = mtime(&nfs, &docs).await;
    env.set_remote_files_at(
        "docs",
        vec![file_in("docs", "a.txt", 1, "e1"), file_in("docs", "b.txt", 2, "e2"), file_in("docs", "c.txt", 3, "e3")],
    )
    .await;
    vfs.remote_changed(&uri("docs/c.txt")).await;
    let second = mtime(&nfs, &docs).await;

    assert!(second > first, "{second:?} must be past {first:?}");
}

/// A new SSE subscription invalidates the drive's root path itself (events
/// may have been missed). Files added meanwhile must still move the root's
/// mtime: the folder's own invalidation keeps what it last listed to diff
/// the fresh listing against.
#[tokio::test]
async fn invalidating_a_folder_itself_still_moves_its_mtime_when_its_children_changed() {
    let env = VfsTestEnv::new().await;
    let (vfs, nfs, docs) = browsed_docs(&env, vec![file_in("docs", "a.txt", 1, "e1")]).await;
    let before = mtime(&nfs, &docs).await;

    env.set_remote_files_at("docs", vec![file_in("docs", "a.txt", 1, "e1"), file_in("docs", "b.txt", 2, "e2")])
        .await;
    vfs.remote_changed(&uri("docs")).await;

    assert!(mtime(&nfs, &docs).await > before);
}
