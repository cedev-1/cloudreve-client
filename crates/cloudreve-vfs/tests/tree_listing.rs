mod common;
use common::{remote_dir, remote_file, VfsTestEnv};
use cloudreve_vfs::tree::VfsTree;

/// Mounting must be instant on huge drives: only the directory being read
/// is listed, never the whole tree.
#[tokio::test]
async fn subdirectories_are_listed_only_when_first_read() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("photos"), remote_file("readme.txt", 12, "e1")]).await;

    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    let root = tree.readdir(tree.root()).await.unwrap();

    let names: Vec<_> = root.iter().map(|(_, a)| a.name.as_str()).collect();
    assert_eq!(names, vec!["photos", "readme.txt"]);
    assert_eq!(env.list_request_count(), 1, "only the root may have been listed");

    // Reading the subdirectory triggers exactly one more listing.
    let (photos_id, attr) = tree.lookup(tree.root(), "photos").await.unwrap().unwrap();
    assert!(attr.is_dir);
    env.set_remote_files_at("photos", vec![remote_file("cat.jpg", 4096, "e2")]).await;
    let photos = tree.readdir(photos_id).await.unwrap();
    assert_eq!(photos[0].1.name, "cat.jpg");
    assert_eq!(photos[0].1.size, 4096);
    assert_eq!(env.list_request_count(), 2);
}

/// Same node asked twice gets the same id — frontends cache by NodeId.
#[tokio::test]
async fn node_ids_are_stable_across_lookups() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    tree.readdir(tree.root()).await.unwrap();
    let first = tree.lookup(tree.root(), "a.txt").await.unwrap().unwrap().0;
    let second = tree.lookup(tree.root(), "a.txt").await.unwrap().unwrap().0;
    assert_eq!(first, second);
}

/// Finder calls readdir in bursts; each burst must cost one HTTP call.
#[tokio::test]
async fn a_fresh_listing_is_not_refetched() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    tree.readdir(tree.root()).await.unwrap();
    tree.readdir(tree.root()).await.unwrap();
    tree.readdir(tree.root()).await.unwrap();
    assert_eq!(env.list_request_count(), 1, "a fresh listing must be served from memory");
}

/// A server-side change (SSE, phase 4) must become visible immediately.
#[tokio::test]
async fn invalidating_a_path_forces_a_refetch() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    tree.readdir(tree.root()).await.unwrap();

    env.set_remote_files(vec![remote_file("a.txt", 99, "e2")]).await;
    tree.invalidate_path(&common::uri_of("a.txt")).await;

    let listing = tree.readdir(tree.root()).await.unwrap();
    assert_eq!(listing[0].1.size, 99, "stale attributes served after invalidation");
    assert_eq!(env.list_request_count(), 2);
}
