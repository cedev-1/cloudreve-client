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

/// A file deleted on the server must vanish — and not just from readdir:
/// its attrs must be forgotten too, or the maps grow forever under churn.
#[tokio::test]
async fn a_file_deleted_remotely_disappears_after_invalidation() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1"), remote_file("gone.txt", 1, "e2")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    let (gone_id, _) = tree.lookup(tree.root(), "gone.txt").await.unwrap().unwrap();

    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1")]).await;
    // Invalidate an unrelated (never-listed) sibling path, not `gone.txt`
    // itself: this only forces the root's cached listing stale — it must
    // NOT be `invalidate_path`'s own targeted per-entry removal (see its
    // second block) that makes `gone.txt`'s attrs disappear. That would
    // mask the real bug under test: the general prune belongs in
    // `ensure_listed`'s refresh path, triggered by ANY re-list, not just
    // one that happens to name the vanished entry directly.
    tree.invalidate_path(&common::uri_of("unrelated.txt")).await;

    assert!(tree.lookup(tree.root(), "gone.txt").await.unwrap().is_none());
    assert!(tree.getattr(gone_id).await.unwrap().is_none(),
        "the ghost's attrs survived the re-list");
}

/// Deleting a whole directory remotely must not just unlink it from its
/// parent: every already-listed descendant's bookkeeping (`attrs`,
/// `children`, `listed_at`, `known_children`) must go with it, or the
/// subtree orphans in memory forever, unreachable yet still resident.
#[tokio::test]
async fn a_deleted_directory_takes_its_whole_subtree_with_it() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("photos"), remote_file("keep.txt", 1, "e1")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    let (photos_id, _) = tree.lookup(tree.root(), "photos").await.unwrap().unwrap();

    env.set_remote_files_at("photos", vec![remote_file("cat.jpg", 4096, "e2")]).await;
    let photos_listing = tree.readdir(photos_id).await.unwrap();
    let cat_id = photos_listing[0].0;

    // Server drops `photos` — and everything under it — entirely.
    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1")]).await;
    // Invalidate an unrelated sibling, not `photos` itself, so this exercises
    // `ensure_listed`'s own subtree prune rather than `invalidate_path`'s
    // targeted per-entry removal (see the comment in the test above).
    tree.invalidate_path(&common::uri_of("unrelated.txt")).await;
    tree.readdir(tree.root()).await.unwrap();

    assert!(
        tree.getattr(photos_id).await.unwrap().is_none(),
        "the deleted directory's own attrs survived"
    );
    assert!(
        tree.getattr(cat_id).await.unwrap().is_none(),
        "a descendant's attrs survived its parent's deletion"
    );
}

/// `invalidate_path` on a deleted entry's own path must remove it
/// immediately, without waiting for the parent's next re-list.
#[tokio::test]
async fn invalidating_a_deleted_files_own_path_removes_it() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1"), remote_file("gone.txt", 1, "e2")]).await;
    let tree = VfsTree::new(env.client(), common::REMOTE_BASE.into());
    let (gone_id, _) = tree.lookup(tree.root(), "gone.txt").await.unwrap().unwrap();

    env.set_remote_files(vec![remote_file("keep.txt", 1, "e1")]).await;
    tree.invalidate_path(&common::uri_of("gone.txt")).await;

    // Checked BEFORE any lookup/readdir on the parent: this must be
    // `invalidate_path`'s own targeted removal taking effect immediately,
    // not a side effect of the general re-list prune (which only runs once
    // the parent is actually re-listed).
    assert!(
        tree.getattr(gone_id).await.unwrap().is_none(),
        "invalidate_path did not remove the entry's own attrs immediately"
    );
    assert!(tree.lookup(tree.root(), "gone.txt").await.unwrap().is_none());
}
