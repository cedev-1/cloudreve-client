//! A file created on the drive but not uploaded yet (offline, server down,
//! app quit within the debounce) lives only in its draft. After a restart
//! the tree is rebuilt from the server listing, which has never heard of
//! it: the file must still be there, and creating it again must not
//! truncate the draft holding its only copy.

mod common;

use std::time::Duration;

use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, VfsTestEnv};

fn open_vfs(env: &VfsTestEnv) -> Vfs {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    vfs
}

const CONTENT: &[u8] = b"draft never uploaded: the only copy";

#[tokio::test]
async fn a_never_uploaded_file_is_still_listed_after_a_restart() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![]).await;
    {
        let vfs = open_vfs(&env);
        let root = vfs.tree().root();
        let (_n, h) = vfs.create(root, "todo.txt").await.unwrap();
        vfs.write(h, 0, CONTENT).await.unwrap();
        vfs.close(h).await.unwrap();
        let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;
        let (_n, h) = vfs.create(docs, "plan.txt").await.unwrap();
        vfs.write(h, 0, CONTENT).await.unwrap();
        vfs.close(h).await.unwrap();
        vfs.shutdown();
    }

    let vfs = open_vfs(&env);
    let root = vfs.tree().root();
    let listing = vfs.readdir(root).await.unwrap();
    let todo = listing.iter().find(|(_, a)| a.name == "todo.txt").expect("todo.txt vanished after restart");
    assert_eq!(todo.1.size, CONTENT.len() as u64);

    let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;
    let (plan, attr) = vfs.lookup(docs, "plan.txt").await.unwrap().expect("docs/plan.txt vanished after restart");
    assert_eq!(attr.size, CONTENT.len() as u64);
    let h = vfs.open(plan).await.unwrap();
    assert_eq!(vfs.read(h, 0, 100).await.unwrap().as_ref(), CONTENT);
    vfs.close(h).await.unwrap();
    vfs.shutdown();
}

/// Before the fix, nothing listed the file, so `create` passed its EEXIST
/// check and `DraftStore::begin` truncated the draft: the content was gone.
#[tokio::test]
async fn creating_it_again_after_a_restart_does_not_truncate_the_draft() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    {
        let vfs = open_vfs(&env);
        let root = vfs.tree().root();
        let (_n, h) = vfs.create(root, "todo.txt").await.unwrap();
        vfs.write(h, 0, CONTENT).await.unwrap();
        vfs.close(h).await.unwrap();
        vfs.shutdown();
    }

    let vfs = open_vfs(&env);
    let root = vfs.tree().root();
    let err = vfs.create(root, "todo.txt").await.expect_err("the file exists: EEXIST");
    assert!(format!("{err:#}").contains("EEXIST"), "{err:#}");
    let (node, attr) = vfs.lookup(root, "todo.txt").await.unwrap().unwrap();
    assert_eq!(attr.size, CONTENT.len() as u64);
    let h = vfs.open(node).await.unwrap();
    assert_eq!(vfs.read(h, 0, 100).await.unwrap().as_ref(), CONTENT);
    vfs.close(h).await.unwrap();
    vfs.shutdown();
}
