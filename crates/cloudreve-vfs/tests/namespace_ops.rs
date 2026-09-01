//! Task 10: mkdir, unlink, rename through the `Vfs` facade.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, remote_file, VfsTestEnv};

/// A created folder is visible in a listing immediately — no separate
/// round-trip beyond the `create_file` call itself and the relist it forces.
#[tokio::test]
async fn mkdir_lists_immediately() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("existing.txt", 1, "e1")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    let new_id = vfs.mkdir(root, "photos").await.expect("mkdir should succeed");
    assert_eq!(env.create_file_call_count(), 1);

    let listing = vfs.readdir(root).await.unwrap();
    let (_, attr) = listing
        .into_iter()
        .find(|(id, _)| *id == new_id)
        .expect("the newly created folder must be visible in readdir immediately");
    assert_eq!(attr.name, "photos");
    assert!(attr.is_dir, "mkdir must create a directory, not a file");
}

/// Unlinking a real remote file removes it from the listing and hits the
/// delete API exactly once, with the right uri.
#[tokio::test]
async fn unlink_removes_the_entry_and_hits_the_delete_api() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    let content = b"delete me".to_vec();
    env.add_remote_file("doomed.txt", content, "e1").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    vfs.unlink(root, "doomed.txt").await.expect("unlink should succeed");

    assert_eq!(env.delete_call_count(), 1, "unlinking a real remote file must hit the delete API");
    assert_eq!(env.last_deleted_uris(), vec![common::uri_of("doomed.txt")]);

    assert!(
        vfs.lookup(root, "doomed.txt").await.unwrap().is_none(),
        "the entry must be gone from the listing"
    );
}

/// A same-directory rename hits `rename_file` exactly once and the listing
/// reflects the new name (and not the old one) afterwards.
#[tokio::test]
async fn same_dir_rename_hits_the_api_and_updates_the_listing() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.add_remote_file("before.txt", b"hello".to_vec(), "e1").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    vfs.rename(root, "before.txt", root, "after.txt").await.expect("rename should succeed");

    assert_eq!(env.rename_call_count(), 1);
    assert_eq!(
        env.last_rename(),
        Some((common::uri_of("before.txt"), "after.txt".to_string()))
    );
    assert_eq!(env.move_call_count(), 0, "a same-directory rename must never call move_files");

    assert!(
        vfs.lookup(root, "before.txt").await.unwrap().is_none(),
        "the old name must no longer resolve"
    );
    let (_, attr) = vfs
        .lookup(root, "after.txt")
        .await
        .unwrap()
        .expect("the new name must resolve after the rename");
    assert_eq!(attr.name, "after.txt");
}

/// A cross-directory rename that also changes the leaf name calls
/// `move_files` (to relocate) THEN `rename_file` (to relabel, operating on
/// the entry's post-move uri) — and the final listing state reflects both:
/// gone from the source directory, present under the new name in the
/// destination.
#[tokio::test]
async fn cross_dir_rename_with_a_new_leaf_name_moves_then_renames() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![
        remote_file("doc.txt", 5, "e1"),
        remote_dir("archive"),
    ])
    .await;
    env.serve_file_content("doc.txt", b"hello").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let archive = vfs.lookup(root, "archive").await.unwrap().unwrap().0;

    vfs.rename(root, "doc.txt", archive, "renamed.txt")
        .await
        .expect("cross-dir rename with a leaf change should succeed");

    assert_eq!(env.move_call_count(), 1, "the cross-directory relocation must call move_files");
    assert_eq!(env.rename_call_count(), 1, "the leaf-name change must call rename_file");
    let (moved_uris, dst) = env.last_move().expect("move_files must have been called");
    assert_eq!(moved_uris, vec![common::uri_of("doc.txt")]);
    assert_eq!(dst, common::uri_of("archive"));
    let (renamed_uri, new_name) = env.last_rename().expect("rename_file must have been called");
    assert_eq!(
        renamed_uri,
        format!("{}/doc.txt", common::uri_of("archive")),
        "rename_file must operate on the entry's post-move uri, not its pre-move one"
    );
    assert_eq!(new_name, "renamed.txt");

    assert!(vfs.lookup(root, "doc.txt").await.unwrap().is_none(), "gone from the source directory");
    let (_, attr) = vfs
        .lookup(archive, "renamed.txt")
        .await
        .unwrap()
        .expect("present under the new name in the destination");
    assert_eq!(attr.name, "renamed.txt");
}

/// Renaming a file that was `create`d but never uploaded (empty base_etag —
/// nothing exists remotely yet) must never call `rename_file`: there is
/// nothing on the server to rename. The eventual upload must instead target
/// the NEW name only. The file is saved and closed (arming its debounce
/// timer) BEFORE the rename, exactly the sequence a frontend renaming an
/// already-saved-but-not-yet-uploaded file would produce — a wide debounce
/// window keeps the timer alive long enough for the rename to land first.
#[tokio::test]
async fn renaming_a_drafted_new_file_uploads_to_the_new_name_with_no_rename_call() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(300));
    let root = vfs.tree().root();

    let (_node, h) = vfs.create(root, "draft.txt").await.unwrap();
    let content = b"never touched the server yet".to_vec();
    vfs.write(h, 0, &content).await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, debounce armed for "draft.txt".

    vfs.rename(root, "draft.txt", root, "renamed-draft.txt")
        .await
        .expect("renaming a drafted-new file should succeed locally");

    assert_eq!(
        env.rename_call_count(),
        0,
        "nothing exists remotely for a never-uploaded draft — rename_file must never be called"
    );
    assert_eq!(env.move_call_count(), 0);

    vfs.wait_for_writeback_idle().await;

    assert_eq!(
        env.uploaded_content("renamed-draft.txt"),
        Some(content),
        "the eventual upload must target the NEW name"
    );
    assert_eq!(
        env.uploaded_content("draft.txt"),
        None,
        "the OLD name must never receive an upload"
    );
}

/// Unlinking a file that was `create`d but never uploaded must never call
/// the delete API: nothing exists remotely to delete.
#[tokio::test]
async fn unlinking_a_drafted_new_file_makes_no_delete_call() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    let (_node, h) = vfs.create(root, "ephemeral.txt").await.unwrap();
    vfs.write(h, 0, b"short lived").await.unwrap();

    vfs.unlink(root, "ephemeral.txt").await.expect("unlink should succeed locally");

    assert_eq!(
        env.delete_call_count(),
        0,
        "nothing exists remotely for a never-uploaded draft — delete_files must never be called"
    );
    assert!(vfs.lookup(root, "ephemeral.txt").await.unwrap().is_none(), "gone from the listing");

    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.upload_session_count(), 0, "an unlinked draft must never be uploaded");
}

// ---------------------------------------------------------------------
// Carried obligation 2 (Task 7 re-review): `create()`'s EEXIST guard and
// `begin` must be atomic with respect to each other under concurrency.
// ---------------------------------------------------------------------

/// Two concurrent `create()` calls for the same name must never both
/// succeed: without a per-path lock around the whole check-then-act
/// sequence, both can observe "nothing here yet" and both proceed, with the
/// second `DraftStore::begin` silently overwriting the first's already-
/// acknowledged draft. A multi-threaded runtime is used deliberately so the
/// two calls can genuinely run in parallel on different OS threads — there
/// is no network or other await point inside the vulnerable section for a
/// single-threaded cooperative scheduler to interleave on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_of_the_same_name_yield_one_file() {
    for iteration in 0..30 {
        let env = VfsTestEnv::new().await;
        let (vfs, _rx) = Vfs::new(
            env.client(),
            common::REMOTE_BASE.into(),
            env.cache_dir(),
            DEFAULT_CACHE_MAX_BYTES,
        )
        .unwrap();
        let vfs = Arc::new(vfs);
        let root = vfs.tree().root();

        let vfs_a = vfs.clone();
        let vfs_b = vfs.clone();
        let (ra, rb) = tokio::join!(
            tokio::spawn(async move { vfs_a.create(root, "race.txt").await }),
            tokio::spawn(async move { vfs_b.create(root, "race.txt").await }),
        );
        let ra = ra.expect("task a panicked");
        let rb = rb.expect("task b panicked");

        let ok_count = [ra.is_ok(), rb.is_ok()].iter().filter(|ok| **ok).count();
        assert_eq!(
            ok_count, 1,
            "iteration {iteration}: expected exactly one create() to win, got a={ra:?} b={rb:?}"
        );
    }
}
