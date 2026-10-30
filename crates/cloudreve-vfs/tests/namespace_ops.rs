//! Task 10: mkdir, unlink, rename through the `Vfs` facade.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::vfs::{
    CreatePauseHook, DirNotEmptyError, OpenPauseHook, RenameBusyError, UnlinkBusyError, Vfs,
    DEFAULT_CACHE_MAX_BYTES,
};
use common::{remote_dir, remote_file, VfsTestEnv};
use serde_json::json;

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
///
/// Phase 4 (deliverable B) closes the handle before unlinking, unlike this
/// test's original phase-2 form: `create`'s own handle stays registered in
/// `open_files` until explicitly closed, and unlinking a file that is still
/// open is now correctly refused with `UnlinkBusyError` (see that guard's
/// own tests) — this test's actual point (a never-uploaded draft's unlink
/// skips the delete API) is unaffected by closing the handle first, which
/// is also the sequence any real editor/frontend produces anyway (save,
/// then delete, never delete while still holding the fd open).
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
    vfs.close(h).await.unwrap();

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

/// Phase-2 debt burn-down (cycle A): renaming an entry with an open handle
/// must be refused with the typed `RenameBusyError`, not silently proceed
/// and leave the handle pointing at a path that's about to become invalid
/// (see `Vfs::rename`'s doc for the failure modes this used to allow).
/// Closing the handle lifts the guard and the rename then succeeds.
#[tokio::test]
async fn renaming_a_file_with_an_open_handle_is_refused() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.add_remote_file("busy.txt", b"hello".to_vec(), "e1").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let node = vfs.lookup(root, "busy.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let result = vfs.rename(root, "busy.txt", root, "renamed.txt").await;
    let err = result.expect_err("renaming a file with an open handle must be refused");
    assert!(
        err.downcast_ref::<RenameBusyError>().is_some(),
        "expected a RenameBusyError, got: {err:#}"
    );
    assert_eq!(
        env.rename_call_count(),
        0,
        "a refused rename must never have reached the server"
    );
    assert!(
        vfs.lookup(root, "busy.txt").await.unwrap().is_some(),
        "the entry must still resolve under its old name"
    );

    vfs.close(h).await.unwrap();

    vfs.rename(root, "busy.txt", root, "renamed.txt")
        .await
        .expect("renaming should succeed once the handle is closed");
    assert!(
        vfs.lookup(root, "busy.txt").await.unwrap().is_none(),
        "the old name must no longer resolve"
    );
    assert!(
        vfs.lookup(root, "renamed.txt").await.unwrap().is_some(),
        "the new name must resolve after the rename"
    );
}

/// Coordinator review (cycle A follow-up): `create()` makes a brand-new
/// name visible in the tree (`insert_local_entry`) and begins its draft
/// several `.await`s before it finishes registering its own handle in
/// `open_files`. A `rename` of that SAME not-yet-fully-created name landing
/// in that window must not be able to see `is_path_open == false` and
/// complete — that would reproduce cycle A's exact stale-handle bug for a
/// name that didn't exist a moment ago, instead of one that already did.
/// The window has no `.await` inside it in production (nothing for a real
/// concurrent-tasks test to reliably land in), so a test-only pause hook
/// widens it to something deterministic — see `CreatePauseHook`'s doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rename_cannot_slip_into_creates_registration_window() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    let vfs = Arc::new(vfs);
    let root = vfs.tree().root();

    let hook = Arc::new(CreatePauseHook::new());
    vfs.pause_create_before_registration_for_tests(hook.clone());

    let vfs_a = vfs.clone();
    let create_task = tokio::spawn(async move { vfs_a.create(root, "brand-new.txt").await });

    // Wait until `create` has made "brand-new.txt" visible in the tree and
    // begun its draft, but has NOT YET registered its handle in
    // `open_files` — the exact window this test targets.
    hook.parked.notified().await;

    // The rename must be spawned, not awaited inline here: with the fix,
    // `rename` BLOCKS on the same `open_lock` `create` is still holding
    // (rather than failing fast), so awaiting it before releasing `resume`
    // below would deadlock the test itself against the very guard it's
    // trying to observe.
    let vfs_b = vfs.clone();
    let rename_task = tokio::spawn(async move {
        vfs_b.rename(root, "brand-new.txt", root, "renamed.txt").await
    });

    // Best-effort widening only: give the rename task a real chance to
    // reach (and block on) `open_lock` before `create` is allowed to
    // finish, so this test actually exercises the lock's blocking
    // behavior rather than a lucky ordering. Correctness below does not
    // depend on this sleep succeeding — `create` cannot finish registering
    // its handle until `resume` is notified regardless of when (or
    // whether) the rename task has reached its lock attempt yet.
    tokio::time::sleep(Duration::from_millis(50)).await;
    hook.resume.notify_one();

    let (_node, h) = create_task.await.unwrap().expect("create must still succeed");
    let rename_result = rename_task.await.unwrap();

    let err = rename_result.expect_err(
        "a rename racing create's registration window must be refused, not silently \
         complete against a name whose handle isn't registered yet",
    );
    assert!(
        err.downcast_ref::<RenameBusyError>().is_some(),
        "expected a RenameBusyError, got: {err:#}"
    );

    // The rename was refused, so nothing must have moved: the handle
    // `create` returned is still valid under the ORIGINAL name.
    assert!(
        vfs.lookup(root, "brand-new.txt").await.unwrap().is_some(),
        "the entry must still resolve under its original name"
    );
    assert!(
        vfs.lookup(root, "renamed.txt").await.unwrap().is_none(),
        "the refused rename's destination name must not resolve to anything"
    );

    let content = b"created while a rename raced its registration".to_vec();
    vfs.write(h, 0, &content).await.expect("the handle create() returned must still work");
    vfs.close(h).await.unwrap();
    vfs.wait_for_writeback_idle().await;

    assert_eq!(
        env.uploaded_content("brand-new.txt"),
        Some(content),
        "the created file's content must reach the server under its original name"
    );
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

// ---------------------------------------------------------------------
// Final fix wave (F1): renaming a DIRECTORY used to bypass the EBUSY
// guard entirely, because both the open-handle check and the draft
// migration matched the exact path only — a directory itself is never
// opened as a file and never has its own draft, so neither check ever
// tripped for one. A file with an open handle or a dirty/pending draft
// underneath a renamed directory kept pointing at the OLD path: the
// server-side rename went through, and the descendant's eventual upload
// then resurrected the just-renamed directory at its old uri with the
// user's edit inside, while the new location kept stale content.
// ---------------------------------------------------------------------

/// Builds the JSON for a remote file nested under `dir`, the way
/// `remote_file` cannot: that helper always stamps a root-level `path`.
fn nested_remote_file(dir: &str, name: &str, size: i64, etag: &str) -> serde_json::Value {
    let mut entry = remote_file(name, size, etag);
    entry["path"] = json!(format!("{}/{dir}/{name}", common::REMOTE_BASE));
    entry
}

/// Renaming a directory with an open handle on a file inside it must be
/// refused with `RenameBusyError`, exactly like renaming the file itself
/// would be — and the refusal must happen before the server is ever
/// contacted. Closing the handle lifts the guard, and the rename then
/// succeeds, moving the child along with it.
#[tokio::test]
async fn renaming_a_directory_with_an_open_child_handle_is_refused() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![nested_remote_file("docs", "inside.txt", 5, "e1")]).await;
    env.serve_file_content("inside.txt", b"hello").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;
    let inside = vfs.lookup(docs, "inside.txt").await.unwrap().unwrap().0;
    let h = vfs.open(inside).await.unwrap();

    let result = vfs.rename(root, "docs", root, "docs-renamed").await;
    let err = result.expect_err(
        "renaming a directory with an open handle on a descendant must be refused",
    );
    assert!(
        err.downcast_ref::<RenameBusyError>().is_some(),
        "expected a RenameBusyError, got: {err:#}"
    );
    assert_eq!(
        env.rename_call_count(),
        0,
        "a refused directory rename must never have reached the server"
    );
    assert!(
        vfs.lookup(root, "docs").await.unwrap().is_some(),
        "the directory must still resolve under its old name"
    );

    vfs.close(h).await.unwrap();

    vfs.rename(root, "docs", root, "docs-renamed")
        .await
        .expect("renaming should succeed once the descendant's handle is closed");
    assert!(vfs.lookup(root, "docs").await.unwrap().is_none(), "the old name must no longer resolve");
    let renamed = vfs
        .lookup(root, "docs-renamed")
        .await
        .unwrap()
        .expect("the new name must resolve after the rename");
    assert!(renamed.1.is_dir);
    let (_, attr) = vfs
        .lookup(renamed.0, "inside.txt")
        .await
        .unwrap()
        .expect("the child must have moved along with its renamed parent");
    assert_eq!(attr.name, "inside.txt");
}

/// Same failure mode, without any handle ever staying open: a descendant
/// file that was edited and closed (its draft is `Pending`, debounced for
/// upload, no open handle left) must ALSO refuse a rename of its parent
/// directory — otherwise `DraftStore::rename`'s exact-path-only migration
/// would never touch it, and its debounced upload would land under the
/// OLD, just-renamed-away directory path once it eventually fires
/// (reviewer's proven resurrection scenario). Once the draft has fully
/// settled (uploaded and removed), the same rename succeeds.
#[tokio::test]
async fn renaming_a_directory_with_a_pending_child_draft_is_refused() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![nested_remote_file("docs", "inside.txt", 5, "e1")]).await;
    env.serve_file_content("inside.txt", b"hello").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(50));
    let root = vfs.tree().root();
    let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;
    let inside = vfs.lookup(docs, "inside.txt").await.unwrap().unwrap().0;
    let h = vfs.open(inside).await.unwrap();
    vfs.write(h, 0, b"edited").await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, debounce armed — no handle left open.

    let result = vfs.rename(root, "docs", root, "docs-renamed").await;
    let err = result.expect_err(
        "renaming a directory with a pending (unopened) draft on a descendant must be refused",
    );
    assert!(
        err.downcast_ref::<RenameBusyError>().is_some(),
        "expected a RenameBusyError, got: {err:#}"
    );
    assert_eq!(
        env.rename_call_count(),
        0,
        "a refused directory rename must never have reached the server"
    );

    // Let the debounce fire and the upload land under the STILL-original
    // path (nothing was renamed remotely yet) — the draft fully settles
    // and is removed.
    vfs.wait_for_writeback_idle().await;
    assert_eq!(
        env.uploaded_content("inside.txt"),
        Some(b"edited".to_vec()),
        "the settled draft must have uploaded under its original (never-renamed) path"
    );

    vfs.rename(root, "docs", root, "docs-renamed")
        .await
        .expect("renaming should succeed once the descendant's draft has settled");
    assert!(vfs.lookup(root, "docs").await.unwrap().is_none(), "the old name must no longer resolve");
    assert!(
        vfs.lookup(root, "docs-renamed").await.unwrap().is_some(),
        "the new name must resolve after the rename"
    );
}

/// The subtree guard's prefix match must key on a real path separator
/// (`old_path` + "/"), not a bare string prefix: `docs` and `docs2` share
/// "docs" as characters, but neither is an ancestor of the other. Only
/// one of the two directions below is actually diagnostic of a missing
/// separator (a naive `starts_with(old_path)` without it would wrongly
/// treat the longer sibling's descendant as nested under the shorter
/// name) — both are pinned so neither direction can silently regress.
#[tokio::test]
async fn renaming_a_directory_ignores_an_open_handle_in_a_same_prefixed_sibling() {
    // Direction A — the actually diagnostic one: the SHORTER name
    // ("docs") is renamed while the LONGER sibling ("docs2") has an open
    // descendant. `"docs2/other.txt".starts_with("docs")` is true as a
    // bare string, but false once the boundary is `"docs/"`.
    {
        let env = VfsTestEnv::new().await;
        env.expect_namespace_ops().await;
        env.set_remote_files(vec![remote_dir("docs"), remote_dir("docs2")]).await;
        env.set_remote_files_at("docs2", vec![nested_remote_file("docs2", "other.txt", 5, "e1")])
            .await;
        env.serve_file_content("other.txt", b"hello").await;

        let (vfs, _rx) = Vfs::new(
            env.client(),
            common::REMOTE_BASE.into(),
            env.cache_dir(),
            DEFAULT_CACHE_MAX_BYTES,
        )
        .unwrap();
        let root = vfs.tree().root();
        let docs2 = vfs.lookup(root, "docs2").await.unwrap().unwrap().0;
        let other = vfs.lookup(docs2, "other.txt").await.unwrap().unwrap().0;
        let _h = vfs.open(other).await.unwrap(); // stays open

        vfs.rename(root, "docs", root, "docs-renamed").await.expect(
            "an open handle in an unrelated, similarly-prefixed sibling must not refuse this rename",
        );
        assert!(vfs.lookup(root, "docs-renamed").await.unwrap().is_some());
    }

    // Direction B — the reviewer's literal wording: the LONGER name
    // ("docs2") is renamed while the SHORTER sibling ("docs") has an open
    // descendant.
    {
        let env = VfsTestEnv::new().await;
        env.expect_namespace_ops().await;
        env.set_remote_files(vec![remote_dir("docs"), remote_dir("docs2")]).await;
        env.set_remote_files_at("docs", vec![nested_remote_file("docs", "file.txt", 5, "e1")]).await;
        env.serve_file_content("file.txt", b"hello").await;

        let (vfs, _rx) = Vfs::new(
            env.client(),
            common::REMOTE_BASE.into(),
            env.cache_dir(),
            DEFAULT_CACHE_MAX_BYTES,
        )
        .unwrap();
        let root = vfs.tree().root();
        let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;
        let file = vfs.lookup(docs, "file.txt").await.unwrap().unwrap().0;
        let _h = vfs.open(file).await.unwrap(); // stays open

        vfs.rename(root, "docs2", root, "docs2-renamed")
            .await
            .expect("renaming docs2 while docs/file.txt is open must succeed");
        assert!(vfs.lookup(root, "docs2-renamed").await.unwrap().is_some());
    }
}

// ---------------------------------------------------------------------
// Phase 4, Task 2: the hierarchical namespace lock (D10), editor-save
// semantics (unlink-of-open-file, rename-onto-open-destination), and
// rmdir NOTEMPTY.
// ---------------------------------------------------------------------

/// D10's whole point: without `namespace_lock`, a directory rename's
/// subtree check and a BRAND-NEW descendant `open` racing it use unrelated
/// per-path lock keys and can interleave — the phase-3 final review's
/// disclosed "check-then-act across different lock keys" residual. This
/// test proves the FIX is a real WAIT, not just an eventual busy answer: the
/// racing `open` is parked deterministically (via `OpenPauseHook`, the same
/// widening idiom `CreatePauseHook` already uses for `create`'s analogous
/// window) mid-registration, and the rename must be observably STILL
/// BLOCKED while the open sits parked — only completing (as EBUSY) once the
/// open resumes and actually registers its handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_directory_rename_waits_for_a_racing_descendant_open_then_refuses_it() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![nested_remote_file("docs", "inside.txt", 5, "e1")]).await;
    env.serve_file_content("inside.txt", b"hello").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let vfs = Arc::new(vfs);
    let root = vfs.tree().root();
    let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;
    let inside = vfs.lookup(docs, "inside.txt").await.unwrap().unwrap().0;

    let hook = Arc::new(OpenPauseHook::new());
    vfs.pause_open_before_registration_for_tests(hook.clone());

    let vfs_a = vfs.clone();
    let open_task = tokio::spawn(async move { vfs_a.open(inside).await });

    // Wait until `open` has resolved the descendant's attrs and is parked
    // just before registering its handle — the exact window D10 must close.
    hook.parked.notified().await;

    let vfs_b = vfs.clone();
    let rename_task =
        tokio::spawn(async move { vfs_b.rename(root, "docs", root, "docs-renamed").await });

    // The rename must still be blocked on `namespace_lock` (held `read()`
    // by the parked open). Give it a real chance to have reached that
    // point, then prove it has NOT finished (i.e. did not interleave with
    // the still-parked open) while the open sits parked.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !rename_task.is_finished(),
        "the directory rename must wait for the racing open to finish, not interleave with it"
    );

    hook.resume.notify_one();

    let open_result = open_task.await.unwrap();
    let h = open_result.expect("the racing open must still succeed once resumed");

    let rename_result = rename_task.await.unwrap();
    let err = rename_result.expect_err(
        "a directory rename must refuse once it sees the descendant's now-registered open handle",
    );
    assert!(err.downcast_ref::<RenameBusyError>().is_some(), "expected a RenameBusyError, got: {err:#}");
    assert_eq!(env.rename_call_count(), 0, "a refused directory rename must never have reached the server");

    vfs.close(h).await.unwrap();
    vfs.rename(root, "docs", root, "docs-renamed")
        .await
        .expect("renaming should succeed once the descendant's handle is closed");
}

/// Deliverable B: unlinking a file with a currently open handle must be
/// refused — an open handle's later write must not be able to resurrect a
/// file someone else just deleted. Closing the handle lifts the guard.
#[tokio::test]
async fn unlinking_a_file_with_an_open_handle_is_refused() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.add_remote_file("busy-unlink.txt", b"hello".to_vec(), "e1").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let node = vfs.lookup(root, "busy-unlink.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let result = vfs.unlink(root, "busy-unlink.txt").await;
    let err = result.expect_err("unlinking a file with an open handle must be refused");
    assert!(err.downcast_ref::<UnlinkBusyError>().is_some(), "expected an UnlinkBusyError, got: {err:#}");
    assert_eq!(env.delete_call_count(), 0, "a refused unlink must never have reached the server");
    assert!(
        vfs.lookup(root, "busy-unlink.txt").await.unwrap().is_some(),
        "the entry must still resolve while the handle is open"
    );

    vfs.close(h).await.unwrap();

    vfs.unlink(root, "busy-unlink.txt")
        .await
        .expect("unlink should succeed once the handle is closed");
    assert_eq!(env.delete_call_count(), 1);
    assert!(vfs.lookup(root, "busy-unlink.txt").await.unwrap().is_none());
}

/// Deliverable C, scenario (a) — REMOTE-source atomic save (fix round 1,
/// C1): the atomic-save idiom (write a tmp file, then rename it OVER the
/// target) must not silently clobber a handle's view of a target it still
/// has open. Closing the destination handle lifts the guard, and the rename
/// then replaces the destination's content — against the mock's REFUSING
/// `rename_file` (the real Cloudreve server's actual 40004 behavior, see
/// `tests/common/mod.rs`), proving the facade's delete-then-rename bridge
/// actually runs (`delete_call_count`), not that the mock quietly allows an
/// overwrite it never would in production.
#[tokio::test]
async fn renaming_onto_an_open_destination_is_refused() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.add_remote_file("source.txt", b"new content".to_vec(), "e1").await;
    env.add_remote_file("target.txt", b"old content".to_vec(), "e2").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let target_node = vfs.lookup(root, "target.txt").await.unwrap().unwrap().0;
    let h = vfs.open(target_node).await.unwrap();

    let result = vfs.rename(root, "source.txt", root, "target.txt").await;
    let err = result.expect_err("renaming onto an open destination must be refused");
    assert!(err.downcast_ref::<RenameBusyError>().is_some(), "expected a RenameBusyError, got: {err:#}");
    assert_eq!(env.rename_call_count(), 0, "a refused rename must never have reached the server");
    assert_eq!(env.move_call_count(), 0);
    assert!(vfs.lookup(root, "source.txt").await.unwrap().is_some(), "the source must be untouched");
    assert!(
        vfs.lookup(root, "target.txt").await.unwrap().is_some(),
        "the still-open destination must be untouched"
    );

    vfs.close(h).await.unwrap();

    vfs.rename(root, "source.txt", root, "target.txt")
        .await
        .expect("renaming should succeed once the destination handle is closed");
    assert_eq!(
        env.delete_call_count(),
        1,
        "the bridge must delete the existing destination before the server-side rename \
         (the real server refuses rename-onto-existing outright)"
    );
    assert!(vfs.lookup(root, "source.txt").await.unwrap().is_none(), "the old name must no longer resolve");
    let (_, attr) = vfs
        .lookup(root, "target.txt")
        .await
        .unwrap()
        .expect("the destination name must still resolve, now to the source's entry");
    assert_eq!(
        attr.size,
        "new content".len() as u64,
        "the destination's old content must have been replaced by the source's"
    );
}

/// Deliverable C, scenario (b) — the REAL atomic-save shape (fix round 1,
/// C1): the tmp file an editor actually creates is DRAFT-ONLY (never
/// uploaded yet, empty `base_etag`) at the moment it gets renamed over the
/// target — unlike scenario (a) above, where both sides were already real
/// remote files. `Vfs::rename` skips the server call entirely for a
/// draft-only source (nothing remote to move), so the bridge here cannot be
/// "delete then rename" — it must instead make the MIGRATED DRAFT adopt the
/// destination's remote identity so its eventual upload lands as a rewrite,
/// not a doomed `overwrite=false` collision that retries the identical
/// 40004 forever (the exact bug this fix closes).
#[tokio::test]
async fn renaming_a_drafted_tmp_file_onto_an_existing_closed_destination_lands_the_save() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;
    env.add_remote_file("target.txt", b"old content".to_vec(), "e2").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(30));
    let root = vfs.tree().root();

    // The real atomic-save sequence: create a fresh tmp, write, close
    // (Pending, debounce armed, base_etag == "") — BEFORE ever renaming it
    // over the target. `target.txt` stays CLOSED throughout (no open
    // handle) — this is deliberately NOT the EBUSY scenario, it's the
    // "rename succeeds, then the save must actually land" scenario.
    let (_node, h) = vfs.create(root, ".tmp-save").await.unwrap();
    let new_bytes = b"freshly saved content".to_vec();
    vfs.write(h, 0, &new_bytes).await.unwrap();
    vfs.close(h).await.unwrap();

    vfs.rename(root, ".tmp-save", root, "target.txt")
        .await
        .expect("renaming the drafted tmp onto the existing, closed destination should succeed");
    assert_eq!(
        env.delete_call_count(),
        0,
        "a draft-only source never existed remotely — there is nothing to delete/rename on \
         the server, the bridge here is identity adoption, not delete-then-rename"
    );
    assert_eq!(env.rename_call_count(), 0);

    vfs.wait_for_writeback_idle().await;

    assert_eq!(
        env.uploaded_content("target.txt"),
        Some(new_bytes),
        "the save must actually land on the server, not park retrying an identical 40004 \
         forever"
    );
    assert_eq!(
        env.upload_session_count(),
        1,
        "exactly one upload session — a correct rewrite-in-place, not a doomed \
         overwrite=false retry loop"
    );
}

/// R1 (phase 4 task 3, routed from the task 2 re-review): a DRAFTED-source
/// (never-uploaded, empty `base_etag`) FILE renamed onto an existing
/// DIRECTORY name must be refused loudly, not silently succeed. Before this
/// fix, `remote_destination_if_exists`'s result was unconditionally
/// filtered with `.filter(|a| !a.is_dir)` regardless of the SOURCE's type —
/// for a drafted source this meant `dest_remote` became `None` no matter
/// what sat at the destination, so no adoption ever happened, `rename()`
/// returned `Ok(())`, and the eventual upload ran `overwrite=false` against
/// the directory's own name — refused by the server with 40004 forever, the
/// same doomed-retry shape R2's bridge exists to close, just through a
/// different door this facade must not leave open. Detectable via
/// `anyhow::Error::downcast_ref::<RenameOntoDirectoryError>()`.
#[tokio::test]
async fn renaming_a_drafted_new_file_onto_an_existing_directory_is_refused_loudly() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_dir("a-directory")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(30));
    let root = vfs.tree().root();

    let (_node, h) = vfs.create(root, "draft.txt").await.unwrap();
    vfs.write(h, 0, b"never touched the server yet").await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, debounce armed.

    let result = vfs.rename(root, "draft.txt", root, "a-directory").await;
    let err = result.expect_err(
        "renaming a drafted file onto an existing directory name must be refused loudly, not \
         silently succeed and let the eventual upload retry a doomed 40004 forever",
    );
    assert!(
        err.downcast_ref::<cloudreve_vfs::vfs::RenameOntoDirectoryError>().is_some(),
        "must be the distinct typed error (frontends map it to EISDIR), got: {err:#}"
    );

    // Nothing must have been silently migrated: the draft still targets its
    // ORIGINAL name, and no upload ever races against the directory's name.
    vfs.wait_for_writeback_idle().await;
    assert_eq!(
        env.uploaded_content("a-directory"),
        None,
        "the directory's name must never receive an upload attempt"
    );
}

/// Deliverable D: `rmdir` of a directory with a real, LISTED child must be
/// refused as `NotEmpty` — this facade never does a recursive delete.
#[tokio::test]
async fn rmdir_with_a_listed_child_is_refused_as_not_empty() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![nested_remote_file("docs", "inside.txt", 5, "e1")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    let result = vfs.unlink(root, "docs").await;
    let err = result.expect_err("rmdir of a non-empty directory must be refused");
    assert!(err.downcast_ref::<DirNotEmptyError>().is_some(), "expected a DirNotEmptyError, got: {err:#}");
    assert_eq!(env.delete_call_count(), 0, "a refused rmdir must never have reached the server");
    assert!(vfs.lookup(root, "docs").await.unwrap().is_some(), "the directory must still resolve");
}

/// Deliverable D's sharper case: a child that only exists as a local,
/// not-yet-uploaded `create()` draft (never confirmed by the server, so it
/// would NOT show up in a real listing) must still count as an occupant —
/// otherwise `rmdir` could destroy an unsaved edit the user just made inside
/// the directory a moment ago.
#[tokio::test]
async fn rmdir_with_only_a_drafted_child_is_refused_as_not_empty() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    let docs = vfs.lookup(root, "docs").await.unwrap().unwrap().0;

    let (_node, h) = vfs.create(docs, "draft.txt").await.unwrap();
    vfs.write(h, 0, b"not uploaded yet").await.unwrap();
    vfs.close(h).await.unwrap(); // Pending, no open handle — never uploaded.

    let result = vfs.unlink(root, "docs").await;
    let err =
        result.expect_err("a drafted-but-unlisted child must still count as a non-empty occupant");
    assert!(err.downcast_ref::<DirNotEmptyError>().is_some(), "expected a DirNotEmptyError, got: {err:#}");
    assert_eq!(env.delete_call_count(), 0, "a refused rmdir must never have reached the server");
}

/// The positive case for both deliverable D tests above: an empty directory
/// (no listed children, no drafted ones either) removes cleanly.
#[tokio::test]
async fn rmdir_of_an_empty_directory_succeeds() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("empty-dir")]).await;
    env.set_remote_files_at("empty-dir", vec![]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();

    vfs.unlink(root, "empty-dir").await.expect("rmdir of an empty directory should succeed");
    assert_eq!(env.delete_call_count(), 1);
    assert!(vfs.lookup(root, "empty-dir").await.unwrap().is_none());
}
