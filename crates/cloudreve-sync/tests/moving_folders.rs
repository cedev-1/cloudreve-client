//! TEMPORARY — characterises the CURRENT (broken) behaviour when a file is
//! moved locally. Not meant to be committed as-is: once the delete policy is
//! decided, this becomes the regression test asserting the fixed behaviour.

mod common;

use common::{remote_file, TestEnv};

/// Moving a file locally duplicates it on the server and brings the old copy
/// back down on the next sync.
///
/// `TaskKind` is `{ Upload, Download }` — the client has no delete operation in
/// either direction. So a move is never seen as a move; it decomposes into a
/// disappearance at the old path and an appearance at the new one:
///
///   old path → (db, local, remote) = (true, false, true)  → forget the DB row,
///              leave the server untouched  (sync.rs:215)
///   new path → (false, true, false)                       → upload  (sync.rs:236)
///
/// The server now holds both copies. And because the old path's DB row was
/// dropped, the next sync sees it as (false, false, true) — an unknown remote
/// file — and downloads it back (sync.rs:254).
#[tokio::test]
async fn moving_a_file_locally_re_downloads_it_at_its_old_path() {
    let env = TestEnv::new().await;

    // Steady state: the file exists on both sides and is tracked.
    env.write_local("old/doc.txt", b"hello");
    env.track_synced("old/doc.txt", "etag-1");
    env.set_remote_files(vec![remote_file("old/doc.txt", 5, "etag-1")]).await;

    // The user moves it in Finder: gone from `old/`, present in `new/`.
    std::fs::remove_file(env.local_path("old/doc.txt")).unwrap();
    env.write_local("new/doc.txt", b"hello");

    env.full_sync().await.unwrap();

    // The new location is uploaded — a SECOND copy on a server that still has
    // the first one, because nothing ever deletes the old path remotely.
    let uploads = env.tasks_of_type("upload");
    assert_eq!(uploads.len(), 1, "expected the moved file to be uploaded");
    assert!(uploads[0].local_path.ends_with("new/doc.txt"));
    assert!(
        env.db_entry("old/doc.txt").is_none(),
        "the old path should have been forgotten from the inventory"
    );

    // Second sync. The remote listing is unchanged (the server still has the
    // old copy — we never asked it to delete anything).
    env.full_sync().await.unwrap();

    let downloads = env.tasks_of_type("download");
    assert!(
        downloads.iter().any(|t| t.local_path.ends_with("old/doc.txt")),
        "the file moved away should NOT come back, but a download was enqueued \
         for its old path. Downloads seen: {:?}",
        downloads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
}
