//! Behavior-driven integration tests for the 3-way sync engine.
//!
//! Each test describes a real user situation and asserts the *outcome the
//! user cares about*: no silent data loss, conflicts surfaced instead of
//! overwritten, deletions never propagated to the server, transfers enqueued
//! only when needed.
//!
//! Setup: real `Mount`, real SQLite inventory, real files on disk, and a
//! mock HTTP server standing in for the Cloudreve API.

mod common;

use cloudreve_sync::inventory::ConflictState;
use common::{TestEnv, remote_file};

// ---------------------------------------------------------------------------
// Initial sync
// ---------------------------------------------------------------------------

/// A user adds a drive that already has files on the server: everything
/// must be downloaded.
#[tokio::test]
async fn first_sync_downloads_all_remote_files() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![
        remote_file("report.pdf", 1024, "etag-1"),
        remote_file("notes.txt", 64, "etag-2"),
    ])
    .await;

    env.full_sync().await.unwrap();

    let downloads = env.tasks_of_type("download");
    assert_eq!(downloads.len(), 2, "both remote files must be downloaded");
    assert!(env.tasks_of_type("upload").is_empty(), "nothing to upload");
}

/// A user drops a new file into the sync folder: it must be uploaded.
#[tokio::test]
async fn new_local_file_is_uploaded() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;
    env.write_local("draft.md", b"hello world");

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].local_path.ends_with("draft.md"));
    assert!(env.tasks_of_type("download").is_empty());
}

/// The same file already exists on both sides (e.g. re-adding a drive):
/// it must be adopted as synced without transferring anything.
#[tokio::test]
async fn identical_file_on_both_sides_is_adopted_without_transfer() {
    let env = TestEnv::new().await;
    env.write_local("photo.jpg", &[0u8; 512]);
    env.set_remote_files(vec![remote_file("photo.jpg", 512, "etag-photo")])
        .await;

    env.full_sync().await.unwrap();

    assert!(env.all_tasks().is_empty(), "no transfer for an identical file");
    let entry = env.db_entry("photo.jpg").expect("file must be tracked");
    assert_eq!(entry.etag, "etag-photo");
    assert!(entry.conflict_state.is_none());
}

// ---------------------------------------------------------------------------
// Modifications on one side only
// ---------------------------------------------------------------------------

/// The user edits a synced file locally (size changes): it must be uploaded,
/// and it is not a conflict.
#[tokio::test]
async fn local_edit_is_uploaded() {
    let env = TestEnv::new().await;
    env.write_local("todo.txt", b"v1");
    env.track_synced("todo.txt", "etag-v1");
    env.write_local("todo.txt", b"v1 plus more content");
    env.set_remote_files(vec![remote_file("todo.txt", 2, "etag-v1")])
        .await;

    env.full_sync().await.unwrap();

    assert_eq!(env.tasks_of_type("upload").len(), 1);
    assert!(env.tasks_of_type("download").is_empty());
    let entry = env.db_entry("todo.txt").unwrap();
    assert!(entry.conflict_state.is_none(), "one-sided edit is not a conflict");
}

/// The user edits a synced file locally WITHOUT changing its size
/// (e.g. fixing a typo of the same length): the edit must still be detected
/// via mtime and uploaded — same-size edits must never be silently lost.
#[tokio::test]
async fn same_size_local_edit_is_detected_and_uploaded() {
    let env = TestEnv::new().await;
    env.write_local("config.ini", b"port=8080");
    env.track_synced("config.ini", "etag-v1");
    // Same size, different content — only the mtime betrays the edit.
    env.write_local("config.ini", b"port=9090");
    env.set_local_mtime("config.ini", 4102444800); // year 2100, clearly newer
    env.set_remote_files(vec![remote_file("config.ini", 9, "etag-v1")])
        .await;

    env.full_sync().await.unwrap();

    assert_eq!(
        env.tasks_of_type("upload").len(),
        1,
        "a same-size edit must not be silently ignored"
    );
}

/// Someone else modified the file on the server (etag changed), the local
/// copy is untouched: the new version must be downloaded.
#[tokio::test]
async fn remote_edit_is_downloaded() {
    let env = TestEnv::new().await;
    env.write_local("shared.doc", b"original");
    env.track_synced("shared.doc", "etag-v1");
    env.set_remote_files(vec![remote_file("shared.doc", 100, "etag-v2")])
        .await;

    env.full_sync().await.unwrap();

    assert_eq!(env.tasks_of_type("download").len(), 1);
    assert!(env.tasks_of_type("upload").is_empty());
}

/// Nothing changed anywhere: sync must be a no-op.
#[tokio::test]
async fn unchanged_file_triggers_no_transfer() {
    let env = TestEnv::new().await;
    env.write_local("stable.txt", b"same");
    env.track_synced("stable.txt", "etag-v1");
    env.set_remote_files(vec![remote_file("stable.txt", 4, "etag-v1")])
        .await;

    env.full_sync().await.unwrap();

    assert!(env.all_tasks().is_empty());
}

// ---------------------------------------------------------------------------
// Conflicts: the core safety property — never silently overwrite user data
// ---------------------------------------------------------------------------

/// The file was modified on BOTH sides since the last sync. The sync engine
/// must NOT pick a side: no transfer, conflict flagged for the user, and the
/// local content untouched.
#[tokio::test]
async fn both_sides_modified_flags_conflict_and_preserves_local_data() {
    let env = TestEnv::new().await;
    env.write_local("thesis.tex", b"chapter one");
    env.track_synced("thesis.tex", "etag-v1");
    // Local edit...
    env.write_local("thesis.tex", b"chapter one, revised locally");
    // ...while the server copy also changed.
    env.set_remote_files(vec![remote_file("thesis.tex", 999, "etag-v2")])
        .await;

    env.full_sync().await.unwrap();

    assert!(
        env.all_tasks().is_empty(),
        "no transfer may run while the conflict is unresolved"
    );
    let entry = env.db_entry("thesis.tex").unwrap();
    assert_eq!(entry.conflict_state, Some(ConflictState::Pending));
    let content = std::fs::read(env.local_path("thesis.tex")).unwrap();
    assert_eq!(
        content, b"chapter one, revised locally",
        "local work must never be overwritten by a conflict"
    );
}

/// A conflicted file stays frozen across subsequent syncs until the user
/// resolves it — repeated syncs must not "work around" the conflict.
#[tokio::test]
async fn conflicted_file_stays_frozen_across_syncs() {
    let env = TestEnv::new().await;
    env.write_local("budget.xlsx", b"numbers");
    env.track_synced("budget.xlsx", "etag-v1");
    env.write_local("budget.xlsx", b"different numbers");
    env.set_remote_files(vec![remote_file("budget.xlsx", 50, "etag-v2")])
        .await;

    env.full_sync().await.unwrap();
    env.full_sync().await.unwrap();
    env.full_sync().await.unwrap();

    assert!(env.all_tasks().is_empty(), "frozen file must never transfer");
    assert_eq!(
        env.db_entry("budget.xlsx").unwrap().conflict_state,
        Some(ConflictState::Pending)
    );
}

// ---------------------------------------------------------------------------
// Deletions: destructive operations must never propagate to the server
// ---------------------------------------------------------------------------

/// The user deletes a synced file locally. The server copy must NOT be
/// deleted — the client only forgets the file.
#[tokio::test]
async fn local_deletion_is_never_propagated_to_server() {
    let env = TestEnv::new().await;
    env.write_local("important.zip", b"data");
    env.track_synced("important.zip", "etag-v1");
    std::fs::remove_file(env.local_path("important.zip")).unwrap();
    env.set_remote_files(vec![remote_file("important.zip", 4, "etag-v1")])
        .await;

    env.full_sync().await.unwrap();

    // No write/delete request may ever reach the server.
    let requests = env.server.received_requests().await.unwrap();
    for req in &requests {
        assert_eq!(
            req.method.as_str(),
            "GET",
            "sync must be read-only towards the server here, got {} {}",
            req.method,
            req.url.path()
        );
    }
    assert!(env.all_tasks().is_empty(), "no transfer tasks");
    assert!(
        env.db_entry("important.zip").is_none(),
        "tracking must be forgotten"
    );
}

/// The file was deleted on the server. The local copy must be preserved —
/// remote deletions must not destroy local data.
#[tokio::test]
async fn remote_deletion_preserves_local_file() {
    let env = TestEnv::new().await;
    env.write_local("memories.jpg", b"precious");
    env.track_synced("memories.jpg", "etag-v1");
    env.set_remote_files(vec![]).await; // gone from the server

    env.full_sync().await.unwrap();

    assert!(
        env.local_path("memories.jpg").exists(),
        "local file must survive a remote deletion"
    );
    assert!(env.db_entry("memories.jpg").is_none(), "tracking forgotten");
    assert!(env.all_tasks().is_empty());
}

// ---------------------------------------------------------------------------
// Limits and filters
// ---------------------------------------------------------------------------

/// Files matching the drive's ignore patterns must never be uploaded.
#[tokio::test]
async fn ignored_files_are_not_uploaded() {
    let env = TestEnv::with_ignore_patterns(vec!["*.tmp".to_string()]).await;
    env.write_local("cache.tmp", b"scratch");
    env.write_local("real.txt", b"keep me");
    env.set_remote_files(vec![]).await;

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert_eq!(uploads.len(), 1, "only the non-ignored file is uploaded");
    assert!(uploads[0].local_path.ends_with("real.txt"));
}

/// Files above the configured size limit are skipped in both directions.
#[tokio::test]
async fn oversized_files_are_skipped() {
    let env = TestEnv::with_max_file_size(1).await; // 1 MB limit
    env.write_local("big-local.bin", &vec![0u8; 2 * 1024 * 1024]);
    env.set_remote_files(vec![remote_file(
        "big-remote.bin",
        3 * 1024 * 1024,
        "etag-big",
    )])
    .await;

    env.full_sync().await.unwrap();

    assert!(
        env.all_tasks().is_empty(),
        "oversized files must not transfer in either direction"
    );
}
