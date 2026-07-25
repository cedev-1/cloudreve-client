//! A sync folder that has vanished is not a folder that became empty.
//!
//! The local scan used to answer "no files here" when the sync root did not
//! exist — an ejected external drive, a deleted folder, an unmounted network
//! share. `full_sync` then reads that as "the user deleted everything" and
//! purges the whole inventory. Nothing is lost on disk, but the last-known
//! sync state is: local-only files get re-uploaded when the volume comes back,
//! and any edit made in the meantime is re-registered against the *remote*
//! etag, so the local change is silently forgotten and never uploaded.

mod common;

use common::{TestEnv, remote_file};

/// The whole point: a vanished sync root must stop the sync, not be mistaken
/// for an empty one.
#[tokio::test]
async fn a_vanished_sync_folder_does_not_wipe_the_inventory() {
    let env = TestEnv::with_max_file_size(0).await;
    env.set_remote_files(vec![remote_file("kept.txt", 4, "etag-kept")])
        .await;
    env.write_local("kept.txt", b"kept");
    env.track_synced("kept.txt", "etag-kept");

    // The volume goes away.
    std::fs::remove_dir_all(&env.sync_dir).expect("remove sync dir");

    let result = env.full_sync().await;

    assert!(
        result.is_err(),
        "a missing sync folder must surface an error, not pass as an empty one"
    );
    assert!(
        env.db_entry("kept.txt").is_some(),
        "the inventory must survive a sync folder that was merely unreachable"
    );
}

/// A local-only file is the one that actually gets re-uploaded once the
/// inventory has been purged, so it deserves its own check.
#[tokio::test]
async fn a_local_only_file_is_not_forgotten_when_the_folder_vanishes() {
    let env = TestEnv::with_max_file_size(0).await;
    env.set_remote_files(vec![]).await;
    env.write_local("local-only.txt", b"mine");
    env.track_synced("local-only.txt", "etag-local");

    std::fs::remove_dir_all(&env.sync_dir).expect("remove sync dir");

    let _ = env.full_sync().await;

    assert!(
        env.db_entry("local-only.txt").is_some(),
        "forgetting it here means re-uploading it when the volume returns"
    );
}

/// The guard must key on the folder being unreachable, not on it being empty:
/// a user who really did delete every local file still gets a working sync.
#[tokio::test]
async fn an_empty_but_present_sync_folder_still_syncs() {
    let env = TestEnv::with_max_file_size(0).await;
    env.set_remote_files(vec![remote_file("remote.txt", 4, "etag-r")])
        .await;

    env.full_sync().await.expect("an empty sync folder is legitimate");

    assert_eq!(
        env.tasks_of_type("download").len(),
        1,
        "the remote file must still be scheduled for download"
    );
}
