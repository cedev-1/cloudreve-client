//! When an on-demand drive goes away (app quit, drive removed, a new drive
//! that failed to mount), its vfs must stop talking to the server: no
//! upload timer firing later, no reconnect re-arming drafts with the
//! tokens of a drive that no longer runs. Drafts stay on disk for the next
//! launch of a drive that still exists.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use common::TestEnv;

/// A save waiting for its debounce when the mount shuts down.
async fn env_with_a_waiting_save() -> (TestEnv, std::sync::Arc<cloudreve_vfs::vfs::Vfs>) {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    env.set_remote_files(vec![]).await;
    let vfs = env.mount.vfs.lock().await.clone().expect("on-demand drive has a vfs");
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    let root = vfs.tree().root();
    let (_node, h) = vfs.create(root, "notes.txt").await.unwrap();
    vfs.write(h, 0, b"saved right before quitting").await.unwrap();
    vfs.close(h).await.unwrap();
    (env, vfs)
}

#[tokio::test]
async fn after_shutdown_the_vfs_queues_no_more_uploads() {
    let (env, vfs) = env_with_a_waiting_save().await;
    assert_eq!(vfs.unsynced_file_count().await, 1, "precondition: one save waiting");

    env.mount.shutdown().await;

    assert_eq!(vfs.retry_pending_uploads().await, 0, "a shut-down drive re-armed an upload");
    assert_eq!(vfs.unsynced_file_count().await, 1, "the draft itself stays for the next launch");
}
