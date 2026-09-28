//! Behavioral tests for `DriveMode::OnDemand`'s `Mount` lifecycle branches
//! (phase 4, task 4): what an on-demand drive skips (task replay, fs
//! watcher, full sync) and what its own lifecycle does instead (requesting
//! an OS mount through the `vfs_mode` seam, unmounting on pause, remounting
//! on resume, removing its cache dir on delete).
//!
//! Its own file (not folded into another suite) for two independent
//! reasons: toast statics are process-wide, so any test asserting on toast
//! behavior needs its own binary (see `TestEnv`'s harness doc); and this
//! plan's own task brief calls for a dedicated file regardless.
//!
//! None of these tests ever perform a real OS mount — see
//! `cloudreve_sync::drive::vfs_mode`'s module doc for the injection seam
//! design. The real OS mount/unmount path is proven end-to-end by
//! `cloudreve-vfs`'s own `tests/mounted_{macos,linux}.rs`.

mod common;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use cloudreve_sync::drive::vfs_mode::{MountSeamCall, cache_dir_for};
use cloudreve_sync::inventory::{NewTaskRecord, TaskStatus};
use common::{TestEnv, remote_file};

/// D2: an on-demand mount has no local mirror for a stale task to apply
/// to, so `TaskQueue::new` must never replay one at startup — the
/// `resume_on_start` seam threaded through `Mount::new`. Mirrors
/// `ignored_files.rs`'s `junk_tasks_left_in_the_database_are_not_replayed_
/// at_the_next_launch` test, which pins the OPPOSITE behavior for
/// `FullMirror`.
#[tokio::test]
async fn an_on_demand_mount_never_replays_parked_tasks() {
    let mut env = TestEnv::with_mode(DriveMode::OnDemand).await;
    env.set_remote_files(vec![]).await;

    // What an interrupted run leaves behind: a plain Pending task, exactly
    // like a real `FullMirror` drive would after being killed mid-upload.
    let record = NewTaskRecord::new(
        "parked-task",
        &env.drive_id,
        "upload",
        env.local_path("report.pdf").to_str().unwrap(),
    )
    .with_status(TaskStatus::Pending);
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    // Simulate the app being relaunched with the SAME on-demand config —
    // this is exactly where `TaskQueue::new` would replay the task above,
    // if it were going to. `resume_incomplete_tasks` only ever SENDS the
    // resumed task to the dispatcher's channel before `Mount::new` returns
    // — actually picking it up and changing its status happens on a
    // separately spawned task, so give it a moment to run before checking.
    env.restart_mount().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let task = env
        .all_tasks()
        .into_iter()
        .find(|t| t.id == "parked-task")
        .expect("the task record should still exist");
    assert_eq!(
        task.status.as_str(),
        "pending",
        "an on-demand mount must never touch a parked task at startup — replay would have \
         moved it to running/cancelled"
    );
    assert!(
        env.server.received_requests().await.unwrap().is_empty(),
        "replaying the parked task would have hit the (mocked) upload endpoint"
    );
}

/// Review finding 1 (twin replay path): `Mount::re_enqueue_offline_tasks`
/// is the SAME choke point `heartbeat.rs` (offline→online), `remote_
/// events.rs` (SSE `Resumed`/`Subscribed`), and the `MountCommand::
/// FullSync` handler in `mounts.rs` all call through — none of them
/// mode-check before calling it. D2 says on-demand skips "TaskQueue
/// replay" outright, not just the launch half `resume_on_start` guards;
/// this pins the reconnect/SSE half at the one place all three callers
/// share, rather than needing three separate integration tests.
#[tokio::test]
async fn an_on_demand_mount_never_replays_offline_parked_tasks_on_reconnect() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    env.set_remote_files(vec![]).await;

    // What a mode switch (or a network drop mid-task) leaves behind: a
    // Pending task explicitly parked offline, exactly like a real
    // `FullMirror` drive's `force_offline_waiting` would produce.
    let record = NewTaskRecord::new(
        "offline-parked-task",
        &env.drive_id,
        "upload",
        env.local_path("report.pdf").to_str().unwrap(),
    )
    .with_status(TaskStatus::Pending)
    .with_custom_state(serde_json::json!({ "offline_waiting": true }));
    env.inventory.insert_task_if_not_exist(&record).unwrap();

    // The exact call every reconnect/SSE path makes.
    let resumed = env.mount.re_enqueue_offline_tasks().await.expect("must not error");
    assert_eq!(resumed, 0, "an on-demand mount must never re-enqueue an offline-parked task");

    let task = env
        .all_tasks()
        .into_iter()
        .find(|t| t.id == "offline-parked-task")
        .expect("the task record should still exist");
    assert_eq!(task.status.as_str(), "pending", "the task must be left untouched");
    assert!(
        env.server.received_requests().await.unwrap().is_empty(),
        "re-enqueuing the offline-parked task would have hit the (mocked) upload endpoint"
    );
}

/// D2: on-demand skips both the fs watcher and any full sync — neither an
/// automatic trigger right after `start()` nor an explicit `full_sync()`
/// call (the choke point's own unreachable guard) may ever reach the
/// remote.
#[tokio::test]
async fn an_on_demand_mount_starts_no_fs_watcher_and_no_full_sync() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    env.set_remote_files(vec![remote_file("report.pdf", 10, "etag-1")]).await;

    // Settle: give any errantly-spawned worker a moment to fire before
    // asserting nothing did.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        env.server.received_requests().await.unwrap().is_empty(),
        "an on-demand mount must not proactively contact the server after start()"
    );

    // The single choke point every full sync funnels through must refuse
    // to run for an on-demand drive rather than perform a real 3-way sync.
    env.full_sync().await.expect("full_sync must no-op, not error, for an on-demand drive");
    assert!(
        env.server.received_requests().await.unwrap().is_empty(),
        "full_sync's on-demand guard must return before ever listing the remote"
    );
}

/// Review finding 2: `FileEvent::Subscribed` (`remote_events.rs`) and
/// `DriveManager::start_sync` both send `MountCommand::FullSync`
/// unconditionally, for ANY drive — a routine, expected event for an
/// on-demand drive (every fresh SSE subscription), not an edge case. The
/// handler itself must recognize on-demand and skip its whole body,
/// rather than relying on `full_sync`'s own guard two calls deep (which
/// would still no-op correctly, but only after marking
/// `initial_sync_completed` true off the back of a sync that never
/// happened, and after firing a should-be-unreachable warning on every
/// single routine subscription).
#[tokio::test]
async fn the_full_sync_command_handler_is_a_no_op_for_an_on_demand_drive() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    env.set_remote_files(vec![]).await;
    env.mount.spawn_command_processor(env.mount.clone()).await;

    assert!(!env.mount.get_status_flags().await.is_initial_sync_completed());

    env.mount
        .command_tx
        .send(cloudreve_sync::drive::commands::MountCommand::FullSync)
        .expect("send FullSync");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        !env.mount.get_status_flags().await.is_initial_sync_completed(),
        "the FullSync handler must not mark an on-demand drive as having completed a full \
         sync it never actually performed"
    );
    assert!(
        env.server.received_requests().await.unwrap().is_empty(),
        "the FullSync handler must never reach the remote for an on-demand drive"
    );
}

/// D2/D5, via the seam (`vfs_mode`'s module doc): starting an on-demand
/// mount pre-cleans, THEN requests exactly one OS mount, at the drive's
/// `sync_path`, with a real positive cache cap. The `PreClean` entry
/// preceding `Mount` also pins review finding 4's ordering fix at the
/// `Mount`/integration level (the low-level unit-test pin lives in
/// `vfs_mode.rs`'s own `attach_pre_cleans_before_checking_the_mountpoint_
/// is_empty`).
#[tokio::test]
async fn adding_an_on_demand_drive_requests_a_mount_at_sync_path() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;

    let hook = env.vfs_mount_hook.clone().expect("an on-demand env must install a test hook");
    let calls = hook.calls();
    assert_eq!(calls.len(), 2, "starting the mount must pre-clean, then request one OS mount");
    assert_eq!(
        calls[0],
        MountSeamCall::PreClean { mountpoint: env.sync_dir.clone() },
        "pre-clean must run before the mount is requested"
    );
    match &calls[1] {
        MountSeamCall::Mount { mountpoint, cache_max_bytes } => {
            assert_eq!(mountpoint, &env.sync_dir, "must mount at the drive's sync_path");
            assert!(*cache_max_bytes > 0, "the effective cache cap must be a real, positive size");
        }
        other => panic!("expected a Mount request, got {other:?}"),
    }
}

/// D5: pause unmounts (the volume disappears — no half-alive mount); a
/// resume-after-pause remounts the same drive. Observed entirely through
/// the seam's call counters.
#[tokio::test]
async fn pausing_unmounts_and_resuming_remounts() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let hook = env.vfs_mount_hook.clone().expect("an on-demand env must install a test hook");
    assert_eq!(hook.mount_count(), 1, "starting the mount already requested one mount");
    assert_eq!(hook.unmount_count(), 0);

    env.mount.pause().await;
    assert_eq!(hook.unmount_count(), 1, "pausing an on-demand drive must unmount it");
    assert_eq!(hook.mount_count(), 1, "pausing must not itself request another mount");

    env.mount.resume().await;
    env.mount.remount_on_demand().await.expect("resuming must be able to remount");
    assert_eq!(hook.mount_count(), 2, "resuming a paused on-demand drive must remount it");
    assert_eq!(hook.unmount_count(), 1, "resuming must not unmount again");
}

/// D5: deleting an on-demand drive removes its per-drive vfs cache
/// directory, not just its inventory rows.
#[tokio::test]
async fn deleting_removes_the_cache_dir() {
    let env = TestEnv::with_mode(DriveMode::OnDemand).await;
    let cache_dir = cache_dir_for(&env.drive_id).expect("resolve the cache dir");
    assert!(cache_dir.exists(), "starting the on-demand mount must have created its cache dir");

    env.mount.delete().await.expect("delete must succeed");

    assert!(!cache_dir.exists(), "deleting an on-demand drive must remove its cache dir");
}
