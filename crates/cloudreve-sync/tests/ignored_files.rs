//! Files the sync engine must never move, in either direction.

mod common;

use common::{remote_file, TestEnv};

/// Poll until `done` holds, failing after `timeout`. A fixed sleep makes any
/// count assertion behind it a race against the command processor on a
/// loaded CI runner — and passes vacuously when nothing was processed at all.
async fn wait_until(timeout: std::time::Duration, msg: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "{msg}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Finder drops a `.DS_Store` in every folder it merely displays. Nobody asked
/// for it and it means nothing on another machine, so it must not be uploaded.
#[tokio::test]
async fn os_junk_appearing_locally_is_never_uploaded() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    env.write_local(".DS_Store", b"finder junk");
    env.write_local("photos/.DS_Store", b"finder junk");
    env.write_local("photos/holiday.jpg", b"a real file");

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.ends_with("photos/holiday.jpg")),
        "junk was queued for upload: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(uploads.len(), 1, "the real file should still be uploaded");
}

/// The path the filesystem watcher actually uses.
///
/// Saving a file in an editor does not run the 3-way merge: the watcher batches
/// the changed paths and sends them straight to the upload queue. Finder having
/// touched the folder's `.DS_Store` in the same batch is the normal case, not
/// the exception.
#[tokio::test]
async fn a_local_change_batch_containing_junk_only_uploads_the_real_file() {
    use cloudreve_sync::drive::commands::MountCommand;
    use cloudreve_sync::drive::sync::SyncMode;
    use std::time::Duration;

    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    env.write_local("notes.txt", b"a real edit");
    env.write_local(".DS_Store", b"finder junk");
    env.write_local(".notes.txt.swp", b"vim swap");

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;

    // The real file goes LAST: the batch is walked in order, so once its
    // upload shows up every junk path before it has already been decided.
    let _ = env.mount.command_tx.send(MountCommand::Sync {
        local_paths: vec![
            env.local_path(".DS_Store"),
            env.local_path(".notes.txt.swp"),
            env.local_path("notes.txt"),
        ],
        mode: SyncMode::LocalChanged,
        user_initiated: false,
    });
    wait_until(Duration::from_secs(5), "the real file's upload never appeared", || {
        env.tasks_of_type("upload").iter().any(|t| t.local_path.ends_with("notes.txt"))
    })
    .await;

    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.ends_with("notes.txt")),
        "junk from the watcher batch was uploaded: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(uploads.len(), 1, "the edited file should still be uploaded");
}

/// The incremental path fed by the event stream must filter too.
///
/// A remote event names a path and goes straight to the download queue without
/// the 3-way merge, so a `.DS_Store` uploaded by an older client — or by another
/// machine still running one — would land here unfiltered.
#[tokio::test]
async fn a_remote_event_for_ignored_junk_does_not_trigger_a_download() {
    use cloudreve_sync::drive::commands::MountCommand;
    use cloudreve_sync::drive::sync::SyncMode;
    use std::time::Duration;

    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    let mount = env.mount.clone();
    mount.spawn_command_processor(mount.clone()).await;

    // Junk first, real file last: the batch is walked in order, so once the
    // real download shows up the junk path has already been decided.
    let _ = env.mount.command_tx.send(MountCommand::Sync {
        local_paths: vec![env.local_path(".DS_Store"), env.local_path("photos/holiday.jpg")],
        mode: SyncMode::RemoteChanged,
        user_initiated: false,
    });
    wait_until(Duration::from_secs(5), "the real file's download never appeared", || {
        env.tasks_of_type("download").iter().any(|t| t.local_path.ends_with("holiday.jpg"))
    })
    .await;

    let downloads = env.tasks_of_type("download");
    assert!(
        downloads.iter().all(|t| t.local_path.ends_with("photos/holiday.jpg")),
        "junk was queued for download: {:?}",
        downloads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(downloads.len(), 1, "the real file should still be downloaded");
}

/// Junk that already made it to the server before the ignore rules existed must
/// go quiet, not keep churning.
///
/// The 3-way merge only consults the ignore list on the "new file" branches; a
/// path already tracked in the inventory and present on both sides goes straight
/// to the etag comparison, so a `.DS_Store` rewritten by Finder on one machine
/// would be downloaded onto every other one, forever.
#[tokio::test]
async fn os_junk_already_tracked_stops_being_transferred() {
    let env = TestEnv::new().await;

    env.write_local(".DS_Store", b"finder junk");
    env.track_synced(".DS_Store", "etag-1");
    // Another machine rewrote it: new etag, so the engine would want it.
    env.set_remote_files(vec![remote_file(".DS_Store", 11, "etag-2")]).await;

    env.full_sync().await.unwrap();

    assert!(
        env.tasks_of_type("download").is_empty(),
        "an ignored file was queued for download"
    );
    assert!(
        env.tasks_of_type("upload").is_empty(),
        "an ignored file was queued for upload"
    );
    // The row stays, dormant: it is the sync baseline, and deleting it loses
    // any edit made while the path is ignored (see the round-trip test below).
    assert!(
        env.db_entry(".DS_Store").is_some(),
        "the inventory row is the sync baseline and must survive being ignored"
    );
}

/// Junk sitting on the server that this machine has never seen must stay there.
///
/// This is the state of anyone upgrading: their drive is already full of
/// `.DS_Store` uploaded by the previous version, and of junk pushed by whatever
/// other client they use. A fresh install must not pull all of it down.
#[tokio::test]
async fn junk_that_only_exists_on_the_server_is_not_downloaded() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![
        remote_file(".DS_Store", 6148, "etag-junk"),
        remote_file("report.pdf", 2048, "etag-real"),
    ])
    .await;

    env.full_sync().await.unwrap();

    let downloads = env.tasks_of_type("download");
    assert!(
        downloads.iter().all(|t| t.local_path.ends_with("report.pdf")),
        "server-side junk was pulled down: {:?}",
        downloads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(downloads.len(), 1, "the real file should still be downloaded");
}

/// Spotlight's index is a directory of thousands of shards, none of which is
/// named `.Spotlight-V100` — the walker only ever reports the files inside it.
/// Ignoring the directory name alone would sync the whole index.
#[tokio::test]
async fn the_contents_of_a_junk_directory_are_not_uploaded() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    env.write_local(".Spotlight-V100/Store-V2/abc/0.indexHead", b"index");
    env.write_local(".Spotlight-V100/VolumeConfiguration.plist", b"plist");
    env.write_local("report.pdf", b"a real file");

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.ends_with("report.pdf")),
        "the Spotlight index was queued for upload: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(uploads.len(), 1, "the real file should still be uploaded");
}

/// The download side of the same problem: another machine pushed its whole
/// Spotlight index. None of those paths is named `.Spotlight-V100`, so only the
/// subtree rule stops them.
#[tokio::test]
async fn the_contents_of_a_junk_directory_are_not_downloaded() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![
        remote_file(".Spotlight-V100/Store-V2/abc/0.indexHead", 512, "etag-a"),
        remote_file(".Trashes/501/deleted.txt", 12, "etag-b"),
        remote_file("report.pdf", 2048, "etag-real"),
    ])
    .await;

    env.full_sync().await.unwrap();

    let downloads = env.tasks_of_type("download");
    assert!(
        downloads.iter().all(|t| t.local_path.ends_with("report.pdf")),
        "junk directory contents were pulled down: {:?}",
        downloads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(downloads.len(), 1, "the real file should still be downloaded");
}

/// A drive shared with a Windows machine.
///
/// Explorer has shipped both `Thumbs.db` and `thumbs.db` over the years, and a
/// `.DS_Store` that has been through a case-insensitive volume comes back as
/// `.ds_store`. The rules are written in one spelling; matching only that
/// spelling lets every other variant through in both directions.
#[tokio::test]
async fn junk_written_in_another_case_is_still_ignored_by_a_real_sync() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![
        remote_file("thumbs.db", 8192, "etag-junk"),
        remote_file("report.pdf", 2048, "etag-real"),
    ])
    .await;

    env.write_local(".ds_store", b"finder junk");
    env.write_local("photos/DESKTOP.INI", b"explorer junk");
    env.write_local("invoice.xlsx", b"a real file");

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.ends_with("invoice.xlsx")),
        "lower/upper-case junk was uploaded: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(uploads.len(), 1, "the real file should still be uploaded");

    let downloads = env.tasks_of_type("download");
    assert!(
        downloads.iter().all(|t| t.local_path.ends_with("report.pdf")),
        "lower-case junk was pulled down: {:?}",
        downloads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(downloads.len(), 1, "the real file should still be downloaded");
}

/// Tasks queued by the *previous* version, before the filter existed.
///
/// A task parked with `offline_waiting` survives in the database across an
/// upgrade, and `re_enqueue_offline_tasks` replays it straight into the worker
/// pool on reconnection — bypassing the merge, the watcher and the event stream
/// alike. Left alone it would push one last `.DS_Store` to the server.
#[tokio::test]
async fn offline_tasks_left_over_from_an_older_version_are_not_replayed() {
    use cloudreve_sync::inventory::{NewTaskRecord, TaskStatus};

    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    for (id, rel) in [("junk-task", ".DS_Store"), ("real-task", "report.pdf")] {
        env.write_local(rel, b"content");
        let record = NewTaskRecord::new(
            id,
            &env.drive_id,
            "upload",
            env.local_path(rel).to_str().unwrap(),
        )
        .with_status(TaskStatus::Pending)
        .with_custom_state(serde_json::json!({ "offline_waiting": true }));
        env.inventory.insert_task_if_not_exist(&record).unwrap();
    }

    // Through the wrapper the reconnection path actually calls, not the queue
    // directly: passing the matcher in by hand would test a call site that does
    // not exist and miss the drive handing over the wrong one.
    let replayed = env.mount.re_enqueue_offline_tasks().await.unwrap();

    assert_eq!(replayed, 1, "only the real file should have been replayed");

    let junk = env
        .all_tasks()
        .into_iter()
        .find(|t| t.id == "junk-task")
        .expect("the junk task record should still exist");
    assert_ne!(
        junk.status.as_str(),
        "pending",
        "the junk task was left pending: it comes back on the next reconnect"
    );
    assert!(
        junk.custom_state.is_none(),
        "the offline_waiting flag must be cleared so it is never picked up again"
    );
}

/// Ignoring a file must never destroy anything.
///
/// Two invariants: the file itself stays on disk, and the inventory row stays
/// in the database. Without the guard, the `(in_db, local, remote) =
/// (true, true, false)` arm would untrack this row — so the db assertion also
/// proves the guard fires before the merge arms do.
#[tokio::test]
async fn ignoring_a_file_never_deletes_it_from_disk() {
    let env = TestEnv::new().await;

    env.write_local(".DS_Store", b"finder junk");
    env.track_synced(".DS_Store", "etag-1");
    env.set_remote_files(vec![]).await;

    env.full_sync().await.unwrap();

    assert!(
        env.tasks_of_type("upload").is_empty(),
        "junk was re-uploaded after the server dropped it"
    );
    assert!(
        env.db_entry(".DS_Store").is_some(),
        "the row stays dormant: it is the baseline, not a transfer order"
    );
    assert!(
        env.local_path(".DS_Store").exists(),
        "ignoring a file must never delete it locally"
    );
}

/// A drive whose *saved config* holds a bad pattern must still start with the
/// defaults on. This is the startup path, not the Settings-save path: the
/// matcher is built in `Mount::new` from the persisted list, with its own
/// fallback.
#[tokio::test]
async fn a_bad_pattern_in_the_saved_config_does_not_disable_the_defaults() {
    let env = TestEnv::with_ignore_patterns(vec!["[unclosed".to_string(), "*.log".to_string()])
        .await;
    env.set_remote_files(vec![]).await;

    env.write_local(".DS_Store", b"finder junk");
    env.write_local("debug.log", b"user-excluded");
    env.write_local("report.pdf", b"a real file");

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.ends_with("report.pdf")),
        "a stored typo disabled the filtering at startup: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(uploads.len(), 1, "the real file should still be uploaded");
}

/// Saving your own rules in Settings must not switch the built-in ones off.
///
/// `update_ignore_patterns` rebuilds the matcher from scratch with only the
/// user's list, so the defaults survive purely because they are re-added inside
/// the constructor. Nothing else pins that down.
#[tokio::test]
async fn saving_your_own_patterns_in_settings_keeps_the_built_in_defaults() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    env.mount.update_ignore_patterns(vec!["*.log".to_string()]).await.unwrap();

    env.write_local(".DS_Store", b"finder junk");
    env.write_local("debug.log", b"user-excluded");
    env.write_local("report.pdf", b"a real file");

    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert!(
        uploads.iter().all(|t| t.local_path.ends_with("report.pdf")),
        "a rule was lost when the user saved their own patterns: {:?}",
        uploads.iter().map(|t| &t.local_path).collect::<Vec<_>>()
    );
    assert_eq!(uploads.len(), 1, "the real file should still be uploaded");
}

/// A typo in Settings is refused at save time, and refusing it changes nothing.
///
/// Loading stays tolerant (see the saved-config test above) — but the dialog
/// is the one place the user can actually fix the line. Accepting it silently
/// would display a rule that does nothing: the Save error string exists in
/// every locale and could never fire.
#[tokio::test]
async fn a_typo_in_the_users_patterns_is_rejected_when_saving() {
    let env = TestEnv::new().await;

    env.mount.update_ignore_patterns(vec!["*.log".to_string()]).await.unwrap();

    let err = env
        .mount
        .update_ignore_patterns(vec!["[unclosed".to_string(), "*.tmp".to_string()])
        .await
        .expect_err("an unparseable pattern must fail the save");
    assert!(
        err.to_string().contains("[unclosed"),
        "the error must name the offending line for the dialog: {err:#}"
    );

    // The rejected list must not half-apply: the old rules stay in force.
    assert!(
        env.mount.is_ignored(&env.local_path("debug.log")).await,
        "the previously saved pattern was lost by a failed save"
    );
    assert!(
        !env.mount.is_ignored(&env.local_path("draft.tmp")).await,
        "part of the rejected list was applied anyway"
    );
}

/// Pausing a file with a pattern, editing it, then removing the pattern.
///
/// The inventory row is the only memory of what was last synced. If ignoring a
/// path throws that row away, un-ignoring it later finds a file present on
/// both sides with no history, stamps it "in sync" as-is — and the edit made
/// in between is never uploaded, with the database claiming both sides match.
#[tokio::test]
async fn edits_made_while_a_path_was_ignored_are_uploaded_once_it_no_longer_is() {
    let env = TestEnv::new().await;

    // In sync on both sides.
    env.write_local("report.psd", b"version 1");
    env.track_synced("report.psd", "etag-1");
    env.set_remote_files(vec![remote_file("report.psd", 9, "etag-1")]).await;

    // The user pauses it with a pattern, a sync runs, then they edit it.
    env.mount.update_ignore_patterns(vec!["*.psd".to_string()]).await.unwrap();
    env.full_sync().await.unwrap();
    assert!(
        env.all_tasks().is_empty(),
        "an ignored file must not be transferred at all"
    );

    env.write_local("report.psd", b"version 2, edited while the pattern was on");

    // Pattern removed: the edit has to reach the server.
    env.mount.update_ignore_patterns(vec![]).await.unwrap();
    env.full_sync().await.unwrap();

    let uploads = env.tasks_of_type("upload");
    assert_eq!(
        uploads.len(),
        1,
        "the edit made while the file was ignored was silently dropped"
    );
    assert!(uploads[0].local_path.ends_with("report.psd"));
}

/// A conflict waiting for the user's decision is frozen, not forgotten.
///
/// If ignoring the path wiped its inventory row, the pending conflict would
/// vanish from the dashboard unresolved — two divergent copies left behind
/// with no record that a decision was ever needed.
#[tokio::test]
async fn a_pending_conflict_survives_its_path_being_ignored() {
    use cloudreve_sync::inventory::ConflictState;

    let env = TestEnv::new().await;

    env.write_local("notes.txt", b"local version");
    env.track_synced("notes.txt", "etag-1");
    env.set_remote_files(vec![remote_file("notes.txt", 14, "etag-2")]).await;

    let path = env.local_path("notes.txt");
    env.inventory
        .mark_as_conflicted(path.to_str().unwrap(), Some(ConflictState::Pending))
        .unwrap();

    env.mount.update_ignore_patterns(vec!["notes.*".to_string()]).await.unwrap();
    env.full_sync().await.unwrap();

    assert!(env.all_tasks().is_empty(), "a frozen conflict must not be transferred");
    let entry = env.db_entry("notes.txt").expect("the conflicted row was wiped");
    assert_eq!(
        entry.conflict_state,
        Some(ConflictState::Pending),
        "the pending conflict was silently discarded"
    );
}

/// The other way stale tasks come back: a normal app relaunch.
///
/// `TaskQueue::new` replays every Pending/Running task straight out of the
/// database at startup — before any reconnection event, so the offline-replay
/// filter never sees them. A junk upload interrupted by quitting the app would
/// be pushed to the server at the very next boot.
#[tokio::test]
async fn junk_tasks_left_in_the_database_are_not_replayed_at_the_next_launch() {
    use cloudreve_sync::inventory::{NewTaskRecord, TaskStatus};

    let mut env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;

    // What an interrupted run leaves behind: plain Pending tasks, no flags.
    for (id, rel) in [("junk-task", ".DS_Store"), ("real-task", "report.pdf")] {
        env.write_local(rel, b"content");
        let record = NewTaskRecord::new(
            id,
            &env.drive_id,
            "upload",
            env.local_path(rel).to_str().unwrap(),
        )
        .with_status(TaskStatus::Pending);
        env.inventory.insert_task_if_not_exist(&record).unwrap();
    }

    env.restart_mount().await;

    let task = |id: &str| {
        env.all_tasks()
            .into_iter()
            .find(|t| t.id == id)
            .expect("task record should still exist")
    };
    assert_eq!(
        task("junk-task").status.as_str(),
        "cancelled",
        "the junk task survived the restart and was replayed"
    );
    assert_ne!(
        task("real-task").status.as_str(),
        "cancelled",
        "the real task must still be replayed at startup"
    );
}
