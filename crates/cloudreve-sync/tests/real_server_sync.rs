//! Drives added through `DriveManager::add_drive` — what the app's "add
//! drive" command runs — against a REAL Cloudreve server (see
//! `cloudreve-vfs/tests/real_server/mod.rs`; skipped unless
//! `CLOUDREVE_E2E_URL` is set).
//!
//! Mirror drives are driven through `std::fs` on their sync folder (the fs
//! watcher, the task queue, SSE and full sync all real). On-demand drives
//! run with the OS mount seam stubbed (`MountTestHook`) and are driven
//! through their real `Vfs` facade, so this file runs on any machine; the
//! same drives mounted for real are covered by `real_server_mounted.rs`.

#[path = "../../cloudreve-vfs/tests/real_server/mod.rs"]
mod real_server;
mod real_server_app;

use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use cloudreve_sync::inventory::ConflictState;
use cloudreve_vfs::tree::NodeId;
use cloudreve_vfs::vfs::Vfs;
use real_server::{eventually, pattern};
use real_server_app::{
    App, MIB, Opts, run, wait_local, wait_local_gone, wait_remote, wait_remote_gone, write,
};

// ------------------------------------------------------------ full mirror

#[test]
fn adding_a_mirror_drive_downloads_the_whole_remote_tree() {
    run(async {
        let big = pattern(30 * MIB + 3, 30);
        let big2 = big.clone();
        let Some(app) = App::add("mirror-initial", Opts::default(), |r| async move {
            r.put("top.txt", b"top").await;
            r.put("a/b/c/deep.txt", b"deep").await;
            r.put("unicode/café 🎉.txt", b"unicode").await;
            r.put("unicode/cafe\u{301}-nfd.txt", b"nfd").await;
            r.put("empty.txt", b"").await;
            r.upload("big.bin", &big2).await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "top.txt", b"top").await;
        wait_local(&app, "a/b/c/deep.txt", b"deep").await;
        wait_local(&app, "unicode/café 🎉.txt", b"unicode").await;
        wait_local(&app, "unicode/cafe\u{301}-nfd.txt", b"nfd").await;
        wait_local(&app, "empty.txt", b"").await;
        wait_local(&app, "big.bin", &big).await;
        app.cleanup().await;
    })
}

#[test]
fn a_mirror_drive_with_more_files_than_one_page_downloads_them_all() {
    run(async {
        let Some(app) = App::add("mirror-paging", Opts::default(), |r| async move {
            for batch in (0..2100).collect::<Vec<usize>>().chunks(16) {
                let mut set = tokio::task::JoinSet::new();
                for i in batch {
                    let r = r.clone_handle();
                    let i = *i;
                    set.spawn(async move { r.put(&format!("many/f-{i:05}.txt"), format!("{i}").as_bytes()).await });
                }
                while set.join_next().await.is_some() {}
            }
        })
        .await
        else {
            return;
        };
        let dir = app.local("many");
        eventually("2100 files mirrored", Duration::from_secs(180), || {
            let dir = dir.clone();
            async move {
                let n = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
                if n == 2100 { Ok(()) } else { Err(format!("{n} files")) }
            }
        })
        .await;
        assert_eq!(std::fs::read(app.local("many/f-02099.txt")).unwrap(), b"2099");
        app.cleanup().await;
    })
}

#[test]
fn local_changes_in_a_mirror_drive_reach_the_server() {
    run(async {
        let Some(app) = App::mirror("mirror-up").await else { return };
        tokio::time::sleep(Duration::from_secs(2)).await; // initial sync of an empty folder

        write(&app.local("new.txt"), b"created locally");
        write(&app.local("deep/er/still/file.txt"), b"deep");
        let big = pattern(45 * MIB + 11, 31);
        write(&app.local("big.bin"), &big);
        wait_remote(&app.remote, "new.txt", b"created locally").await;
        wait_remote(&app.remote, "deep/er/still/file.txt", b"deep").await;
        wait_remote(&app.remote, "big.bin", &big).await;

        // Modify, append, rename, move, delete.
        write(&app.local("new.txt"), b"modified locally");
        wait_remote(&app.remote, "new.txt", b"modified locally").await;
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(app.local("new.txt")).unwrap();
            f.write_all(b" + appended").unwrap();
        }
        wait_remote(&app.remote, "new.txt", b"modified locally + appended").await;
        // Renamed, moved and deleted seconds after being written: the old
        // names leave the server too (see `drive::propagate`), and nothing
        // removed comes back locally — the server's own events for our
        // earlier uploads arrive seconds later and must not resurrect it.
        std::fs::rename(app.local("new.txt"), app.local("renamed.txt")).unwrap();
        wait_remote(&app.remote, "renamed.txt", b"modified locally + appended").await;
        wait_remote_gone(&app.remote, "new.txt").await;
        std::fs::rename(app.local("renamed.txt"), app.local("deep/moved.txt")).unwrap();
        wait_remote(&app.remote, "deep/moved.txt", b"modified locally + appended").await;
        wait_remote_gone(&app.remote, "renamed.txt").await;
        // Same macOS ~30 s "created" rule as for renames (`OLD_ENOUGH`):
        // deleted sooner, they only leave the server at the next full sync.
        tokio::time::sleep(OLD_ENOUGH).await;
        std::fs::remove_dir_all(app.local("deep/er")).unwrap();
        std::fs::remove_file(app.local("big.bin")).unwrap();
        wait_remote_gone(&app.remote, "deep/er").await;
        wait_remote_gone(&app.remote, "big.bin").await;
        tokio::time::sleep(Duration::from_secs(8)).await;
        for gone in ["new.txt", "renamed.txt", "deep/er", "big.bin"] {
            assert!(!app.local(gone).exists(), "{gone} came back locally");
        }
        assert_eq!(app.remote.read("deep/moved.txt").await.unwrap(), b"modified locally + appended");
        app.cleanup().await;
    })
}

/// macOS reports a rename of a file created in the last ~30 s as a new
/// file (see `drive::propagate`); files a user renames have usually been
/// there for longer.
const OLD_ENOUGH: Duration = Duration::from_secs(32);

/// Longer than the fs watcher's 2 s debounce.
const SETTLE: Duration = Duration::from_secs(4);

/// The server's id for `rel`: the same id after a change means the server
/// renamed or moved its file, not that a copy was uploaded.
async fn remote_id(app: &App, rel: &str) -> String {
    app.remote.info(rel).await.unwrap_or_else(|| panic!("{rel} is not on the server"))["id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn deleting_in_a_mirror_drive_moves_files_and_folders_to_the_server_trash() {
    run(async {
        let Some(app) = App::add("mirror-delete", Opts::default(), |r| async move {
            r.put("doc-del.txt", b"doc").await;
            r.put("Folder-del/a.txt", b"a").await;
            r.put("Folder-del/sub/b.txt", b"b").await;
            r.put("keep.txt", b"keep").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "doc-del.txt", b"doc").await;
        wait_local(&app, "Folder-del/sub/b.txt", b"b").await;
        wait_local(&app, "keep.txt", b"keep").await;
        // A file created and removed within the watcher's 2 s window — on
        // macOS, within ~30 s, as FSEvents keeps reporting it "created" — is
        // dropped by the debouncer as one that came and went; the next full
        // sync catches it. Deleting a file just downloaded is not this test.
        tokio::time::sleep(OLD_ENOUGH).await;

        std::fs::remove_file(app.local("doc-del.txt")).unwrap();
        wait_remote_gone(&app.remote, "doc-del.txt").await;
        std::fs::remove_dir_all(app.local("Folder-del")).unwrap();
        wait_remote_gone(&app.remote, "Folder-del").await;

        let trashed = app.remote.trashed().await;
        assert!(trashed.contains(&app.remote.uri("doc-del.txt")), "not in the trash: {trashed:?}");
        assert!(trashed.contains(&app.remote.uri("Folder-del")), "the folder must be trashed as one item: {trashed:?}");
        assert!(!trashed.contains(&app.remote.uri("Folder-del/a.txt")), "{trashed:?}");
        assert_eq!(app.remote.read("keep.txt").await.unwrap(), b"keep");
        app.cleanup().await;
    })
}

/// "Delete and rename in the cloud too" off (every drive added before the
/// switch existed): the server keeps what is deleted here, and a renamed
/// file is uploaded under its new name next to the old one. Turned on from
/// Settings, the next delete goes through — no restart.
#[test]
fn a_mirror_drive_switched_off_keeps_the_server_untouched_until_switched_on() {
    run(async {
        let Some(app) = App::add("mirror-switch", Opts { propagate_deletes: false, ..Opts::default() }, |r| async move {
            r.put("kept.txt", b"kept").await;
            r.put("old-name.txt", b"renamed").await;
            r.put("later.txt", b"later").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "kept.txt", b"kept").await;
        wait_local(&app, "old-name.txt", b"renamed").await;
        wait_local(&app, "later.txt", b"later").await;
        tokio::time::sleep(OLD_ENOUGH).await;

        std::fs::remove_file(app.local("kept.txt")).unwrap();
        std::fs::rename(app.local("old-name.txt"), app.local("new-name.txt")).unwrap();
        wait_remote(&app.remote, "new-name.txt", b"renamed").await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(app.remote.read("kept.txt").await.unwrap(), b"kept", "deleted while switched off");
        assert_eq!(app.remote.read("old-name.txt").await.unwrap(), b"renamed", "renamed while switched off");

        app.manager.set_drive_propagate_deletes(&app.id, true).await.unwrap();
        std::fs::remove_file(app.local("later.txt")).unwrap();
        wait_remote_gone(&app.remote, "later.txt").await;
        app.cleanup().await;
    })
}

#[test]
fn renaming_and_moving_in_a_mirror_drive_renames_and_moves_on_the_server() {
    run(async {
        let Some(app) = App::add("mirror-rename", Opts::default(), |r| async move {
            r.put("old.txt", b"old").await;
            r.put("Docs/a.txt", b"a").await;
            r.put("Docs/sub/b.txt", b"b").await;
            r.mkdir("dst").await;
            r.put("x.txt", b"x").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "old.txt", b"old").await;
        wait_local(&app, "Docs/sub/b.txt", b"b").await;
        tokio::time::sleep(OLD_ENOUGH).await;
        let file_id = remote_id(&app, "old.txt").await;
        let folder_id = remote_id(&app, "Docs").await;

        std::fs::rename(app.local("old.txt"), app.local("new.txt")).unwrap();
        wait_remote_gone(&app.remote, "old.txt").await;
        assert_eq!(remote_id(&app, "new.txt").await, file_id, "renamed by a new upload, not a rename");

        std::fs::rename(app.local("Docs"), app.local("Papers")).unwrap();
        wait_remote_gone(&app.remote, "Docs").await;
        assert_eq!(remote_id(&app, "Papers").await, folder_id, "folder renamed by a re-upload");
        assert_eq!(app.remote.read("Papers/sub/b.txt").await.unwrap(), b"b");

        // `new.txt` only just got its name: same ~30 s rule on macOS.
        tokio::time::sleep(OLD_ENOUGH).await;
        // An empty server folder is not mirrored: the user makes one here.
        std::fs::create_dir_all(app.local("dst")).unwrap();
        // The new folder's own watcher batch first, as when a person
        // makes a folder then drags a file in (a move in the same batch
        // races the folder's scan on Linux).
        tokio::time::sleep(SETTLE).await;
        std::fs::rename(app.local("new.txt"), app.local("dst/new.txt")).unwrap();
        wait_remote_gone(&app.remote, "new.txt").await;
        assert_eq!(remote_id(&app, "dst/new.txt").await, file_id, "moved by a new upload, not a move");

        // Moved and renamed at once, into a folder the server does not have.
        std::fs::create_dir_all(app.local("Archive/2026")).unwrap();
        tokio::time::sleep(SETTLE).await;
        std::fs::rename(app.local("Papers/a.txt"), app.local("Archive/2026/renamed-a.txt")).unwrap();
        wait_remote(&app.remote, "Archive/2026/renamed-a.txt", b"a").await;
        wait_remote_gone(&app.remote, "Papers/a.txt").await;

        let trashed = app.remote.trashed().await;
        for old in ["old.txt", "Docs", "new.txt", "Papers/a.txt"] {
            assert!(!trashed.contains(&app.remote.uri(old)), "{old} was deleted, not renamed: {trashed:?}");
        }
        app.cleanup().await;
    })
}

#[test]
fn a_mirror_delete_and_rename_made_offline_reach_the_server_on_reconnect() {
    run(async {
        let Some(app) = App::add("mirror-offline-delete", Opts { proxied: true, ..Opts::default() }, |r| async move {
            r.put("off.txt", b"off").await;
            r.put("ren.txt", b"ren").await;
            r.put("keep.txt", b"keep").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "off.txt", b"off").await;
        wait_local(&app, "ren.txt", b"ren").await;
        tokio::time::sleep(OLD_ENOUGH).await;
        let id = remote_id(&app, "ren.txt").await;

        app.proxy().set_down(true);
        std::fs::remove_file(app.local("off.txt")).unwrap();
        std::fs::rename(app.local("ren.txt"), app.local("ren2.txt")).unwrap();
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(app.remote.info("off.txt").await.is_some(), "deleted while the server was unreachable?");
        assert!(app.remote.info("ren.txt").await.is_some());

        app.proxy().set_down(false);
        wait_remote_gone(&app.remote, "off.txt").await;
        wait_remote_gone(&app.remote, "ren.txt").await;
        assert_eq!(remote_id(&app, "ren2.txt").await, id, "the parked rename became an upload");
        assert!(app.remote.trashed().await.contains(&app.remote.uri("off.txt")));
        app.cleanup().await;
    })
}

#[test]
fn a_mirror_file_deleted_while_paused_is_deleted_on_resume_unless_the_server_changed_it() {
    run(async {
        let Some(app) = App::add("mirror-paused-delete", Opts::default(), |r| async move {
            r.put("plain.txt", b"plain").await;
            r.put("edited.txt", b"v1").await;
            r.put("keep.txt", b"keep").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "plain.txt", b"plain").await;
        wait_local(&app, "edited.txt", b"v1").await;

        app.manager.pause_drive(&app.id).await.unwrap();
        std::fs::remove_file(app.local("plain.txt")).unwrap();
        std::fs::remove_file(app.local("edited.txt")).unwrap();
        // Someone else edits it meanwhile: their edit wins over the delete.
        app.remote.put("edited.txt", b"v2, edited on the web").await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(app.remote.info("plain.txt").await.is_some(), "a paused drive deleted on the server");
        app.manager.resume_drive(&app.id).await.unwrap();

        wait_remote_gone(&app.remote, "plain.txt").await;
        wait_local(&app, "edited.txt", b"v2, edited on the web").await;
        assert_eq!(app.remote.read("edited.txt").await.unwrap(), b"v2, edited on the web");
        app.cleanup().await;
    })
}

#[test]
fn deleting_most_of_a_mirror_drive_at_once_deletes_nothing_on_the_server() {
    run(async {
        let Some(app) = App::add("mirror-mass-delete", Opts::default(), |r| async move {
            for batch in (0..30).collect::<Vec<usize>>().chunks(10) {
                let mut set = tokio::task::JoinSet::new();
                for i in batch {
                    let r = r.clone_handle();
                    let i = *i;
                    set.spawn(async move { r.put(&format!("bulk/f-{i:02}.txt"), b"bulk").await });
                }
                while set.join_next().await.is_some() {}
            }
            r.put("keep-1.txt", b"1").await;
            r.put("keep-2.txt", b"2").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "bulk/f-29.txt", b"bulk").await;
        wait_local(&app, "bulk/f-00.txt", b"bulk").await;
        wait_local(&app, "keep-2.txt", b"2").await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        std::fs::remove_dir_all(app.local("bulk")).unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;

        assert_eq!(app.remote.list("bulk").await.len(), 30, "a mass deletion reached the server");
        assert!(!app.local("bulk").exists(), "held back is not downloaded back");
        app.cleanup().await;
    })
}

#[test]
fn an_editor_safe_save_in_a_mirror_drive_uploads_the_new_content_only() {
    run(async {
        let Some(app) = App::add("mirror-safe-save", Opts::default(), |r| async move {
            r.put("doc.txt", b"version 1").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "doc.txt", b"version 1").await;
        write(&app.local("doc.txt.sb-5f3a"), b"version 2, saved safely");
        std::fs::rename(app.local("doc.txt.sb-5f3a"), app.local("doc.txt")).unwrap();
        wait_remote(&app.remote, "doc.txt", b"version 2, saved safely").await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(app.remote.names("").await, vec!["doc.txt".to_string()]);
        app.cleanup().await;
    })
}

#[test]
fn copying_2000_files_into_a_mirror_drive_uploads_every_one() {
    run(async {
        let Some(app) = App::mirror("mirror-bulk").await else { return };
        tokio::time::sleep(Duration::from_secs(2)).await;
        // Built aside, then moved in at once, like a Finder copy finishing.
        let staging = app.tmp.path().join("staging");
        for i in 0..2000 {
            write(&staging.join(format!("f-{i:04}.txt")), format!("bulk {i}").as_bytes());
        }
        std::fs::rename(&staging, app.local("bulk")).unwrap();
        eventually("2000 files on the server", Duration::from_secs(300), || async {
            let n = app.remote.list_or_empty("bulk").await.len();
            if n == 2000 { Ok(()) } else { Err(format!("{n} files")) }
        })
        .await;
        for i in [0usize, 777, 1999] {
            assert_eq!(app.remote.read(&format!("bulk/f-{i:04}.txt")).await.unwrap(), format!("bulk {i}").into_bytes());
        }
        app.cleanup().await;
    })
}

#[test]
fn remote_changes_reach_a_mirror_drive_through_the_event_stream() {
    run(async {
        let Some(app) = App::add("mirror-down", Opts::default(), |r| async move {
            r.put("existing.txt", b"existing").await;
            r.mkdir("dst").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "existing.txt", b"existing").await;

        app.remote.put("from-web.txt", b"uploaded on the web").await;
        wait_local(&app, "from-web.txt", b"uploaded on the web").await;
        app.remote.put("existing.txt", b"edited on the web").await;
        wait_local(&app, "existing.txt", b"edited on the web").await;
        app.remote.rename("from-web.txt", "renamed-on-web.txt").await;
        wait_local(&app, "renamed-on-web.txt", b"uploaded on the web").await;
        wait_local_gone(&app, "from-web.txt").await;
        app.remote.mv(&["renamed-on-web.txt"], "dst").await;
        wait_local(&app, "dst/renamed-on-web.txt", b"uploaded on the web").await;
        wait_local_gone(&app, "renamed-on-web.txt").await;
        app.remote.delete("existing.txt").await;
        wait_local_gone(&app, "existing.txt").await;
        app.remote.delete("dst").await;
        wait_local_gone(&app, "dst").await;
        app.cleanup().await;
    })
}

#[test]
fn a_mirror_edit_racing_a_server_edit_overwrites_neither_and_asks_the_user() {
    run(async {
        let Some(app) = App::add("mirror-conflict", Opts::default(), |r| async move {
            r.put("shared.txt", b"original").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "shared.txt", b"original").await;
        // Pausing holds the local edit back while the server edit lands, so
        // both sides really changed from the same base.
        app.manager.pause_drive(&app.id).await.unwrap();
        write(&app.local("shared.txt"), b"edited locally");
        app.remote.put("shared.txt", b"edited on the server").await;
        app.manager.resume_drive(&app.id).await.unwrap();

        // A mirror drive flags the file for the user to choose (the popup's
        // conflict actions) instead of writing a conflicted copy.
        let path = app.local("shared.txt").to_string_lossy().to_string();
        let inventory = app.manager.get_inventory();
        eventually("the conflict to be flagged", Duration::from_secs(30), || {
            let entry = inventory.query_by_path(&path).ok().flatten();
            async move {
                match entry.and_then(|e| e.conflict_state) {
                    Some(ConflictState::Pending) => Ok(()),
                    other => Err(format!("{other:?}")),
                }
            }
        })
        .await;
        // Server events and the next syncs must not settle it on their own.
        tokio::time::sleep(Duration::from_secs(8)).await;
        assert_eq!(std::fs::read(app.local("shared.txt")).unwrap(), b"edited locally");
        assert_eq!(app.remote.read("shared.txt").await.unwrap(), b"edited on the server");
        app.cleanup().await;
    })
}

#[test]
fn mirror_files_written_while_the_server_is_down_upload_after_reconnect() {
    run(async {
        let Some(app) = App::add("mirror-offline", Opts { proxied: true, ..Opts::default() }, |_| async {}).await
        else {
            return;
        };
        tokio::time::sleep(Duration::from_secs(2)).await;
        app.proxy().set_down(true);
        let data = pattern(3 * MIB, 32);
        write(&app.local("offline.bin"), &data);
        write(&app.local("offline.txt"), b"written offline");
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(app.remote.info("offline.txt").await.is_none());
        app.proxy().set_down(false);
        wait_remote(&app.remote, "offline.txt", b"written offline").await;
        wait_remote(&app.remote, "offline.bin", &data).await;
        app.cleanup().await;
    })
}

#[test]
fn a_paused_mirror_drive_uploads_its_pending_files_on_resume() {
    run(async {
        let Some(app) = App::mirror("mirror-pause").await else { return };
        tokio::time::sleep(Duration::from_secs(2)).await;
        app.manager.pause_drive(&app.id).await.unwrap();
        write(&app.local("while-paused.txt"), b"written while paused");
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(app.remote.info("while-paused.txt").await.is_none(), "a paused drive uploaded");
        app.manager.resume_drive(&app.id).await.unwrap();
        wait_remote(&app.remote, "while-paused.txt", b"written while paused").await;
        app.cleanup().await;
    })
}

#[test]
fn removing_a_mirror_drive_keeps_local_files_and_stops_syncing() {
    run(async {
        let Some(app) = App::add("mirror-remove", Opts::default(), |r| async move {
            r.put("keep.txt", b"keep me").await;
        })
        .await
        else {
            return;
        };
        wait_local(&app, "keep.txt", b"keep me").await;
        app.manager.remove_drive(&app.id).await.unwrap();
        write(&app.local("after-removal.txt"), b"not for the server");
        app.remote.put("remote-after.txt", b"not for the folder").await;
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(std::fs::read(app.local("keep.txt")).unwrap(), b"keep me", "removal deleted local files");
        assert!(app.remote.info("after-removal.txt").await.is_none());
        assert!(!app.local("remote-after.txt").exists());
        assert_eq!(app.remote.read("keep.txt").await.unwrap(), b"keep me", "removal deleted remote files");
        app.remote.cleanup().await;
    })
}

#[test]
fn a_file_over_the_drive_size_limit_is_neither_uploaded_nor_downloaded() {
    run(async {
        let small = pattern(MIB / 2, 33);
        let big = pattern(2 * MIB + 5, 34);
        let big2 = big.clone();
        let Some(app) = App::add(
            "mirror-size-limit",
            Opts { max_file_size_mb: 1, ..Opts::default() },
            |r| async move { r.put("remote-big.bin", &big2).await },
        )
        .await
        else {
            return;
        };
        write(&app.local("small.bin"), &small);
        write(&app.local("local-big.bin"), &big);
        wait_remote(&app.remote, "small.bin", &small).await;
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(app.remote.info("local-big.bin").await.is_none(), "an over-limit file was uploaded");
        assert!(!app.local("remote-big.bin").exists(), "an over-limit file was downloaded");
        assert_eq!(std::fs::read(app.local("local-big.bin")).unwrap(), big, "the local file was touched");
        app.cleanup().await;
    })
}

// -------------------------------------------------------------- on demand

async fn vfs_node(vfs: &Vfs, rel: &str) -> Option<NodeId> {
    let mut node = vfs.tree().root();
    for part in rel.split('/').filter(|p| !p.is_empty()) {
        node = vfs.lookup(node, part).await.ok()??.0;
    }
    Some(node)
}

async fn vfs_names(vfs: &Vfs, rel: &str) -> Vec<String> {
    let Some(dir) = vfs_node(vfs, rel).await else { return Vec::new() };
    let mut v: Vec<String> = vfs.readdir(dir).await.unwrap_or_default().into_iter().map(|(_, a)| a.name).collect();
    v.sort();
    v
}

async fn vfs_read(vfs: &Vfs, rel: &str) -> Option<Vec<u8>> {
    let node = vfs_node(vfs, rel).await?;
    let size = vfs.getattr(node).await.ok()??.size;
    let h = vfs.open(node).await.ok()?;
    let mut out = Vec::new();
    while (out.len() as u64) < size {
        let b = vfs.read(h, out.len() as u64, MIB as u32).await.ok()?;
        if b.is_empty() {
            break;
        }
        out.extend_from_slice(&b);
    }
    let _ = vfs.close(h).await;
    Some(out)
}

async fn vfs_write(vfs: &Vfs, rel: &str, data: &[u8]) {
    let (parent, name) = rel.rsplit_once('/').unwrap_or(("", rel));
    let parent = vfs_node(vfs, parent).await.expect("parent");
    let (_, h) = vfs.create(parent, name).await.expect("create");
    for (i, c) in data.chunks(MIB).enumerate() {
        vfs.write(h, (i * MIB) as u64, c).await.expect("write");
    }
    vfs.close(h).await.expect("close");
}

#[test]
fn remote_changes_show_up_in_an_on_demand_drive_within_seconds() {
    run(async {
        let Some(app) = App::add("od-remote", Opts { mode: DriveMode::OnDemand, ..Opts::default() }, |r| async move {
            r.put("a.txt", b"a").await;
            r.mkdir("sub").await;
        })
        .await
        else {
            return;
        };
        let vfs = app.vfs().await;
        assert_eq!(vfs_names(&vfs, "").await, vec!["a.txt".to_string(), "sub".to_string()]);
        assert!(vfs_names(&vfs, "sub").await.is_empty());
        // Give the event stream time to subscribe.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // The server batches its events for 5s (`fs_event_push_debounce`)
        // and listings stay cached for 5s: "a few seconds" is 10 at most.
        let fast = Duration::from_secs(10);
        app.remote.put("new.txt", b"new from the web").await;
        eventually("a new remote file to be listed", fast, || async {
            let n = vfs_names(&vfs, "").await;
            if n.contains(&"new.txt".to_string()) { Ok(()) } else { Err(format!("{n:?}")) }
        })
        .await;
        assert_eq!(vfs_read(&vfs, "new.txt").await.unwrap(), b"new from the web");

        app.remote.put("a.txt", b"a, edited on the web").await;
        eventually("a remote edit to be read", fast, || async {
            match vfs_read(&vfs, "a.txt").await {
                Some(b) if b == b"a, edited on the web" => Ok(()),
                other => Err(format!("{:?}", other.map(|b| String::from_utf8_lossy(&b).to_string()))),
            }
        })
        .await;

        app.remote.rename("new.txt", "renamed.txt").await;
        eventually("a remote rename", fast, || async {
            let n = vfs_names(&vfs, "").await;
            if n.contains(&"renamed.txt".to_string()) && !n.contains(&"new.txt".to_string()) {
                Ok(())
            } else {
                Err(format!("{n:?}"))
            }
        })
        .await;

        app.remote.mv(&["renamed.txt"], "sub").await;
        eventually("a remote move", fast, || async {
            let root = vfs_names(&vfs, "").await;
            let sub = vfs_names(&vfs, "sub").await;
            if sub == vec!["renamed.txt".to_string()] && !root.contains(&"renamed.txt".to_string()) {
                Ok(())
            } else {
                Err(format!("root {root:?} sub {sub:?}"))
            }
        })
        .await;

        app.remote.delete("a.txt").await;
        eventually("a remote delete", fast, || async {
            let n = vfs_names(&vfs, "").await;
            if n == vec!["sub".to_string()] { Ok(()) } else { Err(format!("{n:?}")) }
        })
        .await;
        app.cleanup().await;
    })
}

#[test]
fn an_on_demand_drive_uploads_its_writes_and_conflicts_keep_both() {
    run(async {
        let Some(app) = App::add("od-write", Opts { mode: DriveMode::OnDemand, ..Opts::default() }, |r| async move {
            r.put("shared.txt", b"original").await;
        })
        .await
        else {
            return;
        };
        let vfs = app.vfs().await;
        let big = pattern(20 * MIB + 9, 40);
        vfs_write(&vfs, "big.bin", &big).await;
        wait_remote(&app.remote, "big.bin", &big).await;

        let node = vfs_node(&vfs, "shared.txt").await.unwrap();
        let h = vfs.open(node).await.unwrap();
        assert_eq!(&vfs.read(h, 0, 64).await.unwrap()[..], b"original");
        app.remote.put("shared.txt", b"edited on the server").await;
        vfs.truncate(h, 0).await.unwrap();
        vfs.write(h, 0, b"edited locally").await.unwrap();
        vfs.close(h).await.unwrap();
        eventually("a conflict copy next to the original", Duration::from_secs(60), || async {
            let names = app.remote.names("").await;
            let mut contents = Vec::new();
            for n in &names {
                contents.push(app.remote.read(n).await.unwrap_or_default());
            }
            let has = |c: &[u8]| contents.iter().any(|x| x == c);
            if has(b"edited locally") && has(b"edited on the server") { Ok(()) } else { Err(format!("{names:?}")) }
        })
        .await;
        assert_eq!(app.remote.read("shared.txt").await.unwrap(), b"edited on the server");
        app.cleanup().await;
    })
}

#[test]
fn an_on_demand_drive_paused_with_pending_writes_uploads_them_on_resume() {
    run(async {
        let Some(app) = App::on_demand("od-pause").await else { return };
        let vfs = app.vfs().await;
        assert!(vfs_names(&vfs, "").await.is_empty());
        app.manager.pause_drive(&app.id).await.unwrap();
        // Paused (unmounted) — the facade keeps the draft it already had.
        vfs_write(&vfs, "pending.txt", b"written before resume").await;
        app.manager.resume_drive(&app.id).await.unwrap();
        wait_remote(&app.remote, "pending.txt", b"written before resume").await;
        app.cleanup().await;
    })
}

#[test]
fn an_on_demand_drive_back_online_uploads_what_was_written_offline() {
    run(async {
        let Some(app) =
            App::add("od-offline", Opts { mode: DriveMode::OnDemand, proxied: true, ..Opts::default() }, |_| async {}).await
        else {
            return;
        };
        let vfs = app.vfs().await;
        assert!(vfs_names(&vfs, "").await.is_empty());
        app.proxy().set_down(true);
        let data = pattern(2 * MIB + 1, 41);
        vfs_write(&vfs, "offline.bin", &data).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(app.remote.info("offline.bin").await.is_none());
        assert!(app.mount().await.unsynced_file_count().await >= 1);
        // No manual retry: reconnecting must be enough.
        app.proxy().set_down(false);
        wait_remote(&app.remote, "offline.bin", &data).await;
        app.cleanup().await;
    })
}

#[test]
fn removing_an_on_demand_drive_with_a_failed_upload_does_not_hang() {
    run(async {
        let Some(app) =
            App::add("od-remove", Opts { mode: DriveMode::OnDemand, proxied: true, ..Opts::default() }, |_| async {}).await
        else {
            return;
        };
        let vfs = app.vfs().await;
        assert!(vfs_names(&vfs, "").await.is_empty());
        app.proxy().set_down(true);
        vfs_write(&vfs, "doomed.txt", b"never uploaded").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        tokio::time::timeout(Duration::from_secs(30), app.manager.remove_drive(&app.id))
            .await
            .expect("remove_drive hung")
            .unwrap();
        app.proxy().set_down(false);
        app.remote.cleanup().await;
    })
}
