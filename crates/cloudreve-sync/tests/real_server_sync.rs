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
    App, MIB, Opts, run, wait_local, wait_local_gone, wait_remote, write,
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
        // A mirror drive never deletes on the server what was removed locally
        // (`full_sync`'s "deleted locally" arm, inherited from the Windows
        // client): a rename or move uploads the new name, and what was removed
        // must at least never come back locally — the server's own events for
        // our earlier uploads arrive seconds later and must not resurrect it.
        std::fs::rename(app.local("new.txt"), app.local("renamed.txt")).unwrap();
        wait_remote(&app.remote, "renamed.txt", b"modified locally + appended").await;
        std::fs::rename(app.local("renamed.txt"), app.local("deep/moved.txt")).unwrap();
        wait_remote(&app.remote, "deep/moved.txt", b"modified locally + appended").await;
        std::fs::remove_dir_all(app.local("deep/er")).unwrap();
        std::fs::remove_file(app.local("big.bin")).unwrap();
        tokio::time::sleep(Duration::from_secs(12)).await;
        for gone in ["new.txt", "renamed.txt", "deep/er", "big.bin"] {
            assert!(!app.local(gone).exists(), "{gone} came back locally");
        }
        assert_eq!(app.remote.read("deep/moved.txt").await.unwrap(), b"modified locally + appended");
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
