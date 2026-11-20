//! On-demand drives REALLY mounted through the OS — NFSv3 loopback on
//! macOS, FUSE on Linux — by `DriveManager::add_drive`, over a folder of the
//! real e2e server (see `cloudreve-vfs/tests/real_server/mod.rs`), and used
//! only through `std::fs`, like a file manager or an editor would.
//!
//! Skipped unless `CLOUDREVE_E2E_URL` is set AND `CLOUDREVE_E2E_MOUNT=1`:
//! mounting needs a machine that allows it (on a developer Mac, a sandboxed
//! process gets EPERM from `mount_nfs`; Linux needs `/dev/fuse`). CI runs it
//! on both.
#![cfg(any(target_os = "macos", target_os = "linux"))]

#[path = "../../cloudreve-vfs/tests/real_server/mod.rs"]
mod real_server;
mod real_server_app;

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::Duration;

use cloudreve_sync::drive::mounts::DriveMode;
use real_server::{Remote, eventually, pattern};
use real_server_app::{App, MIB, Opts, REAL_MOUNTS, fs, run, wait_local, wait_local_gone, wait_remote, wait_remote_gone};

/// Every on-demand drive of this binary mounts through the OS.
fn run_mounted<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    REAL_MOUNTS.store(true, std::sync::atomic::Ordering::SeqCst);
    run(body)
}

fn mount_enabled(test: &str) -> bool {
    let on = std::env::var("CLOUDREVE_E2E_MOUNT").is_ok_and(|v| v == "1");
    if !on {
        eprintln!("skipping {test}: CLOUDREVE_E2E_MOUNT is not 1");
    }
    on
}

async fn mounted<F, Fut>(label: &str, proxied: bool, seed: F) -> Option<App>
where
    F: FnOnce(Remote) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if !mount_enabled(label) {
        return None;
    }
    let app = App::add(label, Opts { mode: DriveMode::OnDemand, proxied, real_mount: true, ..Opts::default() }, seed)
        .await?;
    let problem = app.mount().await.mount_problem();
    assert!(problem.is_none(), "the drive did not mount: {problem:?}");
    Some(app)
}

async fn names(path: PathBuf) -> Vec<String> {
    fs(move || {
        let mut v: Vec<String> = std::fs::read_dir(&path)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", path.display()))
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        v.sort();
        v
    })
    .await
}

async fn read(path: PathBuf) -> Vec<u8> {
    fs(move || std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))).await
}

async fn put(path: PathBuf, data: Vec<u8>) {
    fs(move || {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).unwrap();
        }
        std::fs::write(&path, data).unwrap_or_else(|e| panic!("write {}: {e}", path.display()))
    })
    .await
}

#[test]
fn a_mounted_drive_lists_and_reads_the_server_tree() {
    run_mounted(async {
        let big = pattern(120 * MIB + 77, 50);
        let big2 = big.clone();
        let Some(app) = mounted("mnt-read", false, |r| async move {
            r.put("top.txt", b"top").await;
            r.put("empty.txt", b"").await;
            r.put("a/b/c/d/deep.txt", b"deep").await;
            r.put("names/café 🎉.txt", b"nfc").await;
            r.put("names/cafe\u{301} nfd.txt", b"nfd").await;
            r.put(&format!("names/{}.txt", "long-name-".repeat(24)), b"long").await;
            r.upload("big.bin", &big2).await;
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
        assert_eq!(names(app.sync.clone()).await, ["a", "big.bin", "empty.txt", "many", "names", "top.txt"]);
        assert_eq!(read(app.local("top.txt")).await, b"top");
        assert_eq!(read(app.local("empty.txt")).await, b"");
        assert_eq!(read(app.local("a/b/c/d/deep.txt")).await, b"deep");
        assert_eq!(names(app.local("names")).await.len(), 3);
        assert_eq!(read(app.local("names/café 🎉.txt")).await, b"nfc");
        assert_eq!(read(app.local("names/cafe\u{301} nfd.txt")).await, b"nfd");
        assert_eq!(read(app.local(&format!("names/{}.txt", "long-name-".repeat(24)))).await, b"long");
        let many = names(app.local("many")).await;
        assert_eq!(many.len(), 2100);
        assert_eq!(read(app.local("many/f-02099.txt")).await, b"2099");

        // Seeks all over the big file, then the whole of it.
        let path = app.local("big.bin");
        let big_for_seeks = big.clone();
        fs(move || {
            let mut f = std::fs::File::open(&path).unwrap();
            let mut seed = 0x2545f4914f6cdd1du64;
            for _ in 0..30 {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let off = (seed % big_for_seeks.len() as u64) as usize;
                let len = (1 + (seed >> 40) as usize % (256 * 1024)).min(big_for_seeks.len() - off);
                let mut buf = vec![0u8; len];
                f.seek(SeekFrom::Start(off as u64)).unwrap();
                f.read_exact(&mut buf).unwrap();
                assert!(buf[..] == big_for_seeks[off..off + len], "range {off}+{len} differs");
            }
        })
        .await;
        assert!(read(app.local("big.bin")).await == big, "big.bin differs");
        app.cleanup().await;
    })
}

#[test]
fn writes_through_a_mounted_drive_reach_the_server() {
    run_mounted(async {
        let Some(app) = mounted("mnt-write", false, |r| async move {
            r.put("existing.txt", b"existing").await;
            r.put("doc.txt", b"version 1").await;
        })
        .await
        else {
            return;
        };
        // Create, overwrite, append, truncate.
        put(app.local("new.txt"), b"created".to_vec()).await;
        wait_remote(&app.remote, "new.txt", b"created").await;
        put(app.local("existing.txt"), b"overwritten".to_vec()).await;
        wait_remote(&app.remote, "existing.txt", b"overwritten").await;
        let p = app.local("existing.txt");
        fs(move || std::fs::OpenOptions::new().append(true).open(&p).unwrap().write_all(b" + appended").unwrap()).await;
        wait_remote(&app.remote, "existing.txt", b"overwritten + appended").await;
        let p = app.local("existing.txt");
        fs(move || std::fs::OpenOptions::new().write(true).open(&p).unwrap().set_len(4).unwrap()).await;
        wait_remote(&app.remote, "existing.txt", b"over").await;

        // An editor's safe save: temp file next to it, renamed onto it.
        put(app.local("doc.txt.sb-1a2b"), b"version 2".to_vec()).await;
        let (from, to) = (app.local("doc.txt.sb-1a2b"), app.local("doc.txt"));
        fs(move || std::fs::rename(from, to).unwrap()).await;
        wait_remote(&app.remote, "doc.txt", b"version 2").await;
        wait_remote_gone(&app.remote, "doc.txt.sb-1a2b").await;

        // mkdir -p, rename and move a file and a folder, delete both kinds.
        let deep = app.local("x/y/z/w");
        fs(move || std::fs::create_dir_all(deep).unwrap()).await;
        put(app.local("x/y/z/w/leaf.txt"), b"leaf".to_vec()).await;
        wait_remote(&app.remote, "x/y/z/w/leaf.txt", b"leaf").await;
        let (from, to) = (app.local("new.txt"), app.local("x/renamed.txt"));
        fs(move || std::fs::rename(from, to).unwrap()).await;
        wait_remote(&app.remote, "x/renamed.txt", b"created").await;
        wait_remote_gone(&app.remote, "new.txt").await;
        let (from, to) = (app.local("x/y/z"), app.local("z-moved"));
        fs(move || std::fs::rename(from, to).unwrap()).await;
        wait_remote(&app.remote, "z-moved/w/leaf.txt", b"leaf").await;
        wait_remote_gone(&app.remote, "x/y/z").await;
        let p = app.local("x/renamed.txt");
        fs(move || std::fs::remove_file(p).unwrap()).await;
        wait_remote_gone(&app.remote, "x/renamed.txt").await;
        let p = app.local("z-moved");
        fs(move || std::fs::remove_dir_all(p).unwrap()).await;
        wait_remote_gone(&app.remote, "z-moved").await;
        app.cleanup().await;
    })
}

#[test]
fn copying_2000_files_and_big_files_into_a_mounted_drive_uploads_all_of_them() {
    run_mounted(async {
        let Some(app) = mounted("mnt-bulk", false, |_| async {}).await else { return };
        let dir = app.local("bulk");
        fs(move || {
            std::fs::create_dir(&dir).unwrap();
            for i in 0..2000 {
                std::fs::write(dir.join(format!("f-{i:04}.txt")), format!("bulk {i}")).unwrap();
            }
        })
        .await;
        let bigs: Vec<Vec<u8>> = (0..2).map(|i| pattern(60 * MIB + i * 13, 60 + i as u64)).collect();
        for (i, b) in bigs.iter().enumerate() {
            put(app.local(&format!("big-{i}.bin")), b.clone()).await;
        }
        eventually("2000 files on the server", Duration::from_secs(400), || async {
            let n = app.remote.list_or_empty("bulk").await.len();
            if n == 2000 { Ok(()) } else { Err(format!("{n} files")) }
        })
        .await;
        for i in [0usize, 1234, 1999] {
            assert_eq!(app.remote.read(&format!("bulk/f-{i:04}.txt")).await.unwrap(), format!("bulk {i}").into_bytes());
        }
        for (i, b) in bigs.iter().enumerate() {
            wait_remote(&app.remote, &format!("big-{i}.bin"), b).await;
        }
        app.cleanup().await;
    })
}

#[test]
fn remote_changes_show_up_through_the_mount_within_seconds() {
    run_mounted(async {
        let Some(app) = mounted("mnt-remote", false, |r| async move {
            r.put("a.txt", b"a").await;
            r.mkdir("sub").await;
        })
        .await
        else {
            return;
        };
        assert_eq!(names(app.sync.clone()).await, ["a.txt", "sub"]);
        tokio::time::sleep(Duration::from_secs(2)).await; // event stream subscribes

        let fast = Duration::from_secs(10); // server event batching (5s) + slack
        app.remote.put("new.txt", b"from the web").await;
        let root = app.sync.clone();
        eventually("a new remote file", fast, || async {
            let n = names(root.clone()).await;
            if n.contains(&"new.txt".to_string()) { Ok(()) } else { Err(format!("{n:?}")) }
        })
        .await;
        assert_eq!(read(app.local("new.txt")).await, b"from the web");
        app.remote.put("a.txt", b"a, edited on the web").await;
        let p = app.local("a.txt");
        eventually("a remote edit", fast, || async {
            let b = read(p.clone()).await;
            if b == b"a, edited on the web" { Ok(()) } else { Err(String::from_utf8_lossy(&b).to_string()) }
        })
        .await;
        app.remote.mv(&["new.txt"], "sub").await;
        let sub = app.local("sub");
        eventually("a remote move", fast, || async {
            let (r, s) = (names(root.clone()).await, names(sub.clone()).await);
            if s == ["new.txt"] && !r.contains(&"new.txt".to_string()) { Ok(()) } else { Err(format!("{r:?} {s:?}")) }
        })
        .await;
        app.remote.rename("sub", "sub-renamed").await;
        app.remote.delete("a.txt").await;
        eventually("a remote rename and delete", fast, || async {
            let r = names(root.clone()).await;
            if r == ["sub-renamed"] { Ok(()) } else { Err(format!("{r:?}")) }
        })
        .await;
        assert_eq!(read(app.local("sub-renamed/new.txt")).await, b"from the web");
        app.cleanup().await;
    })
}

#[test]
fn a_local_save_racing_a_server_edit_keeps_both_through_the_mount() {
    run_mounted(async {
        let Some(app) = mounted("mnt-conflict", false, |r| async move {
            r.put("shared.txt", b"original").await;
        })
        .await
        else {
            return;
        };
        let p = app.local("shared.txt");
        let remote = app.remote.clone_handle();
        // Opened and read, edited on the server, then saved through the fd.
        let mut f = fs({
            let p = p.clone();
            move || {
                let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&p).unwrap();
                let mut s = String::new();
                f.read_to_string(&mut s).unwrap();
                assert_eq!(s, "original");
                f
            }
        })
        .await;
        remote.put("shared.txt", b"edited on the server").await;
        fs(move || {
            f.set_len(0).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(b"edited locally").unwrap();
            f.sync_all().ok();
            drop(f);
        })
        .await;
        eventually("both versions on the server", Duration::from_secs(60), || async {
            let names = app.remote.names("").await;
            let mut found = Vec::new();
            for n in &names {
                found.push(app.remote.read(n).await.unwrap_or_default());
            }
            let has = |c: &[u8]| found.iter().any(|x| x == c);
            if has(b"edited locally") && has(b"edited on the server") { Ok(()) } else { Err(format!("{names:?}")) }
        })
        .await;
        assert_eq!(app.remote.read("shared.txt").await.unwrap(), b"edited on the server");
        app.cleanup().await;
    })
}

#[test]
fn files_saved_on_a_mounted_drive_while_offline_upload_after_reconnect() {
    run_mounted(async {
        let Some(app) = mounted("mnt-offline", true, |_| async {}).await else { return };
        assert!(names(app.sync.clone()).await.is_empty());
        app.proxy().set_down(true);
        let data = pattern(4 * MIB + 9, 61);
        put(app.local("offline.bin"), data.clone()).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(app.remote.info("offline.bin").await.is_none());
        // Still readable locally while the server is away.
        assert!(read(app.local("offline.bin")).await == data);
        app.proxy().set_down(false);
        wait_remote(&app.remote, "offline.bin", &data).await;
        app.cleanup().await;
    })
}

#[test]
fn pausing_and_resuming_a_mounted_drive_keeps_and_uploads_pending_writes() {
    run_mounted(async {
        let Some(app) = mounted("mnt-pause", true, |_| async {}).await else { return };
        assert!(names(app.sync.clone()).await.is_empty());
        // A save that cannot upload yet, then the drive is paused (unmounted).
        app.proxy().set_down(true);
        put(app.local("pending.txt"), b"saved before pause".to_vec()).await;
        app.manager.pause_drive(&app.id).await.unwrap();
        app.proxy().set_down(false);
        app.manager.resume_drive(&app.id).await.unwrap();
        wait_remote(&app.remote, "pending.txt", b"saved before pause").await;
        wait_local(&app, "pending.txt", b"saved before pause").await;
        app.cleanup().await;
    })
}

#[test]
fn removing_a_mounted_drive_unmounts_it_and_keeps_the_server_files() {
    run_mounted(async {
        let Some(app) = mounted("mnt-remove", false, |r| async move {
            r.put("keep.txt", b"keep").await;
        })
        .await
        else {
            return;
        };
        assert_eq!(read(app.local("keep.txt")).await, b"keep");
        tokio::time::timeout(Duration::from_secs(60), app.manager.remove_drive(&app.id))
            .await
            .expect("remove_drive hung")
            .unwrap();
        wait_local_gone(&app, "keep.txt").await;
        assert_eq!(app.remote.read("keep.txt").await.unwrap(), b"keep");
        app.remote.cleanup().await;
    })
}
