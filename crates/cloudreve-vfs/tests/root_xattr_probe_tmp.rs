#![cfg(target_os = "macos")]
mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::mount;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, VfsTestEnv};

async fn new_test_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES).unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    Arc::new(vfs)
}

fn sh(cmd: &str) -> String {
    let out = std::process::Command::new("/bin/sh").arg("-c").arg(cmd).output().unwrap();
    let s = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    eprintln!("SH $ {cmd}\n{s}");
    s
}

const FI: &str = "0000000000000000040000000000000000000000000000000000000000000000";

async fn dump(vfs: &Vfs, base: &str) {
    let root = vfs.tree().root();
    for name in ["._.", "._", &format!("._{base}"), ".VolumeIcon.icns"] {
        match vfs.lookup(root, name).await {
            Ok(Some((id, attr))) => {
                let h = vfs.open(id).await.unwrap();
                let data = vfs.read(h, 0, attr.size as u32 + 16).await.unwrap();
                vfs.close(h).await.unwrap();
                let hex: String = data.iter().take(128).map(|b| format!("{b:02x}")).collect();
                eprintln!("OVERLAY {name:?}: {} bytes: {hex}", data.len());
            }
            other => eprintln!("OVERLAY {name:?}: {:?}", other.map(|o| o.map(|_| ()))),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_root_xattr() {
    for listed_first in [false, true] {
        let env = VfsTestEnv::new().await;
        env.expect_uploads().await;
        env.expect_namespace_ops().await;
        env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;
        let vfs = new_test_vfs(&env).await;
        let parent = tempfile::tempdir().unwrap();
        let mp = parent.path().join("drive");
        std::fs::create_dir(&mp).unwrap();
        let mounted = mount::mount(vfs.clone(), &mp, "CloudreveTest", env.cache_dir()).await.unwrap();
        let p = mp.display().to_string();
        let pp = parent.path().display().to_string();
        eprintln!("===== listed_first={listed_first}");
        let p2 = p.clone();
        let pp2 = pp.clone();
        tokio::task::spawn_blocking(move || {
            if listed_first {
                sh(&format!("ls -la '{p2}'"));
            }
            sh(&format!("xattr -wx com.apple.FinderInfo {FI} '{p2}'; echo rc=$?; xattr -px com.apple.FinderInfo '{p2}'; ls -la '{pp2}'; ls -la '{p2}'"));
            sh(&format!("xattr -px com.apple.FinderInfo '{p2}'; ls -la@ '{pp2}'"));
        })
        .await
        .unwrap();
        dump(&vfs, "drive").await;
        let p3 = p.clone();
        tokio::task::spawn_blocking(move || {
            sh(&format!("cat /dev/null > /dev/null; xattr -px com.apple.FinderInfo '{p3}'"));
        })
        .await
        .unwrap();
        mounted.unmount().await.unwrap();
        sh(&format!("ls -la@ '{pp}' '{p}'; xattr -l '{p}'"));
    }
}
