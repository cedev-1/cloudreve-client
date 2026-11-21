//! A volume mounted somewhere inside a mirror's folder — an external disk
//! the user mounted there, another drive's on-demand mount, a network
//! share — is not part of the drive. The local scan must not walk into it:
//! every file on it would be uploaded, and unmounting it later would look
//! like the user deleting all of them.
//!
//! macOS only: mounting a volume at a chosen folder without root needs
//! `diskutil`, which a Linux test runner has no equivalent of.
#![cfg(target_os = "macos")]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::TestEnv;

/// A small RAM volume mounted at `at`, detached on drop.
struct VolumeAt {
    device: String,
}

impl VolumeAt {
    fn new(at: &Path) -> Option<Self> {
        let attach = Command::new("hdiutil").args(["attach", "-nomount", "ram://65536"]).output().ok()?;
        let device = String::from_utf8_lossy(&attach.stdout).trim().to_string();
        if !attach.status.success() || device.is_empty() {
            return None;
        }
        let me = Self { device };
        let label = "CrForeignVol";
        let ok = Command::new("diskutil").args(["erasevolume", "HFS+", label, &me.device]).output().ok()?.status.success()
            && Command::new("diskutil").args(["unmount", &format!("/Volumes/{label}")]).output().ok()?.status.success();
        std::fs::create_dir_all(at).ok()?;
        let mounted = ok
            && Command::new("diskutil")
                .args(["mount", "-mountPoint", at.to_str()?, &me.device])
                .output()
                .ok()?
                .status
                .success();
        mounted.then_some(me)
    }
}

impl Drop for VolumeAt {
    fn drop(&mut self) {
        let _ = Command::new("hdiutil").args(["detach", "-force", &self.device]).output();
    }
}

#[tokio::test]
async fn the_local_scan_does_not_walk_into_a_volume_mounted_inside_the_sync_folder() {
    let env = TestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let disk_folder = env.local_path("Backup Disk");
    let Some(_volume) = VolumeAt::new(&disk_folder) else {
        eprintln!("skipping: could not mount a test volume on this machine");
        return;
    };
    std::fs::write(disk_folder.join("holiday.mov"), b"not part of the drive").unwrap();
    env.write_local("notes.txt", b"part of the drive");

    env.full_sync().await.expect("full sync");

    let uploads: Vec<PathBuf> =
        env.tasks_of_type("upload").into_iter().map(|t| PathBuf::from(t.local_path)).collect();
    assert!(uploads.contains(&env.local_path("notes.txt")), "the drive's own file still uploads: {uploads:?}");
    assert!(
        !uploads.iter().any(|p| p.starts_with(&disk_folder)),
        "a file on the mounted volume was queued for upload: {uploads:?}"
    );
}
