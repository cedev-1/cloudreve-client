//! The local-only junk overlay lives in memory. Unbounded, one
//! `truncate -s 50G ._x` aborted the app on the allocation, and a copy of a
//! big tree with xattrs (one `._` sidecar per file) grew it without end.

mod common;

use std::sync::Arc;

use cloudreve_vfs::frontend_util::{classify_error, FrontendErrno};
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::VfsTestEnv;

const MIB: usize = 1024 * 1024;

async fn new_vfs(env: &VfsTestEnv) -> Arc<Vfs> {
    env.set_remote_files(vec![]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    Arc::new(vfs)
}

#[tokio::test]
async fn a_huge_truncate_of_a_sidecar_fails_with_efbig_instead_of_aborting() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (_n, h) = vfs.create(root, "._movie.mov").await.unwrap();

    let err = vfs.truncate(h, 50 * 1024 * 1024 * 1024).await.expect_err("50 GiB must be refused");
    assert_eq!(classify_error(&err), FrontendErrno::FileTooBig);
    vfs.close(h).await.unwrap();
}

#[tokio::test]
async fn a_write_far_past_the_per_file_cap_fails_with_efbig() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (_n, h) = vfs.create(root, ".DS_Store").await.unwrap();
    let err = vfs.write(h, 40 * MIB as u64, b"x").await.expect_err("a 40 MiB .DS_Store must be refused");
    assert_eq!(classify_error(&err), FrontendErrno::FileTooBig);
    vfs.close(h).await.unwrap();
}

/// A sidecar with a real resource fork or a Finder thumbnail is several
/// MB: it must still fit.
#[tokio::test]
async fn a_multi_megabyte_sidecar_still_fits() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();
    let (_n, h) = vfs.create(root, "._photo.heic").await.unwrap();
    vfs.write(h, 0, &vec![7u8; 6 * MIB]).await.unwrap();
    vfs.close(h).await.unwrap();
    assert_eq!(vfs.lookup(root, "._photo.heic").await.unwrap().unwrap().1.size, (6 * MIB) as u64);
}

/// Many sidecars: the oldest closed ones make room for new ones (their
/// xattrs are forgotten, as they already are at every restart); the drive
/// icon, the root sidecar and an open sidecar are never dropped.
#[tokio::test]
async fn the_overlay_stays_bounded_and_keeps_the_icon_and_open_files() {
    let env = VfsTestEnv::new().await;
    let vfs = new_vfs(&env).await;
    let root = vfs.tree().root();

    vfs.put_local_only(root, ".VolumeIcon.icns", vec![1u8; 4 * MIB]).await.unwrap();
    vfs.put_local_only(root, "._.", vec![2u8; 4096]).await.unwrap();
    let (_n, held) = vfs.create(root, "._kept-open.pdf").await.unwrap();
    vfs.write(held, 0, &vec![3u8; 4 * MIB]).await.unwrap();

    // 30 sidecars of 4 MiB: 120 MiB, far past what the overlay may hold.
    for i in 0..30 {
        let name = format!("._file{i:02}.jpg");
        let (_n, h) = vfs.create(root, &name).await.unwrap();
        vfs.write(h, 0, &vec![9u8; 4 * MIB]).await.expect("room must be made for a new sidecar");
        vfs.close(h).await.unwrap();
    }

    let mut kept = 0usize;
    for i in 0..30 {
        if let Some((_, attr)) = vfs.lookup(root, &format!("._file{i:02}.jpg")).await.unwrap() {
            kept += attr.size as usize;
        }
    }
    assert!(kept <= 64 * MIB, "the overlay holds {} MiB of sidecars", kept / MIB);
    assert!(vfs.lookup(root, "._file29.jpg").await.unwrap().is_some(), "the newest sidecar was dropped");
    assert!(vfs.lookup(root, "._file00.jpg").await.unwrap().is_none(), "the oldest sidecar was kept");
    assert_eq!(vfs.lookup(root, ".VolumeIcon.icns").await.unwrap().unwrap().1.size, (4 * MIB) as u64);
    assert_eq!(vfs.lookup(root, "._.").await.unwrap().unwrap().1.size, 4096);
    assert_eq!(vfs.read(held, 0, 16).await.unwrap().as_ref(), &[3u8; 16]);
    vfs.close(held).await.unwrap();
}
