//! Dragging a big folder into the on-demand drive creates thousands of files
//! in one directory. Each create must cost about the same whatever the
//! folder already holds: per-create work that grew with the folder (a
//! linear scan of its children and of every draft per LOOKUP) made 300
//! creates take seconds and 3000 take many minutes.
//!
//! The assertions compare 3000 creates against 300 rather than against a
//! wall-clock budget: ten times the files is ~10x the time when each create
//! is O(1), ~100x when it is O(n), whatever the CI runner's speed.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use cloudreve_vfs::nfs::VfsNfs;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, remote_file, VfsTestEnv};
use nfs3_server::nfs3_types::nfs3::{filename3, nfsstat3, sattr3, stable_how};
use nfs3_server::vfs::{FileHandleU64, NfsFileSystem, NfsReadFileSystem};

const CONTENT: &[u8] = b"0123456789abcdef";

/// A Vfs over a remote folder `big` already holding `existing` files (a
/// second batch of photos copied next to the first), with uploads held
/// back so only the namespace work is timed.
async fn env_with_folder(existing: usize) -> (VfsTestEnv, Arc<Vfs>) {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("big")]).await;
    let files = (0..existing).map(|i| remote_file(&format!("DSC_{i:05}.jpg"), 2_400_000, &format!("e{i}"))).collect();
    env.set_remote_files_at("big", files).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    (env, Arc::new(vfs))
}

fn file_name(i: usize) -> String {
    format!("IMG_{i:05}.jpg")
}

/// `cp` through the facade: LOOKUP miss, CREATE, WRITE, CLOSE per file,
/// into a folder already holding as many files on the server.
async fn copy_in_through_facade(n: usize) -> Duration {
    let (_env, vfs) = env_with_folder(n).await;
    let root = vfs.tree().root();
    let dir = vfs.lookup(root, "big").await.unwrap().unwrap().0;
    let started = Instant::now();
    for i in 0..n {
        let name = file_name(i);
        assert!(vfs.lookup(dir, &name).await.unwrap().is_none());
        let (_node, h) = vfs.create(dir, &name).await.unwrap();
        vfs.write(h, 0, CONTENT).await.unwrap();
        vfs.close(h).await.unwrap();
    }
    let elapsed = started.elapsed();
    assert_eq!(vfs.readdir(dir).await.unwrap().len(), 2 * n);
    vfs.shutdown();
    elapsed
}

/// The same through the NFS adapter, as the macOS kernel drives it.
async fn copy_in_through_nfs(n: usize) -> Duration {
    let (_env, vfs) = env_with_folder(0).await;
    let root = vfs.tree().root();
    let dir = FileHandleU64::new(vfs.lookup(root, "big").await.unwrap().unwrap().0 .0);
    let fs = VfsNfs::new(vfs.clone());
    let started = Instant::now();
    for i in 0..n {
        let name: filename3<'static> = file_name(i).into_bytes().into();
        assert_eq!(fs.lookup(&dir, &name).await.err(), Some(nfsstat3::NFS3ERR_NOENT));
        let (fh, _attr) = fs.create(&dir, &name, sattr3::default()).await.unwrap();
        fs.write(&fh, 0, CONTENT, stable_how::UNSTABLE).await.unwrap();
    }
    let elapsed = started.elapsed();
    let node = cloudreve_vfs::tree::NodeId(dir.as_u64());
    assert_eq!(vfs.readdir(node).await.unwrap().len(), n);
    vfs.shutdown();
    elapsed
}

fn assert_linear(small: Duration, big: Duration, what: &str) {
    let ratio = big.as_secs_f64() / small.as_secs_f64();
    eprintln!("{what}: 300 files {small:?}, 3000 files {big:?}, ratio {ratio:.1}");
    assert!(ratio < 15.0, "{what}: 3000 files took {ratio:.1}x the time of 300 ({small:?} vs {big:?})");
}

#[tokio::test(flavor = "multi_thread")]
async fn copying_files_into_a_big_folder_costs_the_same_per_file() {
    let small = copy_in_through_facade(300).await;
    let big = copy_in_through_facade(3000).await;
    assert_linear(small, big, "facade");
}

#[tokio::test(flavor = "multi_thread")]
async fn copying_files_into_a_big_folder_over_nfs_costs_the_same_per_file() {
    let small = copy_in_through_nfs(300).await;
    let big = copy_in_through_nfs(3000).await;
    assert_linear(small, big, "nfs adapter");
}
