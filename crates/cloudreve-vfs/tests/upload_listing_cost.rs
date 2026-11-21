//! Copying a folder of files into the on-demand drive uploads each file on
//! its own. What each upload costs the server afterwards must not depend on
//! how many files its folder already holds: re-listing the whole folder to
//! learn the new etag of one file made n uploads into one folder cost n
//! listings of up to n entries — 2000 photos meant 2000 listings of a
//! 2000-entry folder, each paginated on a real Cloudreve.
//!
//! As in `big_folder.rs`, 3000 uploads are compared with 300 rather than
//! with a wall-clock budget, so a slow CI runner changes nothing.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, VfsTestEnv, REMOTE_BASE};

struct Cost {
    uploads: Duration,
    list_requests: usize,
}

/// `cp` of `n` photos into the empty remote folder `Album`, through the
/// facade, the server listing every upload once it lands (as a real
/// Cloudreve does). Times the write-back draining every upload and counts
/// the folder listings it asked for meanwhile.
async fn copy_photos_in(n: usize) -> Cost {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("Album")]).await;
    env.set_remote_files_at("Album", vec![]).await;
    env.list_uploaded_files();
    let (vfs, _events) =
        Vfs::new(env.client(), REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES).unwrap();
    // Held back while the files are created, so only the uploads are timed.
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    let vfs = Arc::new(vfs);
    let (album, _) = vfs.lookup(vfs.tree().root(), "Album").await.unwrap().expect("Album is listed");

    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let (node, h) = vfs.create(album, &format!("IMG_{i:05}.jpg")).await.unwrap();
        vfs.write(h, 0, format!("photo number {i}").as_bytes()).await.unwrap();
        nodes.push((node, h));
    }

    vfs.set_debounce_for_tests(Duration::from_millis(1));
    let lists_before = env.list_request_count();
    let started = Instant::now();
    for (_, h) in &nodes {
        vfs.close(*h).await.unwrap();
    }
    while env.uploaded_names().len() < n {
        vfs.wait_for_writeback_idle().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    vfs.wait_for_writeback_idle().await;
    let cost = Cost { uploads: started.elapsed(), list_requests: env.list_request_count() - lists_before };

    assert_eq!(env.uploaded_content("IMG_00007.jpg").as_deref(), Some(&b"photo number 7"[..]));
    vfs.shutdown();
    cost
}

#[tokio::test(flavor = "multi_thread")]
async fn uploading_files_into_one_folder_costs_the_same_per_file_however_full_it_gets() {
    let small = copy_photos_in(300).await;
    let big = copy_photos_in(3000).await;
    let ratio = big.uploads.as_secs_f64() / small.uploads.as_secs_f64();
    eprintln!(
        "300 uploads: {:?}, {} listings; 3000 uploads: {:?}, {} listings; time ratio {ratio:.1}",
        small.uploads, small.list_requests, big.uploads, big.list_requests
    );
    // One listing per upload is what made this quadratic: 3000 uploads may
    // not cost even one listing per hundred files.
    assert!(
        big.list_requests < 30,
        "3000 uploads into one folder asked for {} listings of it (300 uploads: {})",
        big.list_requests,
        small.list_requests
    );
    assert!(ratio < 40.0, "3000 uploads took {ratio:.1}x the time of 300 ({:?} vs {:?})", small.uploads, big.uploads);
}
