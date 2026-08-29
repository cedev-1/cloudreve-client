mod common;
use common::{remote_file, VfsTestEnv};
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};

/// The harness itself must slice: every later test trusts these mocks.
#[tokio::test]
async fn the_mock_server_honors_range_requests() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(3 * 1024 * 1024).collect();
    env.set_remote_files(vec![remote_file("big.bin", body.len() as i64, "etag-1")]).await;
    env.serve_file_content("big.bin", &body).await;

    let url = common::fetch_download_url(&env, "big.bin").await;
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Range", "bytes=1048576-2097151")
        .send().await.unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &body[1_048_576..2_097_152]);
}

/// The whole point of the feature: bytes travel only for what is read.
#[tokio::test]
async fn reading_a_slice_downloads_only_that_slice_plus_readahead() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(20 * 1024 * 1024).collect();
    env.set_remote_files(vec![remote_file("video.mp4", body.len() as i64, "e1")]).await;
    env.serve_file_content("video.mp4", &body).await;

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "video.mp4").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    let bytes = vfs.read(h, 0, 65536).await.unwrap();
    assert_eq!(bytes.as_ref(), &body[..65536]);

    env.wait_for_downloads_to_settle().await; // harness helper: readahead is async
    let ranged: u64 = env.total_bytes_served("video.mp4");
    assert!(ranged <= 5 * 1024 * 1024,
        "read 64 KiB but downloaded {ranged} bytes — more than block + readahead");
    vfs.close(h).await.unwrap();
}

/// Second read of the same data must not touch the network.
#[tokio::test]
async fn a_cached_block_is_served_without_any_http_request() {
    let env = VfsTestEnv::new().await;
    let body = vec![42u8; 2 * 1024 * 1024];
    env.set_remote_files(vec![remote_file("doc.pdf", body.len() as i64, "e1")]).await;
    env.serve_file_content("doc.pdf", &body).await;

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "doc.pdf").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.read(h, 0, 1024).await.unwrap();
    env.wait_for_downloads_to_settle().await;
    let before = env.download_requests("doc.pdf").len();

    let again = vfs.read(h, 0, 1024).await.unwrap();
    assert_eq!(again.as_ref(), &body[..1024]);
    assert_eq!(env.download_requests("doc.pdf").len(), before,
        "a cached read went to the network");
    vfs.close(h).await.unwrap();
}

/// Reads crossing EOF return the honest tail, like a local file.
#[tokio::test]
async fn a_read_past_the_end_returns_the_truncated_tail() {
    let env = VfsTestEnv::new().await;
    let body = b"short file".to_vec();
    env.set_remote_files(vec![remote_file("s.txt", body.len() as i64, "e1")]).await;
    env.serve_file_content("s.txt", &body).await;
    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "s.txt").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    assert_eq!(vfs.read(h, 6, 100).await.unwrap().as_ref(), b"file");
    assert_eq!(vfs.read(h, 500, 100).await.unwrap().len(), 0);
}

/// The cap is enforced through the facade, not just inside cache.rs:
/// reading a 4th file under a 3-file cap evicts the coldest CLOSED file.
#[tokio::test]
async fn the_cache_cap_holds_through_real_reads() {
    let env = VfsTestEnv::new().await;
    let mb = 1024 * 1024;
    for (name, byte) in [("a.bin", 1u8), ("b.bin", 2), ("c.bin", 3), ("d.bin", 4)] {
        env.add_remote_file(name, vec![byte; mb], &format!("e-{name}")).await;
    }
    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       3 * mb as u64).unwrap();
    for name in ["a.bin", "b.bin", "c.bin", "d.bin"] {
        let node = vfs.tree().lookup(vfs.tree().root(), name).await.unwrap().unwrap().0;
        let h = vfs.open(node).await.unwrap();
        vfs.read(h, 0, mb as u32).await.unwrap();
        vfs.close(h).await.unwrap();
    }
    env.wait_for_downloads_to_settle().await;
    let disk: u64 = common::dir_size(env.cache_dir());
    assert!(disk <= 3 * mb as u64 + 64 * 1024,
        "cache dir holds {disk} bytes, cap was {}", 3 * mb);
    // a.bin was evicted: reading it again must hit the network once more.
    let before = env.download_requests("a.bin").len();
    let node = vfs.tree().lookup(vfs.tree().root(), "a.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();
    vfs.read(h, 0, 1024).await.unwrap();
    assert!(env.download_requests("a.bin").len() > before);
}

/// A zero-length read is a legal POSIX call (NFS3/FUSE frontends forward it
/// verbatim), at any offset up to EOF — not just offset 0. It must never
/// panic, and since nothing is actually requested, it must not touch the
/// network either.
#[tokio::test]
async fn a_zero_length_read_returns_empty_without_panicking() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(3 * 1024 * 1024).collect();
    env.set_remote_files(vec![remote_file("z.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("z.bin", &body).await;

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "z.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let before = env.download_requests("z.bin").len();
    assert_eq!(vfs.read(h, 0, 0).await.unwrap().as_ref(), b"", "zero-length read at offset 0");
    assert_eq!(
        vfs.read(h, 2 * 1024 * 1024 + 500, 0).await.unwrap().as_ref(),
        b"",
        "zero-length read at a mid-file offset whose block isn't cached"
    );
    assert_eq!(
        env.download_requests("z.bin").len(),
        before,
        "a zero-length read must never hit the network"
    );
    vfs.close(h).await.unwrap();
}

/// Spec §7: a transient (transport/5xx) download error is retried with
/// backoff rather than surfacing on the first failure.
#[tokio::test]
async fn a_transient_download_error_is_retried_then_served() {
    let env = VfsTestEnv::new().await;
    let body = vec![3u8; 4096];
    env.set_remote_files(vec![remote_file("flaky.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("flaky.bin", &body).await;
    env.fail_downloads_n_times("flaky.bin", 2, 500).await;

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "flaky.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let bytes = vfs.read(h, 0, 4096).await.unwrap();
    assert_eq!(bytes.as_ref(), &body[..]);
    assert_eq!(
        env.download_requests("flaky.bin").len(),
        3,
        "expected 2 failed attempts followed by 1 successful one"
    );
}

/// Spec §7: once retries are exhausted, the error surfaces to the caller
/// (phase 3 maps it to EIO) instead of hanging or panicking.
#[tokio::test]
async fn a_persistent_download_error_surfaces_after_bounded_retries() {
    let env = VfsTestEnv::new().await;
    let body = vec![3u8; 4096];
    env.set_remote_files(vec![remote_file("dead.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("dead.bin", &body).await;
    env.fail_downloads_n_times("dead.bin", 1000, 500).await; // effectively "always fails"

    let vfs = Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(),
                       DEFAULT_CACHE_MAX_BYTES).unwrap();
    let node = vfs.tree().lookup(vfs.tree().root(), "dead.bin").await.unwrap().unwrap().0;
    let h = vfs.open(node).await.unwrap();

    let result = vfs.read(h, 0, 4096).await;
    assert!(result.is_err(), "a persistently failing download must surface an error, not hang or panic");
    assert_eq!(
        env.download_requests("dead.bin").len(),
        3,
        "expected exactly FETCH_RETRIES total attempts before giving up"
    );
}
