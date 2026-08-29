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
