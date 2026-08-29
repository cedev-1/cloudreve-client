mod common;
use common::{remote_file, VfsTestEnv};

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
