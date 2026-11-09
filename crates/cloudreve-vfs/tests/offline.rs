//! Losing the connection (train, plane, server down): what the user already
//! saw keeps working. Folders already browsed stay browsable and files
//! already opened stay readable from the cache; only content never
//! downloaded fails, and only when it is actually read.

mod common;

use std::time::Duration;

use cloudreve_vfs::tree::LISTING_TTL;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_dir, remote_file, VfsTestEnv};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// From now on every listing and every download-URL request fails, like a
/// server that cannot be reached.
async fn go_offline(env: &VfsTestEnv) {
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&env.server)
        .await;
    env.fail_next_file_url_requests(1_000);
}

#[tokio::test]
async fn files_already_opened_stay_readable_offline_and_folders_stay_browsable() {
    let env = VfsTestEnv::new().await;
    let report = b"quarterly report, final".to_vec();
    env.set_remote_files(vec![
        remote_dir("Work"),
        remote_file("report.txt", report.len() as i64, "e1"),
        remote_file("never-opened.txt", 5, "e2"),
    ])
    .await;
    env.serve_file_content("report.txt", &report).await;
    env.serve_file_content("never-opened.txt", b"later").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let root = vfs.tree().root();
    assert_eq!(vfs.readdir(root).await.unwrap().len(), 3);
    let (id, _) = vfs.lookup(root, "report.txt").await.unwrap().unwrap();
    let h = vfs.open(id).await.unwrap();
    assert_eq!(vfs.read(h, 0, 4096).await.unwrap().as_ref(), &report[..]);
    vfs.close(h).await.unwrap();
    env.wait_for_downloads_to_settle().await;

    go_offline(&env).await;
    // Past the listing cache: every listing would now hit the network.
    tokio::time::sleep(LISTING_TTL + Duration::from_millis(300)).await;

    let names: Vec<String> = vfs.readdir(root).await.expect("the folder must still list offline")
        .into_iter().map(|(_, a)| a.name).collect();
    assert_eq!(names.len(), 3, "listed offline: {names:?}");

    let (id, _) = vfs.lookup(root, "report.txt").await.expect("lookup offline").expect("still there");
    let h = vfs.open(id).await.expect("a file already downloaded must open offline");
    assert_eq!(vfs.read(h, 0, 4096).await.expect("read from the cache").as_ref(), &report[..]);
    vfs.close(h).await.unwrap();

    // Never downloaded: there is nothing to show. It may open, but reading
    // it fails (the app shows an error) — it never returns wrong bytes.
    let (id, _) = vfs.lookup(root, "never-opened.txt").await.unwrap().unwrap();
    if let Ok(h) = vfs.open(id).await {
        assert!(vfs.read(h, 0, 4096).await.is_err(), "no bytes can come from nowhere");
        vfs.close(h).await.unwrap();
    }
}
