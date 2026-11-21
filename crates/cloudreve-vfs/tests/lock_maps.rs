//! The per-path lock maps (`open_locks`, `draft_begin_locks`) gained one
//! entry per file ever opened or written and never shrank: a long session
//! over a big tree grew them without bound.

mod common;

use std::time::Duration;

use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::VfsTestEnv;

#[tokio::test]
async fn the_per_path_lock_maps_stay_bounded() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    let root = vfs.tree().root();
    for i in 0..400 {
        let (_n, h) = vfs.create(root, &format!("f{i}.txt")).await.unwrap();
        vfs.write(h, 0, b"x").await.unwrap();
        vfs.close(h).await.unwrap();
    }
    let (open_locks, begin_locks) = vfs.lock_map_sizes_for_tests();
    assert!(open_locks < 300, "{open_locks} open locks kept for 400 closed files");
    assert!(begin_locks < 300, "{begin_locks} draft locks kept for 400 closed files");
    vfs.shutdown();
}
