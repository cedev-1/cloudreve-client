//! The on-demand `Vfs` facade against a REAL Cloudreve server (see
//! `real_server/mod.rs`; skipped unless `CLOUDREVE_E2E_URL` is set).
//!
//! Drives the same facade calls the NFS and FUSE frontends make, without an
//! OS mount, so it runs on any machine that can reach the server. What the
//! server ends up holding is checked over raw HTTP, never through the code
//! under test.

mod real_server;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::tree::NodeId;
use cloudreve_vfs::vfs::{FileHandle, Vfs, VfsEvent, DEFAULT_CACHE_MAX_BYTES};
use real_server::{e2e_or_skip, eventually, pattern, Proxy, Remote, E2e};
use tempfile::TempDir;
use tokio::sync::mpsc::UnboundedReceiver;

const MIB: usize = 1024 * 1024;

struct Fx {
    e2e: E2e,
    remote: Remote,
    cache: TempDir,
    vfs: Arc<Vfs>,
    events: UnboundedReceiver<VfsEvent>,
    /// Kept alive for tests that cut the network; `None` = direct.
    proxy: Option<Proxy>,
}

impl Fx {
    async fn new(label: &str) -> Option<Fx> {
        Self::build(label, false).await
    }

    async fn behind_proxy(label: &str) -> Option<Fx> {
        Self::build(label, true).await
    }

    async fn build(label: &str, proxied: bool) -> Option<Fx> {
        let e2e = e2e_or_skip(label)?;
        let remote = e2e.remote(label).await;
        let proxy = if proxied { Some(Proxy::start(&e2e.url).await) } else { None };
        let base = proxy.as_ref().map(|p| p.url.clone()).unwrap_or_else(|| e2e.url.clone());
        let client = e2e.api_client(&base).await;
        let cache = TempDir::new().unwrap();
        let (vfs, events) = new_vfs(client, &remote.root, &cache);
        Some(Fx { e2e, remote, cache, vfs, events, proxy })
    }

    /// Resolves a `/`-separated path relative to the drive root.
    async fn node(&self, rel: &str) -> NodeId {
        node_of(&self.vfs, rel).await.unwrap_or_else(|| panic!("{rel} not found through the vfs"))
    }

    async fn names(&self, rel: &str) -> Vec<String> {
        let dir = self.node(rel).await;
        let mut names: Vec<String> =
            self.vfs.readdir(dir).await.unwrap().into_iter().map(|(_, a)| a.name).collect();
        names.sort();
        names
    }

    async fn read(&self, rel: &str) -> Vec<u8> {
        let node = self.node(rel).await;
        read_all(&self.vfs, node).await
    }

    /// Creates (or replaces) `rel` the way `cp` does through a mount:
    /// create, sequential 1 MiB writes, close.
    async fn write_new(&self, rel: &str, data: &[u8]) {
        let (parent, name) = split(rel);
        let parent = self.node(parent).await;
        let (_, h) = self.vfs.create(parent, name).await.expect("create");
        write_all(&self.vfs, h, 0, data).await;
        self.vfs.close(h).await.expect("close");
    }

    async fn settle(&self) {
        tokio::time::timeout(Duration::from_secs(600), self.vfs.wait_for_writeback_idle())
            .await
            .expect("write-back never went idle");
    }
}

fn new_vfs(client: Arc<cloudreve_api::Client>, root: &str, cache: &TempDir) -> (Arc<Vfs>, UnboundedReceiver<VfsEvent>) {
    let (vfs, rx) = Vfs::new(client, root.to_string(), cache.path(), DEFAULT_CACHE_MAX_BYTES).unwrap();
    vfs.set_debounce_for_tests(Duration::from_millis(50));
    vfs.set_retry_backoff_for_tests([Duration::from_millis(100), Duration::from_millis(200)]);
    vfs.set_parked_retry_delay_for_tests(Duration::from_millis(500));
    (Arc::new(vfs), rx)
}

fn split(rel: &str) -> (&str, &str) {
    match rel.rsplit_once('/') {
        Some((p, n)) => (p, n),
        None => ("", rel),
    }
}

async fn node_of(vfs: &Vfs, rel: &str) -> Option<NodeId> {
    let mut node = vfs.tree().root();
    for part in rel.split('/').filter(|p| !p.is_empty()) {
        node = vfs.lookup(node, part).await.expect("lookup")?.0;
    }
    Some(node)
}

async fn read_all(vfs: &Vfs, node: NodeId) -> Vec<u8> {
    let size = vfs.getattr(node).await.unwrap().expect("getattr").size;
    let h = vfs.open(node).await.expect("open");
    let mut out = Vec::with_capacity(size as usize);
    while (out.len() as u64) < size {
        let chunk = vfs.read(h, out.len() as u64, MIB as u32).await.expect("read");
        assert!(!chunk.is_empty(), "short read at {} of {size}", out.len());
        out.extend_from_slice(&chunk);
    }
    vfs.close(h).await.unwrap();
    out
}

async fn write_all(vfs: &Vfs, h: FileHandle, offset: u64, data: &[u8]) {
    let mut done = 0usize;
    for chunk in data.chunks(MIB) {
        let n = vfs.write(h, offset + done as u64, chunk).await.expect("write");
        assert_eq!(n as usize, chunk.len());
        done += chunk.len();
    }
}

async fn server_bytes(remote: &Remote, rel: &str) -> Vec<u8> {
    remote.read(rel).await.unwrap_or_else(|| panic!("{rel} is not on the server"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_written_through_the_vfs_lands_on_the_server_byte_for_byte() {
    let Some(fx) = Fx::new("smoke").await else { return };
    let data = pattern(3 * MIB + 17, 1);
    fx.write_new("hello.bin", &data).await;
    fx.settle().await;
    assert_eq!(server_bytes(&fx.remote, "hello.bin").await, data);
    fx.remote.cleanup().await;
}

// ---------------------------------------------------------------- listing

/// Creates `count` files named `f-00000.txt`… in `rel` on the server,
/// 16 requests at a time.
async fn seed_many(remote: &Remote, rel: &str, count: usize) {
    let names: Vec<String> = (0..count).map(|i| format!("f-{i:05}.txt")).collect();
    for batch in names.chunks(16) {
        let mut tasks = tokio::task::JoinSet::new();
        for n in batch {
            let path = if rel.is_empty() { n.clone() } else { format!("{rel}/{n}") };
            let remote = remote.clone_handle();
            tasks.spawn(async move { remote.put(&path, path.as_bytes()).await });
        }
        while let Some(r) = tasks.join_next().await {
            r.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folder_bigger_than_the_server_page_size_lists_every_file() {
    let Some(fx) = Fx::new("paging").await else { return };
    // The server's max_page_size is 2000: 2150 entries need a second page.
    seed_many(&fx.remote, "big", 2150).await;
    assert_eq!(fx.remote.list("big").await.len(), 2150, "seeding went wrong");

    let names = fx.names("big").await;
    assert_eq!(names.len(), 2150);
    assert_eq!(names.first().map(String::as_str), Some("f-00000.txt"));
    assert_eq!(names.last().map(String::as_str), Some("f-02149.txt"));
    assert_eq!(fx.read("big/f-02149.txt").await, b"big/f-02149.txt");
    fx.remote.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unicode_emoji_spaced_long_and_deeply_nested_names_list_and_read_back() {
    let Some(fx) = Fx::new("names").await else { return };
    let long = format!("{}.txt", "long-name-".repeat(24)); // 244 bytes
    let names = [
        "café NFC.txt".to_string(),
        "cafe\u{301} NFD.txt".to_string(),
        "two  inner  spaces .txt".to_string(),
        "emoji 🎉📁 файл 文件.txt".to_string(),
        long.clone(),
    ];
    for (i, n) in names.iter().enumerate() {
        fx.remote.put(&format!("names/{n}"), format!("content {i}").as_bytes()).await;
    }
    fx.remote.put("a/b/c/d/e/f/g/deep.txt", b"deep").await;

    // What the server itself calls these files (it may normalise them).
    let server_names = fx.remote.names("names").await;
    let listed = fx.names("names").await;
    assert_eq!(listed.len(), names.len(), "listed {listed:?}, server has {server_names:?}");
    for (i, n) in names.iter().enumerate() {
        let want = format!("content {i}").into_bytes();
        // Looked up under the name the user typed, as a file manager would.
        assert_eq!(fx.read(&format!("names/{n}")).await, want, "{n:?}");
    }
    assert_eq!(fx.read("a/b/c/d/e/f/g/deep.txt").await, b"deep");
    fx.remote.cleanup().await;
}

// ------------------------------------------------------------------- read

#[tokio::test(flavor = "multi_thread")]
async fn empty_small_and_big_files_read_back_exactly_including_random_ranges() {
    let Some(fx) = Fx::new("reads").await else { return };
    let small = pattern(4321, 2);
    let big = pattern(110 * MIB + 12345, 3);
    fx.remote.put("empty.txt", b"").await;
    fx.remote.put("small.bin", &small).await;
    fx.remote.upload("big.bin", &big).await;

    assert_eq!(fx.read("empty.txt").await, b"");
    assert_eq!(fx.read("small.bin").await, small);

    let node = fx.node("big.bin").await;
    assert_eq!(fx.vfs.getattr(node).await.unwrap().unwrap().size, big.len() as u64);
    let h = fx.vfs.open(node).await.unwrap();
    // Scattered reads, like a video player seeking, including ones that
    // straddle a 1 MiB block boundary and one past the end.
    let mut seed = 0x9e3779b97f4a7c15u64;
    for _ in 0..40 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let off = (seed % big.len() as u64) as usize;
        let len = 1 + (seed >> 40) as usize % (300 * 1024);
        let got = fx.vfs.read(h, off as u64, len as u32).await.unwrap();
        let end = (off + len).min(big.len());
        assert!(got[..] == big[off..end], "range {off}+{len} differs");
    }
    let boundary = fx.vfs.read(h, (5 * MIB - 10) as u64, 20).await.unwrap();
    assert_eq!(&boundary[..], &big[5 * MIB - 10..5 * MIB + 10]);
    let past = fx.vfs.read(h, big.len() as u64 + 10, 100).await.unwrap();
    assert!(past.is_empty());
    fx.vfs.close(h).await.unwrap();

    // And the whole thing, sequentially.
    assert!(fx.read("big.bin").await == big, "big.bin differs");
    fx.remote.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_replaced_on_the_server_while_open_never_reads_as_a_mix_of_both() {
    let Some(fx) = Fx::new("changed-open").await else { return };
    let old = pattern(8 * MIB, 4);
    let new = pattern(8 * MIB, 5);
    fx.remote.put("doc.bin", &old).await;

    let node = fx.node("doc.bin").await;
    let h = fx.vfs.open(node).await.unwrap();
    let first = fx.vfs.read(h, 0, MIB as u32).await.unwrap();
    assert_eq!(&first[..], &old[..MIB]);

    fx.remote.put("doc.bin", &new).await;
    fx.vfs.remote_changed(&fx.remote.uri("doc.bin")).await;

    // The open handle keeps reading: each block must be the OLD file's
    // (or a clean error), never the new file's bytes spliced into the old.
    for block in 1..8usize {
        if let Ok(b) = fx.vfs.read(h, (block * MIB) as u64, MIB as u32).await {
            assert!(
                b[..] == old[block * MIB..(block + 1) * MIB],
                "block {block} of an open handle changed under it (is the new content: {})",
                b[..] == new[block * MIB..(block + 1) * MIB]
            );
        }
    }
    fx.vfs.close(h).await.unwrap();

    // A fresh open sees the new file.
    assert!(fx.read("doc.bin").await == new, "reopen still serves the old content");
    fx.remote.cleanup().await;
}

// ------------------------------------------------------------------ write

#[tokio::test(flavor = "multi_thread")]
async fn create_overwrite_append_and_truncate_reach_the_server() {
    let Some(fx) = Fx::new("writes").await else { return };
    fx.write_new("notes.txt", b"first version").await;
    fx.settle().await;
    assert_eq!(server_bytes(&fx.remote, "notes.txt").await, b"first version");

    // Overwrite: truncate to 0 then write, the editor save pattern.
    let node = fx.node("notes.txt").await;
    let h = fx.vfs.open(node).await.unwrap();
    fx.vfs.truncate(h, 0).await.unwrap();
    fx.vfs.write(h, 0, b"second").await.unwrap();
    fx.vfs.close(h).await.unwrap();
    fx.settle().await;
    assert_eq!(server_bytes(&fx.remote, "notes.txt").await, b"second");

    // Append to a file whose content only the server has: the old bytes
    // must be fetched and kept. (Uploaded elsewhere, announced the way the
    // app's event stream announces it.)
    let base = pattern(3 * MIB + 5, 6);
    fx.remote.put("log.bin", &base).await;
    fx.vfs.remote_changed(&fx.remote.uri("log.bin")).await;
    let node = fx.node("log.bin").await;
    let h = fx.vfs.open(node).await.unwrap();
    fx.vfs.write(h, base.len() as u64, b"--appended--").await.unwrap();
    fx.vfs.close(h).await.unwrap();
    fx.settle().await;
    let mut want = base.clone();
    want.extend_from_slice(b"--appended--");
    assert!(server_bytes(&fx.remote, "log.bin").await == want, "append lost or mangled bytes");

    // Truncate to a non-zero size.
    let h = fx.vfs.open(fx.node("log.bin").await).await.unwrap();
    fx.vfs.truncate(h, 1000).await.unwrap();
    fx.vfs.close(h).await.unwrap();
    fx.settle().await;
    assert_eq!(server_bytes(&fx.remote, "log.bin").await, &base[..1000]);

    // An empty file.
    fx.write_new("empty.txt", b"").await;
    fx.settle().await;
    assert_eq!(server_bytes(&fx.remote, "empty.txt").await, b"");
    fx.remote.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_editor_safe_save_replaces_the_file_with_the_new_content() {
    let Some(fx) = Fx::new("safe-save").await else { return };
    fx.remote.put("report.txt", b"old report").await;

    // Write a temp file next to it, then rename it onto the original.
    fx.write_new("report.txt.sb-1234", b"new report, saved safely").await;
    let root = fx.node("").await;
    fx.vfs.rename(root, "report.txt.sb-1234", root, "report.txt").await.expect("rename onto");
    fx.settle().await;

    assert_eq!(server_bytes(&fx.remote, "report.txt").await, b"new report, saved safely");
    let names = fx.remote.names("").await;
    assert_eq!(names, vec!["report.txt".to_string()], "temp file left behind: {names:?}");
    assert_eq!(fx.names("").await, vec!["report.txt".to_string()]);
    fx.remote.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deep_mkdir_renames_moves_and_deletes_reach_the_server() {
    let Some(fx) = Fx::new("namespace").await else { return };
    // mkdir -p a/b/c/d/e
    let mut parent = fx.node("").await;
    for part in ["a", "b", "c", "d", "e"] {
        parent = fx.vfs.mkdir(parent, part).await.expect("mkdir");
    }
    fx.write_new("a/b/c/d/e/leaf.txt", b"leaf").await;
    fx.write_new("a/top.txt", b"top").await;
    fx.vfs.mkdir(fx.node("").await, "other").await.unwrap();
    fx.settle().await;
    assert_eq!(server_bytes(&fx.remote, "a/b/c/d/e/leaf.txt").await, b"leaf");

    // Rename a file in place, then move it to another folder.
    let a = fx.node("a").await;
    fx.vfs.rename(a, "top.txt", a, "renamed.txt").await.unwrap();
    let other = fx.node("other").await;
    fx.vfs.rename(a, "renamed.txt", other, "moved.txt").await.unwrap();
    // Move a whole folder (with content) across, renaming it on the way.
    let d = fx.node("a/b/c/d").await;
    fx.vfs.rename(d, "e", other, "e-moved").await.unwrap();
    fx.settle().await;

    assert_eq!(server_bytes(&fx.remote, "other/moved.txt").await, b"top");
    assert_eq!(server_bytes(&fx.remote, "other/e-moved/leaf.txt").await, b"leaf");
    assert!(fx.remote.info("a/top.txt").await.is_none());
    assert!(fx.remote.info("a/renamed.txt").await.is_none());
    assert!(fx.remote.info("a/b/c/d/e").await.is_none());
    assert_eq!(fx.read("other/e-moved/leaf.txt").await, b"leaf");

    // Delete a file, then a non-empty folder the way rm -rf does.
    fx.vfs.unlink(other, "moved.txt").await.unwrap();
    let em = fx.node("other/e-moved").await;
    fx.vfs.unlink(em, "leaf.txt").await.unwrap();
    fx.vfs.unlink(other, "e-moved").await.unwrap();
    // "a" still holds b/c/d: refused like rmdir on a non-empty folder.
    let err = fx.vfs.unlink(fx.node("").await, "a").await.expect_err("non-empty folder removed");
    assert!(err.downcast_ref::<cloudreve_vfs::vfs::DirNotEmptyError>().is_some(), "{err:#}");
    fx.settle().await;
    assert_eq!(fx.remote.names("other").await, Vec::<String>::new());
    assert!(fx.remote.info("a/b/c/d").await.is_some());
    fx.remote.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn copying_in_2000_small_files_and_a_few_big_ones_uploads_every_byte() {
    let Some(fx) = Fx::new("bulk").await else { return };
    let dir = fx.vfs.mkdir(fx.node("").await, "many").await.unwrap();
    for i in 0..2000 {
        let (_, h) = fx.vfs.create(dir, &format!("file-{i:04}.txt")).await.unwrap();
        fx.vfs.write(h, 0, format!("small file {i}").as_bytes()).await.unwrap();
        fx.vfs.close(h).await.unwrap();
    }
    let bigs: Vec<Vec<u8>> = (0..3).map(|i| pattern(40 * MIB + i * 7, 10 + i as u64)).collect();
    for (i, b) in bigs.iter().enumerate() {
        fx.write_new(&format!("big-{i}.bin"), b).await;
    }
    fx.settle().await;

    let listed = fx.remote.list("many").await;
    assert_eq!(listed.len(), 2000, "server holds {} of 2000 files", listed.len());
    for i in [0usize, 1, 999, 1500, 1999] {
        assert_eq!(
            server_bytes(&fx.remote, &format!("many/file-{i:04}.txt")).await,
            format!("small file {i}").into_bytes()
        );
    }
    for (i, b) in bigs.iter().enumerate() {
        assert!(server_bytes(&fx.remote, &format!("big-{i}.bin")).await == *b, "big-{i}.bin differs");
    }
    assert_eq!(fx.vfs.unsynced_file_count().await, 0);
    fx.remote.cleanup().await;
}

// -------------------------------------------------------------- conflicts

#[tokio::test(flavor = "multi_thread")]
async fn a_local_edit_racing_a_server_edit_keeps_both_as_a_conflict_copy() {
    let Some(mut fx) = Fx::new("conflict").await else { return };
    fx.remote.put("shared.txt", b"original").await;

    let node = fx.node("shared.txt").await;
    let h = fx.vfs.open(node).await.unwrap();
    assert_eq!(&fx.vfs.read(h, 0, 100).await.unwrap()[..], b"original");
    // Someone else saves first, from the web or another device.
    fx.remote.put("shared.txt", b"edited on the server").await;
    // Then the local edit closes.
    fx.vfs.truncate(h, 0).await.unwrap();
    fx.vfs.write(h, 0, b"edited locally").await.unwrap();
    fx.vfs.close(h).await.unwrap();
    fx.settle().await;

    let names = fx.remote.names("").await;
    assert_eq!(names.len(), 2, "expected the original and one conflict copy, got {names:?}");
    assert_eq!(server_bytes(&fx.remote, "shared.txt").await, b"edited on the server");
    let copy = names.iter().find(|n| n.as_str() != "shared.txt").unwrap();
    assert_eq!(server_bytes(&fx.remote, copy).await, b"edited locally");
    let mut saw_conflict = false;
    while let Ok(ev) = fx.events.try_recv() {
        saw_conflict |= matches!(ev, VfsEvent::ConflictSaved { .. });
    }
    assert!(saw_conflict, "no ConflictSaved event for the user");
    fx.remote.cleanup().await;
}

// ------------------------------------------------------- offline/restart

#[tokio::test(flavor = "multi_thread")]
async fn writes_made_while_the_server_is_unreachable_upload_once_it_is_back() {
    let Some(fx) = Fx::behind_proxy("offline").await else { return };
    let proxy = fx.proxy.as_ref().unwrap();
    // The root is listed while online, as it would be on a mounted drive.
    assert!(fx.names("").await.is_empty());

    proxy.set_down(true);
    let data = pattern(2 * MIB + 3, 20);
    fx.write_new("offline.bin", &data).await;
    // Let the upload attempts fail against the dead server.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(fx.vfs.unsynced_file_count().await, 1, "the draft must survive failed uploads");
    assert!(fx.remote.info("offline.bin").await.is_none());

    proxy.set_down(false);
    fx.vfs.retry_pending_uploads().await;
    eventually("the draft to upload after reconnect", Duration::from_secs(60), || async {
        match fx.remote.read("offline.bin").await {
            Some(b) if b == data => Ok(()),
            other => Err(format!("{:?} bytes on the server", other.map(|b| b.len()))),
        }
    })
    .await;
    fx.settle().await;
    assert_eq!(fx.vfs.unsynced_file_count().await, 0);
    fx.remote.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drafts_left_by_a_restart_while_offline_upload_on_the_next_launch() {
    let Some(fx) = Fx::behind_proxy("restart").await else { return };
    let proxy = fx.proxy.as_ref().unwrap();
    assert!(fx.names("").await.is_empty());
    proxy.set_down(true);
    let data = pattern(MIB + 1, 21);
    fx.write_new("pending.bin", &data).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    fx.vfs.shutdown();

    // Next launch: a new facade over the same cache dir, network back.
    proxy.set_down(false);
    let client = fx.e2e.api_client(&proxy.url).await;
    let (vfs2, _rx2) = new_vfs(client, &fx.remote.root, &fx.cache);
    eventually("the restored draft to upload", Duration::from_secs(60), || async {
        match fx.remote.read("pending.bin").await {
            Some(b) if b == data => Ok(()),
            other => Err(format!("{:?} bytes on the server", other.map(|b| b.len()))),
        }
    })
    .await;
    tokio::time::timeout(Duration::from_secs(60), vfs2.wait_for_writeback_idle()).await.unwrap();
    assert_eq!(vfs2.unsynced_file_count().await, 0);
    fx.remote.cleanup().await;
}

// ------------------------------------------------------------ credentials

#[tokio::test(flavor = "multi_thread")]
async fn an_expired_access_token_is_refreshed_against_the_real_server() {
    let Some(e2e) = e2e_or_skip("token-refresh") else { return };
    let remote = e2e.remote("token").await;
    remote.put("a.txt", b"still readable").await;
    let mut session = e2e.oauth_session().await;
    session.access_expires = (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339();
    let client = cloudreve_api::Client::new(cloudreve_api::ClientConfig::new(e2e.url.clone()));
    client.load_tokens(&session.token()).await;
    let cache = TempDir::new().unwrap();
    let (vfs, _rx) = new_vfs(Arc::new(client), &remote.root, &cache);
    let node = node_of(&vfs, "a.txt").await.expect("listing with a refreshed token");
    assert_eq!(read_all(&vfs, node).await, b"still readable");
    remote.cleanup().await;
}

// ------------------------------------------------------------------ quota

#[tokio::test(flavor = "multi_thread")]
async fn a_save_over_the_storage_quota_fails_visibly_and_keeps_the_local_copy() {
    let Some(admin) = e2e_or_skip("quota") else { return };
    let user = admin.user_with_quota(4 * MIB as u64).await;
    let remote = user.remote("quota").await;
    let client = user.api_client(&user.url).await;
    let cache = TempDir::new().unwrap();
    let (vfs, mut events) = new_vfs(client, &remote.root, &cache);
    let root = vfs.tree().root();
    assert!(vfs.readdir(root).await.unwrap().is_empty());

    let small = pattern(MIB, 70);
    let (_, h) = vfs.create(root, "fits.bin").await.unwrap();
    write_all(&vfs, h, 0, &small).await;
    vfs.close(h).await.unwrap();
    let too_big = pattern(6 * MIB, 71);
    let (_, h) = vfs.create(root, "too-big.bin").await.unwrap();
    write_all(&vfs, h, 0, &too_big).await;
    vfs.close(h).await.unwrap();

    let failure = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match events.recv().await.expect("event channel closed") {
                VfsEvent::UploadFailed { remote_path, error, .. } if remote_path.ends_with("too-big.bin") => {
                    return error;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("no UploadFailed event for the over-quota file");
    assert!(!failure.trim().is_empty(), "the failure carries no reason");
    eprintln!("over-quota failure reported as: {failure}");

    assert_eq!(server_bytes(&remote, "fits.bin").await, small);
    assert!(remote.info("too-big.bin").await.is_none());
    // Nothing the user wrote is lost: the draft stays, readable.
    assert_eq!(vfs.unsynced_file_count().await, 1);
    let node = node_of(&vfs, "too-big.bin").await.expect("the unsaved file is still listed");
    assert!(read_all(&vfs, node).await == too_big, "the unsaved file changed");
    vfs.shutdown();
    remote.cleanup().await;
}
