//! Phase 3, Task 2: drives `VfsNfs` — `nfs3_server`'s filesystem trait
//! implemented over the `Vfs` facade — directly as a trait object. No
//! mounting happens anywhere in this file (that's Task 3's job); every test
//! here calls `NfsReadFileSystem`/`NfsFileSystem` methods straight on the
//! `VfsNfs` value, exactly like the harness's mocked HTTP layer stands in
//! for the real Cloudreve server.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::nfs::VfsNfs;
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use nfs3_server::nfs3_types::nfs3::{filename3, nfsstat3, sattr3, stable_how, Nfs3Option};
use nfs3_server::vfs::{FileHandleU64, NextResult, NfsFileSystem, NfsReadFileSystem, ReadDirPlusIterator};
use common::{remote_dir, remote_file, VfsTestEnv};

/// Builds an owned NFS wire filename from a plain `&str`.
fn fname(s: &str) -> filename3<'static> {
    s.as_bytes().to_vec().into()
}

/// Decodes an NFS wire filename back into a `String` for assertions.
fn name_of(entry_name: &filename3<'_>) -> String {
    String::from_utf8(entry_name.as_ref().to_vec()).unwrap()
}

/// Drains up to `max` entries from one `readdirplus` call, returning
/// `(entries, last_cookie_seen, hit_eof)` — the paging test needs the last
/// cookie to resume from, and whether the FIRST page hit EOF tells the test
/// whether it needs a second page at all.
async fn collect_page(
    fs: &VfsNfs,
    dir: &FileHandleU64,
    cookie: u64,
    max: usize,
) -> (Vec<(u64, String)>, u64, bool) {
    let mut iter = fs.readdirplus(dir, cookie).await.expect("readdirplus should succeed");
    let mut out = Vec::new();
    let mut last_cookie = cookie;
    let mut eof = false;
    for _ in 0..max {
        match iter.next().await {
            NextResult::Ok(entry) => {
                last_cookie = entry.cookie;
                out.push((entry.fileid, name_of(&entry.name)));
            }
            NextResult::Eof => {
                eof = true;
                break;
            }
            NextResult::Err(err) => panic!("readdirplus iterator error: {err}"),
        }
    }
    (out, last_cookie, eof)
}

/// Full listing helper for tests that don't care about paging.
async fn collect_all(fs: &VfsNfs, dir: &FileHandleU64) -> Vec<(u64, String, u64)> {
    let mut iter = fs.readdirplus(dir, 0).await.expect("readdirplus should succeed");
    let mut out = Vec::new();
    loop {
        match iter.next().await {
            NextResult::Ok(entry) => {
                let size = entry.name_attributes.as_ref().map(|a| a.size).unwrap_or(0);
                out.push((entry.fileid, name_of(&entry.name), size));
            }
            NextResult::Eof => break,
            NextResult::Err(err) => panic!("readdirplus iterator error: {err}"),
        }
    }
    out
}

#[tokio::test]
async fn root_readdir_lists_mocked_files_with_sizes() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 11, "e1"), remote_file("b.txt", 22, "e2")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let listing = collect_all(&fs, &root).await;
    let names: Vec<(String, u64)> = listing.into_iter().map(|(_, name, size)| (name, size)).collect();
    assert!(names.contains(&("a.txt".to_string(), 11)));
    assert!(names.contains(&("b.txt".to_string(), 22)));
}

#[tokio::test]
async fn lookup_miss_maps_to_noent() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("a.txt", 1, "e1")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let err = fs.lookup(&root, &fname("missing.txt")).await.unwrap_err();
    assert_eq!(err, nfsstat3::NFS3ERR_NOENT);
}

#[tokio::test]
async fn read_returns_the_exact_ranged_slice() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    env.set_remote_files(vec![remote_file("big.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("big.bin", &body).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();
    let id = fs.lookup(&root, &fname("big.bin")).await.unwrap();

    let (data, eof) = fs.read(&id, 100, 50).await.unwrap();
    assert_eq!(data, body[100..150]);
    assert!(!eof, "a slice well before EOF must not report eof");

    // The read must have gone out over the wire as a ranged GET — proves
    // this went through the facade's real block-cache/download path, not a
    // fake/short-circuited response.
    let requests = env.download_requests("big.bin");
    assert!(!requests.is_empty(), "the ranged read must have hit the download endpoint");
    assert!(
        requests.iter().any(|r| r.is_some()),
        "at least one of the requests must have carried a Range header"
    );

    // A read that reaches the last byte reports eof.
    let (tail, eof) = fs.read(&id, (body.len() - 10) as u64, 100).await.unwrap();
    assert_eq!(tail, &body[body.len() - 10..]);
    assert!(eof, "a read reaching the file's end must report eof");
}

#[tokio::test]
async fn write_lands_in_a_draft_and_eventually_uploads() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let vfs = Arc::new(vfs);
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    let fs = VfsNfs::new(vfs.clone());
    let root = fs.root_dir();

    let (id, _attr) = fs.create(&root, &fname("new.txt"), sattr3::default()).await.unwrap();
    let content = b"created through the mount";
    let (attr_after_write, stable) = fs.write(&id, 0, content, stable_how::FILE_SYNC).await.unwrap();
    assert_eq!(attr_after_write.size, content.len() as u64);
    assert_eq!(stable, stable_how::FILE_SYNC);

    // getattr on the still-drafted file reports the draft's size (D3
    // overlay), before any upload has landed.
    let mid_attr = fs.getattr(&id).await.unwrap();
    assert_eq!(mid_attr.size, content.len() as u64);

    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_content("new.txt").as_deref(), Some(&content[..]));
}

#[tokio::test]
async fn create_mkdir_remove_and_rename_map_through_with_api_hits_recorded() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.add_remote_file("doomed.txt", b"bye".to_vec(), "e1").await;
    env.set_remote_files(vec![
        remote_file("doomed.txt", 3, "e1"),
        remote_file("old.txt", 4, "e2"),
    ])
    .await;
    env.serve_file_content("old.txt", b"data").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    // mkdir hits the create-folder API and is immediately visible.
    let (dir_id, dir_attr) = fs.mkdir(&root, &fname("photos")).await.unwrap();
    assert_eq!(env.create_file_call_count(), 1);
    let looked_up = fs.getattr(&dir_id).await.unwrap();
    assert!(looked_up.type_ == nfs3_server::nfs3_types::nfs3::ftype3::NF3DIR);
    let _ = dir_attr;

    // remove hits the delete API and the entry is gone from a lookup.
    fs.remove(&root, &fname("doomed.txt")).await.unwrap();
    assert_eq!(env.delete_call_count(), 1);
    let err = fs.lookup(&root, &fname("doomed.txt")).await.unwrap_err();
    assert_eq!(err, nfsstat3::NFS3ERR_NOENT);

    // rename (same directory) hits the rename API and the old name is gone,
    // the new one resolves.
    fs.rename(&root, &fname("old.txt"), &root, &fname("renamed.txt")).await.unwrap();
    assert_eq!(env.rename_call_count(), 1);
    assert!(fs.lookup(&root, &fname("old.txt")).await.is_err());
    assert!(fs.lookup(&root, &fname("renamed.txt")).await.is_ok());
}

#[tokio::test]
async fn setattr_with_size_zero_truncates() {
    let env = VfsTestEnv::new().await;
    env.add_remote_file("existing.txt", b"some content here".to_vec(), "e1").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();
    let id = fs.lookup(&root, &fname("existing.txt")).await.unwrap();

    let attr = fs
        .setattr(&id, sattr3 { size: Nfs3Option::Some(0), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(attr.size, 0, "setattr(size=0) must truncate immediately");

    let after = fs.getattr(&id).await.unwrap();
    assert_eq!(after.size, 0, "the draft overlay must report the truncated size");
}

#[tokio::test]
async fn getattr_on_a_drafted_file_reports_the_draft_size() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let (id, _) = fs.create(&root, &fname("draft.txt"), sattr3::default()).await.unwrap();
    fs.write(&id, 0, b"twelve bytes", stable_how::FILE_SYNC).await.unwrap();

    let attr = fs.getattr(&id).await.unwrap();
    assert_eq!(attr.size, "twelve bytes".len() as u64);
}

/// A paged listing over a directory large enough to force two `readdirplus`
/// calls: every entry must appear exactly once across both pages (D4's
/// cookie contract). Mutating the cookie handling to always restart at
/// index 0 makes the second page repeat the first, which this test catches
/// via the deduplicated name count falling below 300.
#[tokio::test]
async fn a_paged_readdir_over_300_entries_has_no_duplicates_or_gaps() {
    let env = VfsTestEnv::new().await;
    let files: Vec<_> = (0..300).map(|i| remote_file(&format!("file-{i:03}"), i, &format!("e{i}"))).collect();
    env.set_remote_files(files).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let (first_page, cookie_after_first, first_eof) = collect_page(&fs, &root, 0, 150).await;
    assert_eq!(first_page.len(), 150, "the first page must return exactly what was asked for");
    assert!(!first_eof, "300 entries must not fit in the first 150-entry page");

    let (second_page, _last_cookie, second_eof) =
        collect_page(&fs, &root, cookie_after_first, 1000).await;
    assert!(second_eof, "the second page must reach the end of the directory");

    // "No duplicates" checked on the RAW combined count first: a broken
    // cookie that always restarts at index 0 would make the second page
    // repeat (some of) the first page's entries, which a plain `HashSet`
    // union would silently absorb without this length check ever noticing.
    let combined_len = first_page.len() + second_page.len();
    assert_eq!(combined_len, 300, "the two pages combined must total exactly 300 raw entries");
    let first_names: HashSet<&String> = first_page.iter().map(|(_, name)| name).collect();
    let second_names: HashSet<&String> = second_page.iter().map(|(_, name)| name).collect();
    assert!(
        first_names.is_disjoint(&second_names),
        "no duplicates: the two pages must not share any entry"
    );

    // "No gaps" checked on the union of names.
    let all_names: HashSet<String> =
        first_page.iter().chain(second_page.iter()).map(|(_, name)| name.clone()).collect();
    assert_eq!(all_names.len(), 300);
    for i in 0..300 {
        assert!(all_names.contains(&format!("file-{i:03}")), "no gaps: file-{i:03} must be present");
    }
}

/// A directory the server already knows about (not one this adapter just
/// created) must still report `NF3DIR` through `getattr` — the `is_dir`
/// flag comes straight from the facade's `NodeAttr`, not from having gone
/// through `mkdir`.
#[tokio::test]
async fn readdir_reports_a_preexisting_remote_directory_as_a_directory() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_dir("folder")]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();
    let id = fs.lookup(&root, &fname("folder")).await.unwrap();
    let attr = fs.getattr(&id).await.unwrap();
    assert_eq!(attr.type_, nfs3_server::nfs3_types::nfs3::ftype3::NF3DIR);
}

/// Phase 4, deliverable D: `remove` (NFSv3's single call for both files and
/// directories, see `nfs.rs`'s doc) on a non-empty directory must surface
/// `NFS3ERR_NOTEMPTY`, driven through the real `NfsFileSystem` trait object
/// (not just a unit test of the mapping function) — this facade never does a
/// recursive delete.
#[tokio::test]
async fn removing_a_non_empty_directory_maps_to_notempty() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("docs")]).await;
    let mut inside = remote_file("inside.txt", 5, "e1");
    inside["path"] = serde_json::json!(format!("{}/docs/inside.txt", common::REMOTE_BASE));
    env.set_remote_files_at("docs", vec![inside]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let err = fs.remove(&root, &fname("docs")).await.unwrap_err();
    assert_eq!(err, nfsstat3::NFS3ERR_NOTEMPTY);
    assert_eq!(env.delete_call_count(), 0, "a refused rmdir must never have reached the server");
    assert!(fs.lookup(&root, &fname("docs")).await.is_ok(), "the directory must still resolve");
}

/// The positive case: an empty directory removes cleanly through `remove`.
#[tokio::test]
async fn removing_an_empty_directory_succeeds() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_dir("empty-dir")]).await;
    env.set_remote_files_at("empty-dir", vec![]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    fs.remove(&root, &fname("empty-dir")).await.expect("removing an empty directory should succeed");
    assert_eq!(env.delete_call_count(), 1);
    assert_eq!(fs.lookup(&root, &fname("empty-dir")).await.unwrap_err(), nfsstat3::NFS3ERR_NOENT);
}

/// A READ the server cannot answer quickly (cold hydration from a slow
/// Cloudreve server) must be answered with `NFS3ERR_JUKEBOX` ("retry
/// later") instead of being held open: on a `soft` mount, Apple's NFS
/// client gives up on an RPC that sees no reply within a few of its
/// retransmit timeouts and hands the app EIO/ETIMEDOUT, whereas a JUKEBOX
/// reply makes it sleep and resend the very same READ. The fetch itself
/// must keep running in the background, and the client's retries must
/// pick up its result — not start a fresh download each time.
#[tokio::test]
async fn a_slow_read_answers_jukebox_then_serves_the_bytes_from_one_download() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("slow.txt", 11, "e1")]).await;
    env.serve_file_content("slow.txt", b"hello world").await;
    env.slow_down_downloads("slow.txt", Duration::from_millis(3000));

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();
    let id = fs.lookup(&root, &fname("slow.txt")).await.unwrap();

    let started = std::time::Instant::now();
    let first = fs.read(&id, 0, 11).await;
    assert_eq!(first.unwrap_err(), nfsstat3::NFS3ERR_JUKEBOX);
    assert!(
        started.elapsed() < Duration::from_millis(2600),
        "the JUKEBOX reply must come back well before the 3s download finishes, took {:?}",
        started.elapsed()
    );

    // The client's retry loop: resend the same READ until it succeeds.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let data = loop {
        match fs.read(&id, 0, 11).await {
            Ok((data, eof)) => {
                assert!(eof, "an 11-byte read of an 11-byte file is at eof");
                break data;
            }
            Err(nfsstat3::NFS3ERR_JUKEBOX) => {
                assert!(std::time::Instant::now() < deadline, "read never completed");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(other) => panic!("retried read failed with {other:?}"),
        }
    };
    assert_eq!(data, b"hello world");
    assert_eq!(
        env.download_requests("slow.txt").len(),
        1,
        "every retry must join the one in-flight download, not start its own"
    );
}

/// The counterpart: a block that takes 1.5s to download — an ordinary cold
/// read from a remote Cloudreve server — is answered on the first try.
/// JUKEBOX is only for genuinely slow fetches, since each one costs the app
/// a 4s client-side sleep before the resend.
#[tokio::test]
async fn a_fast_read_is_answered_directly_without_jukebox() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file("fast.txt", 11, "e1")]).await;
    env.serve_file_content("fast.txt", b"hello world").await;
    env.slow_down_downloads("fast.txt", Duration::from_millis(1500));

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();
    let id = fs.lookup(&root, &fname("fast.txt")).await.unwrap();

    let (data, _eof) = fs.read(&id, 0, 11).await.expect("a fast read must not be deferred");
    assert_eq!(data, b"hello world");
}

/// macOS junk through the NFS trait object: a reserved volume folder name is
/// refused with ACCES, a sidecar works locally, and renaming it onto a real
/// name answers XDEV so `mv`/Finder fall back to copy-and-delete.
#[tokio::test]
async fn junk_names_map_to_acces_and_a_sidecar_renamed_onto_a_real_name_to_xdev() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    assert_eq!(fs.mkdir(&root, &fname(".Trashes")).await.unwrap_err(), nfsstat3::NFS3ERR_ACCES);
    assert_eq!(
        fs.create(&root, &fname(".fseventsd"), sattr3::default()).await.unwrap_err(),
        nfsstat3::NFS3ERR_ACCES
    );

    let (id, _) = fs.create(&root, &fname("._a.txt"), sattr3::default()).await.unwrap();
    fs.write(&id, 0, b"xattrs", stable_how::FILE_SYNC).await.unwrap();
    assert_eq!(fs.lookup(&root, &fname("._a.txt")).await.unwrap().as_u64(), id.as_u64());
    let (data, _) = fs.read(&id, 0, 64).await.unwrap();
    assert_eq!(data, b"xattrs");

    assert_eq!(
        fs.rename(&root, &fname("._a.txt"), &root, &fname("a.txt")).await.unwrap_err(),
        nfsstat3::NFS3ERR_XDEV
    );
    assert_eq!(env.upload_session_count() + env.create_file_call_count(), 0);
}

/// The client reads and writes in chunks of at most the server's FSINFO
/// `rtmax`/`wtmax`, whatever `rsize`/`wsize` the mount asked for. 1 MiB is
/// the macOS kext's own ceiling and one block of the vfs cache.
#[tokio::test]
async fn fsinfo_advertises_one_mebibyte_reads_and_writes() {
    let env = VfsTestEnv::new().await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let info = fs.fsinfo(&root).await.unwrap();
    assert_eq!(info.rtmax, 1_048_576);
    assert_eq!(info.rtpref, 1_048_576);
    assert_eq!(info.wtmax, 1_048_576);
    assert_eq!(info.wtpref, 1_048_576);
}

/// Spotlight: an empty, read-only `.metadata_never_index` sits at the
/// volume root (Apple DTS's documented way to keep `mds` off a volume). It
/// is served by the adapter alone: never listed, never uploaded, never
/// fetched, and a server-side file of the same name stays invisible.
#[tokio::test]
async fn the_volume_root_serves_an_empty_read_only_metadata_never_index() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file(".metadata_never_index", 5, "e1"), remote_dir("docs")]).await;
    env.serve_file_content(".metadata_never_index", b"stale").await;
    env.set_remote_files_at("docs", vec![]).await;
    env.add_remote_file("a.txt", b"a".to_vec(), "e2").await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let id = fs.lookup(&root, &fname(".metadata_never_index")).await.expect("must resolve at the root");
    let attr = fs.getattr(&id).await.unwrap();
    assert_eq!(attr.size, 0);
    assert_eq!(attr.type_, nfs3_server::nfs3_types::nfs3::ftype3::NF3REG);
    assert_eq!(fs.read(&id, 0, 4096).await.unwrap(), (Vec::new(), true));

    assert_eq!(fs.write(&id, 0, b"x", stable_how::FILE_SYNC).await.unwrap_err(), nfsstat3::NFS3ERR_ACCES);
    assert_eq!(
        fs.setattr(&id, sattr3 { size: Nfs3Option::Some(0), ..Default::default() }).await.unwrap_err(),
        nfsstat3::NFS3ERR_ACCES
    );
    assert_eq!(fs.remove(&root, &fname(".metadata_never_index")).await.unwrap_err(), nfsstat3::NFS3ERR_ACCES);
    assert_eq!(
        fs.rename(&root, &fname(".metadata_never_index"), &root, &fname("x")).await.unwrap_err(),
        nfsstat3::NFS3ERR_ACCES
    );
    assert_eq!(
        fs.rename(&root, &fname("a.txt"), &root, &fname(".metadata_never_index")).await.unwrap_err(),
        nfsstat3::NFS3ERR_ACCES
    );
    assert_eq!(
        fs.create(&root, &fname(".metadata_never_index"), sattr3::default()).await.unwrap_err(),
        nfsstat3::NFS3ERR_EXIST
    );

    let names: Vec<String> = collect_all(&fs, &root).await.into_iter().map(|(_, n, _)| n).collect();
    assert!(!names.contains(&".metadata_never_index".to_string()), "never listed: {names:?}");
    let docs = fs.lookup(&root, &fname("docs")).await.unwrap();
    assert_eq!(
        fs.lookup(&docs, &fname(".metadata_never_index")).await.unwrap_err(),
        nfsstat3::NFS3ERR_NOENT,
        "only the volume root has one"
    );
    assert_eq!(env.file_url_request_count(".metadata_never_index"), 0);
    assert_eq!(env.upload_session_count() + env.create_file_call_count() + env.delete_call_count(), 0);
}

/// With `rsize=1048576` every READ covers exactly one 1 MiB cache block, so
/// the client's resend of a JUKEBOX'd READ (same handle, offset and count)
/// must join that block's one download.
#[tokio::test]
async fn a_one_mebibyte_read_resent_after_jukebox_joins_the_one_block_download() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..2 * 1_048_576u32).map(|i| (i % 251) as u8).collect();
    env.set_remote_files(vec![remote_file("big.bin", body.len() as i64, "e1")]).await;
    env.serve_file_content("big.bin", &body).await;
    env.slow_down_downloads("big.bin", Duration::from_millis(3000));

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();
    let id = fs.lookup(&root, &fname("big.bin")).await.unwrap();

    assert_eq!(fs.read(&id, 1_048_576, 1_048_576).await.unwrap_err(), nfsstat3::NFS3ERR_JUKEBOX);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (data, eof) = loop {
        match fs.read(&id, 1_048_576, 1_048_576).await {
            Ok(res) => break res,
            Err(nfsstat3::NFS3ERR_JUKEBOX) => {
                assert!(std::time::Instant::now() < deadline, "read never completed");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(other) => panic!("retried read failed with {other:?}"),
        }
    };
    assert!(eof);
    assert!(data == body[1_048_576..], "wrong bytes for the second block");
    let block_fetches = env
        .download_requests("big.bin")
        .into_iter()
        .filter(|r| r.as_deref().is_some_and(|r| r.starts_with("bytes=1048576-")))
        .count();
    assert_eq!(block_fetches, 1, "the resends must join the one download of that block");
}

/// The volume root's own AppleDouble sidecar `._.` (where macOS keeps the
/// root's FinderInfo, so the drive folder's custom-icon bit) is LISTED at
/// the root once it exists: the macOS client answers a lookup of a `._`
/// name missing from a listing it holds with ENOENT on its own, which made
/// the drive's icon impossible to set or read once Finder had listed the
/// folder (see `cloudreve_vfs::appledouble`). Every other sidecar stays
/// unlisted, and a `._.` inside a subfolder too.
#[tokio::test]
async fn the_volume_root_lists_its_own_appledouble_sidecar_and_no_other_junk() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("a.txt", 3, "e1"), remote_dir("docs")]).await;
    env.set_remote_files_at("docs", vec![]).await;

    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let vfs = Arc::new(vfs);
    let root_node = vfs.tree().root();
    let mut finder_info = [0u8; 32];
    finder_info[8] = 0x04;
    let sidecar = cloudreve_vfs::appledouble::finder_info_sidecar(finder_info);
    vfs.put_local_only(root_node, "._.", sidecar.clone()).await.unwrap();
    vfs.put_local_only(root_node, "._a.txt", b"xattrs".to_vec()).await.unwrap();
    vfs.put_local_only(root_node, ".VolumeIcon.icns", b"icns".to_vec()).await.unwrap();
    let fs = VfsNfs::new(vfs.clone());
    let root = fs.root_dir();
    let docs = fs.lookup(&root, &fname("docs")).await.unwrap();
    let (id, _) = fs.create(&docs, &fname("._."), sattr3::default()).await.unwrap();
    fs.write(&id, 0, b"inner", stable_how::FILE_SYNC).await.unwrap();

    let mut names: Vec<String> = collect_all(&fs, &root).await.into_iter().map(|(_, n, _)| n).collect();
    names.sort();
    let in_docs: Vec<String> = collect_all(&fs, &docs).await.into_iter().map(|(_, n, _)| n).collect();
    let listed = collect_all(&fs, &root).await.into_iter().find(|(_, n, _)| n == "._.");
    let looked_up = fs.lookup(&root, &fname("._.")).await.unwrap();
    let (data, _) = fs.read(&looked_up, 0, 8192).await.unwrap();

    assert_eq!(names, vec!["._.", "a.txt", "docs"]);
    assert!(in_docs.is_empty(), "a subfolder listed its junk: {in_docs:?}");
    assert_eq!(listed.map(|(id, _, _)| id), Some(looked_up.as_u64()), "the listed entry is not the sidecar");
    assert_eq!(data, sidecar);
    assert_eq!(env.upload_session_count() + env.create_file_call_count(), 0);
}

/// RFC 1813 exclusive CREATE: a client that resends the call (its first
/// reply was lost) with the SAME verifier must get the file it created,
/// not EEXIST — while a different client's (different verifier) still
/// gets EEXIST.
#[tokio::test]
async fn a_retransmitted_exclusive_create_succeeds_and_a_different_one_does_not() {
    use nfs3_server::nfs3_types::nfs3::createverf3;
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    let fs = VfsNfs::new(Arc::new(vfs));
    let root = fs.root_dir();

    let first = fs.create_exclusive(&root, &fname("lock.pid"), createverf3([7; 8])).await.unwrap();
    let again = fs.create_exclusive(&root, &fname("lock.pid"), createverf3([7; 8])).await;
    assert_eq!(again.map(|h| h.as_u64()), Ok(first.as_u64()), "the resend must get the same file");
    let other = fs.create_exclusive(&root, &fname("lock.pid"), createverf3([9; 8])).await;
    assert_eq!(other.map(|h| h.as_u64()), Err(nfsstat3::NFS3ERR_EXIST));
}

/// The first WRITE (or a resize) of a file not yet drafted downloads the
/// whole file first. For a big remote file that outlived the client's soft
/// timeout (~9 s) and the app got EIO; READ already answered JUKEBOX past
/// a threshold, WRITE/SETATTR now do the same: the work keeps running, and
/// the client's resend of the same call picks up its result.
#[tokio::test]
async fn a_slow_first_write_answers_jukebox_and_its_resend_completes_it() {
    let env = VfsTestEnv::new().await;
    let body: Vec<u8> = (0..=255u8).cycle().take(3 * 1024 * 1024).collect();
    env.set_remote_files(vec![
        remote_file("video.mov", body.len() as i64, "e1"),
        remote_file("other.mov", body.len() as i64, "e2"),
    ])
    .await;
    env.serve_file_content("video.mov", &body).await;
    env.slow_down_downloads("video.mov", Duration::from_millis(1200));
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs.set_debounce_for_tests(Duration::from_secs(3600));
    let fs = VfsNfs::with_read_jukebox_after(Arc::new(vfs), Duration::from_millis(200));
    let file = fs.lookup(&fs.root_dir(), &fname("video.mov")).await.unwrap();

    let started = std::time::Instant::now();
    let first = fs.write(&file, 10, b"PATCH", stable_how::UNSTABLE).await;
    assert_eq!(first.err(), Some(nfsstat3::NFS3ERR_JUKEBOX), "a slow write must not hold the RPC");
    assert!(started.elapsed() < Duration::from_secs(1), "held {:?}", started.elapsed());

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        match fs.write(&file, 10, b"PATCH", stable_how::UNSTABLE).await {
            Ok(_) => break,
            Err(nfsstat3::NFS3ERR_JUKEBOX) => assert!(std::time::Instant::now() < deadline, "never completed"),
            Err(other) => panic!("resend failed: {other:?}"),
        }
    }
    let (data, _) = fs.read(&file, 0, 20).await.unwrap();
    let mut expected = body[..20].to_vec();
    expected[10..15].copy_from_slice(b"PATCH");
    assert_eq!(data, expected);

    // Same for a resize of an undrafted big file.
    env.serve_file_content("other.mov", &body).await;
    env.slow_down_downloads("other.mov", Duration::from_millis(1200));
    let other = fs.lookup(&fs.root_dir(), &fname("other.mov")).await.unwrap();
    let resize = sattr3 { size: Nfs3Option::Some(1000), ..Default::default() };
    let started = std::time::Instant::now();
    let first = fs.setattr(&other, resize.clone()).await;
    assert_eq!(first.err(), Some(nfsstat3::NFS3ERR_JUKEBOX));
    assert!(started.elapsed() < Duration::from_secs(1));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        match fs.setattr(&other, resize.clone()).await {
            Ok(attr) => {
                assert_eq!(attr.size, 1000);
                break;
            }
            Err(nfsstat3::NFS3ERR_JUKEBOX) => assert!(std::time::Instant::now() < deadline, "never completed"),
            Err(other) => panic!("resend failed: {other:?}"),
        }
    }
}
