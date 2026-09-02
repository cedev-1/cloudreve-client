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
