//! Accented names across Unicode normalization forms.
//!
//! macOS hands file names to a network volume in decomposed form (NFD:
//! "e" + U+0301 COMBINING ACUTE ACCENT) when they come from Finder or Cocoa
//! (`fileSystemRepresentation`), while Linux, Windows and the Cloudreve web
//! UI produce composed names (NFC: a single U+00E9). Both render as "é".
//! Without normalization a file created on the Mac reached the server as
//! NFD (a second, look-alike "Café.txt" next to an existing one), and a
//! name typed in composed form could not open a file stored decomposed.
//!
//! The byte strings below are written out by hand, never produced by the
//! implementation's own normalization.

mod common;

use std::time::Duration;

use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::{remote_file, VfsTestEnv};

/// "Café.txt", composed: U+00E9.
const CAFE_NFC: &str = "Caf\u{e9}.txt";
/// "Café.txt", decomposed: "e" + U+0301.
const CAFE_NFD: &str = "Cafe\u{301}.txt";
/// "Résumé 2026", composed.
const RESUME_NFC: &str = "R\u{e9}sum\u{e9} 2026";
/// "Résumé 2026", decomposed.
const RESUME_NFD: &str = "Re\u{301}sume\u{301} 2026";

async fn vfs_for(env: &VfsTestEnv) -> Vfs {
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    vfs
}

#[tokio::test]
async fn a_decomposed_lookup_finds_a_composed_remote_name() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file(CAFE_NFC, 3, "e1")]).await;
    let vfs = vfs_for(&env).await;
    let root = vfs.tree().root();

    let found = vfs.lookup(root, CAFE_NFD).await.unwrap();
    assert!(found.is_some(), "Finder's decomposed name must open the composed remote file");
}

#[tokio::test]
async fn a_composed_lookup_finds_a_decomposed_remote_name() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file(CAFE_NFD, 3, "e1")]).await;
    let vfs = vfs_for(&env).await;
    let root = vfs.tree().root();

    let found = vfs.lookup(root, CAFE_NFC).await.unwrap();
    assert!(found.is_some(), "a composed name must open a file another Mac uploaded decomposed");
}

#[tokio::test]
async fn creating_a_decomposed_name_next_to_its_composed_twin_is_refused() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![remote_file(CAFE_NFC, 3, "e1")]).await;
    let vfs = vfs_for(&env).await;
    let root = vfs.tree().root();

    let err = vfs
        .create(root, CAFE_NFD)
        .await
        .err()
        .expect("the same visible name must not become a second, look-alike file");
    assert!(format!("{err:#}").contains("EEXIST"), "unexpected error: {err:#}");
}

#[tokio::test]
async fn a_file_created_with_a_decomposed_name_uploads_composed() {
    let env = VfsTestEnv::new().await;
    env.expect_uploads().await;
    env.set_remote_files(vec![]).await;
    let vfs = vfs_for(&env).await;
    vfs.set_debounce_for_tests(Duration::from_millis(20));
    let root = vfs.tree().root();

    let name = format!("{RESUME_NFD}.txt");
    let (_node, h) = vfs.create(root, &name).await.unwrap();
    vfs.write(h, 0, b"cv").await.unwrap();

    let expected = format!("{RESUME_NFC}.txt");
    let listing = vfs.readdir(root).await.unwrap();
    assert!(
        listing.iter().any(|(_, a)| a.name == expected),
        "the listing must show the composed name the server will get"
    );
    assert!(vfs.lookup(root, &name).await.unwrap().is_some(), "the decomposed name still resolves");

    vfs.close(h).await.unwrap();
    vfs.wait_for_writeback_idle().await;
    assert_eq!(env.uploaded_names(), vec![expected]);
}

#[tokio::test]
async fn a_folder_created_with_a_decomposed_name_is_created_composed() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![]).await;
    let vfs = vfs_for(&env).await;
    let root = vfs.tree().root();

    vfs.mkdir(root, RESUME_NFD).await.unwrap();

    let listing = vfs.readdir(root).await.unwrap();
    let names: Vec<&str> = listing.iter().map(|(_, a)| a.name.as_str()).collect();
    assert_eq!(names, vec![RESUME_NFC]);
}

#[tokio::test]
async fn renaming_to_a_decomposed_name_renames_composed() {
    let env = VfsTestEnv::new().await;
    env.expect_namespace_ops().await;
    env.set_remote_files(vec![remote_file("cv.txt", 2, "e1")]).await;
    let vfs = vfs_for(&env).await;
    let root = vfs.tree().root();

    let new_name = format!("{RESUME_NFD}.txt");
    vfs.rename(root, "cv.txt", root, &new_name).await.unwrap();

    assert_eq!(env.last_rename().map(|(_, to)| to), Some(format!("{RESUME_NFC}.txt")));
}
