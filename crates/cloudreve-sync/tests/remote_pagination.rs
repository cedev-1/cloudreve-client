//! The remote listing must be read to the end, whatever the server's page size.
//!
//! Cloudreve returns a directory listing one page at a time and paginates in one
//! of two ways depending on the storage policy: an offset (`total_items` + `page`)
//! or a cursor (`next_token`). A client that reads only the first page silently
//! ignores everything past it — the files are never downloaded, and worse, a file
//! that was already synced but has drifted past the page boundary looks like it was
//! deleted from the server, which purges its inventory row and gets it re-uploaded
//! on the next pass.

mod common;

use std::time::Duration;

use common::{REMOTE_BASE, TestEnv, remote_file};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

/// A listing page as the Cloudreve API returns it.
fn page(files: Vec<Value>, pagination: Value, max_page_size: i32) -> Value {
    json!({
        "code": 0,
        "msg": "",
        "data": {
            "files": files,
            "pagination": pagination,
            "props": {
                "max_page_size": max_page_size,
                "order_by_options": ["name"],
                "order_direction_options": ["asc"],
            },
        },
    })
}

fn remote_folder(name: &str) -> Value {
    json!({
        "type": 1,
        "id": format!("folder-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": 0,
        "path": format!("{REMOTE_BASE}/{name}"),
    })
}

/// Names of the files a download task was created for.
async fn downloaded_names(env: &TestEnv) -> Vec<String> {
    for _ in 0..40 {
        if !env.tasks_of_type("download").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut names: Vec<String> = env
        .tasks_of_type("download")
        .into_iter()
        .filter_map(|t| t.local_path.rsplit('/').next().map(|s| s.to_string()))
        .collect();
    names.sort();
    names
}

/// Offset pagination: the server reports `total_items` larger than one page.
/// Every page must be walked, not just the first.
#[tokio::test]
async fn an_offset_paginated_directory_is_listed_to_the_end() {
    let env = TestEnv::with_max_file_size(0).await;
    env.server.reset().await;

    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_file("c.txt", 1, "etag-c")],
            json!({ "page": 1, "page_size": 2, "total_items": 3 }),
            2,
        )))
        .mount(&env.server)
        .await;

    // The first request carries no `page` parameter at all.
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![
                remote_file("a.txt", 1, "etag-a"),
                remote_file("b.txt", 1, "etag-b"),
            ],
            json!({ "page": 0, "page_size": 2, "total_items": 3 }),
            2,
        )))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    assert_eq!(
        downloaded_names(&env).await,
        vec!["a.txt", "b.txt", "c.txt"],
        "the file on the second page must be downloaded too"
    );
}

/// Cursor pagination: the server hands back a `next_token` instead of a total.
#[tokio::test]
async fn a_cursor_paginated_directory_is_listed_to_the_end() {
    let env = TestEnv::with_max_file_size(0).await;
    env.server.reset().await;

    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("next_page_token", "cursor-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_file("second.txt", 1, "etag-2")],
            json!({ "page": 0, "page_size": 1 }),
            1000,
        )))
        .mount(&env.server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_file("first.txt", 1, "etag-1")],
            json!({ "page": 0, "page_size": 1, "next_token": "cursor-2" }),
            1000,
        )))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    assert_eq!(
        downloaded_names(&env).await,
        vec!["first.txt", "second.txt"],
        "the file behind the cursor must be downloaded too"
    );
}

/// A subfolder only revealed on a later page must still be descended into,
/// otherwise a whole branch of the tree goes missing.
#[tokio::test]
async fn a_subfolder_found_on_a_later_page_is_still_traversed() {
    let env = TestEnv::with_max_file_size(0).await;
    env.server.reset().await;

    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("uri", format!("{REMOTE_BASE}/sub")))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![json!({
                "type": 0,
                "id": "file-inside",
                "name": "inside.txt",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
                "size": 1,
                "path": format!("{REMOTE_BASE}/sub/inside.txt"),
                "primary_entity": "etag-inside",
            })],
            json!({ "page": 0, "page_size": 10, "total_items": 1 }),
            10,
        )))
        .mount(&env.server)
        .await;

    // The folder is only listed on page 2 of the root directory.
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_folder("sub")],
            json!({ "page": 1, "page_size": 1, "total_items": 2 }),
            1,
        )))
        .mount(&env.server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_file("root.txt", 1, "etag-root")],
            json!({ "page": 0, "page_size": 1, "total_items": 2 }),
            1,
        )))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    assert_eq!(
        downloaded_names(&env).await,
        vec!["inside.txt", "root.txt"],
        "a folder discovered on a later page must still be walked"
    );
}

/// Once the server has advertised its own limit, the client must ask for that many
/// items per page instead of guessing — fewer round trips on a large drive.
#[tokio::test]
async fn the_page_size_advertised_by_the_server_is_used() {
    let env = TestEnv::with_max_file_size(0).await;
    env.server.reset().await;

    // The subdirectory is listed only if it is requested with the advertised size.
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("uri", format!("{REMOTE_BASE}/sub")))
        .and(query_param("page_size", "37"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![json!({
                "type": 0,
                "id": "file-inside",
                "name": "inside.txt",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
                "size": 1,
                "path": format!("{REMOTE_BASE}/sub/inside.txt"),
                "primary_entity": "etag-inside",
            })],
            json!({ "page": 0, "page_size": 37, "total_items": 1 }),
            37,
        )))
        .mount(&env.server)
        .await;

    // Scoped to the root: a request for `sub` with the wrong page size must fail
    // outright rather than fall through here and hand back `sub` again forever.
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .and(query_param("uri", REMOTE_BASE))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_folder("sub")],
            json!({ "page": 0, "page_size": 37, "total_items": 1 }),
            37,
        )))
        .mount(&env.server)
        .await;

    env.full_sync().await.expect("full sync");

    assert_eq!(
        downloaded_names(&env).await,
        vec!["inside.txt"],
        "the second directory must be requested with the server's max_page_size"
    );
}

/// A server that keeps claiming there is more without ever advancing must not
/// hang the sync forever.
#[tokio::test]
async fn listing_gives_up_on_a_server_that_never_advances() {
    let env = TestEnv::with_max_file_size(0).await;
    env.server.reset().await;

    // Always the same page, always "there is more".
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![remote_file("stuck.txt", 1, "etag-stuck")],
            json!({ "page": 0, "page_size": 1, "total_items": 9_999_999 }),
            1,
        )))
        .mount(&env.server)
        .await;

    let result = tokio::time::timeout(Duration::from_secs(30), env.full_sync()).await;

    let result = result.expect("listing must not loop forever on a stuck server");
    assert!(
        result.is_err(),
        "a server that never advances should surface an error, not pretend success"
    );
}
