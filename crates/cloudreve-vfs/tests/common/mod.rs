//! Shared test harness: an authenticated `cloudreve_api::Client` talking to a
//! wiremock server standing in for the Cloudreve API, plus a range-aware
//! download endpoint every read-path test in this crate relies on.
//!
//! Each integration test file compiles this module independently and only
//! uses part of the harness, so dead_code warnings are suppressed.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cloudreve_api::api::ExplorerApi;
use cloudreve_api::models::explorer::FileURLService;
use cloudreve_api::models::user::Token;
use cloudreve_api::{Client, ClientConfig};
use serde_json::{json, Value};
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

pub const REMOTE_BASE: &str = "cloudreve://my/sync";

/// Full `cloudreve://` uri of a root-level file/dir under [`REMOTE_BASE`].
pub fn uri_of(name: &str) -> String {
    format!("{REMOTE_BASE}/{name}")
}

/// Files to serve for a listing, keyed by the full remote directory uri
/// (e.g. `REMOTE_BASE` for the root, `"{REMOTE_BASE}/photos"` for a subdir).
type FilesState = Arc<Mutex<HashMap<String, Vec<Value>>>>;
type ContentStore = Arc<Mutex<HashMap<String, Vec<u8>>>>;
type RequestLog = Arc<Mutex<HashMap<String, Vec<Option<String>>>>>;
type BytesServedLog = Arc<Mutex<HashMap<String, u64>>>;

pub struct VfsTestEnv {
    pub server: MockServer,
    client: Arc<Client>,
    cache_dir: TempDir,
    files: FilesState,
    contents: ContentStore,
    requests: RequestLog,
    bytes_served: BytesServedLog,
    download_request_count: Arc<AtomicUsize>,
    list_requests: Arc<AtomicUsize>,
}

impl VfsTestEnv {
    pub async fn new() -> Self {
        let server = MockServer::start().await;
        let cache_dir = TempDir::new().expect("create cache dir");

        let client_config = ClientConfig::new(server.uri()).with_client_id("test-client");
        let client = Client::new(client_config);
        client
            .load_tokens(&Token {
                access_token: "test-access-token".to_string(),
                refresh_token: "test-refresh-token".to_string(),
                access_expires: "2099-01-01T00:00:00Z".to_string(),
                refresh_expires: "2099-01-01T00:00:00Z".to_string(),
            })
            .await;
        let client = Arc::new(client);

        let files: FilesState = Arc::new(Mutex::new(HashMap::new()));
        let contents: ContentStore = Arc::new(Mutex::new(HashMap::new()));
        let requests: RequestLog = Arc::new(Mutex::new(HashMap::new()));
        let bytes_served: BytesServedLog = Arc::new(Mutex::new(HashMap::new()));
        let download_request_count = Arc::new(AtomicUsize::new(0));
        let list_requests = Arc::new(AtomicUsize::new(0));

        // Listing endpoint: path-aware, keyed by the `uri` query param exactly
        // as `ExplorerApiExt::list_files_all` sends it, so a directory only
        // ever sees the files registered for *it* — never a sibling's list.
        // Reflects whatever `set_remote_files`/`set_remote_files_at` last
        // stored for that uri, so a single mount survives repeated calls
        // without needing `server.reset()`.
        {
            let files = files.clone();
            let list_requests = list_requests.clone();
            Mock::given(method("GET"))
                .and(path("/api/v4/file"))
                .respond_with(move |req: &Request| {
                    list_requests.fetch_add(1, Ordering::SeqCst);
                    let uri = req
                        .url
                        .query_pairs()
                        .find(|(k, _)| k == "uri")
                        .map(|(_, v)| v.into_owned())
                        .unwrap_or_default();
                    let dir_files = files.lock().unwrap().get(&uri).cloned().unwrap_or_default();
                    ResponseTemplate::new(200).set_body_json(json!({
                        "code": 0,
                        "msg": "",
                        "data": {
                            "files": dir_files,
                            "pagination": { "page": 1, "page_size": 500, "total_items": 0 },
                            "props": {
                                "max_page_size": 10000,
                                "order_by_options": ["name"],
                                "order_direction_options": ["asc"],
                            },
                        },
                    }))
                })
                .mount(&server)
                .await;
        }

        // Download-URL endpoint: mirrors `POST /api/v4/file/url`. Reads the
        // requested uri out of the request body and points the returned URL
        // back at this same mock server, keyed by file name.
        {
            let base = server.uri();
            Mock::given(method("POST"))
                .and(path("/api/v4/file/url"))
                .respond_with(move |req: &Request| {
                    let request: FileURLService =
                        req.body_json().expect("decode FileURLService request body");
                    let uri = request.uris.first().cloned().unwrap_or_default();
                    let name = uri.rsplit('/').next().unwrap_or_default();
                    let download_url = format!("{base}/vfs-download/{name}");
                    ResponseTemplate::new(200).set_body_json(json!({
                        "code": 0,
                        "msg": "",
                        "data": {
                            "urls": [{ "url": download_url }],
                            "expires": "2099-01-01T00:00:00Z",
                        },
                    }))
                })
                .mount(&server)
                .await;
        }

        // Download content endpoint: honors `Range: bytes=a-b` with a 206 and
        // the exact slice, and records every hit so tests can assert on the
        // exact ranges a caller requested.
        {
            let contents = contents.clone();
            let requests = requests.clone();
            let bytes_served = bytes_served.clone();
            let download_request_count = download_request_count.clone();
            Mock::given(method("GET"))
                .and(path_regex(r"^/vfs-download/.+$"))
                .respond_with(move |req: &Request| {
                    download_request_count.fetch_add(1, Ordering::SeqCst);
                    let name = req
                        .url
                        .path()
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    let range_header = req
                        .headers
                        .get("Range")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    requests
                        .lock()
                        .unwrap()
                        .entry(name.clone())
                        .or_default()
                        .push(range_header.clone());

                    let Some(content) = contents.lock().unwrap().get(&name).cloned() else {
                        return ResponseTemplate::new(404);
                    };
                    let len = content.len();

                    if let Some((start, end)) =
                        range_header.as_deref().and_then(|h| parse_range(h, len))
                    {
                        let body = content[start..=end].to_vec();
                        *bytes_served.lock().unwrap().entry(name.clone()).or_default() +=
                            body.len() as u64;
                        return ResponseTemplate::new(206)
                            .insert_header("Accept-Ranges", "bytes")
                            .insert_header("Content-Range", format!("bytes {start}-{end}/{len}"))
                            .set_body_bytes(body);
                    }

                    // A Range header that fails to parse as satisfiable (e.g.
                    // start past the end of the file) gets a real 416, exactly
                    // like the production server — tests exercising EOF must
                    // see the same response shape a real out-of-range GET
                    // would produce, not a silent full-body fallback.
                    if range_header.is_some() {
                        return ResponseTemplate::new(416)
                            .insert_header("Content-Range", format!("bytes */{len}"));
                    }

                    *bytes_served.lock().unwrap().entry(name.clone()).or_default() +=
                        content.len() as u64;
                    ResponseTemplate::new(200)
                        .insert_header("Accept-Ranges", "bytes")
                        .set_body_bytes(content)
                })
                .mount(&server)
                .await;
        }

        Self {
            server,
            client,
            cache_dir,
            files,
            contents,
            requests,
            bytes_served,
            download_request_count,
            list_requests,
        }
    }

    /// Configure the mock listing endpoint to return these files for the root
    /// directory ([`REMOTE_BASE`]).
    pub async fn set_remote_files(&self, files: Vec<Value>) {
        self.files
            .lock()
            .unwrap()
            .insert(REMOTE_BASE.to_string(), files);
    }

    /// Configure the mock listing endpoint to return these files for
    /// `"{REMOTE_BASE}/{subdir}"`, independently of what the root (or any
    /// other directory) returns.
    pub async fn set_remote_files_at(&self, subdir: &str, files: Vec<Value>) {
        self.files
            .lock()
            .unwrap()
            .insert(format!("{REMOTE_BASE}/{subdir}"), files);
    }

    /// Total number of `GET /api/v4/file` (listing) requests served so far.
    pub fn list_request_count(&self) -> usize {
        self.list_requests.load(Ordering::SeqCst)
    }

    /// Register file content behind the mocked download flow: both the
    /// `/api/v4/file/url` lookup and the download endpoint itself serve `name`.
    pub async fn serve_file_content(&self, name: &str, content: &[u8]) {
        self.contents
            .lock()
            .unwrap()
            .insert(name.to_string(), content.to_vec());
    }

    /// The `Range` header of every recorded hit against `name`'s download
    /// endpoint, in request order (`None` when a request carried no `Range`).
    pub fn download_requests(&self, name: &str) -> Vec<Option<String>> {
        self.requests
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or_default()
    }

    pub fn client(&self) -> Arc<Client> {
        self.client.clone()
    }

    pub fn cache_dir(&self) -> &Path {
        self.cache_dir.path()
    }

    /// Total bytes actually returned in response bodies by `name`'s download
    /// endpoint across every request so far (ranged or whole-body).
    pub fn total_bytes_served(&self, name: &str) -> u64 {
        self.bytes_served.lock().unwrap().get(name).copied().unwrap_or(0)
    }

    /// Blocks until no new download request has landed for 200ms, so a test
    /// can observe the fire-and-forget readahead spawned by a read without
    /// racing it. Panics if downloads are still arriving after 5s — readahead
    /// that never quiesces is a bug, not something worth waiting out forever.
    pub async fn wait_for_downloads_to_settle(&self) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut last_count = self.download_request_count.load(Ordering::SeqCst);
        let mut last_change = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let count = self.download_request_count.load(Ordering::SeqCst);
            let now = tokio::time::Instant::now();
            if count != last_count {
                last_count = count;
                last_change = now;
            }
            if now.duration_since(last_change) >= std::time::Duration::from_millis(200) {
                return;
            }
            assert!(now < deadline, "downloads never settled (still arriving after 5s)");
        }
    }
}

/// Build the JSON for a remote file as the Cloudreve list API would return it.
pub fn remote_file(name: &str, size: i64, etag: &str) -> Value {
    json!({
        "type": 0,
        "id": format!("file-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": size,
        "path": format!("{REMOTE_BASE}/{name}"),
        "primary_entity": etag,
    })
}

/// Build the JSON for a remote directory as the Cloudreve list API would
/// return it.
pub fn remote_dir(name: &str) -> Value {
    json!({
        "type": 1,
        "id": format!("dir-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": 0,
        "path": format!("{REMOTE_BASE}/{name}"),
    })
}

/// Resolve the download URL for `name` exactly the way
/// `crates/cloudreve-sync/src/tasks/download.rs` does: request a signed URL
/// for the file's uri, then rewrite its origin to this client's base URL.
pub async fn fetch_download_url(env: &VfsTestEnv, name: &str) -> String {
    let request = FileURLService {
        uris: vec![uri_of(name)],
        ..Default::default()
    };
    let res = env
        .client()
        .get_file_url(&request)
        .await
        .expect("get_file_url");
    let raw = res
        .urls
        .first()
        .expect("no download url in response")
        .url
        .clone();
    env.client().rewrite_url_origin(&raw)
}

/// Parse a single-range `Range: bytes=a-b` header into an inclusive
/// `(start, end)` byte range, clamped to `content_len`. Returns `None` for
/// anything this harness does not need to support (missing range, multi-range,
/// malformed, out of bounds).
fn parse_range(header: &str, content_len: usize) -> Option<(usize, usize)> {
    let spec = header.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (start_str, end_str) = spec.split_once('-')?;
    let start: usize = start_str.parse().ok()?;
    let end: usize = if end_str.is_empty() {
        content_len.saturating_sub(1)
    } else {
        end_str.parse().ok()?
    };
    if start > end || end >= content_len {
        return None;
    }
    Some((start, end))
}
