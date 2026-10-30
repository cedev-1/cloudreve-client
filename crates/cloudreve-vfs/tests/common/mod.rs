//! Shared test harness: an authenticated `cloudreve_api::Client` talking to a
//! wiremock server standing in for the Cloudreve API, plus a range-aware
//! download endpoint every read-path test in this crate relies on.
//!
//! Each integration test file compiles this module independently and only
//! uses part of the harness, so dead_code warnings are suppressed.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cloudreve_api::api::ExplorerApi;
use cloudreve_api::models::explorer::{
    CreateFileService, DeleteFileService, FileURLService, MoveFileService, RenameFileService,
    UploadSessionRequest,
};
use cloudreve_api::models::user::Token;
use cloudreve_api::{Client, ClientConfig};
use serde_json::{json, Value};
use tempfile::TempDir;
use uuid::Uuid;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Chunk size the mocked upload-session endpoint hands back. Deliberately
/// small so even a short smoke-test file spans multiple chunks, exercising
/// index-keyed reassembly rather than a single-shot upload.
pub const UPLOAD_CHUNK_SIZE: i64 = 5;

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
/// `(uris, dst)` recorded from the most recent mocked `move_files` call.
type LastMove = Arc<Mutex<Option<(Vec<String>, String)>>>;
type BytesServedLog = Arc<Mutex<HashMap<String, u64>>>;
/// Bytes received per chunk index, keyed by upload session id. A `BTreeMap`
/// keeps chunks in index order regardless of the (possibly concurrent)
/// order the POSTs actually arrive in.
type UploadChunksStore = Arc<Mutex<HashMap<String, BTreeMap<usize, Vec<u8>>>>>;
/// Remote file name -> most recent upload session id created for it, so
/// `uploaded_content` can find the right session without the caller
/// needing to track session ids itself.
type LatestSessionForName = Arc<Mutex<HashMap<String, String>>>;
/// Per-name call counter for `/api/v4/file/url`, doubling as the version
/// number stamped onto the URL it hands back (`?v={n}`) — a real Cloudreve
/// server issues a genuinely different signed URL on every call, and a
/// mock that always returns the identical string can hide bugs where code
/// under test never actually re-fetches a URL it claims to have refreshed.
type FileUrlCalls = Arc<Mutex<HashMap<String, u32>>>;
/// Download hits against `name`'s content endpoint, broken out by which
/// `?v=` the request carried — lets a test tell "the original url" and
/// "the refreshed url" apart even though both are served by the same mock.
type DownloadHitsByVersion = Arc<Mutex<HashMap<(String, u32), u32>>>;
type DownloadDelays = Arc<Mutex<HashMap<String, Duration>>>;
/// Full remote uris a successful `overwrite=false` (or otherwise confirmed
/// to exist) upload session has ever been created for — lets the session-
/// creation mock simulate the real server's `ObjectExisted` (40004) refusal
/// when a SECOND `overwrite=false` session targets the same uri (see
/// `expect_uploads`'s doc). Deliberately separate from `files`/`FilesState`:
/// this only needs to answer "has an upload session for this exact uri ever
/// succeeded", not model a whole directory listing.
type ExistingUploadUris = Arc<Mutex<std::collections::HashSet<String>>>;

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
    upload_chunks: UploadChunksStore,
    latest_session_for_name: LatestSessionForName,
    existing_upload_uris: ExistingUploadUris,
    upload_session_count: Arc<AtomicUsize>,
    upload_session_failures_remaining: Arc<AtomicUsize>,
    file_url_calls: FileUrlCalls,
    file_url_failures_remaining: Arc<AtomicUsize>,
    download_hits_by_version: DownloadHitsByVersion,
    download_delays: DownloadDelays,
    /// Expected chunk count per session id (derived from the size the
    /// session-creation request declared), so the chunk-upload mock can
    /// tell "this session's last chunk just arrived" without any explicit
    /// completion/callback call to hook (the local policy has none — see
    /// `expect_uploads`'s doc).
    session_expected_chunks: Arc<Mutex<HashMap<String, usize>>>,
    /// How many upload sessions are between "created" and "received their
    /// last expected chunk" right now — review finding 3's signal for
    /// `uploads_drain_one_at_a_time`: the write-back queue is supposed to
    /// drain sequentially, so this must never exceed 1.
    concurrent_uploads: Arc<AtomicUsize>,
    max_concurrent_uploads: Arc<AtomicUsize>,
    /// Artificial per-chunk delay applied by the chunk-upload mock, so a
    /// concurrency test has a wide enough window to actually observe two
    /// sessions overlapping if the write-back queue ever failed to
    /// serialize them (real network calls are fast enough that a broken
    /// gate could otherwise get lucky and still look sequential).
    chunk_upload_delay: Arc<Mutex<Duration>>,
    /// Task 10 namespace-op call counters and last-call recordings — see
    /// `expect_namespace_ops`.
    create_file_calls: Arc<AtomicUsize>,
    delete_calls: Arc<AtomicUsize>,
    last_deleted_uris: Arc<Mutex<Vec<String>>>,
    rename_calls: Arc<AtomicUsize>,
    last_rename: Arc<Mutex<Option<(String, String)>>>,
    move_calls: Arc<AtomicUsize>,
    last_move: LastMove,
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
        let upload_chunks: UploadChunksStore = Arc::new(Mutex::new(HashMap::new()));
        let latest_session_for_name: LatestSessionForName = Arc::new(Mutex::new(HashMap::new()));
        let existing_upload_uris: ExistingUploadUris =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        let upload_session_count = Arc::new(AtomicUsize::new(0));
        let upload_session_failures_remaining = Arc::new(AtomicUsize::new(0));
        let file_url_calls: FileUrlCalls = Arc::new(Mutex::new(HashMap::new()));
        let file_url_failures_remaining = Arc::new(AtomicUsize::new(0));
        let download_hits_by_version: DownloadHitsByVersion = Arc::new(Mutex::new(HashMap::new()));
        let download_delays: DownloadDelays = Arc::new(Mutex::new(HashMap::new()));
        let session_expected_chunks: Arc<Mutex<HashMap<String, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let concurrent_uploads = Arc::new(AtomicUsize::new(0));
        let max_concurrent_uploads = Arc::new(AtomicUsize::new(0));
        let chunk_upload_delay = Arc::new(Mutex::new(Duration::ZERO));
        let create_file_calls = Arc::new(AtomicUsize::new(0));
        let delete_calls = Arc::new(AtomicUsize::new(0));
        let last_deleted_uris = Arc::new(Mutex::new(Vec::new()));
        let rename_calls = Arc::new(AtomicUsize::new(0));
        let last_rename = Arc::new(Mutex::new(None));
        let move_calls = Arc::new(AtomicUsize::new(0));
        let last_move = Arc::new(Mutex::new(None));

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
        // back at this same mock server, keyed by file name — and, unlike a
        // naive mock, a genuinely different URL every call (`?v={n}`,
        // per-name): a real Cloudreve server never reissues the identical
        // signed URL twice, and code that only "refreshes" a URL in name
        // (without a test able to tell) can hide real bugs (see the ledger
        // debt test in write_back.rs for exactly this).
        {
            let base = server.uri();
            let file_url_calls = file_url_calls.clone();
            let file_url_failures_remaining = file_url_failures_remaining.clone();
            Mock::given(method("POST"))
                .and(path("/api/v4/file/url"))
                .respond_with(move |req: &Request| {
                    // Atomically claim one scheduled failure, if any, before
                    // touching any other state — mirrors
                    // `expect_uploads`'s session-creation failure injection.
                    let consumed_failure = file_url_failures_remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    if consumed_failure {
                        return ResponseTemplate::new(500).set_body_json(json!({
                            "code": 500,
                            "msg": "mock: injected file/url failure",
                        }));
                    }

                    let request: FileURLService =
                        req.body_json().expect("decode FileURLService request body");
                    let uri = request.uris.first().cloned().unwrap_or_default();
                    let name = uri.rsplit('/').next().unwrap_or_default().to_string();
                    let version = {
                        let mut calls = file_url_calls.lock().unwrap();
                        let entry = calls.entry(name.clone()).or_insert(0);
                        *entry += 1;
                        *entry
                    };
                    let download_url = format!("{base}/vfs-download/{name}?v={version}");
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
        // the exact slice, and records every hit (overall, and split out by
        // `?v=` — see `DownloadHitsByVersion`) so tests can assert on the
        // exact ranges/urls a caller requested. An artificial per-name delay
        // (`slow_down_downloads`) can be layered on to widen a race window
        // for concurrency tests.
        {
            let contents = contents.clone();
            let requests = requests.clone();
            let bytes_served = bytes_served.clone();
            let download_request_count = download_request_count.clone();
            let download_hits_by_version = download_hits_by_version.clone();
            let download_delays = download_delays.clone();
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
                    let version = extract_version(req);
                    *download_hits_by_version
                        .lock()
                        .unwrap()
                        .entry((name.clone(), version))
                        .or_default() += 1;
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

                    let delay = download_delays.lock().unwrap().get(&name).copied();
                    let with_delay = |resp: ResponseTemplate| match delay {
                        Some(d) => resp.set_delay(d),
                        None => resp,
                    };

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
                        return with_delay(
                            ResponseTemplate::new(206)
                                .insert_header("Accept-Ranges", "bytes")
                                .insert_header("Content-Range", format!("bytes {start}-{end}/{len}"))
                                .set_body_bytes(body),
                        );
                    }

                    // A Range header that fails to parse as satisfiable (e.g.
                    // start past the end of the file) gets a real 416, exactly
                    // like the production server — tests exercising EOF must
                    // see the same response shape a real out-of-range GET
                    // would produce, not a silent full-body fallback.
                    if range_header.is_some() {
                        return with_delay(
                            ResponseTemplate::new(416)
                                .insert_header("Content-Range", format!("bytes */{len}")),
                        );
                    }

                    *bytes_served.lock().unwrap().entry(name.clone()).or_default() +=
                        content.len() as u64;
                    with_delay(
                        ResponseTemplate::new(200)
                            .insert_header("Accept-Ranges", "bytes")
                            .set_body_bytes(content),
                    )
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
            upload_chunks,
            latest_session_for_name,
            existing_upload_uris,
            upload_session_count,
            upload_session_failures_remaining,
            file_url_calls,
            file_url_failures_remaining,
            download_hits_by_version,
            download_delays,
            session_expected_chunks,
            concurrent_uploads,
            max_concurrent_uploads,
            chunk_upload_delay,
            create_file_calls,
            delete_calls,
            last_deleted_uris,
            rename_calls,
            last_rename,
            move_calls,
            last_move,
        }
    }

    /// Mounts mocks for the namespace-mutating endpoints Task 10's `Vfs`
    /// facade drives: `POST /file/create`, `DELETE /file`, `POST
    /// /file/rename`, `POST /file/move`. Opt-in (like `expect_uploads`)
    /// rather than always mounted in `new()`, since most tests in this crate
    /// never touch these ops.
    ///
    /// Each mock emulates the real server's actual effect on the directory
    /// listing it stands in for (`files`), not just a canned response body —
    /// mirroring how `set_remote_files`'s siblings behave for the download/
    /// listing endpoints — so a test can prove a namespace op "really
    /// happened" via a plain `readdir`/`lookup` afterwards, exactly as it
    /// would against the real server, instead of only trusting a call
    /// counter.
    pub async fn expect_namespace_ops(&self) {
        // POST /file/create: adds the created file/folder to its parent's
        // listing and echoes it back as the `FileResponse` the real
        // endpoint returns.
        {
            let files = self.files.clone();
            let create_file_calls = self.create_file_calls.clone();
            Mock::given(method("POST"))
                .and(path("/api/v4/file/create"))
                .respond_with(move |req: &Request| {
                    create_file_calls.fetch_add(1, Ordering::SeqCst);
                    let request: CreateFileService =
                        req.body_json().expect("decode CreateFileService body");
                    let (parent_uri, name) =
                        request.uri.rsplit_once('/').unwrap_or(("", request.uri.as_str()));
                    let mut entry = if request.file_type == "folder" {
                        remote_dir(name)
                    } else {
                        remote_file(name, 0, "")
                    };
                    // The helpers above assume a root-level path; overwrite
                    // with the real requested uri so a nested parent is
                    // reflected correctly too.
                    entry["path"] = json!(request.uri.clone());
                    files
                        .lock()
                        .unwrap()
                        .entry(parent_uri.to_string())
                        .or_default()
                        .push(entry.clone());
                    ResponseTemplate::new(200)
                        .set_body_json(json!({ "code": 0, "msg": "", "data": entry }))
                })
                .mount(&self.server)
                .await;
        }

        // DELETE /file: removes every matching entry (by exact `path`) from
        // whichever directory listing currently holds it.
        {
            let files = self.files.clone();
            let delete_calls = self.delete_calls.clone();
            let last_deleted_uris = self.last_deleted_uris.clone();
            Mock::given(method("DELETE"))
                .and(path("/api/v4/file"))
                .respond_with(move |req: &Request| {
                    delete_calls.fetch_add(1, Ordering::SeqCst);
                    let request: DeleteFileService =
                        req.body_json().expect("decode DeleteFileService body");
                    *last_deleted_uris.lock().unwrap() = request.uris.clone();
                    let mut files = files.lock().unwrap();
                    for uri in &request.uris {
                        remove_entry_by_path(&mut files, uri);
                    }
                    ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "" }))
                })
                .mount(&self.server)
                .await;
        }

        // POST /file/rename: renames the matching entry in place (same
        // parent directory), updating both its `name` and `path`.
        {
            let files = self.files.clone();
            let rename_calls = self.rename_calls.clone();
            let last_rename = self.last_rename.clone();
            Mock::given(method("POST"))
                .and(path("/api/v4/file/rename"))
                .respond_with(move |req: &Request| {
                    rename_calls.fetch_add(1, Ordering::SeqCst);
                    let request: RenameFileService =
                        req.body_json().expect("decode RenameFileService body");
                    *last_rename.lock().unwrap() =
                        Some((request.uri.clone(), request.new_name.clone()));
                    let mut files = files.lock().unwrap();
                    let (parent_uri, _old_name) =
                        request.uri.rsplit_once('/').unwrap_or(("", request.uri.as_str()));
                    let new_path = format!("{parent_uri}/{}", request.new_name);
                    // Fix round 1 (C1): the REAL Cloudreve server refuses a
                    // rename onto an existing sibling name with
                    // `ErrFileExisted`/`ObjectExisted` (40004) —
                    // `dbfs/manage.go`'s `Rename` returns it on the
                    // sibling-name uniqueness constraint violation. An
                    // earlier version of this mock made this an overwrite
                    // instead, which is NOT what the real server does; the
                    // facade (`Vfs::rename`) is what bridges POSIX/NFSv3's
                    // replace-on-rename expectation, not this mock. No
                    // state is mutated on refusal — same idiom
                    // `expect_uploads`'s `ObjectExisted` simulation already
                    // uses for the identical code.
                    if path_exists_in_files(&files, &new_path) {
                        return ResponseTemplate::new(200).set_body_json(json!({
                            "code": 40004,
                            "msg": "mock: object already exists (rename destination)",
                        }));
                    }
                    let mut entry = remove_entry_by_path(&mut files, &request.uri)
                        .expect("mock: rename_file on an unknown uri");
                    entry["name"] = json!(request.new_name.clone());
                    entry["path"] = json!(new_path.clone());
                    // A renamed directory takes its own nested listing (if
                    // any) along with it, keyed by its full path.
                    if let Some(children) = files.remove(&request.uri) {
                        files.insert(new_path.clone(), children);
                    }
                    files.entry(parent_uri.to_string()).or_default().push(entry.clone());
                    ResponseTemplate::new(200)
                        .set_body_json(json!({ "code": 0, "msg": "", "data": entry }))
                })
                .mount(&self.server)
                .await;
        }

        // POST /file/move: relocates each matching entry into `dst`'s
        // listing, keeping its name.
        {
            let files = self.files.clone();
            let move_calls = self.move_calls.clone();
            let last_move = self.last_move.clone();
            Mock::given(method("POST"))
                .and(path("/api/v4/file/move"))
                .respond_with(move |req: &Request| {
                    move_calls.fetch_add(1, Ordering::SeqCst);
                    let request: MoveFileService =
                        req.body_json().expect("decode MoveFileService body");
                    *last_move.lock().unwrap() = Some((request.uris.clone(), request.dst.clone()));
                    let mut files = files.lock().unwrap();
                    // Fix round 1 (C1): same real-server refusal as
                    // `rename_file` above — `SetParent` (the move path)
                    // returns the identical `ErrFileExisted` on a
                    // destination-name collision. Checked for every source
                    // uri BEFORE mutating anything, so a batch that would
                    // partially collide fails atomically rather than
                    // relocating some entries and refusing others.
                    for uri in &request.uris {
                        let name = uri.rsplit('/').next().unwrap_or_default();
                        let new_path = format!("{}/{name}", request.dst);
                        if path_exists_in_files(&files, &new_path) {
                            return ResponseTemplate::new(200).set_body_json(json!({
                                "code": 40004,
                                "msg": "mock: object already exists (move destination)",
                            }));
                        }
                    }
                    for uri in &request.uris {
                        let Some(mut entry) = remove_entry_by_path(&mut files, uri) else {
                            continue;
                        };
                        let name =
                            entry.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
                        let new_path = format!("{}/{name}", request.dst);
                        entry["path"] = json!(new_path.clone());
                        if let Some(children) = files.remove(uri) {
                            files.insert(new_path.clone(), children);
                        }
                        files.entry(request.dst.clone()).or_default().push(entry);
                    }
                    ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "" }))
                })
                .mount(&self.server)
                .await;
        }
    }

    /// Total `POST /file/create` requests received so far.
    pub fn create_file_call_count(&self) -> usize {
        self.create_file_calls.load(Ordering::SeqCst)
    }

    /// Total `DELETE /file` requests received so far.
    pub fn delete_call_count(&self) -> usize {
        self.delete_calls.load(Ordering::SeqCst)
    }

    /// The `uris` carried by the most recent `DELETE /file` call.
    pub fn last_deleted_uris(&self) -> Vec<String> {
        self.last_deleted_uris.lock().unwrap().clone()
    }

    /// Total `POST /file/rename` requests received so far.
    pub fn rename_call_count(&self) -> usize {
        self.rename_calls.load(Ordering::SeqCst)
    }

    /// `(uri, new_name)` carried by the most recent `POST /file/rename` call.
    pub fn last_rename(&self) -> Option<(String, String)> {
        self.last_rename.lock().unwrap().clone()
    }

    /// Total `POST /file/move` requests received so far.
    pub fn move_call_count(&self) -> usize {
        self.move_calls.load(Ordering::SeqCst)
    }

    /// `(uris, dst)` carried by the most recent `POST /file/move` call.
    pub fn last_move(&self) -> Option<(Vec<String>, String)> {
        self.last_move.lock().unwrap().clone()
    }

    /// Mounts the upload endpoints the real `cloudreve-uploader` protocol
    /// uses for the "local" storage policy — the only policy this harness
    /// speaks, and the one every session gets here because the mocked
    /// session-create response never sets `storage_policy` (so
    /// `UploadSession::new` defaults to `PolicyType::Local`, see
    /// `crates/cloudreve-uploader/src/session.rs`):
    ///
    /// - `PUT /file/upload` creates a session, mirroring
    ///   `ExplorerApi::create_upload_session`.
    /// - `POST /file/upload/{session_id}/{chunk_index}` uploads one chunk,
    ///   mirroring `ExplorerApi::upload_chunk_stream` — the call
    ///   `providers::local::upload_chunk_local_generic` makes whenever the
    ///   session has no `upload_urls` (i.e. never a slave-relay session).
    ///
    /// The local policy completes uploads automatically server-side (see
    /// `providers::complete_upload`), so there is no completion/callback
    /// endpoint to mock.
    pub async fn expect_uploads(&self) {
        // Session creation.
        {
            let upload_chunks = self.upload_chunks.clone();
            let latest_session_for_name = self.latest_session_for_name.clone();
            let existing_upload_uris = self.existing_upload_uris.clone();
            let files = self.files.clone();
            let upload_session_count = self.upload_session_count.clone();
            let failures_remaining = self.upload_session_failures_remaining.clone();
            let session_expected_chunks = self.session_expected_chunks.clone();
            let concurrent_uploads = self.concurrent_uploads.clone();
            let max_concurrent_uploads = self.max_concurrent_uploads.clone();
            Mock::given(method("PUT"))
                .and(path("/api/v4/file/upload"))
                .respond_with(move |req: &Request| {
                    upload_session_count.fetch_add(1, Ordering::SeqCst);

                    // Atomically claim one scheduled failure, if any, before
                    // touching any state — a failed creation must not
                    // register a session or a name mapping.
                    let consumed_failure = failures_remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                            n.checked_sub(1)
                        })
                        .is_ok();
                    if consumed_failure {
                        return ResponseTemplate::new(500).set_body_json(json!({
                            "code": 500,
                            "msg": "mock: injected upload session failure",
                        }));
                    }

                    let request: UploadSessionRequest =
                        req.body_json().expect("decode UploadSessionRequest body");

                    // Real Cloudreve behavior this mock must mirror for
                    // cycle B (phase-2 debt burn-down) to be testable at
                    // all: `overwrite=false` (no `entity_type`) against a
                    // uri that already exists — either a file registered up
                    // front (`set_remote_files`/`add_remote_file`) or one an
                    // earlier session already created here — is refused
                    // with Cloudreve's own `ObjectExisted` code (40004), not
                    // silently accepted. No session is registered for a
                    // refused request.
                    let already_exists = path_exists_in_files(&files.lock().unwrap(), &request.uri)
                        || existing_upload_uris.lock().unwrap().contains(&request.uri);
                    if request.entity_type.is_none() && already_exists {
                        return ResponseTemplate::new(200).set_body_json(json!({
                            "code": 40004,
                            "msg": "mock: object already exists (overwrite=false)",
                        }));
                    }

                    let name = request
                        .uri
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    let session_id = format!("mock-upload-session-{}", Uuid::new_v4());

                    upload_chunks
                        .lock()
                        .unwrap()
                        .insert(session_id.clone(), BTreeMap::new());
                    latest_session_for_name
                        .lock()
                        .unwrap()
                        .insert(name, session_id.clone());
                    existing_upload_uris.lock().unwrap().insert(request.uri.clone());

                    // A session is "in flight" from creation until its last
                    // expected chunk arrives (see the chunk-upload mock
                    // below) — review finding 3's concurrency signal.
                    let expected_chunks =
                        ((request.size.max(1) + UPLOAD_CHUNK_SIZE - 1) / UPLOAD_CHUNK_SIZE).max(1)
                            as usize;
                    session_expected_chunks
                        .lock()
                        .unwrap()
                        .insert(session_id.clone(), expected_chunks);
                    let now_in_flight = concurrent_uploads.fetch_add(1, Ordering::SeqCst) + 1;
                    max_concurrent_uploads.fetch_max(now_in_flight, Ordering::SeqCst);

                    ResponseTemplate::new(200).set_body_json(json!({
                        "code": 0,
                        "msg": "",
                        "data": {
                            "session_id": session_id,
                            "expires": 9_999_999_999i64,
                            "chunk_size": UPLOAD_CHUNK_SIZE,
                            "callback_secret": "",
                            "uri": request.uri,
                        },
                    }))
                })
                .mount(&self.server)
                .await;
        }

        // Chunk upload: `POST /file/upload/{session_id}/{chunk_index}`.
        // Recorded by the index in the URL (not arrival order), exactly
        // like the real server keys chunks — so reassembly is correct even
        // if a storage policy with concurrency > 1 uploads out of order.
        {
            let upload_chunks = self.upload_chunks.clone();
            let session_expected_chunks = self.session_expected_chunks.clone();
            let concurrent_uploads = self.concurrent_uploads.clone();
            let chunk_upload_delay = self.chunk_upload_delay.clone();
            Mock::given(method("POST"))
                .and(path_regex(r"^/api/v4/file/upload/[^/]+/[0-9]+$"))
                .respond_with(move |req: &Request| {
                    let mut segments = req.url.path().rsplit('/');
                    let chunk_index: usize = segments
                        .next()
                        .unwrap_or_default()
                        .parse()
                        .expect("chunk index segment must be numeric");
                    let session_id = segments.next().unwrap_or_default().to_string();

                    let received_count = {
                        let mut chunks = upload_chunks.lock().unwrap();
                        let entry = chunks.entry(session_id.clone()).or_default();
                        entry.insert(chunk_index, req.body.clone());
                        entry.len()
                    };

                    // This session's last expected chunk just landed: it's
                    // no longer "in flight" (see the session-creation mock
                    // above). No explicit completion call to hook for the
                    // local policy, so chunk-count-reached is the honest
                    // stand-in signal.
                    let expected = session_expected_chunks
                        .lock()
                        .unwrap()
                        .get(&session_id)
                        .copied();
                    if expected == Some(received_count) {
                        concurrent_uploads.fetch_sub(1, Ordering::SeqCst);
                    }

                    let delay = *chunk_upload_delay.lock().unwrap();
                    let resp = ResponseTemplate::new(200).set_body_json(json!({ "code": 0, "msg": "" }));
                    if delay > Duration::ZERO { resp.set_delay(delay) } else { resp }
                })
                .mount(&self.server)
                .await;
        }
    }

    /// The highest number of upload sessions ever simultaneously "in
    /// flight" (created but not yet having received their last expected
    /// chunk) since `expect_uploads` was mounted. Review finding 3's
    /// pinning signal: the write-back queue drains sequentially, so this
    /// must never exceed 1 across a whole test.
    pub fn max_concurrent_uploads(&self) -> usize {
        self.max_concurrent_uploads.load(Ordering::SeqCst)
    }

    /// Adds an artificial delay to every future chunk-upload response,
    /// widening the window in which two sessions running concurrently
    /// (a broken write-back queue) would actually overlap and be caught by
    /// `max_concurrent_uploads` — real loopback HTTP calls are otherwise
    /// fast enough that even a broken gate could get lucky.
    pub fn slow_down_chunk_uploads(&self, delay: Duration) {
        *self.chunk_upload_delay.lock().unwrap() = delay;
    }

    /// Bytes the mock actually received for `remote_name`, reassembled in
    /// ascending chunk-index order from the most recent upload session
    /// created for it. `None` if no session for that name ever received a
    /// chunk.
    pub fn uploaded_content(&self, remote_name: &str) -> Option<Vec<u8>> {
        let session_id = self
            .latest_session_for_name
            .lock()
            .unwrap()
            .get(remote_name)
            .cloned()?;
        let chunks = self.upload_chunks.lock().unwrap();
        let chunk_map = chunks.get(&session_id)?;
        if chunk_map.is_empty() {
            return None;
        }
        let mut content = Vec::new();
        for bytes in chunk_map.values() {
            content.extend_from_slice(bytes);
        }
        Some(content)
    }

    /// Total number of `PUT /file/upload` (session-creation) requests
    /// received so far — successes and injected failures alike.
    pub fn upload_session_count(&self) -> usize {
        self.upload_session_count.load(Ordering::SeqCst)
    }

    /// Makes the next `n` upload-session-creation requests answer with a
    /// server error instead of a credential, so tests can exercise the
    /// uploader's session-creation failure path.
    pub fn fail_next_upload_sessions(&self, n: usize) {
        self.upload_session_failures_remaining
            .fetch_add(n, Ordering::SeqCst);
    }

    /// Makes the next `n` `POST /api/v4/file/url` (signed download-URL)
    /// requests answer with a server error instead of a URL — simulates
    /// being offline for exactly the fetch `Vfs::open` performs, so tests
    /// can exercise `open`'s failure path (e.g. a reopen attempted while
    /// offline).
    pub fn fail_next_file_url_requests(&self, n: usize) {
        self.file_url_failures_remaining.fetch_add(n, Ordering::SeqCst);
    }

    /// Mutates the etag the mocked listing endpoint reports for `name`,
    /// wherever it currently appears across every directory registered so
    /// far. Used by conflict-detection tests that need the "remote" file to
    /// look like it changed concurrently with a local upload.
    pub async fn set_remote_etag(&self, name: &str, etag: &str) {
        let mut files = self.files.lock().unwrap();
        for dir_files in files.values_mut() {
            for file in dir_files.iter_mut() {
                if file.get("name").and_then(Value::as_str) == Some(name) {
                    file["primary_entity"] = json!(etag);
                }
            }
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

    /// Adds one more file to the root listing (accumulating, unlike
    /// `set_remote_files` which replaces the whole listing) and registers its
    /// content for download in the same call — the common case of building up
    /// a multi-file fixture one file at a time.
    pub async fn add_remote_file(&self, name: &str, content: Vec<u8>, etag: &str) {
        {
            let mut files = self.files.lock().unwrap();
            files
                .entry(REMOTE_BASE.to_string())
                .or_default()
                .push(remote_file(name, content.len() as i64, etag));
        }
        self.serve_file_content(name, &content).await;
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

    /// Makes `name`'s download endpoint answer `status` (e.g. 500) for the
    /// first `times` requests against it, then fall through to the normal
    /// range-aware responder mounted by `new()` — used to exercise the
    /// ranged-fetch retry-with-backoff path (spec §7) without a second,
    /// disconnected request log: every hit here is recorded through the
    /// same `requests`/`download_request_count` state as a real download
    /// hit, so `download_requests(name).len()` counts both the injected
    /// failures and the eventual real attempt(s).
    ///
    /// Mounted at a higher priority (lower number) than the default-
    /// priority responder from `new()` so it is checked first regardless
    /// of mount order; wiremock stops matching it once `times` requests
    /// have been served, letting the normal responder take back over.
    pub async fn fail_downloads_n_times(&self, name: &str, times: u64, status: u16) {
        let requests = self.requests.clone();
        let download_request_count = self.download_request_count.clone();
        let download_hits_by_version = self.download_hits_by_version.clone();
        let name = name.to_string();
        Mock::given(method("GET"))
            .and(path(format!("/vfs-download/{name}")))
            .respond_with(move |req: &Request| {
                download_request_count.fetch_add(1, Ordering::SeqCst);
                *download_hits_by_version
                    .lock()
                    .unwrap()
                    .entry((name.clone(), extract_version(req)))
                    .or_default() += 1;
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
                    .push(range_header);
                ResponseTemplate::new(status)
            })
            .up_to_n_times(times)
            .with_priority(1)
            .mount(&self.server)
            .await;
    }

    /// Like `fail_downloads_n_times`, but each successive call against
    /// `name`'s download endpoint answers with the next status in
    /// `statuses` instead of a single repeated one — used to exercise a
    /// non-uniform failure sequence (e.g. a transient 500 followed by an
    /// expired-url 403) that `fail_downloads_n_times` can't express. Once
    /// every status in the list has been served once, the normal range-
    /// aware responder from `new()` takes back over, exactly like
    /// `fail_downloads_n_times`.
    pub async fn fail_downloads_with_sequence(&self, name: &str, statuses: Vec<u16>) {
        let requests = self.requests.clone();
        let download_request_count = self.download_request_count.clone();
        let download_hits_by_version = self.download_hits_by_version.clone();
        let name_owned = name.to_string();
        let call = Arc::new(AtomicUsize::new(0));
        let n = statuses.len() as u64;
        Mock::given(method("GET"))
            .and(path(format!("/vfs-download/{name}")))
            .respond_with(move |req: &Request| {
                download_request_count.fetch_add(1, Ordering::SeqCst);
                *download_hits_by_version
                    .lock()
                    .unwrap()
                    .entry((name_owned.clone(), extract_version(req)))
                    .or_default() += 1;
                let range_header = req
                    .headers
                    .get("Range")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                requests
                    .lock()
                    .unwrap()
                    .entry(name_owned.clone())
                    .or_default()
                    .push(range_header);
                let idx = call.fetch_add(1, Ordering::SeqCst).min(statuses.len() - 1);
                ResponseTemplate::new(statuses[idx])
            })
            .up_to_n_times(n)
            .with_priority(1)
            .mount(&self.server)
            .await;
    }

    /// Total number of `POST /api/v4/file/url` calls made for `name` so
    /// far — each one hands back a distinct `?v=` url (see `new()`'s
    /// comment). A caller that "refreshes" a url after a 403 without
    /// actually re-requesting one will leave this stuck, which is the
    /// whole point of exposing it: it distinguishes a real refresh from a
    /// mutation that merely retries the same url again.
    pub fn file_url_request_count(&self, name: &str) -> usize {
        self.file_url_calls
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0) as usize
    }

    /// Download hits against `name`'s content endpoint that carried
    /// `?v={version}` specifically — lets a test tell the original url and
    /// a refreshed one apart even though the same mock responder serves
    /// both.
    pub fn download_hits_for_version(&self, name: &str, version: u32) -> u32 {
        self.download_hits_by_version
            .lock()
            .unwrap()
            .get(&(name.to_string(), version))
            .copied()
            .unwrap_or(0)
    }

    /// Adds an artificial delay to every future response from `name`'s
    /// download endpoint (only the default/success responder mounted by
    /// `new()`, not an injected-failure one) — used to widen a race window
    /// in concurrency tests (e.g. two writers materializing the same file
    /// at once) without relying on scheduler luck alone.
    pub fn slow_down_downloads(&self, name: &str, delay: Duration) {
        self.download_delays
            .lock()
            .unwrap()
            .insert(name.to_string(), delay);
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

    /// Root of the on-disk block cache — `Vfs::new` segregates it from
    /// `drafts/` under `cache_dir` (D1's TRAP: `BlockCache::open` deletes
    /// any directory lacking `meta.json`, so it must never be pointed at a
    /// root that also holds draft directories). Tests measuring the
    /// cache's on-disk footprint against its configured cap must measure
    /// this subdir specifically, not the whole `cache_dir`, or draft bytes
    /// would incorrectly count against the block-cache budget.
    pub fn blocks_dir(&self) -> PathBuf {
        self.cache_dir.path().join("blocks")
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

/// Whether any directory listing registered so far already holds an entry
/// at exactly `target_path` — the read-only counterpart of
/// `remove_entry_by_path`, used by the upload-session mock's `ObjectExisted`
/// simulation (see `expect_uploads`'s doc).
fn path_exists_in_files(files: &HashMap<String, Vec<Value>>, target_path: &str) -> bool {
    files
        .values()
        .any(|list| list.iter().any(|f| f.get("path").and_then(Value::as_str) == Some(target_path)))
}

/// Removes and returns the first entry whose `path` field equals
/// `target_path`, scanning every directory listing registered so far —
/// namespace-op mocks use this to find an entry regardless of which
/// directory's listing currently holds it.
fn remove_entry_by_path(files: &mut HashMap<String, Vec<Value>>, target_path: &str) -> Option<Value> {
    for list in files.values_mut() {
        if let Some(idx) =
            list.iter().position(|f| f.get("path").and_then(Value::as_str) == Some(target_path))
        {
            return Some(list.remove(idx));
        }
    }
    None
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

/// Total size in bytes of every regular file under `dir`, recursing into
/// subdirectories — used to check the on-disk cache footprint against a
/// configured cap without knowing the cache's internal layout.
pub fn dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += dir_size(&path);
        } else if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    total
}

/// Reads the `?v=` query parameter off a download-endpoint request,
/// defaulting to `1` for a url that somehow carries none (shouldn't happen
/// given `new()` always stamps one, but a sane default beats a panic if it
/// ever does).
fn extract_version(req: &Request) -> u32 {
    req.url
        .query_pairs()
        .find(|(k, _)| k == "v")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(1)
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
