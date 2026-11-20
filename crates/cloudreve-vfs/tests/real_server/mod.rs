//! Harness for the end-to-end suites that run against a REAL Cloudreve v4
//! server instead of wiremock (shared by `cloudreve-vfs` and, through
//! `#[path]`, `cloudreve-sync`).
//!
//! Every mock in the other suites was written from our reading of the API,
//! so a wrong reading passes both the mock and the code (a mocked listing
//! that never showed uploaded files hid a real bug once). Here the server is
//! the official binary; the only thing faked is the network (see [`Proxy`]).
//!
//! Skipped unless `CLOUDREVE_E2E_URL` is set (e.g. `http://127.0.0.1:5212`),
//! with `CLOUDREVE_E2E_EMAIL` / `CLOUDREVE_E2E_PASSWORD` naming an admin
//! account on it. `scripts/e2e-server.sh` starts a pinned release and
//! creates that account.
//!
//! Two kinds of credentials, on purpose:
//! - the code under test gets OAuth tokens minted through the same
//!   authorization-code + PKCE flow the desktop client's "add drive" page
//!   runs (consent granted through the web app's own API instead of a
//!   browser), so scope enforcement is the one a real install meets;
//! - the harness's own checks ([`Remote`]) talk raw HTTP with a password
//!   session token and never go through `cloudreve-api`, so a bug in the
//!   client cannot also hide itself from the verification.
//!
//! Compiled into each test binary separately and only partly used by each,
//! hence the dead_code allowance. Written for edition 2021 AND 2024.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// The desktop client's registered OAuth application (`ui/src/utils/constants.ts`).
const CLIENT_ID: &str = "393a1839-f52e-498e-9972-e77cc2241eee";
const CLIENT_SECRET: &str = "8GaQIu3lOSdqYoDHi9cR8IZ4pvuMH8ya";
const SCOPES: &str =
    "profile email openid offline_access UserInfo.Write Workflow.Write Files.Write Shares.Write";
const CALLBACK_PATH: &str = "/callback/desktop";

/// Where the e2e server lives and the admin account on it.
#[derive(Clone, Debug)]
pub struct E2e {
    pub url: String,
    pub email: String,
    pub password: String,
}

/// The token set the app stores for a drive after "add drive".
#[derive(Clone, Debug)]
pub struct Session {
    pub access_token: String,
    pub refresh_token: String,
    /// RFC 3339, like `Credentials::access_expires`.
    pub access_expires: String,
    pub refresh_expires: String,
}

impl Session {
    pub fn token(&self) -> cloudreve_api::models::user::Token {
        cloudreve_api::models::user::Token {
            access_token: self.access_token.clone(),
            refresh_token: self.refresh_token.clone(),
            access_expires: self.access_expires.clone(),
            refresh_expires: self.refresh_expires.clone(),
        }
    }
}

/// `None` (and a line on stderr) when the e2e server is not configured: the
/// caller returns early, so a plain `cargo test` stays green offline.
pub fn e2e_or_skip(test: &str) -> Option<E2e> {
    match std::env::var("CLOUDREVE_E2E_URL") {
        Ok(url) if !url.trim().is_empty() => Some(E2e {
            url: url.trim().trim_end_matches('/').to_string(),
            email: std::env::var("CLOUDREVE_E2E_EMAIL").unwrap_or_else(|_| "admin@e2e.test".into()),
            password: std::env::var("CLOUDREVE_E2E_PASSWORD")
                .unwrap_or_else(|_| "e2e-password-123".into()),
        }),
        _ => {
            eprintln!("skipping {test}: CLOUDREVE_E2E_URL is not set");
            None
        }
    }
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        let chars = chunk.len() + 1;
        for i in 0..chars {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .expect("build reqwest client")
}

/// `{code, data, msg}` envelope → `data`, panicking with the server's
/// message on a non-zero code.
async fn unwrap_envelope(what: &str, resp: reqwest::Response) -> Value {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let v: Value = serde_json::from_str(&text)
        .unwrap_or_else(|_| panic!("{what}: HTTP {status}, not json: {text}"));
    if v["code"].as_i64() != Some(0) {
        panic!("{what}: server refused: {text}");
    }
    v["data"].clone()
}

impl E2e {
    /// Password login → a session token for the admin account (never
    /// handed to the code under test).
    pub async fn password_token(&self) -> String {
        let resp = http()
            .post(format!("{}/api/v4/session/token", self.url))
            .json(&json!({ "email": self.email, "password": self.password }))
            .send()
            .await
            .expect("password login request");
        unwrap_envelope("password login", resp).await["token"]["access_token"]
            .as_str()
            .expect("access_token")
            .to_string()
    }

    /// The desktop client's OAuth flow, end to end, with consent granted
    /// the way the web app's consent page grants it.
    pub async fn oauth_session(&self) -> Session {
        let bearer = self.password_token().await;
        let verifier = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
        let challenge = base64url(&Sha256::digest(verifier.as_bytes()));
        let consent = http()
            .post(format!("{}/api/v4/session/oauth/consent", self.url))
            .bearer_auth(&bearer)
            .json(&json!({
                "client_id": CLIENT_ID,
                "response_type": "code",
                "redirect_uri": CALLBACK_PATH,
                "state": "e2e",
                "scope": SCOPES,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }))
            .send()
            .await
            .expect("oauth consent request");
        let code = unwrap_envelope("oauth consent", consent).await["code"]
            .as_str()
            .expect("consent code")
            .to_string();
        let resp = http()
            .post(format!("{}/api/v4/session/oauth/token", self.url))
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", CLIENT_ID),
                ("client_secret", CLIENT_SECRET),
                ("code", code.as_str()),
                ("redirect_uri", CALLBACK_PATH),
                ("code_verifier", verifier.as_str()),
            ])
            .send()
            .await
            .expect("oauth token request");
        let v: Value = resp.json().await.expect("oauth token json");
        let access = v["access_token"].as_str().unwrap_or_else(|| panic!("oauth token: {v}"));
        let now = chrono::Utc::now();
        Session {
            access_token: access.to_string(),
            refresh_token: v["refresh_token"].as_str().expect("refresh_token").to_string(),
            access_expires: (now + chrono::Duration::seconds(v["expires_in"].as_i64().unwrap_or(3600)))
                .to_rfc3339(),
            refresh_expires: (now
                + chrono::Duration::seconds(v["refresh_token_expires_in"].as_i64().unwrap_or(86400)))
            .to_rfc3339(),
        }
    }

    /// A `cloudreve_api::Client` logged in like the app is, talking to
    /// `base_url` (the server itself, or a [`Proxy`] in front of it).
    pub async fn api_client(&self, base_url: &str) -> Arc<cloudreve_api::Client> {
        let session = self.oauth_session().await;
        let client = cloudreve_api::Client::new(cloudreve_api::ClientConfig::new(base_url.to_string()));
        client.load_tokens(&session.token()).await;
        Arc::new(client)
    }

    /// A brand-new ordinary user whose storage quota is `quota_bytes`.
    /// Sets the quota of the server's default "User" group (id 2), which
    /// only ever holds users made by this helper: the suites log in as the
    /// admin.
    pub async fn user_with_quota(&self, quota_bytes: u64) -> E2e {
        let admin = self.password_token().await;
        let resp = http()
            .get(format!("{}/api/v4/admin/group/2", self.url))
            .bearer_auth(&admin)
            .send()
            .await
            .expect("get group");
        let mut group = unwrap_envelope("get group", resp).await;
        group["max_storage"] = json!(quota_bytes);
        let resp = http()
            .put(format!("{}/api/v4/admin/group/2", self.url))
            .bearer_auth(&admin)
            .json(&json!({ "group": group }))
            .send()
            .await
            .expect("put group");
        unwrap_envelope("put group", resp).await;

        let user = E2e {
            url: self.url.clone(),
            email: format!("quota-{}@e2e.test", &uuid::Uuid::new_v4().simple().to_string()[..8]),
            password: "quota-password-123".to_string(),
        };
        let resp = http()
            .post(format!("{}/api/v4/user", self.url))
            .json(&json!({ "email": user.email, "password": user.password, "language": "en-US" }))
            .send()
            .await
            .expect("register");
        unwrap_envelope("register", resp).await;
        user
    }

    /// A fresh, empty folder under the admin's root, unique to one test, so
    /// tests never see each other's files on the shared server.
    pub async fn remote(&self, label: &str) -> Remote {
        let token = self.password_token().await;
        let root = format!("cloudreve://my/e2e-{label}-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let remote = Remote { url: self.url.clone(), token, root, http: http() };
        remote.mkdir("").await;
        remote
    }
}

/// One entry of a raw listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}

/// "Another device": raw HTTP against the server, independent of the code
/// under test. Paths are relative to [`Remote::root`], `/`-separated.
pub struct Remote {
    pub url: String,
    token: String,
    pub root: String,
    http: reqwest::Client,
}

fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(*b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Remote {
    /// Another handle on the same folder, for spawned tasks.
    pub fn clone_handle(&self) -> Remote {
        Remote { url: self.url.clone(), token: self.token.clone(), root: self.root.clone(), http: self.http.clone() }
    }

    pub fn uri(&self, rel: &str) -> String {
        let rel = rel.trim_matches('/');
        if rel.is_empty() {
            self.root.clone()
        } else {
            format!("{}/{}", self.root, rel)
        }
    }

    fn api(&self, path: &str) -> String {
        format!("{}/api/v4{}", self.url, path)
    }

    pub async fn mkdir(&self, rel: &str) {
        let resp = self
            .http
            .post(self.api("/file/create"))
            .bearer_auth(&self.token)
            .json(&json!({ "uri": self.uri(rel), "type": "folder" }))
            .send()
            .await
            .expect("mkdir request");
        unwrap_envelope("mkdir", resp).await;
    }

    /// Create or overwrite `rel` with `data` (parents created by the server).
    pub async fn put(&self, rel: &str, data: &[u8]) {
        let resp = self
            .http
            .put(format!("{}?uri={}", self.api("/file/content"), enc(&self.uri(rel))))
            .bearer_auth(&self.token)
            .header("content-type", "application/octet-stream")
            .body(data.to_vec())
            .send()
            .await
            .expect("put request");
        unwrap_envelope("put", resp).await;
    }

    /// Uploads `data` to `rel` through the chunked upload API (what the web
    /// app does): `put` is capped by the server's online-edit size limit,
    /// this is not. The parent folder must exist.
    pub async fn upload(&self, rel: &str, data: &[u8]) {
        let parent = rel.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
        let resp = self
            .http
            .get(format!("{}?uri={}&page_size=1", self.api("/file"), enc(&self.uri(parent))))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("list for policy");
        let policy = unwrap_envelope("list for policy", resp).await["storage_policy"]["id"]
            .as_str()
            .expect("storage policy id")
            .to_string();
        let resp = self
            .http
            .put(self.api("/file/upload"))
            .bearer_auth(&self.token)
            .json(&json!({ "uri": self.uri(rel), "size": data.len(), "policy_id": policy }))
            .send()
            .await
            .expect("upload session request");
        let session = unwrap_envelope("upload session", resp).await;
        let id = session["session_id"].as_str().expect("session_id").to_string();
        let chunk = session["chunk_size"].as_u64().filter(|c| *c > 0).unwrap_or(data.len().max(1) as u64) as usize;
        let chunks: Vec<&[u8]> = if data.is_empty() { vec![&[][..]] } else { data.chunks(chunk).collect() };
        for (i, c) in chunks.into_iter().enumerate() {
            let resp = self
                .http
                .post(self.api(&format!("/file/upload/{id}/{i}")))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .body(c.to_vec())
                .send()
                .await
                .expect("chunk request");
            unwrap_envelope("upload chunk", resp).await;
        }
    }

    /// Like [`Remote::list`], empty while `rel` does not exist yet.
    pub async fn list_or_empty(&self, rel: &str) -> Vec<Entry> {
        if self.info(rel).await.is_none() {
            return Vec::new();
        }
        self.list(rel).await
    }

    /// The whole listing of `rel`, following the server's cursor pagination.
    pub async fn list(&self, rel: &str) -> Vec<Entry> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = format!("{}?uri={}&page_size=2000", self.api("/file"), enc(&self.uri(rel)));
            if let Some(t) = &token {
                url.push_str(&format!("&next_page_token={}", enc(t)));
            }
            let resp = self.http.get(url).bearer_auth(&self.token).send().await.expect("list request");
            let data = unwrap_envelope("list", resp).await;
            for f in data["files"].as_array().cloned().unwrap_or_default() {
                out.push(Entry {
                    name: f["name"].as_str().unwrap_or_default().to_string(),
                    is_dir: f["type"].as_i64() == Some(1),
                    size: f["size"].as_u64().unwrap_or(0),
                });
            }
            match data["pagination"]["next_token"].as_str() {
                Some(t) if !t.is_empty() => token = Some(t.to_string()),
                _ => break,
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub async fn names(&self, rel: &str) -> Vec<String> {
        self.list(rel).await.into_iter().map(|e| e.name).collect()
    }

    /// `None` when `rel` does not exist.
    pub async fn info(&self, rel: &str) -> Option<Value> {
        let resp = self
            .http
            .get(format!("{}?uri={}", self.api("/file/info"), enc(&self.uri(rel))))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("info request");
        let v: Value = resp.json().await.expect("info json");
        if v["code"].as_i64() == Some(0) {
            Some(v["data"].clone())
        } else {
            None
        }
    }

    /// The bytes the server holds for `rel`, `None` when it does not exist.
    pub async fn read(&self, rel: &str) -> Option<Vec<u8>> {
        self.info(rel).await?;
        let resp = self
            .http
            .post(self.api("/file/url"))
            .bearer_auth(&self.token)
            .json(&json!({ "uris": [self.uri(rel)], "download": true }))
            .send()
            .await
            .expect("file url request");
        let v: Value = resp.json().await.expect("file url json");
        if v["code"].as_i64() != Some(0) {
            // Same: listed, but no content to hand out yet.
            return None;
        }
        let url = v["data"]["urls"][0]["url"].as_str().expect("download url").to_string();
        // The server signs urls with its own SiteURL; aim them at the server
        // we actually talk to.
        let path = url.splitn(4, '/').nth(3).unwrap_or_default();
        let resp = self
            .http
            .get(format!("{}/{}", self.url, path))
            .send()
            .await
            .expect("download request");
        // A file whose upload is still running is listed before it has any
        // content: no bytes yet, like a file that does not exist.
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return None;
        }
        assert!(resp.status().is_success(), "download of {rel}: HTTP {}", resp.status());
        Some(resp.bytes().await.expect("download body").to_vec())
    }

    pub async fn rename(&self, rel: &str, new_name: &str) {
        let resp = self
            .http
            .post(self.api("/file/rename"))
            .bearer_auth(&self.token)
            .json(&json!({ "uri": self.uri(rel), "new_name": new_name }))
            .send()
            .await
            .expect("rename request");
        unwrap_envelope("rename", resp).await;
    }

    /// Move `rels` into the folder `dst_rel`.
    pub async fn mv(&self, rels: &[&str], dst_rel: &str) {
        let uris: Vec<String> = rels.iter().map(|r| self.uri(r)).collect();
        let resp = self
            .http
            .post(self.api("/file/move"))
            .bearer_auth(&self.token)
            .json(&json!({ "uris": uris, "dst": self.uri(dst_rel) }))
            .send()
            .await
            .expect("move request");
        unwrap_envelope("move", resp).await;
    }

    pub async fn delete(&self, rel: &str) {
        let resp = self
            .http
            .delete(self.api("/file"))
            .bearer_auth(&self.token)
            .json(&json!({ "uris": [self.uri(rel)], "skip_soft_delete": true }))
            .send()
            .await
            .expect("delete request");
        unwrap_envelope("delete", resp).await;
    }

    /// Where each item in the user's trash was deleted from (its
    /// `sys:restore_uri`), across every test folder of this user.
    pub async fn trashed(&self) -> Vec<String> {
        let resp = self
            .http
            .get(format!("{}?uri={}&page_size=2000", self.api("/file"), enc("cloudreve://trash")))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("trash list request");
        let data = unwrap_envelope("trash list", resp).await;
        data["files"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|f| f["metadata"]["sys:restore_uri"].as_str().map(str::to_string))
            .collect()
    }

    /// Best-effort removal of the whole test folder.
    pub async fn cleanup(&self) {
        let _ = self
            .http
            .delete(self.api("/file"))
            .bearer_auth(&self.token)
            .json(&json!({ "uris": [self.root.clone()], "skip_soft_delete": true }))
            .send()
            .await;
    }
}

/// Polls `check` until it returns `Some`, or panics after `timeout` with
/// `what` and the last state `describe` reported.
pub async fn eventually<T, F, Fut>(what: &str, timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let start = Instant::now();
    loop {
        match check().await {
            Ok(v) => return v,
            Err(last) => {
                if start.elapsed() > timeout {
                    panic!("timed out after {timeout:?} waiting for {what}; last state: {last}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Deterministic, non-repeating-looking content: a byte-for-byte check over
/// it catches a shifted, truncated or duplicated block (a constant fill
/// would not).
pub fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// A TCP forwarder in front of the server: the only fake in these suites.
/// `set_down(true)` cuts every open connection and refuses new ones (the
/// client sees "connection reset/refused", like a server that died or a
/// laptop that went offline); `set_down(false)` brings it back. Download
/// urls come back with the server's own origin, which the client rewrites
/// to its base url, so they go through the proxy too.
pub struct Proxy {
    pub url: String,
    down: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    accept: tokio::task::JoinHandle<()>,
}

impl Proxy {
    pub async fn start(target_url: &str) -> Proxy {
        let target = target_url
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let down = Arc::new(AtomicBool::new(false));
        let conns: Arc<Mutex<Vec<tokio::task::AbortHandle>>> = Arc::new(Mutex::new(Vec::new()));
        let accept = {
            let down = down.clone();
            let conns = conns.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut inbound, _)) = listener.accept().await else { continue };
                    if down.load(Ordering::SeqCst) {
                        drop(inbound);
                        continue;
                    }
                    let target = target.clone();
                    let task = tokio::spawn(async move {
                        if let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await {
                            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                    });
                    let mut conns = conns.lock().unwrap();
                    conns.retain(|c| !c.is_finished());
                    conns.push(task.abort_handle());
                }
            })
        };
        Proxy { url, down, conns, accept }
    }

    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
        if down {
            for c in self.conns.lock().unwrap().drain(..) {
                c.abort();
            }
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.set_down(true);
        self.accept.abort();
    }
}
