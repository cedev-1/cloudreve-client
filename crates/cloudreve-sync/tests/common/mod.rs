//! Shared test harness: real Mount + real SQLite inventory + real temp files,
//! with a mock HTTP server standing in for the Cloudreve API.
//!
//! Tests written on top of this harness describe *user-visible behavior*
//! (data safety, conflict handling, deletions) rather than implementation
//! details.

use std::path::PathBuf;
use std::sync::Arc;

use cloudreve_sync::drive::commands::ManagerCommand;
use cloudreve_sync::drive::mounts::{Credentials, DriveConfig, Mount};
use cloudreve_sync::inventory::{InventoryDb, MetadataEntry, TaskRecord};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::mpsc;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub const REMOTE_BASE: &str = "cloudreve://my/sync";

pub struct TestEnv {
    pub server: MockServer,
    pub mount: Mount,
    pub inventory: Arc<InventoryDb>,
    pub sync_dir: PathBuf,
    pub drive_id: String,
    _tmp: TempDir,
    _manager_rx: mpsc::UnboundedReceiver<ManagerCommand>,
}

impl TestEnv {
    pub async fn new() -> Self {
        Self::with_max_file_size(1024).await
    }

    pub async fn with_max_file_size(max_file_size_mb: u64) -> Self {
        Self::build(max_file_size_mb, Vec::new()).await
    }

    pub async fn with_ignore_patterns(patterns: Vec<String>) -> Self {
        Self::build(1024, patterns).await
    }

    async fn build(max_file_size_mb: u64, ignore_patterns: Vec<String>) -> Self {
        let tmp = TempDir::new().expect("create temp dir");
        let sync_dir = tmp.path().join("sync");
        std::fs::create_dir_all(&sync_dir).expect("create sync dir");

        let inventory = Arc::new(
            InventoryDb::with_path(tmp.path().join("meta.db")).expect("create inventory db"),
        );

        let server = MockServer::start().await;
        let drive_id = Uuid::new_v4().to_string();

        let config = DriveConfig {
            id: drive_id.clone(),
            name: "Test Drive".to_string(),
            instance_url: server.uri(),
            remote_path: REMOTE_BASE.to_string(),
            credentials: Credentials {
                access_token: Some("test-access-token".to_string()),
                refresh_token: "test-refresh-token".to_string(),
                refresh_expires: "2099-01-01T00:00:00Z".to_string(),
                access_expires: Some("2099-01-01T00:00:00Z".to_string()),
            },
            sync_path: sync_dir.clone(),
            enabled: true,
            user_id: "test-user".to_string(),
            ignore_patterns,
            max_file_size_mb,
            sse_client_id: Uuid::new_v4().to_string(),
            ..Default::default()
        };

        let (manager_tx, manager_rx) = mpsc::unbounded_channel();
        let mount = Mount::new(config, inventory.clone(), manager_tx).await;

        Self {
            server,
            mount,
            inventory,
            sync_dir,
            drive_id,
            _tmp: tmp,
            _manager_rx: manager_rx,
        }
    }

    /// Configure the mock server so the remote listing returns these files.
    /// Replaces any previously registered listing.
    pub async fn set_remote_files(&self, files: Vec<Value>) {
        self.server.reset().await;
        let body = json!({
            "code": 0,
            "msg": "",
            "data": {
                "files": files,
                "pagination": { "page": 1, "page_size": 500, "total_items": 0 },
                "props": {
                    "max_page_size": 10000,
                    "order_by_options": ["name"],
                    "order_direction_options": ["asc"],
                },
            },
        });
        Mock::given(method("GET"))
            .and(path("/api/v4/file"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Run a full 3-way sync against the mock remote.
    pub async fn full_sync(&self) -> anyhow::Result<()> {
        cloudreve_sync::drive::sync::full_sync(&self.mount, &self.sync_dir, REMOTE_BASE).await
    }

    /// Absolute path of a file inside the local sync directory.
    pub fn local_path(&self, rel: &str) -> PathBuf {
        self.sync_dir.join(rel)
    }

    /// Create/overwrite a local file with the given content.
    pub fn write_local(&self, rel: &str, content: &[u8]) -> PathBuf {
        let path = self.local_path(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dirs");
        }
        std::fs::write(&path, content).expect("write local file");
        path
    }

    /// Set the mtime (unix seconds) of a local file — simulates an edit at a
    /// specific point in time without relying on sleeps.
    pub fn set_local_mtime(&self, rel: &str, unix_secs: i64) {
        let path = self.local_path(rel);
        let ft = filetime::FileTime::from_unix_time(unix_secs, 0);
        filetime::set_file_mtime(&path, ft).expect("set file mtime");
    }

    /// Record a file in the inventory as "synced" with the given etag,
    /// using the file's current size and mtime on disk (the state a real
    /// upload/download would have recorded).
    pub fn track_synced(&self, rel: &str, etag: &str) {
        let path = self.local_path(rel);
        let meta = std::fs::metadata(&path).expect("stat local file");
        let mtime = cloudreve_sync::inventory::local_mtime_secs(&path);
        let entry = MetadataEntry::new(
            Uuid::parse_str(&self.drive_id).unwrap(),
            path.to_str().unwrap(),
            false,
        )
        .with_etag(etag)
        .with_size(meta.len() as i64)
        .with_local_mtime(mtime);
        self.inventory.upsert(&entry).expect("upsert inventory entry");
    }

    /// All task records ever enqueued for this drive (any status).
    pub fn all_tasks(&self) -> Vec<TaskRecord> {
        self.inventory
            .list_tasks(Some(&self.drive_id), None)
            .expect("list tasks")
    }

    /// Task records of a given type ("upload" / "download") for this drive.
    pub fn tasks_of_type(&self, task_type: &str) -> Vec<TaskRecord> {
        self.all_tasks()
            .into_iter()
            .filter(|t| t.task_type == task_type)
            .collect()
    }

    /// The inventory entry for a local file, if tracked.
    pub fn db_entry(&self, rel: &str) -> Option<cloudreve_sync::inventory::FileMetadata> {
        let path = self.local_path(rel);
        self.inventory
            .query_by_path(path.to_str().unwrap())
            .expect("query inventory")
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
