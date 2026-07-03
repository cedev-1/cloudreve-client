use cloudreve_api::models::explorer::StoragePolicy;
use cloudreve_api::models::user::{Capacity, UserSettings};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents the conflict state of a file with the remote
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictState {
    /// File has a conflict, pending user action
    Pending,
    /// User chose to override the remote version
    Override,
}

impl ConflictState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConflictState::Pending => "pending",
            ConflictState::Override => "override",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(ConflictState::Pending),
            "override" => Some(ConflictState::Override),
            _ => None,
        }
    }
}

/// Metadata map key storing the local file mtime (unix seconds) recorded at last sync.
/// Used to detect local modifications that keep the same file size.
pub const LOCAL_MTIME_KEY: &str = "local_mtime";

/// Read the mtime (unix seconds) of a local file, if available.
pub fn local_mtime_secs(path: &std::path::Path) -> Option<i64> {
    std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() as i64)
}

/// Represents a file metadata entry in the inventory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub id: i64,
    pub drive_id: Uuid,
    pub is_folder: bool,
    pub local_path: String,
    pub created_at: i64, // Unix timestamp
    pub updated_at: i64, // Unix timestamp
    pub etag: String,
    pub metadata: HashMap<String, String>,
    pub props: Option<serde_json::Value>,
    pub permissions: String,
    pub shared: bool,
    pub size: i64,
    pub conflict_state: Option<ConflictState>,
}

impl FileMetadata {
    /// The local file mtime (unix seconds) recorded at last sync, if any.
    pub fn local_mtime(&self) -> Option<i64> {
        self.metadata.get(LOCAL_MTIME_KEY).and_then(|v| v.parse().ok())
    }

    /// Whether the local file diverged from the last synced state.
    ///
    /// Size is the primary signal; the recorded mtime (when available)
    /// additionally catches modifications that keep the same size.
    /// Returns false if the local file does not exist.
    pub fn is_locally_modified(&self, local_path: &std::path::Path) -> bool {
        let Ok(fs_meta) = std::fs::metadata(local_path) else {
            return false;
        };
        if fs_meta.len() as i64 != self.size {
            return true;
        }
        match (self.local_mtime(), local_mtime_secs(local_path)) {
            (Some(recorded), Some(current)) => current != recorded,
            _ => false,
        }
    }
}

/// Entry for inserting or updating file metadata
#[derive(Debug, Clone)]
pub struct MetadataEntry {
    pub drive_id: Uuid,
    pub is_folder: bool,
    pub created_at: i64, // Unix timestamp
    pub updated_at: i64, // Unix timestamp
    pub local_path: String,
    pub etag: String,
    pub permissions: String,
    pub shared: bool,
    pub size: i64,
    pub metadata: HashMap<String, String>,
    pub props: Option<serde_json::Value>,
    pub conflict_state: Option<ConflictState>,
}

impl MetadataEntry {
    pub fn new(drive_id: Uuid, local_path: impl Into<String>, is_folder: bool) -> Self {
        Self {
            drive_id,
            is_folder,
            local_path: local_path.into(),
            created_at: chrono::Utc::now().timestamp(),
            updated_at: chrono::Utc::now().timestamp(),
            etag: String::new(),
            metadata: HashMap::new(),
            props: None,
            permissions: String::new(),
            shared: false,
            size: 0,
            conflict_state: None,
        }
    }

    pub fn with_permissions(mut self, permissions: impl Into<String>) -> Self {
        self.permissions = permissions.into();
        self
    }

    pub fn with_shared(mut self, shared: bool) -> Self {
        self.shared = shared;
        self
    }

    pub fn with_size(mut self, size: i64) -> Self {
        self.size = size;
        self
    }

    pub fn with_created_at(mut self, created_at: i64) -> Self {
        self.created_at = created_at;
        self
    }

    pub fn with_updated_at(mut self, updated_at: i64) -> Self {
        self.updated_at = updated_at;
        self
    }

    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = etag.into();
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Record the local file mtime (unix seconds) at sync time.
    /// A `None` value leaves the metadata map untouched.
    pub fn with_local_mtime(mut self, mtime: Option<i64>) -> Self {
        if let Some(mtime) = mtime {
            self.metadata.insert(LOCAL_MTIME_KEY.to_string(), mtime.to_string());
        }
        self
    }

    pub fn with_props(mut self, props: serde_json::Value) -> Self {
        self.props = Some(props);
        self
    }
}

impl From<&FileMetadata> for MetadataEntry {
    fn from(file_metadata: &FileMetadata) -> Self {
        Self {
            drive_id: file_metadata.drive_id.clone(),
            is_folder: file_metadata.is_folder,
            created_at: file_metadata.created_at,
            updated_at: file_metadata.updated_at.clone(),
            local_path: file_metadata.local_path.clone(),
            etag: file_metadata.etag.clone(),
            permissions: file_metadata.permissions.clone(),
            shared: file_metadata.shared,
            metadata: file_metadata.metadata.clone(),
            props: file_metadata.props.clone(),
            size: file_metadata.size,
            conflict_state: file_metadata.conflict_state,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub drive_id: String,
    pub task_type: String,
    pub local_path: String,
    pub status: TaskStatus,
    pub progress: f64,
    pub total_bytes: i64,
    pub processed_bytes: i64,
    pub priority: i32,
    pub custom_state: Option<serde_json::Value>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewTaskRecord {
    pub id: String,
    pub drive_id: String,
    pub task_type: String,
    pub local_path: String,
    pub status: TaskStatus,
    pub progress: f64,
    pub total_bytes: i64,
    pub processed_bytes: i64,
    pub priority: i32,
    pub custom_state: Option<serde_json::Value>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl NewTaskRecord {
    pub fn new(
        id: impl Into<String>,
        drive_id: impl Into<String>,
        task_type: impl Into<String>,
        local_path: impl Into<String>,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: id.into(),
            drive_id: drive_id.into(),
            task_type: task_type.into(),
            local_path: local_path.into(),
            status: TaskStatus::Pending,
            progress: 0.0,
            total_bytes: 0,
            processed_bytes: 0,
            priority: 0,
            custom_state: None,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_status(mut self, status: TaskStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_progress(mut self, progress: f64) -> Self {
        self.progress = progress;
        self
    }

    pub fn with_totals(mut self, total_bytes: i64, processed_bytes: i64) -> Self {
        self.total_bytes = total_bytes;
        self.processed_bytes = processed_bytes;
        self
    }

    pub fn with_custom_state(mut self, state: serde_json::Value) -> Self {
        self.custom_state = Some(state);
        self
    }

    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }

    pub fn touch(mut self) -> Self {
        self.updated_at = chrono::Utc::now().timestamp();
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(TaskStatus::Pending),
            "running" => Some(TaskStatus::Running),
            "completed" => Some(TaskStatus::Completed),
            "failed" => Some(TaskStatus::Failed),
            "cancelled" => Some(TaskStatus::Cancelled),
            _ => None,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, TaskStatus::Pending | TaskStatus::Running)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TaskUpdate {
    pub status: Option<TaskStatus>,
    pub progress: Option<f64>,
    pub total_bytes: Option<i64>,
    pub processed_bytes: Option<i64>,
    pub custom_state: Option<Option<serde_json::Value>>,
    pub error: Option<Option<String>>,
}

impl TaskUpdate {
    pub fn is_empty(&self) -> bool {
        self.status.is_none()
            && self.progress.is_none()
            && self.total_bytes.is_none()
            && self.processed_bytes.is_none()
            && self.custom_state.is_none()
            && self.error.is_none()
    }
}

/// Cached properties for a drive
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DriveProps {
    pub id: i64,
    pub drive_id: String,
    pub capacity: Option<Capacity>,
    pub capacity_updated_at: Option<i64>,
    pub storage_policies: Option<Vec<StoragePolicy>>,
    pub storage_policies_updated_at: Option<i64>,
    pub user_settings: Option<UserSettings>,
    pub user_settings_updated_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Update entry for drive props
#[derive(Debug, Clone, Default)]
pub struct DrivePropsUpdate {
    pub capacity: Option<Option<Capacity>>,
    pub storage_policies: Option<Option<Vec<StoragePolicy>>>,
    pub user_settings: Option<Option<UserSettings>>,
}

impl DrivePropsUpdate {
    pub fn is_empty(&self) -> bool {
        self.capacity.is_none() && self.storage_policies.is_none() && self.user_settings.is_none()
    }

    pub fn with_capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = Some(Some(capacity));
        self
    }

    pub fn with_storage_policies(mut self, policies: Vec<StoragePolicy>) -> Self {
        self.storage_policies = Some(Some(policies));
        self
    }

    pub fn with_user_settings(mut self, settings: UserSettings) -> Self {
        self.user_settings = Some(Some(settings));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a FileMetadata reflecting the state recorded after a sync of
    /// the given real file (its actual size and mtime).
    fn synced_metadata(path: &std::path::Path) -> FileMetadata {
        let size = std::fs::metadata(path).map(|m| m.len() as i64).unwrap_or(0);
        let mut metadata = HashMap::new();
        if let Some(mtime) = local_mtime_secs(path) {
            metadata.insert(LOCAL_MTIME_KEY.to_string(), mtime.to_string());
        }
        FileMetadata {
            id: 1,
            drive_id: Uuid::new_v4(),
            is_folder: false,
            local_path: path.to_string_lossy().to_string(),
            created_at: 0,
            updated_at: 0,
            etag: "etag".to_string(),
            metadata,
            props: None,
            permissions: String::new(),
            shared: false,
            size,
            conflict_state: None,
        }
    }

    #[test]
    fn untouched_file_is_not_modified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.txt");
        std::fs::write(&path, b"content").unwrap();
        let meta = synced_metadata(&path);

        assert!(!meta.is_locally_modified(&path));
    }

    #[test]
    fn size_change_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.txt");
        std::fs::write(&path, b"content").unwrap();
        let meta = synced_metadata(&path);

        std::fs::write(&path, b"content plus more").unwrap();
        assert!(meta.is_locally_modified(&path));
    }

    #[test]
    fn same_size_edit_is_detected_via_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.txt");
        std::fs::write(&path, b"aaaa").unwrap();
        let meta = synced_metadata(&path);

        // Same size, different content, mtime pushed forward.
        std::fs::write(&path, b"bbbb").unwrap();
        filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(4102444800, 0))
            .unwrap();
        assert!(meta.is_locally_modified(&path));
    }

    #[test]
    fn missing_file_is_not_modified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.txt");
        std::fs::write(&path, b"content").unwrap();
        let meta = synced_metadata(&path);
        std::fs::remove_file(&path).unwrap();

        assert!(!meta.is_locally_modified(&path));
    }

    #[test]
    fn legacy_entry_without_recorded_mtime_relies_on_size_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.txt");
        std::fs::write(&path, b"aaaa").unwrap();
        let mut meta = synced_metadata(&path);
        meta.metadata.clear(); // entry written before mtime tracking existed

        // Same size → considered unchanged (no mtime to compare).
        assert!(!meta.is_locally_modified(&path));
        // Different size → still detected.
        std::fs::write(&path, b"aaaaaa").unwrap();
        assert!(meta.is_locally_modified(&path));
    }
}
