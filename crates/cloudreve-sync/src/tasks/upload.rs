use std::{path::PathBuf, str::FromStr, sync::Arc, time::SystemTime};

use crate::utils::toast::send_conflict_toast;
use crate::{
    drive::utils::local_path_to_cr_uri,
    inventory::{local_mtime_secs, ConflictState, FileMetadata, InventoryDb, MetadataEntry},
    tasks::queue::QueuedTask,
    uploader::{ProgressCallback, ProgressUpdate, UploadParams, Uploader, UploaderConfig},
};
use anyhow::{Context, Result};
use bytes::Bytes;
use cloudreve_api::{
    ApiError, Client,
    api::ExplorerApi,
    error::ErrorCode,
    models::explorer::{CreateFileService, FileResponse, FileUpdateService, file_type},
};
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use super::types::TaskProgress;

pub struct InMemoryProgressReporter {
    task_id: String,
    progress_map: Arc<DashMap<String, TaskProgress>>,
}

impl InMemoryProgressReporter {
    pub fn new(task_id: String, progress_map: Arc<DashMap<String, TaskProgress>>) -> Self {
        Self { task_id, progress_map }
    }
}

impl ProgressCallback for InMemoryProgressReporter {
    fn on_progress(&self, update: ProgressUpdate) {
        if let Some(mut entry) = self.progress_map.get_mut(&self.task_id) {
            entry.update_from_progress(&update);
        }
    }
}

/// Simple local file metadata — replaces CrPlaceholder on macOS/Linux.
struct LocalFileInfo {
    pub exists: bool,
    pub is_directory: bool,
    pub file_size: u64,
    pub last_modified: Option<SystemTime>,
}

impl LocalFileInfo {
    fn from_path(path: &PathBuf) -> Self {
        match std::fs::metadata(path) {
            Ok(meta) => Self {
                exists: true,
                is_directory: meta.is_dir(),
                file_size: meta.len(),
                last_modified: meta.modified().ok(),
            },
            Err(_) => Self {
                exists: false,
                is_directory: false,
                file_size: 0,
                last_modified: None,
            },
        }
    }
}

pub struct UploadTask<'a> {
    inventory: Arc<InventoryDb>,
    cr_client: Arc<Client>,
    drive_id: &'a str,
    sync_path: PathBuf,
    remote_base: String,
    task: &'a QueuedTask,
    local_info: Option<LocalFileInfo>,
    inventory_meta: Option<FileMetadata>,
    cancel_token: CancellationToken,
    progress_map: Arc<DashMap<String, TaskProgress>>,
}

impl<'a> UploadTask<'a> {
    pub fn new(
        inventory: Arc<InventoryDb>,
        cr_client: Arc<Client>,
        drive_id: &'a str,
        task: &'a QueuedTask,
        sync_path: PathBuf,
        remote_base: String,
        progress_map: Arc<DashMap<String, TaskProgress>>,
    ) -> Self {
        Self {
            inventory,
            cr_client,
            drive_id,
            local_info: None,
            inventory_meta: None,
            task,
            sync_path,
            remote_base,
            cancel_token: CancellationToken::new(),
            progress_map,
        }
    }

    #[allow(dead_code)]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = token;
        self
    }

    pub async fn execute(&mut self) -> Result<()> {
        let local_info = LocalFileInfo::from_path(&self.task.payload.local_path);

        if !local_info.exists {
            info!(target: "tasks::upload", task_id = %self.task.task_id, path = %self.task.payload.local_path_display(), "Local file does not exist, skipping");
            return Ok(());
        }

        let is_directory = local_info.is_directory;
        let file_size = local_info.file_size;
        self.local_info = Some(local_info);

        let path_str = self.task.payload.local_path.to_str().context("invalid path")?;
        self.inventory_meta = self.inventory.query_by_path(path_str).context("failed to query inventory")?;

        let upload_res = match (
            is_directory,
            file_size == 0 && !self.task.payload.force_override,
            self.inventory_meta.is_none(),
        ) {
            (true, _, _) => self.create_empty_file_or_folder().await,
            (false, true, true) => self.create_empty_file_or_folder().await,
            (false, true, false) => self.clear_file_content().await,
            (false, false, _) => self.upload_file_with_uploader().await,
        };

        self.handle_error(upload_res).await
    }

    async fn handle_error(&mut self, r: Result<()>) -> Result<()> {
        match r {
            Ok(()) => Ok(()),
            Err(e) => {
                let is_conflict_error = e.chain().any(|cause| {
                    if let Some(api_err) = cause.downcast_ref::<ApiError>() {
                        matches!(
                            api_err,
                            ApiError::ApiError { code, .. }
                                if *code == ErrorCode::StaleVersion as i32
                                || *code == ErrorCode::ObjectExisted as i32
                        )
                    } else {
                        false
                    }
                });

                if is_conflict_error {
                    warn!(target: "tasks::upload", task_id = %self.task.task_id, path = %self.task.payload.local_path_display(), "Conflict detected");

                    let path_str = self.task.payload.local_path.to_str().unwrap_or_default();
                    if let Err(mark_err) = self.inventory.mark_as_conflicted(path_str, Some(ConflictState::Pending)) {
                        warn!(target: "tasks::upload", error = ?mark_err, "Failed to mark conflict");
                    }

                    send_conflict_toast(
                        self.drive_id,
                        &self.task.payload.local_path,
                        self.inventory_meta.as_ref().map(|m| m.id).unwrap_or(0),
                    );
                }

                Err(e)
            }
        }
    }

    async fn clear_file_content(&mut self) -> Result<()> {
        info!(target: "tasks::upload", task_id = %self.task.task_id, path = %self.task.payload.local_path_display(), "Clearing file content");
        let uri = local_path_to_cr_uri(
            self.task.payload.local_path.clone(),
            self.sync_path.clone(),
            self.remote_base.clone(),
        ).context("failed to compute remote URI")?.to_string();
        let etag = self.inventory_meta.as_ref()
            .ok_or_else(|| anyhow::anyhow!("inventory metadata required for clear_file_content but none found"))?
            .etag.clone();
        let res = self.cr_client.update_file(&FileUpdateService { uri, previous: Some(etag) }, Bytes::new()).await;
        match res {
            Ok(file) => self.file_uploaded(&file),
            Err(e) => Err(e.into()),
        }
    }

    async fn upload_file_with_uploader(&mut self) -> Result<()> {
        let local_info = self.local_info.as_ref()
            .ok_or_else(|| anyhow::anyhow!("local file info not available — file may have been deleted before upload started"))?;
        let file_size = local_info.file_size;
        let last_modified = local_info.last_modified;
        let is_new_file = self.inventory_meta.is_none();

        info!(target: "tasks::upload", task_id = %self.task.task_id, path = %self.task.payload.local_path_display(), size = file_size, "Starting upload");

        let uri = local_path_to_cr_uri(
            self.task.payload.local_path.clone(),
            self.sync_path.clone(),
            self.remote_base.clone(),
        ).context("failed to compute remote URI")?.to_string();

        let previous_version = if let Some(meta) = &self.inventory_meta {
            if matches!(meta.conflict_state, Some(ConflictState::Override)) {
                String::new()
            } else {
                meta.etag.clone()
            }
        } else {
            String::new()
        };

        let params = UploadParams {
            local_path: self.task.payload.local_path.clone(),
            remote_uri: uri,
            file_size,
            mime_type: None,
            last_modified: last_modified
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64),
            overwrite: !is_new_file || self.task.payload.force_override,
            previous_version,
            task_id: self.task.task_id.clone(),
            drive_id: self.drive_id.to_string(),
        };

        let config = UploaderConfig::default();
        let uploader = Uploader::new(self.cr_client.clone(), self.inventory.clone(), config)
            .with_cancel_token(self.cancel_token.clone());
        let progress = InMemoryProgressReporter::new(self.task.task_id.clone(), Arc::clone(&self.progress_map));

        let upload_result = uploader.upload(params.clone(), progress).await;

        match upload_result {
            Err(e) if e.chain().any(|cause| {
                matches!(
                    cause.downcast_ref::<ApiError>(),
                    Some(ApiError::ApiError { code, .. })
                        if *code == ErrorCode::ObjectExisted as i32
                )
            }) => {
                tracing::info!(
                    target: "tasks::upload",
                    task_id = %self.task.task_id,
                    path = %self.task.payload.local_path_display(),
                    "File already exists on server, retrying with overwrite"
                );
                let retry_params = UploadParams {
                    overwrite: true,
                    ..params
                };
                let retry_progress = InMemoryProgressReporter::new(
                    self.task.task_id.clone(),
                    Arc::clone(&self.progress_map),
                );
                let retry_config = UploaderConfig::default();
                let retry_uploader = Uploader::new(
                    self.cr_client.clone(),
                    self.inventory.clone(),
                    retry_config,
                ).with_cancel_token(self.cancel_token.clone());
                retry_uploader.upload(retry_params, retry_progress).await.context("upload failed (retry with overwrite)")?;
            }
            Err(e) => return Err(e).context("upload failed"),
            Ok(()) => {}
        }

        self.finalize_upload().await
    }

    async fn finalize_upload(&mut self) -> Result<()> {
        let uri = local_path_to_cr_uri(
            self.task.payload.local_path.clone(),
            self.sync_path.clone(),
            self.remote_base.clone(),
        ).context("failed to compute remote URI")?.to_string();

        let file_info = self.cr_client
            .get_file_info(&cloudreve_api::models::explorer::GetFileInfoService {
                uri: Some(uri),
                id: None,
                extended: None,
                folder_summary: None,
            })
            .await
            .context("failed to get file info after upload")?;

        self.file_uploaded(&file_info)
    }

    async fn create_empty_file_or_folder(&mut self) -> Result<()> {
        info!(target: "tasks::upload", task_id = %self.task.task_id, path = %self.task.payload.local_path_display(), "Creating empty file/folder");
        let is_directory = self.local_info.as_ref().map(|i| i.is_directory).unwrap_or(false);
        let uri = local_path_to_cr_uri(
            self.task.payload.local_path.clone(),
            self.sync_path.clone(),
            self.remote_base.clone(),
        ).context("failed to compute remote URI")?.to_string();

        let res = self.cr_client.create_file(&CreateFileService {
            uri,
            file_type: if is_directory { "folder".to_string() } else { "file".to_string() },
            err_on_conflict: Some(!is_directory),
            metadata: None,
        }).await;

        match res {
            Ok(file) => self.file_uploaded(&file),
            Err(e) => Err(e.into()),
        }
    }

    fn file_uploaded(&mut self, file: &FileResponse) -> Result<()> {
        info!(target: "tasks::upload", task_id = %self.task.task_id, path = %self.task.payload.local_path_display(), "File uploaded");

        // Update inventory with remote file info
        let drive_id = Uuid::from_str(self.drive_id).context("invalid drive_id")?;
        let local_path_str = self.task.payload.local_path.to_str().unwrap_or_default().to_string();
        let is_folder = file.file_type == file_type::FOLDER;

        let parse_ts = |s: &str| -> i64 {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.timestamp())
                .unwrap_or(0)
        };
        let entry = MetadataEntry::new(drive_id, local_path_str, is_folder)
            .with_created_at(parse_ts(&file.created_at))
            .with_updated_at(parse_ts(&file.updated_at))
            .with_etag(file.primary_entity.clone().unwrap_or_default())
            .with_size(file.size)
            .with_local_mtime(local_mtime_secs(&self.task.payload.local_path));
        self.inventory.upsert(&entry).context("failed to update inventory")?;

        Ok(())
    }
}
