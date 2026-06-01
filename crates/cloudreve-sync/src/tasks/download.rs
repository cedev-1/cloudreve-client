//! Download task: downloads a remote file to its local path.
//! On macOS/Linux all files are real (no virtual placeholders).

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use cloudreve_api::{Client, api::ExplorerApi, models::explorer::FileURLService};
use dashmap::DashMap;
use futures::StreamExt;
use notify_debouncer_full::notify::event::{CreateKind, EventKind, ModifyKind};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    drive::{event_blocker::EventBlocker, utils::local_path_to_cr_uri},
    inventory::{FileMetadata, InventoryDb, MetadataEntry},
    tasks::queue::QueuedTask,
};

use super::types::TaskProgress;

pub struct DownloadProgressTracker {
    total_size: u64,
    downloaded_bytes: AtomicU64,
    samples: std::sync::Mutex<Vec<(Instant, u64)>>,
    window_duration: Duration,
}

impl DownloadProgressTracker {
    pub fn new(total_size: u64) -> Self {
        Self {
            total_size,
            downloaded_bytes: AtomicU64::new(0),
            samples: std::sync::Mutex::new(Vec::with_capacity(32)),
            window_duration: Duration::from_secs(10),
        }
    }

    pub fn add_bytes(&self, bytes: u64) {
        self.downloaded_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn downloaded(&self) -> u64 {
        self.downloaded_bytes.load(Ordering::SeqCst)
    }

    pub fn create_update(&self) -> DownloadProgressUpdate {
        let downloaded = self.downloaded();
        let now = Instant::now();

        let speed = {
            let mut samples = self.samples.lock().unwrap();
            samples.push((now, downloaded));
            let cutoff = now - self.window_duration;
            samples.retain(|(t, _)| *t >= cutoff);
            if samples.len() >= 2 {
                let (oldest_time, oldest_bytes) = samples.first().unwrap();
                let elapsed = now.duration_since(*oldest_time);
                if elapsed.as_millis() > 0 {
                    let bytes_diff = downloaded.saturating_sub(*oldest_bytes);
                    (bytes_diff as f64 / elapsed.as_secs_f64()) as u64
                } else {
                    0
                }
            } else {
                0
            }
        };

        let progress = if self.total_size > 0 {
            (downloaded as f64 / self.total_size as f64).clamp(0.0, 1.0)
        } else {
            1.0
        };

        let eta_seconds = if speed > 0 && downloaded < self.total_size {
            Some((self.total_size - downloaded) / speed)
        } else {
            None
        };

        DownloadProgressUpdate {
            total_size: self.total_size,
            downloaded,
            progress,
            speed_bytes_per_sec: speed,
            eta_seconds,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadProgressUpdate {
    pub total_size: u64,
    pub downloaded: u64,
    pub progress: f64,
    pub speed_bytes_per_sec: u64,
    pub eta_seconds: Option<u64>,
}

pub struct InMemoryDownloadProgressReporter {
    task_id: String,
    progress_map: Arc<DashMap<String, TaskProgress>>,
}

impl InMemoryDownloadProgressReporter {
    pub fn new(task_id: String, progress_map: Arc<DashMap<String, TaskProgress>>) -> Self {
        Self { task_id, progress_map }
    }

    pub fn on_progress(&self, update: &DownloadProgressUpdate) {
        if let Some(mut entry) = self.progress_map.get_mut(&self.task_id) {
            entry.progress = update.progress;
            entry.processed_bytes = Some(update.downloaded as i64);
            entry.total_bytes = Some(update.total_size as i64);
            entry.speed_bytes_per_sec = update.speed_bytes_per_sec;
            entry.eta_seconds = update.eta_seconds;
        }
    }
}

pub struct DownloadTask<'a> {
    inventory: Arc<InventoryDb>,
    cr_client: Arc<Client>,
    drive_id: &'a str,
    sync_path: PathBuf,
    remote_base: String,
    task: &'a QueuedTask,
    inventory_meta: Option<FileMetadata>,
    remote_file_info: Option<cloudreve_api::models::explorer::FileResponse>,
    cancel_token: CancellationToken,
    progress_map: Arc<DashMap<String, TaskProgress>>,
    event_blocker: EventBlocker,
}

impl<'a> DownloadTask<'a> {
    pub fn new(
        inventory: Arc<InventoryDb>,
        cr_client: Arc<Client>,
        drive_id: &'a str,
        task: &'a QueuedTask,
        sync_path: PathBuf,
        remote_base: String,
        progress_map: Arc<DashMap<String, TaskProgress>>,
        event_blocker: EventBlocker,
    ) -> Self {
        Self {
            inventory,
            cr_client,
            drive_id,
            inventory_meta: None,
            remote_file_info: None,
            task,
            sync_path,
            remote_base,
            cancel_token: CancellationToken::new(),
            progress_map,
            event_blocker,
        }
    }

    #[allow(dead_code)]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = token;
        self
    }

    pub async fn execute(&mut self) -> Result<()> {
        let local_path = &self.task.payload.local_path;

        // Skip directories
        if local_path.is_dir() {
            info!(target: "tasks::download", "Skipping directory: {}", local_path.display());
            return Ok(());
        }

        // Get inventory metadata if available
        let path_str = local_path.to_str().context("invalid path")?;
        self.inventory_meta = self.inventory.query_by_path(path_str).context("failed to query inventory")?;

        self.download_file().await
    }

    async fn download_file(&mut self) -> Result<()> {
        let local_path = self.task.payload.local_path.clone();
        info!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Starting download");

        // Compute remote URI
        let uri = local_path_to_cr_uri(local_path.clone(), self.sync_path.clone(), self.remote_base.clone())
            .context("failed to compute remote URI")?
            .to_string();

        // Get remote file info
        let file_info = match self.cr_client
            .get_file_info(&cloudreve_api::models::explorer::GetFileInfoService {
                uri: Some(uri.clone()),
                id: None,
                extended: None,
                folder_summary: None,
            })
            .await
        {
            Ok(info) => info,
            Err(cloudreve_api::ApiError::ApiError { code, message: _, .. }) 
                if code == 40016 || code == 404 => {
                // File no longer exists on the server — treat as a remote deletion.
                // Remove the local file if it exists so we stay in sync.
                info!(
                    target: "tasks::download",
                    task_id = %self.task.task_id,
                    path = %local_path.display(),
                    code = code,
                    "Remote file no longer exists, removing local file"
                );
                if local_path.exists() && !local_path.is_dir() {
                    std::fs::remove_file(&local_path).ok();
                }
                return Ok(());
            }
            Err(e) => return Err(anyhow::Error::from(e).context("failed to get remote file info")),
        };

        // If the local inventory already tracks this exact remote entity (same etag),
        // the file is already in sync — skip the download to avoid a needless
        // re-download that would generate FS events and trigger a re-upload loop.
        if let Some(ref meta) = self.inventory_meta {
            if let Some(ref remote_etag) = file_info.primary_entity {
                if !meta.etag.is_empty() && meta.etag == *remote_etag {
                    debug!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Local file already matches remote etag, skipping download");
                    return Ok(());
                }
            }
        }

        let file_size = file_info.size as u64;
        self.remote_file_info = Some(file_info);

        // Get download URL
        let mut request = FileURLService::default();
        request.uris.push(uri.clone());
        if let Some(entity) = self.remote_file_info.as_ref().and_then(|f| f.primary_entity.clone()) {
            request.entity = Some(entity);
        }

        let url_res = self.cr_client.get_file_url(&request).await.context("failed to get download URL")?;
        let download_url = {
            let raw = url_res.urls.first().context("no download URL")?.url.clone();
            // The server may return a URL with its internally configured SiteURL as the origin
            // (e.g. http://localhost:5212/...) even when the client is talking to a remote host.
            // Rewrite the origin to match the base_url this client is connected to.
            self.cr_client.rewrite_url_origin(&raw)
        };

        debug!(target: "tasks::download", task_id = %self.task.task_id, url = %download_url, size = file_size, "Got download URL");

        // Create parent directory
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent).context("failed to create parent directory")?;
        }

        // Download to temp file
        let temp_path = std::env::temp_dir().join(format!("cloudreve_dl_{}", self.task.task_id));
        if temp_path.exists() {
            std::fs::remove_file(&temp_path).ok();
        }

        let tracker = Arc::new(DownloadProgressTracker::new(file_size));
        let reporter = InMemoryDownloadProgressReporter::new(self.task.task_id.clone(), Arc::clone(&self.progress_map));

        let result = self.download_to_temp(&download_url, &temp_path, tracker.clone(), &reporter).await;

        match result {
            Ok(()) => {
                reporter.on_progress(&tracker.create_update());
                // Block FS events the rename will generate so we don't re-upload the file.
                // macOS FSEvents can emit an unpredictable number of Create/Modify events
                // per rename, so we use a time-based block (5 s) instead of a fixed count.
                let block_duration = Duration::from_secs(5);
                self.event_blocker.register_for_duration(&EventKind::Create(CreateKind::Any), local_path.clone(), block_duration);
                self.event_blocker.register_for_duration(&EventKind::Modify(ModifyKind::Any), local_path.clone(), block_duration);
                // Atomic move temp → final path
                std::fs::rename(&temp_path, &local_path).context("failed to move downloaded file")?;
                info!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Download complete");
                // Update inventory so 3-way sync knows this file was last synced now
                if let Some(file_info) = &self.remote_file_info {
                    let parse_ts = |s: &str| -> i64 {
                        chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.timestamp()).unwrap_or(0)
                    };
                    if let Ok(drive_uuid) = Uuid::parse_str(self.drive_id) {
                        let path_str = local_path.to_str().unwrap_or("");
                        let entry = MetadataEntry::new(drive_uuid, path_str, false)
                            .with_created_at(parse_ts(&file_info.created_at))
                            .with_updated_at(parse_ts(&file_info.updated_at))
                            .with_etag(file_info.primary_entity.clone().unwrap_or_default())
                            .with_size(file_info.size);
                        if let Err(e) = self.inventory.upsert(&entry) {
                            warn!(target: "tasks::download", task_id = %self.task.task_id, error = %e, "Failed to update inventory after download");
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                std::fs::remove_file(&temp_path).ok();
                Err(e)
            }
        }
    }

    async fn download_to_temp(
        &self,
        url: &str,
        temp_path: &PathBuf,
        tracker: Arc<DownloadProgressTracker>,
        reporter: &InMemoryDownloadProgressReporter,
    ) -> Result<()> {
        let client = reqwest::Client::new();
        let response = client.get(url).send().await.context("download request failed")?;

        if !response.status().is_success() {
            anyhow::bail!("Download failed with status: {}", response.status());
        }

        let mut file = tokio::fs::File::create(temp_path).await.context("failed to create temp file")?;
        let mut stream = response.bytes_stream();
        let mut last_report = Instant::now();
        const REPORT_INTERVAL: Duration = Duration::from_millis(200);

        while let Some(chunk_result) = stream.next().await {
            if self.cancel_token.is_cancelled() {
                anyhow::bail!("Download cancelled");
            }
            let chunk = chunk_result.context("failed to read download chunk")?;
            file.write_all(&chunk).await.context("failed to write chunk")?;
            tracker.add_bytes(chunk.len() as u64);
            if last_report.elapsed() >= REPORT_INTERVAL {
                reporter.on_progress(&tracker.create_update());
                last_report = Instant::now();
            }
        }

        file.flush().await.context("failed to flush temp file")?;
        Ok(())
    }
}
