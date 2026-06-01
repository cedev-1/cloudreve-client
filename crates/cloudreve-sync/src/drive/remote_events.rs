use crate::drive::{commands::MountCommand, mounts::Mount, sync::SyncMode};
use anyhow::{Context, Result};
use cloudreve_api::{
    api::explorer::FileEventsApi,
    models::explorer::{FileEvent, FileEventData, FileEventType},
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_SECS: u64 = 1;
const MAX_BACKOFF_SECS: u64 = 32;
const LONG_RETRY_DELAY_SECS: u64 = 3600;

struct BackoffState {
    retry_count: u32,
    current_delay: Duration,
}

impl BackoffState {
    fn new() -> Self {
        Self {
            retry_count: 0,
            current_delay: Duration::from_secs(INITIAL_BACKOFF_SECS),
        }
    }

    fn reset(&mut self) {
        self.retry_count = 0;
        self.current_delay = Duration::from_secs(INITIAL_BACKOFF_SECS);
    }

    fn next_delay(&mut self) -> Option<Duration> {
        if self.retry_count >= MAX_RETRIES {
            return None;
        }
        let delay = self.current_delay;
        self.retry_count += 1;
        self.current_delay =
            Duration::from_secs((self.current_delay.as_secs() * 2).min(MAX_BACKOFF_SECS));
        Some(delay)
    }
}

enum ListenResult {
    Error(anyhow::Error),
    ReconnectRequired,
    StreamEnded,
}

/// Entry point called from `Mount::spawn_remote_event_processor`.
pub async fn run_remote_event_loop(mount: Arc<Mount>) {
    mount.process_remote_events(mount.clone()).await;
}

impl Mount {
    pub async fn process_remote_events(&self, s: Arc<Self>) {
        tracing::info!(target: "drive::remote_events", "Listening to remote events");
        let mut backoff = BackoffState::new();

        let _sync_path = {
            let config = s.config.read().await;
            config.sync_path.clone()
        };

        loop {
            let result = s.listen_remote_events().await;
            match result {
                ListenResult::ReconnectRequired => {
                    tracing::info!(target: "drive::remote_events", "Reconnect required, re-subscribing immediately");
                    backoff.reset();
                    continue;
                }
                ListenResult::StreamEnded => {
                    tracing::warn!(target: "drive::remote_events", "Event stream ended unexpectedly, reconnecting");
                    backoff.reset();
                    continue;
                }
                ListenResult::Error(e) => {
                    if let Some(delay) = backoff.next_delay() {
                        tracing::error!(
                            target: "drive::remote_events",
                            error = %e,
                            retry_count = backoff.retry_count,
                            delay_secs = delay.as_secs(),
                            "Failed to listen to remote events, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        tracing::error!(
                            target: "drive::remote_events",
                            error = %e,
                            "Max retries reached, triggering full sync and waiting 1 hour"
                        );
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        let _ = s.command_tx.send(MountCommand::FullSync);
                        tokio::time::sleep(Duration::from_secs(LONG_RETRY_DELAY_SECS)).await;
                        backoff.reset();
                    }
                }
            }
        }
    }

    async fn listen_remote_events(&self) -> ListenResult {
        let (remote_base, sync_path) = {
            let config = self.config.read().await;
            (config.remote_path.clone(), config.sync_path.clone())
        };

        let mut subscription = match self.cr_client.subscribe_file_events(&remote_base).await {
            Ok(sub) => {
                tracing::info!(target: "drive::remote_events", id = %self.id, remote_base = %remote_base, "SSE subscription established successfully");
                self.set_event_push_subscribed(true).await;
                sub
            }
            Err(e) => {
                tracing::warn!(target: "drive::remote_events", id = %self.id, remote_base = %remote_base, error = %e, "SSE subscription failed");
                self.set_event_push_subscribed(false).await;
                return ListenResult::Error(e.into());
            }
        };

        loop {
            match subscription.next_event().await {
                Ok(Some(event)) => match event {
                    FileEvent::Event(events) => {
                        tracing::info!(target: "drive::remote_events", id = %self.id, count = events.len(), "Received remote file events");
                        if let Err(e) = self.handle_file_events(sync_path.clone(), events).await {
                            tracing::error!(target: "drive::remote_events", error = ?e, "Failed to handle file events");
                        }
                    }
                    FileEvent::Resumed => {
                        self.set_event_push_subscribed(true).await;
                        if let Err(e) = self.task_queue.re_enqueue_offline_tasks() {
                            tracing::warn!(target: "drive::remote_events", error = %e, "Failed to re-enqueue offline tasks on resume");
                        }
                        tracing::info!(target: "drive::remote_events", "Subscription resumed, triggering full sync");
                        let _ = self.command_tx.send(MountCommand::FullSync);
                    }
                    FileEvent::Subscribed => {
                        self.set_event_push_subscribed(true).await;
                        if let Err(e) = self.task_queue.re_enqueue_offline_tasks() {
                            tracing::warn!(target: "drive::remote_events", error = %e, "Failed to re-enqueue offline tasks on subscribe");
                        }
                        tracing::info!(target: "drive::remote_events", "New subscription, triggering full sync");
                        let _ = self.command_tx.send(MountCommand::FullSync);
                    }
                    FileEvent::KeepAlive => {
                        tracing::trace!(target: "drive::remote_events", "Keep-alive");
                    }
                    FileEvent::ReconnectRequired => {
                        self.set_event_push_subscribed(false).await;
                        return ListenResult::ReconnectRequired;
                    }
                },
                Ok(None) => {
                    self.set_event_push_subscribed(false).await;
                    return ListenResult::StreamEnded;
                }
                Err(e) => {
                    self.set_event_push_subscribed(false).await;
                    return ListenResult::Error(e.into());
                }
            }
        }
    }

    async fn handle_file_events(
        &self,
        sync_root: PathBuf,
        events: Vec<FileEventData>,
    ) -> Result<()> {
        let mut create_update: Vec<FileEventData> = Vec::new();
        let mut rename: Vec<FileEventData> = Vec::new();
        let mut delete: Vec<FileEventData> = Vec::new();

        for event in events {
            match event.event_type {
                FileEventType::Create | FileEventType::Modify => create_update.push(event),
                FileEventType::Rename => rename.push(event),
                FileEventType::Delete => delete.push(event),
            }
        }

        if !create_update.is_empty() {
            self.handle_create_update_events(sync_root.clone(), create_update).await?;
        }
        if !delete.is_empty() {
            self.handle_delete_events(sync_root.clone(), delete).await?;
        }
        if !rename.is_empty() {
            self.handle_rename_events(sync_root.clone(), rename).await?;
        }

        Ok(())
    }

    async fn handle_rename_events(&self, sync_root: PathBuf, events: Vec<FileEventData>) -> Result<()> {
        let mut from_grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut to_grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

        for event in events {
            let from_rel: PathBuf = event.from.trim_start_matches('/').split('/').collect();
            let local_from = sync_root.join(&from_rel);
            if local_from.exists() {
                if let Some(parent) = local_from.parent() {
                    from_grouped.entry(parent.to_path_buf()).or_default().push(local_from.clone());
                }
            }

            // Cancel any pending tasks for the old path since the file was moved remotely
            if let Err(e) = self.task_queue.cancel_by_path(&local_from).await {
                tracing::warn!(
                    target: "drive::remote_events",
                    path = %local_from.display(),
                    error = %e,
                    "Failed to cancel tasks for renamed-from path"
                );
            }

            let to_rel: PathBuf = event.to.trim_start_matches('/').split('/').collect();
            let local_to = sync_root.join(&to_rel);
            if let Some(parent) = local_to.parent() {
                to_grouped.entry(parent.to_path_buf()).or_default().push(local_to);
            }
        }

        for (parent, paths) in from_grouped.into_iter().chain(to_grouped) {
            self.sync_parent(sync_root.clone(), parent, paths).await?;
        }
        Ok(())
    }

    async fn handle_delete_events(&self, sync_root: PathBuf, events: Vec<FileEventData>) -> Result<()> {
        let mut grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for event in events {
            let rel: PathBuf = event.from.trim_start_matches('/').split('/').collect();
            let local_path = sync_root.join(&rel);
            if local_path.exists() {
                if let Some(parent) = local_path.parent() {
                    grouped.entry(parent.to_path_buf()).or_default().push(local_path);
                }
            }
        }
        for (parent, paths) in grouped {
            self.sync_parent(sync_root.clone(), parent, paths).await?;
        }
        Ok(())
    }

    async fn handle_create_update_events(&self, sync_root: PathBuf, events: Vec<FileEventData>) -> Result<()> {
        let mut grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for event in events {
            let rel: PathBuf = event.from.trim_start_matches('/').split('/').collect();
            let local_path = sync_root.join(&rel);
            if let Some(parent) = local_path.parent() {
                grouped.entry(parent.to_path_buf()).or_default().push(local_path);
            }
        }
        for (parent, paths) in grouped {
            self.sync_parent(sync_root.clone(), parent, paths).await?;
        }
        Ok(())
    }

    /// On macOS/Linux: all local files are real, just trigger a remote-changed sync.
    async fn sync_parent(
        &self,
        sync_root: PathBuf,
        parent: PathBuf,
        paths: Vec<PathBuf>,
    ) -> Result<()> {
        if !parent.starts_with(&sync_root) {
            tracing::warn!(target: "drive::remote_events", "Event parent outside sync root, skipping");
            return Ok(());
        }

        self.command_tx
            .send(MountCommand::Sync {
                local_paths: paths,
                mode: SyncMode::RemoteChanged,
                user_initiated: false,
            })
            .context("Failed to send sync command")?;
        Ok(())
    }

    pub async fn set_event_push_subscribed(&self, subscribed: bool) {
        let mut flags = self.status_flags.lock().await;
        flags.set_event_push_subscribed(subscribed);
    }
}
