use crate::drive::{commands::MountCommand, mounts::DriveMode, mounts::Mount, sync::SyncMode};
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

const INITIAL_BACKOFF_SECS: u64 = 1;
// Cap low enough that the client recovers quickly once the server is back:
// while the SSE stream is down, only the periodic full sync keeps us in sync.
const MAX_BACKOFF_SECS: u64 = 60;

/// Exponential backoff that never gives up: a subscription failure only means
/// the server is unreachable *right now*, going deaf for a long period (the
/// old behavior was a 1-hour pause after 5 failures) just delays recovery.
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

    fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;
        self.retry_count = self.retry_count.saturating_add(1);
        self.current_delay =
            Duration::from_secs((self.current_delay.as_secs() * 2).min(MAX_BACKOFF_SECS));
        delay
    }
}

#[cfg(test)]
mod backoff_tests {
    use super::*;

    /// The event loop must never go deaf: whatever the number of consecutive
    /// failures, the backoff keeps producing delays, capped at MAX_BACKOFF.
    #[test]
    fn backoff_never_gives_up_and_caps() {
        let mut backoff = BackoffState::new();
        let mut last = Duration::ZERO;
        for _ in 0..50 {
            let delay = backoff.next_delay();
            assert!(delay <= Duration::from_secs(MAX_BACKOFF_SECS));
            last = delay;
        }
        assert_eq!(
            last,
            Duration::from_secs(MAX_BACKOFF_SECS),
            "repeated failures must settle at the max delay, not stop"
        );
    }

    /// A successful connection resets the backoff to the initial delay.
    #[test]
    fn backoff_resets_after_success() {
        let mut backoff = BackoffState::new();
        for _ in 0..10 {
            backoff.next_delay();
        }
        backoff.reset();
        assert_eq!(
            backoff.next_delay(),
            Duration::from_secs(INITIAL_BACKOFF_SECS)
        );
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
                    let delay = backoff.next_delay();
                    tracing::error!(
                        target: "drive::remote_events",
                        error = %e,
                        retry_count = backoff.retry_count,
                        delay_secs = delay.as_secs(),
                        "Failed to listen to remote events, retrying"
                    );
                    tokio::time::sleep(delay).await;
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

        let idle_timeout = Duration::from_secs(
            self.sse_idle_timeout_secs.load(std::sync::atomic::Ordering::Relaxed),
        );

        loop {
            let next = tokio::time::timeout(idle_timeout, subscription.next_event()).await;
            match next {
                Err(_elapsed) => {
                    // No data (not even a keep-alive) for the whole idle window:
                    // the connection is silently dead (half-open socket, proxy
                    // dropped without FIN/RST, etc.).
                    tracing::warn!(
                        target: "drive::remote_events",
                        id = %self.id,
                        timeout_secs = idle_timeout.as_secs(),
                        "SSE stream idle timeout, reconnecting"
                    );
                    self.set_event_push_subscribed(false).await;
                    return ListenResult::StreamEnded;
                }
                Ok(Err(e)) => {
                    self.set_event_push_subscribed(false).await;
                    return ListenResult::Error(e.into());
                }
                Ok(Ok(None)) => {
                    self.set_event_push_subscribed(false).await;
                    return ListenResult::StreamEnded;
                }
                Ok(Ok(Some(event))) => match event {
                    FileEvent::Event(events) => {
                        tracing::info!(target: "drive::remote_events", id = %self.id, count = events.len(), "Received remote file events");
                        if let Err(e) = self.handle_file_events(sync_path.clone(), events).await {
                            tracing::error!(target: "drive::remote_events", error = ?e, "Failed to handle file events");
                        }
                    }
                    FileEvent::Resumed => {
                        self.set_event_push_subscribed(true).await;
                        self.retry_after_reconnect().await;
                        // The server replayed every event missed while we were
                        // disconnected (buffered per Client-Id): nothing was
                        // lost, so no full sync is needed. Proxies like
                        // Cloudflare cut idle SSE connections every couple of
                        // minutes — re-listing the whole drive on each resume
                        // would be pure waste.
                        tracing::info!(target: "drive::remote_events", "Subscription resumed");
                    }
                    FileEvent::Subscribed => {
                        self.set_event_push_subscribed(true).await;
                        self.retry_after_reconnect().await;
                        // D4/D2: a brand-new subscription means the server-side
                        // event buffer is gone — events may have been missed.
                        // `FullMirror` recovers by re-listing the whole drive
                        // (`FullSync`, already a no-op for on-demand — Task 4's
                        // guard in `mounts.rs`'s command processor). `OnDemand`
                        // has no local mirror to re-list at all: instead,
                        // forget the root's cached listing so the very next
                        // readdir/lookup anywhere under it refetches rather
                        // than possibly serving a listing that predates
                        // whatever was missed.
                        if self.config.read().await.mode == DriveMode::OnDemand {
                            if let Some(vfs) = self.vfs.lock().await.clone() {
                                let remote_base = self.config.read().await.remote_path.clone();
                                vfs.tree().invalidate_path(&remote_base).await;
                            }
                        } else {
                            tracing::info!(target: "drive::remote_events", "New subscription, triggering full sync");
                            let _ = self.command_tx.send(MountCommand::FullSync);
                        }
                    }
                    FileEvent::KeepAlive => {
                        tracing::trace!(target: "drive::remote_events", "Keep-alive");
                    }
                    FileEvent::ReconnectRequired => {
                        self.set_event_push_subscribed(false).await;
                        return ListenResult::ReconnectRequired;
                    }
                },
            }
        }
    }

    /// D4: reconnect recovery is mode-aware. `FullMirror` replays parked
    /// tasks through the task queue (`re_enqueue_offline_tasks`, which
    /// already no-ops for on-demand — Task 4's guard in `mounts.rs`). An
    /// `OnDemand` drive has no task queue for that to go through at all —
    /// instead, every draft still `Pending` gets re-armed for immediate
    /// upload (`Vfs::retry_pending_uploads`), the on-demand equivalent of
    /// "catch up on whatever this drive owed the server while disconnected".
    async fn retry_after_reconnect(&self) {
        if self.config.read().await.mode == DriveMode::OnDemand {
            if let Some(vfs) = self.vfs.lock().await.clone() {
                let n = vfs.retry_pending_uploads().await;
                tracing::info!(
                    target: "drive::remote_events",
                    id = %self.id,
                    count = n,
                    "Reconnect: retried pending on-demand uploads"
                );
            }
            return;
        }
        if let Err(e) = self.re_enqueue_offline_tasks().await {
            tracing::warn!(target: "drive::remote_events", error = %e, "Failed to re-enqueue offline tasks on reconnect");
        }
    }

    async fn handle_file_events(
        &self,
        sync_root: PathBuf,
        events: Vec<FileEventData>,
    ) -> Result<()> {
        // Tracked obligation (Task-4 review): an on-demand drive has no
        // local mirror for the full-mirror handlers below to enqueue
        // downloads/deletes INTO — routing SSE events through them would
        // write into the mounted volume behind the vfs's back. This branch
        // is the ONLY thing an on-demand drive's file events ever reach.
        if self.config.read().await.mode == DriveMode::OnDemand {
            return self.handle_file_events_on_demand(events).await;
        }

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

    /// D4 (on-demand): folds a batch of SSE events straight into vfs tree
    /// invalidation — no local paths, no `task_queue`, no `command_tx`.
    /// `VfsTree::invalidate_path` already does the heavy lifting for a
    /// single call (self-if-known-dir, plus its parent — see its own doc),
    /// so this only needs to call it once per event's `from`, plus once
    /// more for `to` on a rename: the source's old parent and the
    /// destination's (possibly different) new parent both need their
    /// cached listing forgotten.
    async fn handle_file_events_on_demand(&self, events: Vec<FileEventData>) -> Result<()> {
        let Some(vfs) = self.vfs.lock().await.clone() else {
            // `start()` hasn't finished building the vfs yet (or this drive
            // was never actually on-demand — shouldn't happen, the caller
            // already checked `mode`). Nothing to invalidate against.
            return Ok(());
        };
        let remote_base = self.config.read().await.remote_path.clone();

        for event in &events {
            vfs.tree().invalidate_path(&event_path_to_remote_uri(&remote_base, &event.from)).await;
            if event.event_type == FileEventType::Rename && !event.to.is_empty() {
                vfs.tree().invalidate_path(&event_path_to_remote_uri(&remote_base, &event.to)).await;
            }
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

/// Turns an SSE event's `from`/`to` into the full `cloudreve://` uri
/// `VfsTree`'s `NodeAttr::remote_path` uses, so `invalidate_path`'s exact-
/// string comparisons actually match.
///
/// The server sends `from`/`to` already relative to the URI this drive
/// subscribed with (`remote_base`/`config.remote_path`) — decoded, leading-
/// slash-prefixed, root as `"/"` (confirmed against the Cloudreve server's
/// `relativePath`, `pkg/filemanager/fs/dbfs/events.go`). Deliberately plain
/// string concatenation, NOT `CrUri::join` (which percent-encodes each
/// segment it's given): the tree's own `NodeAttr::remote_path` entries are
/// never percent-encoded either — `VfsTree::ensure_listed` stores the
/// server listing's `path` field verbatim — so re-encoding here would make
/// every invalidation silently miss its target for any name that needed
/// encoding at all.
fn event_path_to_remote_uri(remote_base: &str, event_path: &str) -> String {
    if event_path.is_empty() || event_path == "/" {
        remote_base.to_string()
    } else {
        format!("{remote_base}{event_path}")
    }
}

#[cfg(test)]
mod event_path_tests {
    use super::*;

    #[test]
    fn root_event_path_maps_to_the_remote_base_itself() {
        assert_eq!(event_path_to_remote_uri("cloudreve://my/sync", "/"), "cloudreve://my/sync");
    }

    #[test]
    fn a_nested_event_path_is_appended_verbatim() {
        assert_eq!(
            event_path_to_remote_uri("cloudreve://my/sync", "/a/b.txt"),
            "cloudreve://my/sync/a/b.txt"
        );
    }
}
