use crate::drive::mounts::Mount;
use crate::EventBroadcaster;
use crate::config::ConfigManager;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

const OFFLINE_THRESHOLD: u32 = 1;
const PING_TIMEOUT_SECS: u64 = 10;

/// Manages periodic heartbeat pings to determine real network connectivity.
/// The online/offline status is broadcasted via EventBroadcaster.
pub struct HeartbeatManager {
    drives: Arc<RwLock<HashMap<String, Arc<Mount>>>>,
    event_broadcaster: Arc<EventBroadcaster>,
    consecutive_failures: Arc<AtomicU32>,
    online: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl HeartbeatManager {
    pub fn new(
        drives: Arc<RwLock<HashMap<String, Arc<Mount>>>>,
        event_broadcaster: Arc<EventBroadcaster>,
    ) -> Self {
        Self {
            drives,
            event_broadcaster,
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            online: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
        }
    }

    pub async fn start(&self) {
        let mut guard = self.handle.lock().await;
        if guard.is_some() {
            return;
        }

        let drives = self.drives.clone();
        let event_broadcaster = self.event_broadcaster.clone();
        let consecutive_failures = self.consecutive_failures.clone();
        let online = self.online.clone();

        let handle = tokio::spawn(async move {
            tracing::info!(target: "heartbeat", "Heartbeat monitoring started");

            // TODO: Consider switching to an SSE-triggered or hybrid model to reduce
            // server load. E.g.: normal=60s intervals when SSE is healthy, 15s when
            // SSE is down. This would cut pings from ~240/hr to ~60/hr while still
            // detecting offline within ~30s after an SSE error.
            loop {
                let interval = ConfigManager::try_get()
                    .map(|cm| cm.heartbeat_interval_secs())
                    .unwrap_or(10);

                // Collect mounts to ping outside the read lock to avoid holding it across await
                let mounts_to_ping: Vec<_> = {
                    let read_guard = drives.read().await;
                    if read_guard.is_empty() {
                        tracing::trace!(target: "heartbeat", "No drives to ping, skipping tick");
                        tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
                        continue;
                    }
                    read_guard.values().cloned().collect()
                };

                tracing::debug!(
                    target: "heartbeat",
                    drive_count = mounts_to_ping.len(),
                    "Heartbeat tick"
                );

                let mut any_success = false;

                for mount in &mounts_to_ping {
                    let ping_result = tokio::time::timeout(
                        tokio::time::Duration::from_secs(PING_TIMEOUT_SECS),
                        mount.cr_client.ping(),
                    ).await;

                    match ping_result {
                        Ok(Ok(_)) => {
                            any_success = true;
                            break;
                        }
                        Ok(Err(e)) => {
                            tracing::debug!(
                                target: "heartbeat",
                                drive_id = %mount.id,
                                error = %e,
                                "Ping request failed"
                            );
                        }
                        Err(_) => {
                            tracing::debug!(
                                target: "heartbeat",
                                drive_id = %mount.id,
                                "Ping timed out"
                            );
                        }
                    }
                }

                if any_success {
                    if !online.swap(true, Ordering::SeqCst) {
                        // Transitioned from offline to online
                        tracing::info!(target: "heartbeat", "Connection restored");
                        event_broadcaster.connection_status_changed(true);

                        // Re-enqueue offline tasks on all drives
                        let read_guard = drives.read().await;
                        for mount in read_guard.values() {
                            if let Err(e) = mount.task_queue.re_enqueue_offline_tasks() {
                                tracing::warn!(
                                    target: "heartbeat",
                                    drive_id = %mount.id,
                                    error = %e,
                                    "Failed to re-enqueue offline tasks"
                                );
                            }
                        }
                    }
                    consecutive_failures.store(0, Ordering::SeqCst);
                } else {
                    let failures = consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
                    if failures >= OFFLINE_THRESHOLD {
                        if online.swap(false, Ordering::SeqCst) {
                            // Transitioned from online to offline
                            tracing::warn!(
                                target: "heartbeat",
                                failures = failures,
                                "Connection lost"
                            );
                            event_broadcaster.connection_status_changed(false);

                            // Force all active tasks to offline waiting
                            let read_guard = drives.read().await;
                            for mount in read_guard.values() {
                                if let Err(e) = mount.task_queue.force_offline_waiting().await {
                                    tracing::warn!(
                                        target: "heartbeat",
                                        drive_id = %mount.id,
                                        error = %e,
                                        "Failed to force offline waiting"
                                    );
                                }
                            }
                        }
                    } else {
                        tracing::debug!(
                            target: "heartbeat",
                            failures = failures,
                            "Ping failure (below offline threshold)"
                        );
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
            }
        });

        *guard = Some(handle);
    }

    pub async fn stop(&self) {
        let mut guard = self.handle.lock().await;
        if let Some(handle) = guard.take() {
            handle.abort();
        }
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }
}
