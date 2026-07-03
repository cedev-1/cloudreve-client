use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing;

/// Different types of events that can be broadcast to GUI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    ConnectionStatusChanged {
        connected: bool,
    },
    NoDrive {
    },
    /// Request to open the sync status window
    OpenSyncStatusWindow,
    /// Request to open the settings window
    OpenSettingsWindow,
    /// Task/conflict state changed; the frontend should refetch the status summary
    SummaryChanged,
}

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Event::ConnectionStatusChanged { .. } => "ConnectionStatusChanged",
            Event::NoDrive {  } => "NoDrive",
            Event::OpenSyncStatusWindow => "OpenSyncStatusWindow",
            Event::OpenSettingsWindow => "OpenSettingsWindow",
            Event::SummaryChanged => "SummaryChanged",
        }
    }
}

/// Event broadcaster for Server-Sent Events (SSE)
#[derive(Clone)]
pub struct EventBroadcaster {
    sender: Arc<broadcast::Sender<Event>>,
}

impl EventBroadcaster {
    /// Create a new event broadcaster
    ///
    /// # Arguments
    /// * `capacity` - The capacity of the broadcast channel (default: 100)
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(sender),
        }
    }

    /// Subscribe to events and get a receiver
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Broadcast an event to all subscribers
    ///
    /// # Arguments
    /// * `event` - The event to broadcast
    ///
    /// # Returns
    /// The number of receivers that received the event
    pub fn broadcast(&self, event: Event) -> usize {
        match self.sender.send(event.clone()) {
            Ok(count) => {
                tracing::debug!(target: "events", subscribers = count, "Broadcast event to subscriber(s)");
                tracing::trace!(target: "events", event = ?event, "Event details");
                count
            }
            Err(e) => {
                tracing::warn!(target: "events", error = ?e, "Failed to broadcast event (no active subscribers)");
                0
            }
        }
    }

    /// Helper: Broadcast no drive event
    pub fn no_drive(&self) {
        self.broadcast(Event::NoDrive {  });
    }

    /// Helper: Broadcast connection status changed event
    pub fn connection_status_changed(&self, connected: bool) {
        self.broadcast(Event::ConnectionStatusChanged { connected });
    }

    /// Helper: Broadcast open sync status window event
    pub fn open_sync_status_window(&self) {
        self.broadcast(Event::OpenSyncStatusWindow);
    }

    /// Helper: Broadcast open settings window event
    pub fn open_settings_window(&self) {
        self.broadcast(Event::OpenSettingsWindow);
    }

    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBroadcaster {
    fn default() -> Self {
        Self::new(100)
    }
}

/// Interval used to throttle continuous progress notifications.
const SUMMARY_THROTTLE: Duration = Duration::from_millis(500);

/// Notifies the GUI that the status summary changed (tasks, progress, conflicts).
///
/// Discrete changes (task created/finished/failed, conflict detected...) use
/// [`SummaryNotifier::notify`]. Continuous progress updates use
/// [`SummaryNotifier::notify_throttled`], a leading-edge throttle so events are
/// emitted at most once per [`SUMMARY_THROTTLE`]. The final state of a task is
/// never lost because completion always triggers an immediate `notify()`.
pub struct SummaryNotifier {
    broadcaster: Arc<EventBroadcaster>,
    /// Millis since `epoch` of the last throttled emission.
    last_emit_ms: AtomicU64,
    epoch: Instant,
}

impl SummaryNotifier {
    pub fn new(broadcaster: Arc<EventBroadcaster>) -> Self {
        Self {
            broadcaster,
            last_emit_ms: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    /// Emit `SummaryChanged` immediately.
    pub fn notify(&self) {
        self.broadcaster.broadcast(Event::SummaryChanged);
    }

    /// Emit `SummaryChanged` at most once per throttle interval (leading edge).
    pub fn notify_throttled(&self) {
        // Offset by one interval so the first call always passes (last_emit_ms starts at 0)
        let now_ms = self.epoch.elapsed().as_millis() as u64 + SUMMARY_THROTTLE.as_millis() as u64;
        let last = self.last_emit_ms.load(AtomicOrdering::Relaxed);
        if now_ms.saturating_sub(last) < SUMMARY_THROTTLE.as_millis() as u64 {
            return;
        }
        if self
            .last_emit_ms
            .compare_exchange(last, now_ms, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
            .is_ok()
        {
            self.broadcaster.broadcast(Event::SummaryChanged);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttled_notify_respects_interval() {
        let broadcaster = Arc::new(EventBroadcaster::new(16));
        let mut rx = broadcaster.subscribe();
        let notifier = SummaryNotifier::new(broadcaster);

        // First call emits
        notifier.notify_throttled();
        assert!(matches!(rx.try_recv(), Ok(Event::SummaryChanged)));

        // Second call within the interval is suppressed
        notifier.notify_throttled();
        assert!(rx.try_recv().is_err());

        // After the interval, a new call emits again
        std::thread::sleep(SUMMARY_THROTTLE);
        notifier.notify_throttled();
        assert!(matches!(rx.try_recv(), Ok(Event::SummaryChanged)));
    }

    #[test]
    fn notify_is_immediate() {
        let broadcaster = Arc::new(EventBroadcaster::new(16));
        let mut rx = broadcaster.subscribe();
        let notifier = SummaryNotifier::new(broadcaster);

        notifier.notify();
        notifier.notify();
        assert!(matches!(rx.try_recv(), Ok(Event::SummaryChanged)));
        assert!(matches!(rx.try_recv(), Ok(Event::SummaryChanged)));
    }
}