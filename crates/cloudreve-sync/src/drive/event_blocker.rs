use notify_debouncer_full::notify::Event;
use notify_debouncer_full::notify::event::{EventKind, ModifyKind};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// A key for identifying blocked events, consisting of a normalized EventKind and a path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BlockKey {
    kind: NormalizedEventKind,
    path: PathBuf,
}

/// A normalized representation of EventKind for use as a HashMap key.
/// This enum provides granular distinction for Modify::Name events with different RenameMode,
/// while normalizing other event types to their first level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum NormalizedEventKind {
    Any,
    Access,
    Create,
    /// Modify events other than Name renames
    Modify,
    /// Modify::Name with RenameMode::From (file/folder that was renamed)
    ModifyNameFrom,
    /// Modify::Name with RenameMode::Both (single event with both paths)
    Remove,
    Other,
}

impl From<&EventKind> for NormalizedEventKind {
    fn from(kind: &EventKind) -> Self {
        match kind {
            EventKind::Any => NormalizedEventKind::Any,
            EventKind::Access(_) => NormalizedEventKind::Access,
            EventKind::Create(_) => NormalizedEventKind::Create,
            EventKind::Modify(modify_kind) => match modify_kind {
                ModifyKind::Name(_) => NormalizedEventKind::ModifyNameFrom,
                _ => NormalizedEventKind::Modify,
            },
            EventKind::Remove(_) => NormalizedEventKind::Remove,
            EventKind::Other => NormalizedEventKind::Other,
        }
    }
}

/// A block entry that supports both count-based and time-based blocking.
#[derive(Debug)]
struct BlockEntry {
    /// Remaining count for count-based blocking (0 means exhausted).
    count: usize,
    /// Optional deadline: while `Instant::now() < deadline`, the event is blocked
    /// regardless of `count`.
    deadline: Option<Instant>,
}

impl BlockEntry {
    fn count(n: usize) -> Self {
        Self { count: n, deadline: None }
    }

    /// Returns true and consumes one block slot if this entry should block.
    fn try_block(&mut self) -> bool {
        // Time-based: block while within the window (no decrement needed).
        if let Some(deadline) = self.deadline {
            if Instant::now() < deadline {
                return true;
            }
            // Expired — fall through to count check.
            self.deadline = None;
        }
        // Count-based.
        if self.count > 0 {
            self.count -= 1;
            return true;
        }
        false
    }

    /// Returns true if this entry has no blocking power left and can be removed.
    fn is_exhausted(&self) -> bool {
        self.count == 0
            && self
                .deadline
                .map(|d| Instant::now() >= d)
                .unwrap_or(true)
    }
}

/// EventBlocker is used to filter out filesystem events that have already been
/// processed through other means (e.g., rename operations).
///
/// When a rename operation is processed, it may trigger additional filesystem events
/// (like Remove for the source and Create for the target). These events should be
/// blocked to avoid duplicate processing.
#[derive(Debug, Clone, Default)]
pub struct EventBlocker {
    blocked: Arc<Mutex<HashMap<BlockKey, BlockEntry>>>,
}

impl EventBlocker {
    /// Creates a new EventBlocker instance.
    pub fn new() -> Self {
        Self {
            blocked: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Block events of `kind` for `path` for `duration` after this call.
    ///
    /// Any FS event matching this kind+path that arrives before the deadline is
    /// silently dropped. This is more robust than count-based blocking because the
    /// OS (especially macOS FSEvents) may emit an unpredictable number of events
    /// per file operation.
    pub fn register_for_duration(&self, kind: &EventKind, path: PathBuf, duration: Duration) {
        let key = BlockKey {
            kind: NormalizedEventKind::from(kind),
            path,
        };
        let mut blocked = self.blocked.lock().unwrap();
        let entry = blocked.entry(key).or_insert_with(|| BlockEntry::count(0));
        // Extend the deadline if one already exists.
        let new_deadline = Instant::now() + duration;
        entry.deadline = Some(match entry.deadline {
            Some(existing) if existing > new_deadline => existing,
            _ => new_deadline,
        });
    }

    /// Registers an event stub to be blocked `count` times (count-based).
    pub fn register(&self, kind: &EventKind, path: PathBuf, count: usize) {
        let key = BlockKey {
            kind: NormalizedEventKind::from(kind),
            path,
        };
        let mut blocked = self.blocked.lock().unwrap();
        let entry = blocked.entry(key).or_insert_with(|| BlockEntry::count(0));
        entry.count += count;
    }

    /// Convenience method to register an event to be blocked once.
    pub fn register_once(&self, kind: &EventKind, path: PathBuf) {
        self.register(kind, path, 1);
    }

    /// Checks if an event should be blocked. Consumes one count slot or checks the
    /// time-based deadline. Removes exhausted entries automatically.
    pub fn should_block(&self, kind: &EventKind, path: &PathBuf) -> bool {
        let key = BlockKey {
            kind: NormalizedEventKind::from(kind),
            path: path.clone(),
        };

        let mut blocked = self.blocked.lock().unwrap();

        if let Some(entry) = blocked.get_mut(&key) {
            if entry.try_block() {
                tracing::debug!(
                    target: "drive::event_blocker",
                    kind = ?kind,
                    path = %path.display(),
                    "Blocked pre-registered event"
                );
                return true;
            }
            // Entry exhausted — remove it.
            if entry.is_exhausted() {
                blocked.remove(&key);
            }
        }

        false
    }

    /// Filters a vector of events, removing those that have been pre-registered.
    ///
    /// For events with multiple paths, the event is only blocked if ALL paths are blocked.
    ///
    /// # Arguments
    /// * `events` - Vector of events to filter
    /// * `kind` - The EventKind for all events in this batch
    ///
    /// # Returns
    /// Filtered vector with blocked events removed
    pub fn filter_events(&self, events: Vec<Event>, kind: &EventKind) -> Vec<Event> {
        events
            .into_iter()
            .filter(|event| {
                // For events with paths, check if any path should be blocked
                for path in &event.paths {
                    if self.should_block(kind, path) {
                        // Event has at least one blocked path, filter it out
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Clears all registered event blocks.
    pub fn clear(&self) {
        let mut blocked = self.blocked.lock().unwrap();
        blocked.clear();
    }

    /// Returns the number of currently registered event blocks.
    pub fn len(&self) -> usize {
        let blocked = self.blocked.lock().unwrap();
        blocked.len()
    }

    /// Returns true if there are no registered event blocks.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
