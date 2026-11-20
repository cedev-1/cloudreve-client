//! The mirror drive's fs watcher, between the OS and the debouncer.
//!
//! macOS FSEvents keeps a file's "created" flag for ~30 s: every later
//! event for that path carries it again. `notify` turns each flag into an
//! event of its own, so deleting a file synced (downloaded, saved) less
//! than ~30 s earlier arrives as `Create` + `Remove`, and renaming it as
//! `Create` + `Rename` for both names. `notify-debouncer-full` reads a
//! path whose first event is a creation as a file that came and went
//! within its window, and drops it without a trace: the delete or rename
//! never reached `drive::propagate`, and the server kept the file (or got a
//! second copy under the new name) until the next full sync.
//!
//! [`StaleCreateFilter`] sits in front of the debouncer and drops a
//! creation whose path is already gone when it is reported: whatever
//! followed it (the removal, the rename) then reaches the debouncer as it
//! is. A real temporary file that came and went is still reported, as a
//! removal — of a path the inventory never tracked, which
//! `drive::propagate` never sends to the server.

use notify_debouncer_full::notify::{
    self, Config, Event, EventHandler, EventKind, RecursiveMode, Watcher, WatcherKind,
};
use std::path::Path;

/// A creation reported for a path that no longer exists: FSEvents
/// repeating an old "created" flag, or a file already removed again.
fn is_stale_create(event: &Event) -> bool {
    matches!(event.kind, EventKind::Create(_)) && event.paths.first().is_some_and(|p| !p.exists())
}

/// `W` with stale creations taken out of its events (see the module doc).
pub(crate) struct StaleCreateFilter<W> {
    inner: W,
}

impl<W: Watcher> Watcher for StaleCreateFilter<W> {
    fn new<F: EventHandler>(mut handler: F, config: Config) -> notify::Result<Self> {
        let inner = W::new(
            move |result: notify::Result<Event>| match result {
                Ok(event) if is_stale_create(&event) => {}
                other => handler.handle_event(other),
            },
            config,
        )?;
        Ok(Self { inner })
    }

    fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> notify::Result<()> {
        self.inner.watch(path, recursive_mode)
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        self.inner.unwatch(path)
    }

    fn configure(&mut self, option: Config) -> notify::Result<bool> {
        self.inner.configure(option)
    }

    fn kind() -> WatcherKind {
        W::kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify_debouncer_full::notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
    use notify_debouncer_full::{DebounceEventResult, FileIdMap, new_debouncer_opt};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type Sink = Arc<Mutex<Option<Box<dyn FnMut(notify::Result<Event>) + Send>>>>;

    /// Stands in for the OS watcher: the test hands it the raw events the
    /// OS backend would. One per test thread, set by `new`.
    struct FakeOs;

    thread_local! {
        static SINK: Sink = Arc::new(Mutex::new(None));
    }

    impl Watcher for FakeOs {
        fn new<F: EventHandler>(mut handler: F, _config: Config) -> notify::Result<Self> {
            SINK.with(|s| *s.lock().unwrap() = Some(Box::new(move |e| handler.handle_event(e))));
            Ok(FakeOs)
        }
        fn watch(&mut self, _: &Path, _: RecursiveMode) -> notify::Result<()> {
            Ok(())
        }
        fn unwatch(&mut self, _: &Path) -> notify::Result<()> {
            Ok(())
        }
        fn kind() -> WatcherKind {
            WatcherKind::NullWatcher
        }
    }

    fn os_sends(kind: EventKind, path: &Path) {
        let event = Event::new(kind).add_path(path.to_path_buf());
        SINK.with(|s| (s.lock().unwrap().as_mut().unwrap())(Ok(event)));
    }

    /// The debouncer the mount runs (`FileIdMap`, as on macOS), fed by
    /// `FakeOs` through the filter; returns what it delivers.
    fn debounce(root: &Path, feed: impl FnOnce()) -> Vec<(EventKind, Vec<PathBuf>)> {
        let out = Arc::new(Mutex::new(Vec::new()));
        let sink = out.clone();
        let mut debouncer = new_debouncer_opt::<_, StaleCreateFilter<FakeOs>, FileIdMap>(
            Duration::from_millis(100),
            None,
            move |result: DebounceEventResult| {
                for e in result.unwrap() {
                    sink.lock().unwrap().push((e.kind, e.paths.clone()));
                }
            },
            FileIdMap::new(),
            Config::default(),
        )
        .unwrap();
        debouncer.watch(root, RecursiveMode::Recursive).unwrap();
        feed();
        std::thread::sleep(Duration::from_millis(600));
        drop(debouncer);
        out.lock().unwrap().clone()
    }

    /// Deleting a file synced seconds ago: FSEvents still flags it
    /// "created", so `notify` reports a creation then the removal. The
    /// removal must come out of the debouncer.
    #[test]
    fn deleting_a_file_still_flagged_created_is_reported_as_a_removal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let file = root.join("report.pdf");
        std::fs::write(&file, b"pdf").unwrap();

        let events = debounce(&root, || {
            std::fs::remove_file(&file).unwrap();
            os_sends(EventKind::Create(CreateKind::File), &file);
            os_sends(EventKind::Remove(RemoveKind::File), &file);
        });

        assert!(
            events.iter().any(|(k, p)| matches!(k, EventKind::Remove(_)) && p == &vec![file.clone()]),
            "{events:?}"
        );
    }

    /// Renaming it: both names carry the old "created" flag. One rename
    /// must come out, not a new file under the new name.
    #[test]
    fn renaming_a_file_still_flagged_created_is_reported_as_a_rename() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let old = root.join("Untitled.txt");
        let new = root.join("Notes.txt");
        std::fs::write(&old, b"notes").unwrap();

        let events = debounce(&root, || {
            std::fs::rename(&old, &new).unwrap();
            for path in [&old, &new] {
                os_sends(EventKind::Create(CreateKind::File), path);
                os_sends(EventKind::Modify(ModifyKind::Name(RenameMode::Any)), path);
            }
        });

        assert!(
            events.iter().any(|(k, p)| *k == EventKind::Modify(ModifyKind::Name(RenameMode::Both)) && p == &vec![old.clone(), new.clone()]),
            "{events:?}"
        );
    }

    /// A file created and kept is still a creation.
    #[test]
    fn a_new_file_is_still_reported() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let file = root.join("new.txt");

        let events = debounce(&root, || {
            std::fs::write(&file, b"new").unwrap();
            os_sends(EventKind::Create(CreateKind::File), &file);
        });

        assert!(events.iter().any(|(k, p)| matches!(k, EventKind::Create(_)) && p == &vec![file.clone()]), "{events:?}");
    }
}
