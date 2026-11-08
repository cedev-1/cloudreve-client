//! Refreshing folders a file manager already shows, after a remote change.
//!
//! Neither frontend can tell the OS that a folder changed on the server:
//! NFSv3 has no change notification at all, and FUSE's `notify_inval_*`
//! messages drop kernel caches without emitting the fsnotify event a file
//! manager watches (`fs/fuse/dir.c`, `fuse_reverse_inval_entry`: plain
//! `d_delete`, no `fsnotify_*`). Finder (kqueue/FSEvents) and Nautilus
//! (inotify) only hear about operations performed locally through the VFS:
//! xnu's `VNOP_CREATE`/`VNOP_REMOVE` wrappers post `NOTE_WRITE` on the
//! directory (`bsd/vfs/kpi_vfs.c`, `post_event_if_success`) and the
//! `open(O_CREAT)`/`unlink` syscalls emit `FSE_CREATE_FILE`/`FSE_DELETE`;
//! Linux's VFS emits `IN_CREATE`/`IN_DELETE` for the same calls.
//!
//! So after a remote change, the app performs one such operation itself:
//! it creates and immediately removes [`NUDGE_NAME`] in the affected folder,
//! through the mountpoint. The name is local-only junk (`crate::junk`): it
//! lives in the facade's in-memory overlay, is never uploaded and never
//! listed, so the user cannot see it. Its CREATE also makes the kernel
//! re-read the folder (the NFS client marks a directory it modified as
//! needing a fresh READDIR; `nfs_vnop_readdir`, `NMODIFIED`), and the
//! adapter's pre-op GETATTR re-lists the invalidated folder first, so what
//! the file manager reads back is already the server's new state.
//!
//! None of rclone `nfsmount`, hf-mount or FUSE-T's NFS backend does this —
//! they rely on attribute-cache expiry or polling (FUSE-T's wiki lists
//! notifications as "unsupported for NFS") — and Mountain Duck polls the
//! folders open in Finder.
//!
//! Requests are coalesced per folder and debounced ([`NUDGE_DEBOUNCE`]): a
//! burst of SSE events for one folder costs one nudge. Each nudge runs on
//! the blocking pool, never on a runtime worker — its syscalls are served
//! by this same process's NFS server tasks — bounded by [`NUDGE_TIMEOUT`].
//! The nudger lives exactly as long as the OS mount (`crate::mount` starts
//! it after attaching and stops it before detaching), so nothing is nudged
//! while the drive is paused or unmounted.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// The local-only junk name created and removed to wake watchers.
pub const NUDGE_NAME: &str = "._cloudreve-refresh";

/// How long requests are gathered before the folders they name are nudged.
pub const NUDGE_DEBOUNCE: Duration = Duration::from_millis(200);

/// How long one nudge may take before the nudger stops waiting for it and
/// drops the rest of its batch (a wedged mount; those folders still refresh
/// on their next access through the short attribute cache).
pub const NUDGE_TIMEOUT: Duration = Duration::from_secs(5);

/// Nudges one folder, given as a path relative to the mount root (`""` is
/// the root). Blocking.
pub type Perform = Arc<dyn Fn(&str) -> std::io::Result<()> + Send + Sync>;

/// The production [`Perform`]: create then remove [`NUDGE_NAME`] in the
/// folder under `mountpoint`.
pub fn touch_through(mountpoint: PathBuf) -> Perform {
    Arc::new(move |rel: &str| touch_and_remove(&mountpoint.join(rel)))
}

fn touch_and_remove(dir: &Path) -> std::io::Result<()> {
    let probe = dir.join(NUDGE_NAME);
    drop(std::fs::OpenOptions::new().write(true).create(true).truncate(false).open(&probe)?);
    std::fs::remove_file(&probe)
}

#[derive(Default)]
struct Queue {
    pending: Mutex<BTreeSet<String>>,
    wake: Notify,
}

/// A running nudger: a queue of folders and the task draining it.
pub struct Nudger {
    queue: Arc<Queue>,
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl Nudger {
    /// Starts the draining task on the current tokio runtime.
    pub fn start(perform: Perform, debounce: Duration) -> Self {
        let queue = Arc::new(Queue::default());
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(drain(queue.clone(), stop.clone(), perform, debounce));
        Self { queue, stop, task }
    }

    /// Asks for `rel` (relative to the mount root) to be nudged soon.
    pub fn request(&self, rel: String) {
        self.queue.pending.lock().unwrap().insert(rel);
        self.queue.wake.notify_one();
    }

    /// Stops the task, waiting for an in-flight nudge to finish (bounded),
    /// so no nudge can still be running once the caller detaches the mount.
    pub async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        self.queue.wake.notify_one();
        let mut task = self.task;
        if tokio::time::timeout(NUDGE_TIMEOUT + Duration::from_secs(1), &mut task).await.is_err() {
            task.abort();
        }
    }

    /// Non-async stop for `Drop` paths: no new nudge starts.
    pub fn stop_now(self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.abort();
    }
}

async fn drain(queue: Arc<Queue>, stop: Arc<AtomicBool>, perform: Perform, debounce: Duration) {
    loop {
        queue.wake.notified().await;
        if stop.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(debounce).await;
        let batch = std::mem::take(&mut *queue.pending.lock().unwrap());
        for rel in batch {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            let p = perform.clone();
            let r = rel.clone();
            match tokio::time::timeout(NUDGE_TIMEOUT, tokio::task::spawn_blocking(move || p(&r))).await {
                Ok(Ok(Ok(()))) => {}
                // The folder is gone (deleted or renamed remotely, mount
                // being torn down): nothing to refresh.
                Ok(Ok(Err(err))) => tracing::debug!(%rel, %err, "vfs nudge: folder refresh failed"),
                Ok(Err(err)) => tracing::warn!(%rel, %err, "vfs nudge: refresh task panicked"),
                Err(_) => {
                    tracing::warn!(%rel, "vfs nudge: folder refresh timed out, skipping the rest of the batch");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorder() -> (Perform, Arc<Mutex<Vec<String>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let l = log.clone();
        (
            Arc::new(move |rel: &str| {
                l.lock().unwrap().push(rel.to_string());
                Ok(())
            }),
            log,
        )
    }

    #[tokio::test]
    async fn a_burst_of_requests_for_one_folder_nudges_it_once() {
        let (perform, log) = recorder();
        let nudger = Nudger::start(perform, Duration::from_millis(50));
        for _ in 0..500 {
            nudger.request("docs".into());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        nudger.stop().await;
        assert_eq!(*log.lock().unwrap(), vec!["docs".to_string()]);
    }

    #[tokio::test]
    async fn each_folder_of_a_burst_is_nudged_once() {
        let (perform, log) = recorder();
        let nudger = Nudger::start(perform, Duration::from_millis(50));
        for i in 0..300 {
            nudger.request(["docs", "photos/2026", ""][i % 3].into());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        nudger.stop().await;
        let mut seen = log.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, vec!["".to_string(), "docs".to_string(), "photos/2026".to_string()]);
    }

    #[tokio::test]
    async fn nothing_is_nudged_after_stop() {
        let (perform, log) = recorder();
        let nudger = Nudger::start(perform, Duration::from_millis(50));
        let queue = nudger.queue.clone();
        let start = std::time::Instant::now();
        nudger.stop().await;
        // An idle nudger stops at once: unmount waits on this.
        assert!(start.elapsed() < Duration::from_millis(500), "stop took {:?}", start.elapsed());
        queue.pending.lock().unwrap().insert("docs".into());
        queue.wake.notify_one();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(log.lock().unwrap().is_empty());
    }

    #[test]
    fn the_nudge_name_is_local_only_junk() {
        assert_eq!(crate::junk::classify(NUDGE_NAME), Some(crate::junk::JunkKind::LocalOnly));
    }

    #[test]
    fn touching_a_plain_folder_leaves_it_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();
        touch_and_remove(dir.path()).unwrap();
        let names: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names, vec![std::ffi::OsString::from("a.txt")]);
    }
}
