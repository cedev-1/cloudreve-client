//! Local deletes and renames in a mirror drive, sent to the server.
//!
//! A mirror drive used to only forget a file deleted on this computer:
//! the server kept it, and a rename left the old name there next to an
//! upload of the new one. It now behaves like Dropbox or Nextcloud: what
//! is deleted here is deleted on the server, what is renamed or moved here
//! is renamed or moved there. On-demand drives are not concerned: their
//! volume (`cloudreve-vfs`) sends deletes and renames itself.
//!
//! # Two ways in
//!
//! **Live** — the fs watcher (`Mount::start_fs_watcher`, 2 s debounce)
//! reports deletions, and renames it could pair up (`group_fs_events`).
//! [`Mount::propagate_local_removals`] turns them into tasks:
//! - a deleted file or folder → `TaskKind::Delete`;
//! - a rename/move inside the drive → `TaskKind::Move` (one server rename
//!   or move, never a delete plus a re-upload: the server keeps the file's
//!   history, shares and id). A folder is one task, whatever it holds;
//! - moved out of the drive (to the Trash, the Desktop) = deleted; moved
//!   in = a new file, uploaded as before.
//!
//! On macOS, a file created less than ~30 s before it is renamed is
//! reported as a creation of the new name only (FSEvents keeps its
//! "created" flag, and the debouncer drops the old name as a file that
//! came and went). That rename becomes a delete of the old name plus an
//! upload of the new one: a new untracked file next to a tracked file that
//! is gone is swept for right away (`vanished_files_in`), so the server
//! never keeps both names.
//!
//! **Offline** — whatever happened while the app was not running (or the
//! watcher missed) is found by `full_sync`'s 3-way merge: tracked in the
//! inventory, gone locally, still on the server.
//! - The server copy is exactly what was last synced (same etag, no
//!   pending conflict) → deleted on the server. A folder whose server
//!   content is all in that case goes as one item.
//! - The server copy changed since → NOT deleted: the server's change
//!   wins over a delete, and the file is downloaded back. Losing someone
//!   else's edit to a delete made blind is the one outcome to avoid.
//! - Renames are not detected offline: they arrive as a delete of the old
//!   name plus an upload of the new one. Pairing them would mean guessing
//!   from size and mtime (a wrong pair renames the wrong file on the
//!   server) or hashing every new file; delete + upload loses only the
//!   server-side history, and the old version stays in the server's trash.
//!
//! # Guards
//!
//! - **Into the trash.** Deletes never set `skip_soft_delete` nor `unlink`:
//!   Cloudreve moves the item to the user's trash, one "Restore" away.
//! - **Only what this computer synced.** A path with no inventory row was
//!   never synced from here, so it is never deleted on the server (this is
//!   also what keeps a file the client itself removed — deleted on the
//!   server first — from being deleted back: its row goes first).
//! - **Re-checked when it runs.** The executors (`tasks::remote_ops`) check
//!   again that the local file is still gone and that the server copy is
//!   still the synced one; a delete for a folder whose server content is
//!   not all known deletes only the known files and downloads the rest.
//! - **The sync folder must be there.** A vanished sync folder (unmounted
//!   volume, deleted folder) makes every file look deleted: nothing is
//!   deleted then, live or offline (`walk_local` refuses to scan it, and
//!   the executors refuse to run).
//! - **Paused or removed drives** delete nothing: the watcher is stopped,
//!   `full_sync` and `perform_sync` return early, and pausing cancels the
//!   queued tasks (the next sync recomputes them from the inventory).
//! - **Ignored files** (junk, ignore patterns) never cause a server delete.
//! - **Mass deletion.** Deleting [`MASS_DELETE_FILES`] files at once, or
//!   more than half of a drive (from [`MASS_DELETE_MIN_FOR_SHARE`] files),
//!   looks like an accident — a folder dragged out by mistake, a script
//!   gone wrong — more than a cleanup. Nothing is deleted then: the files
//!   stay tracked (so every later sync holds them back too, rather than
//!   downloading them back) and the user is notified once an hour, told to
//!   delete them on the web if it was intended. Moves are never held: they
//!   lose nothing.
//! - **Conflicts never lose data.** A file with a pending conflict counts
//!   as changed on the server; a move whose source changed on the server
//!   keeps both (the server version under the old name, the local one
//!   uploaded under the new name).
//!
//! # Replay
//!
//! Delete and move tasks are ordinary queued tasks: parked while offline
//! and replayed on reconnect (`re_enqueue_offline_tasks`), resumed at the
//! next launch when the app quit first (`resume_incomplete_tasks`). A
//! move's source rides in its `custom_state`, which both keep. While one
//! is queued, `full_sync` leaves its paths alone ([`busy_paths`]) so a
//! rename is not also seen as a delete plus an upload.

use crate::drive::mounts::Mount;
use crate::drive::utils::is_inside;
use crate::inventory::TaskStatus;
use crate::tasks::{TaskKind, TaskPayload, move_from_state};
use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Deleting this many files at once is held back, whatever the drive's size.
pub(crate) const MASS_DELETE_FILES: usize = 200;

/// From this many files, deleting more than half of the drive is held back.
/// Below it, a small drive can be emptied by hand without being stopped.
pub(crate) const MASS_DELETE_MIN_FOR_SHARE: usize = 20;

/// Whether deleting `deleting` of the drive's `tracked` files at once must
/// be held back instead (see the module doc).
pub(crate) fn is_mass_deletion(deleting: usize, tracked: usize) -> bool {
    deleting >= MASS_DELETE_FILES || (deleting >= MASS_DELETE_MIN_FOR_SHARE && deleting * 2 > tracked)
}

/// Paths a queued delete or move is about to act on (its own path, and a
/// move's source): `full_sync` must not plan anything else for them.
pub(crate) fn busy_paths(mount: &Mount) -> Result<Vec<PathBuf>> {
    queued_paths(mount, true)
}

/// Paths a queued move is about to act on. An upload of anything there
/// would race the move: uploading the destination first makes the server
/// refuse the move (the upload session locks it), and the move becomes a
/// delete plus a second upload. The move uploads what changed in it once
/// it is done.
pub(crate) fn moving_paths(mount: &Mount) -> Result<Vec<PathBuf>> {
    queued_paths(mount, false)
}

fn queued_paths(mount: &Mount, with_deletes: bool) -> Result<Vec<PathBuf>> {
    let tasks = mount
        .inventory
        .list_tasks(Some(&mount.id), Some(&[TaskStatus::Pending, TaskStatus::Running]))?;
    let mut paths = Vec::new();
    for task in tasks {
        match TaskKind::from_str(&task.task_type) {
            Some(TaskKind::Delete) if with_deletes => paths.push(PathBuf::from(&task.local_path)),
            Some(TaskKind::Move) => {
                paths.push(PathBuf::from(&task.local_path));
                paths.extend(move_from_state(task.custom_state.as_ref()));
            }
            _ => {}
        }
    }
    Ok(paths)
}

/// `path` is one of `busy`, or inside one.
pub(crate) fn is_busy(busy: &[PathBuf], path: &Path) -> bool {
    busy.iter().any(|b| b == path || is_inside(b, path))
}

/// What to delete on the server for `candidates` (files, relative to the
/// sync folder, each safe to delete on its own): a folder as one item when
/// it is gone locally and every file the server lists in it is a
/// candidate — otherwise the files one by one. Never a folder holding a
/// file the server has and this computer never saw: that file is not a
/// candidate. `counts` says which remote files count (junk does not keep a
/// folder from going as one item).
pub(crate) fn deletion_units<'a>(
    candidates: &[PathBuf],
    remote_files: impl Iterator<Item = &'a PathBuf>,
    remote_folders: &HashSet<PathBuf>,
    local_dir_exists: impl Fn(&Path) -> bool,
    counts: impl Fn(&Path) -> bool,
) -> Vec<PathBuf> {
    let wanted: HashSet<&PathBuf> = candidates.iter().collect();
    let remote_files: Vec<&PathBuf> = remote_files.filter(|f| counts(f)).collect();
    let whole = |folder: &Path| {
        remote_folders.contains(folder)
            && !local_dir_exists(folder)
            && remote_files.iter().filter(|f| f.starts_with(folder)).all(|f| wanted.contains(f))
    };
    let mut units: Vec<PathBuf> = Vec::new();
    for candidate in candidates {
        // Topmost first: `ancestors()` goes up, so keep the last that fits.
        let unit = candidate
            .ancestors()
            .skip(1)
            .filter(|a| !a.as_os_str().is_empty())
            .filter(|a| whole(a))
            .last()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| candidate.clone());
        if !units.contains(&unit) {
            units.push(unit);
        }
    }
    units
}

/// Keeps only the outermost of `paths`: a folder's own delete covers what
/// is inside it.
fn outermost(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    paths.dedup();
    let mut out: Vec<PathBuf> = Vec::new();
    for p in paths {
        if !out.iter().any(|o| is_inside(o, &p)) {
            out.push(p);
        }
    }
    out
}

/// `a` and `b` are the same file: a case-only rename (`a.txt` → `A.txt`)
/// on a case-insensitive volume, where the old name still "exists".
fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

impl Mount {
    /// How many tracked files are at or under `path`.
    fn tracked_files_under(&self, path: &Path) -> usize {
        path.to_str()
            .and_then(|p| self.inventory.query_under(p).ok())
            .map(|rows| rows.iter().filter(|r| !r.is_folder).count())
            .unwrap_or(0)
    }

    /// Tracked files directly in `dir` that are gone locally.
    fn vanished_files_in(&self, dir: &Path) -> Vec<PathBuf> {
        let Some(dir_str) = dir.to_str() else { return Vec::new() };
        let rows = self.inventory.query_under(dir_str).unwrap_or_default();
        rows.into_iter()
            .filter(|r| !r.is_folder)
            .map(|r| PathBuf::from(r.local_path))
            .filter(|p| p.parent() == Some(dir) && !p.exists())
            .collect()
    }

    /// Tell the user a mass deletion was held back (see the module doc).
    pub(crate) async fn hold_mass_deletion(&self, count: usize) {
        let name = self.get_config().await.name;
        tracing::warn!(
            target: "drive::propagate",
            id = %self.id,
            count,
            "Not deleting on the server: too many files deleted at once, holding them back"
        );
        crate::utils::toast::send_mass_delete_held_toast(&self.id, &name, count);
    }

    /// Live path (see the module doc): queues the server deletes and moves
    /// for one watcher batch, and returns the paths still to upload —
    /// `changed` minus what a move takes care of, plus what turned out to
    /// be new content.
    pub(crate) async fn propagate_local_removals(
        &self,
        changed: Vec<PathBuf>,
        deleted: Vec<PathBuf>,
        renamed: Vec<(PathBuf, PathBuf)>,
    ) -> Result<Vec<PathBuf>> {
        let sync_root = self.get_sync_path().await;
        if !sync_root.is_dir() {
            tracing::warn!(
                target: "drive::propagate",
                id = %self.id,
                path = %sync_root.display(),
                "Sync folder is not reachable: not deleting anything on the server"
            );
            return Ok(Vec::new());
        }
        let ignore = self.ignore_matcher.read().await.clone();

        let mut uploads = Vec::new();
        let mut deletions = deleted;
        // A rename the watcher could not pair (moved to the Trash, out of
        // the drive) arrives as a "change" of a path that is gone.
        for path in changed {
            if path.exists() {
                uploads.push(path);
            } else {
                deletions.push(path);
            }
        }

        let mut moves: Vec<(PathBuf, PathBuf)> = Vec::new();
        for (from, to) in renamed {
            if from == sync_root || to == sync_root || !is_inside(&sync_root, &from) || !is_inside(&sync_root, &to) {
                continue;
            }
            if !to.exists() {
                deletions.push(from);
            } else if from.exists() && !same_file(&from, &to) {
                // The old name is back already (an editor's safe save):
                // `to` is simply new content.
                uploads.push(to);
            } else if ignore.is_match(&from) {
                // A junk name renamed into a real one (a temp file saved
                // over): an upload. Never a move of something not synced.
                if !ignore.is_match(&to) {
                    uploads.push(to);
                }
            } else if ignore.is_match(&to) {
                // Leaves the synced set, like a delete would.
                deletions.push(from);
            } else if self.tracked_files_under(&from) == 0 {
                uploads.push(to);
            } else if self.tracked_files_under(&to) > 0 && !same_file(&from, &to) {
                // Replaces a synced file: the server has both names, a
                // move would collide. Delete the old one, upload the new.
                deletions.push(from);
                uploads.push(to);
            } else {
                moves.push((from, to));
            }
        }

        // macOS reports a rename of a file created in the last ~30 s as a
        // creation of the new name only (FSEvents keeps the "created" flag,
        // and the watcher's debouncer then drops the old name as a file
        // that came and went). A new untracked file next to a tracked one
        // that is gone is that signature: the vanished one is a delete —
        // found now rather than at the next full sync, so the server does
        // not keep both names meanwhile.
        let mut swept: HashSet<PathBuf> = HashSet::new();
        for upload in &uploads {
            let Some(parent) = upload.parent() else { continue };
            if !upload.is_file() || swept.contains(parent) {
                continue;
            }
            if upload.to_str().is_some_and(|p| self.inventory.query_by_path(p).ok().flatten().is_some()) {
                continue;
            }
            swept.insert(parent.to_path_buf());
            deletions.extend(self.vanished_files_in(parent));
        }

        // A queued move still has its rows at the old name: never also a
        // delete of it.
        let busy = busy_paths(self)?;
        let mut planned = Vec::new();
        let mut count = 0;
        for path in outermost(deletions) {
            if path == sync_root || ignore.is_match(&path) || path.exists() || is_busy(&busy, &path) {
                continue;
            }
            let n = self.tracked_files_under(&path);
            if n == 0 {
                continue;
            }
            count += n;
            planned.push(path);
        }
        if !planned.is_empty() {
            let tracked = self.inventory.count_files_for_drive(&self.id)?.max(0) as usize;
            if is_mass_deletion(count, tracked) {
                self.hold_mass_deletion(count).await;
            } else {
                for path in planned {
                    tracing::debug!(target: "drive::propagate", path = %path.display(), "Enqueuing server delete for a local delete");
                    self.task_queue.enqueue(TaskPayload::delete(path)).await?;
                }
            }
        }

        for (from, to) in &moves {
            tracing::debug!(target: "drive::propagate", from = %from.display(), to = %to.display(), "Enqueuing server move for a local rename");
            self.task_queue.enqueue(TaskPayload::moved(from, to)).await?;
        }
        // The move uploads whatever changed in it once it is done; an
        // upload racing it would create the destination first.
        uploads.retain(|u| !moves.iter().any(|(_, to)| u == to || is_inside(to, u)));
        Ok(uploads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deliberate cleanup goes through; a wipe-out is held back.
    #[test]
    fn a_few_deletes_pass_and_a_wipe_out_is_held() {
        // A few files in a big drive.
        assert!(!is_mass_deletion(5, 300));
        assert!(!is_mass_deletion(150, 20_000));
        // Hundreds at once, however big the drive.
        assert!(is_mass_deletion(250, 1000));
        assert!(is_mass_deletion(200, 50_000));
        // Most of a drive.
        assert!(is_mass_deletion(30, 40));
        assert!(is_mass_deletion(21, 40));
        // Exactly half is not "more than half".
        assert!(!is_mass_deletion(20, 40));
        // A small drive emptied by hand.
        assert!(!is_mass_deletion(12, 12));
        assert!(!is_mass_deletion(19, 25));
    }

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    /// Gone locally, and all its server content is what was synced: one
    /// delete for the folder, so it is restored in one click.
    #[test]
    fn a_folder_gone_with_all_its_files_is_one_unit() {
        let candidates = vec![p("Photos/a.jpg"), p("Photos/2024/b.jpg"), p("notes.txt")];
        let remote = [p("Photos/a.jpg"), p("Photos/2024/b.jpg"), p("notes.txt"), p("other.txt")];
        let folders = HashSet::from([p("Photos"), p("Photos/2024")]);

        let mut units = deletion_units(&candidates, remote.iter(), &folders, |_| false, |_| true);
        units.sort();

        assert_eq!(units, vec![p("Photos"), p("notes.txt")]);
    }

    /// A file the server has that this computer never saw keeps its folder
    /// from going as a whole; a subfolder that is all known still can.
    #[test]
    fn a_folder_with_an_unknown_file_goes_file_by_file() {
        let candidates = vec![p("Shared/old.txt"), p("Shared/sub/x.txt")];
        let remote = [p("Shared/old.txt"), p("Shared/new-on-web.txt"), p("Shared/sub/x.txt")];
        let folders = HashSet::from([p("Shared"), p("Shared/sub")]);

        let mut units = deletion_units(&candidates, remote.iter(), &folders, |_| false, |_| true);
        units.sort();

        assert_eq!(units, vec![p("Shared/old.txt"), p("Shared/sub")]);
    }

    /// A folder still there locally was not deleted, whatever is missing
    /// from it.
    #[test]
    fn a_folder_still_present_locally_is_never_a_unit() {
        let candidates = vec![p("Docs/a.txt")];
        let remote = [p("Docs/a.txt")];
        let folders = HashSet::from([p("Docs")]);

        let units = deletion_units(&candidates, remote.iter(), &folders, |d| d == Path::new("Docs"), |_| true);

        assert_eq!(units, vec![p("Docs/a.txt")]);
    }

    /// A junk file the server holds in the folder (never synced, so never
    /// a candidate) does not keep it from going as one item.
    #[test]
    fn junk_on_the_server_does_not_split_a_folder() {
        let candidates = vec![p("Trip/a.jpg")];
        let remote = [p("Trip/a.jpg"), p("Trip/.DS_Store")];
        let folders = HashSet::from([p("Trip")]);

        let units = deletion_units(&candidates, remote.iter(), &folders, |_| false, |f| !f.ends_with(".DS_Store"));

        assert_eq!(units, vec![p("Trip")]);
    }

    #[test]
    fn a_deleted_folder_covers_what_was_inside() {
        assert_eq!(outermost(vec![p("/s/A/b.txt"), p("/s/A"), p("/s/C.txt"), p("/s/A")]), vec![p("/s/A"), p("/s/C.txt")]);
    }
}
