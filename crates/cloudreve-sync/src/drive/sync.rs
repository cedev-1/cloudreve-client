use crate::drive::mounts::Mount;
use crate::drive::utils::remote_path_to_local_relative_path;
use crate::inventory::{MetadataEntry, FileMetadata};
use crate::tasks::TaskPayload;
use anyhow::{Context, Result};
use cloudreve_api::{
    api::explorer::ExplorerApi,
    models::{
        explorer::{FileResponse, ListFileService, file_type},
        uri::CrUri,
    },
};
use notify_debouncer_full::DebouncedEvent;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use uuid::Uuid;

/// Sync direction / trigger mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Local file changed — upload to remote
    LocalChanged,
    /// Remote file changed — download to local
    RemoteChanged,
    /// Full bidirectional sync
    Full,
}

/// Groups of filesystem events reduced to affected paths
pub struct GroupedFsEvents {
    pub changed: Vec<PathBuf>,
    pub deleted: Vec<PathBuf>,
}

impl GroupedFsEvents {
    pub fn all_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.changed.clone();
        paths.extend(self.deleted.iter().cloned());
        paths
    }
}

/// Reduce a list of debounced FS events into changed/deleted path sets
pub fn group_fs_events(events: Vec<DebouncedEvent>) -> GroupedFsEvents {
    use notify_debouncer_full::notify::event::EventKind;
    let mut changed = HashSet::new();
    let mut deleted = HashSet::new();

    for event in events {
        for path in &event.paths {
            match event.kind {
                EventKind::Remove(_) => {
                    changed.remove(path);
                    deleted.insert(path.clone());
                }
                EventKind::Create(_) | EventKind::Modify(_) => {
                    deleted.remove(path);
                    changed.insert(path.clone());
                }
                _ => {}
            }
        }
    }

    GroupedFsEvents {
        changed: changed.into_iter().collect(),
        deleted: deleted.into_iter().collect(),
    }
}

/// Perform a full bidirectional sync between local and remote using 3-way comparison.
///
/// State sources:
/// - **DB** : last known synced state (populated after each upload/download)
/// - **Local** : current local filesystem state
/// - **Remote** : current remote server state
///
/// Decision table (DB | Local | Remote):
/// - `(true, false, true)` : deleted locally → delete from server
/// - `(true, true, false)` : deleted from server → delete locally
/// - `(true, false, false)` : deleted from both → clean DB entry
/// - `(false, true, false)` : new local file → upload
/// - `(false, false, true)` : new remote file → download
/// - `(false, true, true)` : exists on both sides, not yet tracked → mark as synced in DB
/// - `(true, true, true)` : already in sync, SSE handles live changes
pub async fn full_sync(mount: &Mount, local_root: &PathBuf, remote_path: &str) -> Result<()> {
    if mount.is_paused() {
        tracing::info!(target: "drive::sync", id = %mount.id, "Sync skipped: drive is paused");
        return Ok(());
    }
    tracing::info!(target: "drive::sync", id = %mount.id, remote = remote_path, "Starting 3-way full sync");

    let remote_base = CrUri::new(remote_path)?;

    // 1. Remote state: relative_path → FileResponse
    let remote_files = list_remote_recursive(mount, &remote_base).await?;
    let remote_map: HashMap<PathBuf, &FileResponse> = remote_files
        .iter()
        .filter(|f| f.file_type != file_type::FOLDER)
        .filter_map(|f| {
            let uri = CrUri::new(&f.path).ok()?;
            let rel = remote_path_to_local_relative_path(&uri, &remote_base).ok()?;
            Some((rel, f))
        })
        .collect();

    // 2. Local state
    let local_files = walk_local(local_root)?;
    let local_set: HashSet<PathBuf> = local_files
        .iter()
        .filter_map(|p| p.strip_prefix(local_root).ok().map(|r| r.to_path_buf()))
        .collect();

    // 3. DB state (last known synced state)
    let db_entries = mount.inventory.query_all_for_drive(&mount.id)?;
    let db_set: HashSet<PathBuf> = db_entries
        .iter()
        .filter(|e| !e.is_folder)
        .filter_map(|e| {
            PathBuf::from(&e.local_path)
                .strip_prefix(local_root)
                .ok()
                .map(|r| r.to_path_buf())
        })
        .collect();
    let db_map: HashMap<PathBuf, &FileMetadata> = db_entries
        .iter()
        .filter(|e| !e.is_folder)
        .filter_map(|e| {
            PathBuf::from(&e.local_path)
                .strip_prefix(local_root)
                .ok()
                .map(|r| (r.to_path_buf(), e))
        })
        .collect();

    // 4. Union of all known paths
    let all_paths: HashSet<PathBuf> = remote_map
        .keys()
        .chain(local_set.iter())
        .chain(db_set.iter())
        .cloned()
        .collect();

    let drive_uuid = Uuid::parse_str(&mount.id).ok();

    // A file with an active upload session has no downloadable entity yet
    // (the listing shows it, but downloading it fails with "Entity not exist").
    let is_uploading = |f: &FileResponse| {
        f.metadata.as_ref().is_some_and(|m| {
            m.contains_key(cloudreve_api::models::explorer::metadata::UPLOAD_SESSION_ID)
        })
    };

    for rel in &all_paths {
        let local_path = local_root.join(rel);
        let in_remote = remote_map.contains_key(rel);
        let in_local = local_set.contains(rel);
        let in_db = db_set.contains(rel);
        let path_str = local_path.to_str().unwrap_or("").to_string();

        match (in_db, in_local, in_remote) {
            // Deleted locally (still on server) → forget it, don't re-download, don't touch server
            (true, false, true) => {
                let _ = mount.inventory.batch_delete_by_path(vec![&path_str]);
            }
            // Deleted from server (still local) → keep local copy, forget tracking
            (true, true, false) => {
                let _ = mount.inventory.batch_delete_by_path(vec![&path_str]);
            }
            // Deleted from both → clean DB
            (true, false, false) => {
                let _ = mount.inventory.batch_delete_by_path(vec![&path_str]);
            }
            // New local file → upload
            (false, true, false) => {
                if should_ignore(mount, &local_path).await {
                    continue;
                }
                if let Ok(meta) = std::fs::metadata(&local_path) {
                    if !mount.is_file_size_allowed(meta.len()).await {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            size = meta.len(),
                            "Skipping upload: file exceeds size limit"
                        );
                        continue;
                    }
                }
                mount.task_queue.enqueue(TaskPayload::upload(local_path)).await?;
            }
            // New remote file → download
            (false, false, true) => {
                if should_ignore(mount, &local_path).await {
                    continue;
                }
                if let Some(rf) = remote_map.get(rel) {
                    if is_uploading(rf) {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            "Skipping download: remote file is still being uploaded"
                        );
                        continue;
                    }
                    if !mount.is_file_size_allowed(rf.size as u64).await {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            size = rf.size,
                            "Skipping download: file exceeds size limit"
                        );
                        continue;
                    }
                    let mut payload = TaskPayload::download(local_path).with_totals(0, rf.size);
                    if let Some(etag) = &rf.primary_entity {
                        payload = payload
                            .with_custom_state(serde_json::json!({ "remote_etag": etag }));
                    }
                    mount.task_queue.enqueue(payload).await?;
                }
            }
            // Exists on both sides but not tracked → mark as synced in DB (no transfer needed)
            (false, true, true) => {
                if let (Some(rf), Some(uuid)) = (remote_map.get(rel), drive_uuid) {
                    let parse_ts = |s: &str| -> i64 {
                        chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.timestamp()).unwrap_or(0)
                    };
                    let entry = MetadataEntry::new(uuid, &path_str, false)
                        .with_created_at(parse_ts(&rf.created_at))
                        .with_updated_at(parse_ts(&rf.updated_at))
                        .with_etag(rf.primary_entity.clone().unwrap_or_default())
                        .with_size(rf.size)
                        .with_local_mtime(crate::inventory::local_mtime_secs(&local_path));
                    let _ = mount.inventory.upsert(&entry);
                }
            }
            // Already tracked and present everywhere → check for modifications
            (true, true, true) => {
                if let (Some(db_entry), Some(rf)) = (db_map.get(rel), remote_map.get(rel)) {
                    // Files with an unresolved conflict are frozen: no transfer
                    // until the user resolves them.
                    if db_entry.conflict_state.is_some() {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            "Skipping transfer: file has an unresolved conflict"
                        );
                        continue;
                    }

                    // Check if remote was modified (etag changed)
                    let remote_etag = rf.primary_entity.clone().unwrap_or_default();
                    let remote_changed = !remote_etag.is_empty()
                        && !db_entry.etag.is_empty()
                        && remote_etag != db_entry.etag;
                    let local_changed = db_entry.is_locally_modified(&local_path);

                    // Both sides modified since last sync → conflict, let the user decide
                    if remote_changed && local_changed {
                        tracing::warn!(
                            target: "drive::sync",
                            path = %rel.display(),
                            db_etag = %db_entry.etag,
                            remote_etag = %remote_etag,
                            "Conflict detected: both local and remote modified since last sync"
                        );
                        let _ = mount.inventory.mark_as_conflicted(
                            &path_str,
                            Some(crate::inventory::ConflictState::Pending),
                        );
                        crate::utils::toast::send_conflict_toast(
                            &mount.id,
                            &local_path,
                            db_entry.id,
                        );
                        mount.task_queue.notifier().notify();
                        continue;
                    }

                    if remote_changed {
                        if is_uploading(rf) {
                            tracing::debug!(
                                target: "drive::sync",
                                path = %rel.display(),
                                "Skipping download: remote file is still being uploaded"
                            );
                            continue;
                        }
                        if !mount.is_file_size_allowed(rf.size as u64).await {
                            tracing::debug!(
                                target: "drive::sync",
                                path = %rel.display(),
                                size = rf.size,
                                "Skipping download: file exceeds size limit"
                            );
                            continue;
                        }
                        tracing::info!(
                            target: "drive::sync",
                            path = %rel.display(),
                            db_etag = %db_entry.etag,
                            remote_etag = %remote_etag,
                            "Remote file modified since last sync, downloading"
                        );
                        let mut payload =
                            TaskPayload::download(local_path).with_totals(0, rf.size);
                        if !remote_etag.is_empty() {
                            payload = payload.with_custom_state(
                                serde_json::json!({ "remote_etag": remote_etag }),
                            );
                        }
                        mount.task_queue.enqueue(payload).await?;
                    } else if local_changed {
                        let local_size = std::fs::metadata(&local_path)
                            .map(|m| m.len() as i64)
                            .unwrap_or(0);
                        if !mount.is_file_size_allowed(local_size as u64).await {
                            tracing::debug!(
                                target: "drive::sync",
                                path = %rel.display(),
                                size = local_size,
                                "Skipping upload: file exceeds size limit"
                            );
                            continue;
                        }
                        tracing::info!(
                            target: "drive::sync",
                            path = %rel.display(),
                            db_size = db_entry.size,
                            local_size = local_size,
                            "Local file modified since last sync, uploading"
                        );
                        mount.task_queue.enqueue(TaskPayload::upload(local_path)).await?;
                    }
                }
            }
            (false, false, false) => {}
        }
    }

    tracing::info!(target: "drive::sync", id = %mount.id, "Full sync complete");
    Ok(())
}

/// Check if a path should be ignored based on the drive's ignore patterns.
async fn should_ignore(mount: &Mount, path: &Path) -> bool {
    let matcher = mount.ignore_matcher.read().await;
    matcher.is_match(path)
}

/// List all remote files recursively starting from `base`.
async fn list_remote_recursive(mount: &Mount, base: &CrUri) -> Result<Vec<FileResponse>> {
    let mut all = Vec::new();
    let mut dirs = vec![base.clone()];

    while let Some(dir) = dirs.pop() {
        let listing = mount
            .cr_client
            .list_files(&ListFileService {
                uri: dir.to_string(),
                page: None,
                page_size: Some(500),
                order_by: None,
                order_direction: None,
                next_page_token: None,
            })
            .await
            .context("Failed to list remote directory")?;

        for item in listing.files {
            if item.file_type == file_type::FOLDER {
                if let Ok(child_uri) = CrUri::new(&item.path) {
                    dirs.push(child_uri);
                }
            }
            all.push(item);
        }
    }

    Ok(all)
}

/// Walk local directory and collect all file paths (excluding directories).
fn walk_local(root: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    walk_dir_recursive(root, &mut files)?;
    Ok(files)
}

fn walk_dir_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir).context("Failed to read directory")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_dir_recursive(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify_debouncer_full::DebouncedEvent;
    use notify_debouncer_full::notify::event::{
        CreateKind, Event, EventKind, ModifyKind, RemoveKind,
    };
    use std::time::Instant;

    fn ev(kind: EventKind, path: &str) -> DebouncedEvent {
        DebouncedEvent::new(Event::new(kind).add_path(PathBuf::from(path)), Instant::now())
    }

    /// An editor that saves via "write temp + delete + recreate" must end up
    /// as a change, not a deletion — otherwise the file would vanish.
    #[test]
    fn delete_then_recreate_is_a_change() {
        let grouped = group_fs_events(vec![
            ev(EventKind::Remove(RemoveKind::File), "/sync/doc.txt"),
            ev(EventKind::Create(CreateKind::File), "/sync/doc.txt"),
        ]);

        assert_eq!(grouped.changed, vec![PathBuf::from("/sync/doc.txt")]);
        assert!(grouped.deleted.is_empty());
    }

    /// A file created then deleted within the debounce window is a deletion:
    /// it must not be uploaded.
    #[test]
    fn create_then_delete_is_a_deletion() {
        let grouped = group_fs_events(vec![
            ev(EventKind::Create(CreateKind::File), "/sync/tmp.bin"),
            ev(EventKind::Modify(ModifyKind::Any), "/sync/tmp.bin"),
            ev(EventKind::Remove(RemoveKind::File), "/sync/tmp.bin"),
        ]);

        assert!(grouped.changed.is_empty());
        assert_eq!(grouped.deleted, vec![PathBuf::from("/sync/tmp.bin")]);
    }

    /// Repeated modifications of the same file collapse into a single change.
    #[test]
    fn repeated_modifications_are_deduplicated() {
        let grouped = group_fs_events(vec![
            ev(EventKind::Modify(ModifyKind::Any), "/sync/log.txt"),
            ev(EventKind::Modify(ModifyKind::Any), "/sync/log.txt"),
            ev(EventKind::Modify(ModifyKind::Any), "/sync/log.txt"),
        ]);

        assert_eq!(grouped.changed.len(), 1);
        assert!(grouped.deleted.is_empty());
    }
}
