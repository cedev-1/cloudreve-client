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
        .map(|p| p.strip_prefix(local_root).unwrap().to_path_buf())
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
                    if !mount.is_file_size_allowed(rf.size as u64).await {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            size = rf.size,
                            "Skipping download: file exceeds size limit"
                        );
                        continue;
                    }
                    mount.task_queue.enqueue(
                        TaskPayload::download(local_path).with_totals(0, rf.size)
                    ).await?;
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
                        .with_size(rf.size);
                    let _ = mount.inventory.upsert(&entry);
                }
            }
            // Already tracked and present everywhere → check for modifications
            (true, true, true) => {
                if let (Some(db_entry), Some(rf)) = (db_map.get(rel), remote_map.get(rel)) {
                    // Check if remote was modified (etag changed)
                    let remote_etag = rf.primary_entity.clone().unwrap_or_default();
                    if !remote_etag.is_empty() && !db_entry.etag.is_empty() && remote_etag != db_entry.etag {
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
                        mount.task_queue.enqueue(
                            TaskPayload::download(local_path).with_totals(0, rf.size)
                        ).await?;
                    } else if let Ok(meta) = std::fs::metadata(&local_path) {
                        // Check if local file was modified (size changed)
                        let local_size = meta.len() as i64;
                        if local_size != db_entry.size {
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
