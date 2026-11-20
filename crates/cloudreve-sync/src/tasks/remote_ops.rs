//! Server side of a local delete or move in a mirror drive: what runs
//! behind `TaskKind::Delete` and `TaskKind::Move`.
//!
//! The design, and why each check below exists, is documented on
//! `drive::propagate`. These executors re-check everything at the moment
//! they run rather than trusting what was true when the task was queued:
//! a queued task can wait for minutes (offline, app quit), and the local
//! file can come back or the server copy change in the meantime.
//!
//! Each one returns follow-up tasks instead of failing when the server
//! moved on (its newer version is downloaded, a move that cannot be made
//! becomes an upload plus a delete): no outcome loses a version.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use cloudreve_api::{
    ApiError, Client,
    api::ExplorerApi,
    models::{
        explorer::{
            CreateFileService, DeleteFileService, FileResponse, GetFileInfoService,
            MoveFileService, RenameFileService, file_type, metadata::UPLOAD_SESSION_ID,
        },
        uri::CrUri,
    },
};
use tracing::{info, warn};

use crate::drive::ignore::IgnoreMatcher;
use crate::drive::utils::{is_inside, local_path_to_cr_uri, remote_path_to_local_relative_path};
use crate::inventory::{FileMetadata, InventoryDb};
use crate::tasks::TaskPayload;

/// How many uris go in one `DELETE /file`: well under the server's batch
/// limit (`ValidateBatchFileCount`).
const DELETE_BATCH: usize = 100;

pub(crate) struct RemoteOps<'a> {
    pub inventory: Arc<InventoryDb>,
    pub cr_client: Arc<Client>,
    pub drive_id: &'a str,
    pub sync_path: PathBuf,
    pub remote_base: String,
    pub ignore: IgnoreMatcher,
    pub max_file_size_mb: u64,
}

/// The server no longer has the path (Cloudreve's "object not found", or
/// a plain 404).
fn is_not_found(err: &ApiError) -> bool {
    matches!(err, ApiError::ApiError { code, .. } if *code == 40016 || *code == 404)
}

/// The server's copy is exactly what this computer last synced: same
/// content (etag), no conflict pending, not being replaced by an upload
/// right now. Anything else means the server moved on since, and its
/// version wins over a local delete. An unknown etag counts as moved on.
fn server_unchanged(row: &FileMetadata, remote: &FileResponse) -> bool {
    let uploading = remote.metadata.as_ref().is_some_and(|m| m.contains_key(UPLOAD_SESSION_ID));
    row.conflict_state.is_none()
        && !uploading
        && !row.etag.is_empty()
        && remote.primary_entity.as_deref() == Some(row.etag.as_str())
}

fn download_of(local: &Path, remote: &FileResponse) -> TaskPayload {
    let mut payload = TaskPayload::download(local).with_totals(0, remote.size);
    if let Some(etag) = &remote.primary_entity {
        payload = payload.with_custom_state(serde_json::json!({ "remote_etag": etag }));
    }
    payload
}

/// `a` and `b` are the same file: what a case-only rename (`a.txt` →
/// `A.txt`) looks like on a case-insensitive volume, where the old name
/// still "exists".
fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

impl RemoteOps<'_> {
    /// The sync folder must be there: a vanished one (unmounted volume,
    /// deleted folder) makes every file look deleted.
    fn check_root(&self) -> Result<()> {
        if !self.sync_path.is_dir() {
            anyhow::bail!(
                "Sync folder is not reachable: {} — not touching the server",
                self.sync_path.display()
            );
        }
        Ok(())
    }

    fn check_inside(&self, path: &Path) -> Result<()> {
        if !is_inside(&self.sync_path, path) {
            anyhow::bail!("refusing to act on a path outside the sync folder: {}", path.display());
        }
        Ok(())
    }

    fn uri(&self, local: &Path) -> Result<CrUri> {
        local_path_to_cr_uri(local.to_path_buf(), self.sync_path.clone(), self.remote_base.clone())
    }

    /// The local path of a file the server listed, `None` when its path
    /// does not map safely under the sync folder.
    fn local_of(&self, remote_path: &str) -> Option<PathBuf> {
        let uri = CrUri::new(remote_path).ok()?;
        let base = CrUri::new(&self.remote_base).ok()?;
        remote_path_to_local_relative_path(&uri, &base).ok().map(|rel| self.sync_path.join(rel))
    }

    /// The tracked files at or under `path`.
    fn tracked_files_under(&self, path: &Path) -> Result<Vec<FileMetadata>> {
        let path_str = path.to_str().context("invalid path")?;
        Ok(self.inventory.query_under(path_str)?.into_iter().filter(|m| !m.is_folder).collect())
    }

    fn forget(&self, path: &Path) {
        if let Some(path_str) = path.to_str()
            && let Err(e) = self.inventory.batch_delete_by_path(vec![path_str])
        {
            warn!(target: "tasks::remote_ops", path = %path.display(), error = %format_args!("{e:#}"), "Failed to forget a deleted path in the inventory");
        }
    }

    /// What the server has at `uri`, `None` when it has nothing there.
    async fn info(&self, uri: &CrUri) -> Result<Option<FileResponse>> {
        match self
            .cr_client
            .get_file_info(&GetFileInfoService { uri: Some(uri.to_string()), id: None, extended: None, folder_summary: None })
            .await
        {
            Ok(remote) => Ok(Some(remote)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context("failed to get remote file info")),
        }
    }

    /// Into the server's trash: neither `skip_soft_delete` nor `unlink`,
    /// so a mistake is one "Restore" away.
    async fn trash(&self, uris: Vec<String>) -> Result<()> {
        for chunk in uris.chunks(DELETE_BATCH) {
            self.cr_client
                .delete_files(&DeleteFileService { uris: chunk.to_vec(), unlink: None, skip_soft_delete: None })
                .await
                .context("failed to delete on the server")?;
        }
        Ok(())
    }

    /// `local` was deleted on this computer: delete it on the server too —
    /// only what the server still has exactly as last synced.
    pub async fn delete(&self, local: &Path) -> Result<Vec<TaskPayload>> {
        self.check_root()?;
        self.check_inside(local)?;
        if self.ignore.is_match(local) {
            // Junk never causes a server delete, however it was queued.
            return Ok(Vec::new());
        }
        if local.exists() {
            info!(target: "tasks::remote_ops", path = %local.display(), "Not deleting on the server: the file is back");
            return Ok(Vec::new());
        }
        let rows = self.tracked_files_under(local)?;
        if rows.is_empty() {
            // Never synced from here: not this computer's to delete.
            return Ok(Vec::new());
        }
        let uri = self.uri(local)?;
        let Some(remote) = self.info(&uri).await? else {
            self.forget(local);
            return Ok(Vec::new());
        };

        if remote.file_type == file_type::FOLDER {
            return self.delete_folder(local, &uri, rows).await;
        }
        let Some(row) = rows.iter().find(|r| Path::new(&r.local_path) == local) else {
            // A folder here, a file on the server: not the same thing.
            return Ok(Vec::new());
        };
        if !server_unchanged(row, &remote) {
            info!(target: "tasks::remote_ops", path = %local.display(), "Not deleting on the server: it changed there since the last sync, downloading it back");
            return Ok(vec![download_of(local, &remote)]);
        }
        self.trash(vec![uri.to_string()]).await?;
        info!(target: "tasks::remote_ops", drive = self.drive_id, path = %local.display(), "Deleted on the server (moved to its trash)");
        self.forget(local);
        Ok(Vec::new())
    }

    /// The whole folder in one item when everything the server holds in
    /// it is exactly what this computer had; otherwise only those files,
    /// and the rest (added or changed on the server since) comes back.
    async fn delete_folder(&self, local: &Path, uri: &CrUri, rows: Vec<FileMetadata>) -> Result<Vec<TaskPayload>> {
        let listed = crate::drive::sync::list_remote_recursive(&self.cr_client, uri).await?;
        let mut matched: Vec<(String, PathBuf)> = Vec::new();
        let mut follow_ups = Vec::new();
        for remote in listed.iter().filter(|f| f.file_type != file_type::FOLDER) {
            let Some(path) = self.local_of(&remote.path) else { continue };
            match rows.iter().find(|r| Path::new(&r.local_path) == path) {
                Some(row) if server_unchanged(row, remote) => matched.push((remote.path.clone(), path)),
                _ => follow_ups.push(download_of(&path, remote)),
            }
        }

        if follow_ups.is_empty() {
            self.trash(vec![uri.to_string()]).await?;
            info!(target: "tasks::remote_ops", drive = self.drive_id, path = %local.display(), files = matched.len(), "Deleted a folder on the server (moved to its trash)");
            self.forget(local);
            return Ok(Vec::new());
        }

        info!(target: "tasks::remote_ops", path = %local.display(), kept = follow_ups.len(), "Not deleting a whole folder on the server: it holds files this computer never had in that state");
        if !matched.is_empty() {
            self.trash(matched.iter().map(|(u, _)| u.clone()).collect()).await?;
        }
        for row in &rows {
            let path = Path::new(&row.local_path);
            let still_listed = listed.iter().any(|f| self.local_of(&f.path).as_deref() == Some(path));
            if !still_listed || matched.iter().any(|(_, p)| p == path) {
                self.forget(path);
            }
        }
        Ok(follow_ups)
    }

    /// `from` was renamed or moved to `to` on this computer: do the same on
    /// the server, so it keeps one copy (with its history and shares)
    /// instead of gaining a second one.
    pub async fn move_to(&self, from: &Path, to: &Path) -> Result<Vec<TaskPayload>> {
        self.check_root()?;
        self.check_inside(from)?;
        self.check_inside(to)?;
        if from.exists() && !same_file(from, to) {
            // The old name came back: `to` is new content.
            return Ok(self.uploads_under(to).await);
        }
        if !to.exists() {
            // Moved on again, or deleted: what is left to do is the delete.
            return Ok(vec![TaskPayload::delete(from)]);
        }
        let rows = self.tracked_files_under(from)?;
        if rows.is_empty() {
            return Ok(self.uploads_under(to).await);
        }
        let from_uri = self.uri(from)?;
        let Some(remote) = self.info(&from_uri).await? else {
            self.forget(from);
            return Ok(self.uploads_under(to).await);
        };
        if remote.file_type != file_type::FOLDER
            && let Some(row) = rows.iter().find(|r| Path::new(&r.local_path) == from)
            && !server_unchanged(row, &remote)
        {
            // Changed on the server since: keep that version under the old
            // name, and this one under the new name.
            info!(target: "tasks::remote_ops", from = %from.display(), to = %to.display(), "Not moving on the server: the file changed there since the last sync");
            let mut follow_ups = vec![download_of(from, &remote)];
            follow_ups.extend(self.uploads_under(to).await);
            return Ok(follow_ups);
        }

        match self.server_move(&from_uri, from, to).await {
            Ok(()) => {}
            Err(MoveError { error, landed }) => {
                if crate::tasks::queue::is_network_error(&error) && landed.is_none() {
                    return Err(error);
                }
                warn!(target: "tasks::remote_ops", from = %from.display(), to = %to.display(), error = %format_args!("{error:#}"), "Could not move on the server, uploading the new name and deleting the old one instead");
                let old = match landed {
                    Some(landed) => {
                        self.rename_rows(from, &landed);
                        landed
                    }
                    None => from.to_path_buf(),
                };
                let mut follow_ups = self.uploads_under(to).await;
                follow_ups.push(TaskPayload::delete(old));
                return Ok(follow_ups);
            }
        }
        info!(target: "tasks::remote_ops", drive = self.drive_id, from = %from.display(), to = %to.display(), "Moved on the server");
        self.rename_rows(from, to);
        // Whatever changed in it on the way (edited, added) still goes up.
        Ok(self.uploads_under(to).await)
    }

    fn rename_rows(&self, from: &Path, to: &Path) {
        if let (Some(f), Some(t)) = (from.to_str(), to.to_str())
            && let Err(e) = self.inventory.rename_path(f, t)
        {
            warn!(target: "tasks::remote_ops", from = %from.display(), to = %to.display(), error = %format_args!("{e:#}"), "Failed to move inventory rows");
        }
    }

    /// Same folder: one rename. Another folder: a move into it (created
    /// first if the server lacks it), then a rename if the name changed
    /// too. `landed` tells where the item is if the rename failed after a
    /// successful move.
    async fn server_move(&self, from_uri: &CrUri, from: &Path, to: &Path) -> std::result::Result<(), MoveError> {
        let fail = |error: anyhow::Error| MoveError { error, landed: None };
        let (Some(from_parent), Some(to_parent)) = (from.parent(), to.parent()) else {
            return Err(fail(anyhow::anyhow!("no parent folder")));
        };
        let (Some(old_name), Some(new_name)) = (from.file_name(), to.file_name()) else {
            return Err(fail(anyhow::anyhow!("no file name")));
        };
        let new_name = new_name.to_string_lossy().to_string();

        if from_parent == to_parent {
            self.cr_client
                .rename_file(&RenameFileService { uri: from_uri.to_string(), new_name })
                .await
                .map_err(|e| fail(anyhow::Error::from(e).context("rename failed")))?;
            return Ok(());
        }

        let dst = self.uri(to_parent).map_err(fail)?;
        if to_parent != self.sync_path {
            self.cr_client
                .create_file(&CreateFileService {
                    uri: dst.to_string(),
                    file_type: "folder".to_string(),
                    err_on_conflict: Some(false),
                    metadata: None,
                })
                .await
                .map_err(|e| fail(anyhow::Error::from(e).context("failed to create the destination folder")))?;
        }
        self.cr_client
            .move_files(&MoveFileService { uris: vec![from_uri.to_string()], dst: dst.to_string(), copy: None })
            .await
            .map_err(|e| fail(anyhow::Error::from(e).context("move failed")))?;

        if old_name.to_string_lossy() != new_name {
            let landed = to_parent.join(old_name);
            let landed_uri = self.uri(&landed).map_err(fail)?;
            if let Err(e) = self.cr_client.rename_file(&RenameFileService { uri: landed_uri.to_string(), new_name }).await {
                return Err(MoveError { error: anyhow::Error::from(e).context("rename after move failed"), landed: Some(landed) });
            }
        }
        Ok(())
    }

    /// Uploads for whatever at or under `path` the server does not have in
    /// its current state: untracked, or changed since the last sync. Junk
    /// files and files over the size limit are left out, like every upload.
    async fn uploads_under(&self, path: &Path) -> Vec<TaskPayload> {
        let files = if path.is_dir() {
            match crate::drive::sync::walk_local(path.to_path_buf()).await {
                Ok(files) => files,
                Err(e) => {
                    warn!(target: "tasks::remote_ops", path = %path.display(), error = %format_args!("{e:#}"), "Failed to list a moved folder");
                    return Vec::new();
                }
            }
        } else if path.is_file() {
            vec![path.to_path_buf()]
        } else {
            Vec::new()
        };
        let max_bytes = self.max_file_size_mb.saturating_mul(1024 * 1024);
        files
            .into_iter()
            .filter(|f| !self.ignore.is_match(f))
            .filter(|f| self.max_file_size_mb == 0 || std::fs::metadata(f).is_ok_and(|m| m.len() <= max_bytes))
            .filter(|f| {
                let row = f.to_str().and_then(|s| self.inventory.query_by_path(s).ok().flatten());
                row.is_none_or(|row| row.is_locally_modified(f))
            })
            .map(TaskPayload::upload)
            .collect()
    }
}

struct MoveError {
    error: anyhow::Error,
    landed: Option<PathBuf>,
}
