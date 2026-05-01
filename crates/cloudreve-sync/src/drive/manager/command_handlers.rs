use super::DriveManager;
use crate::drive::commands::{ManagerCommand, MountCommand};
use crate::drive::utils::{local_path_to_cr_uri, view_online_url};
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::mpsc;

impl DriveManager {
    /// Spawn the command processor task
    pub async fn spawn_command_processor(self: &Arc<Self>) {
        let mut command_rx_guard = self.command_rx.lock().await;
        if let Some(command_rx) = command_rx_guard.take() {
            let manager = self.clone();
            let handle = tokio::spawn(async move {
                Self::process_commands(manager, command_rx).await;
            });
            *self.processor_handle.lock().await = Some(handle);
        }
    }

    pub(super) async fn process_commands(
        manager: Arc<Self>,
        mut command_rx: mpsc::UnboundedReceiver<ManagerCommand>,
    ) {
        tracing::info!(target: "drive::manager", "Command processor started");

        while let Some(command) = command_rx.recv().await {
            tracing::trace!(target: "drive::manager", command = ?command, "Processing command");
            let manager = manager.clone();
            match command {
                ManagerCommand::ViewOnline { path } => {
                    spawn(async move {
                        if let Err(e) = manager.handle_view_online(path.clone()).await {
                            tracing::error!(target: "drive::manager", path = %path.display(), error = %e, "ViewOnline failed");
                        }
                    });
                }
                ManagerCommand::PersistConfig => {
                    if let Err(e) = manager.persist().await {
                        tracing::error!(target: "drive::manager", error = %e, "Failed to persist config");
                    }
                }
                ManagerCommand::SyncNow { paths, mode } => {
                    if paths.is_empty() {
                        tracing::error!(target: "drive::manager", "No paths provided for SyncNow");
                        continue;
                    }
                    spawn(async move {
                        let first = paths.first().unwrap().to_str().unwrap_or("");
                        if let Some(drive) = manager.search_drive_by_child_path(first).await {
                            let _ = drive.command_tx.send(MountCommand::Sync {
                                local_paths: paths,
                                mode,
                                user_initiated: true,
                            });
                        } else {
                            tracing::error!(target: "drive::manager", path = %first, "No drive found for SyncNow");
                        }
                    });
                }
            }
        }

        tracing::info!(target: "drive::manager", "Command processor stopped");
    }

    pub(super) async fn handle_view_online(&self, path: PathBuf) -> Result<()> {
        let mount = self
            .search_drive_by_child_path(path.to_str().unwrap_or(""))
            .await
            .ok_or_else(|| anyhow::anyhow!("No drive found for path: {:?}", path))?;

        let file_meta = self
            .inventory
            .query_by_path(path.to_str().unwrap_or(""))
            .context("Failed to query file metadata")?;

        let config = mount.get_config().await;
        let uri = local_path_to_cr_uri(path.clone(), config.sync_path.clone(), config.remote_path.clone())
            .context("failed to convert local path to uri")?
            .to_string();

        let url = match file_meta {
            None => view_online_url(&config.remote_path, None, &config)?,
            Some(ref meta) if meta.is_folder => view_online_url(&uri, None, &config)?,
            Some(_) => {
                use cloudreve_api::models::uri::CrUri;
                let parent_path = CrUri::new(&uri)?.parent()?.to_string();
                view_online_url(&parent_path, Some(&uri), &config)?
            }
        };

        open::that(url)?;
        Ok(())
    }
}
