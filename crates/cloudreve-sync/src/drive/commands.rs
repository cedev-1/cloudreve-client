use crate::drive::sync::SyncMode;
use cloudreve_api::models::user::Token;
use std::path::PathBuf;

/// Commands sent to the DriveManager
#[derive(Debug)]
pub enum ManagerCommand {
    /// View a file/folder online
    ViewOnline { path: PathBuf },
    /// Persist drive configurations to disk
    PersistConfig,
    /// Trigger sync for a set of paths
    SyncNow { paths: Vec<PathBuf>, mode: SyncMode },
}

/// Commands sent to an individual Mount
#[derive(Debug)]
pub enum MountCommand {
    /// Sync a set of local paths
    Sync {
        local_paths: Vec<PathBuf>,
        mode: SyncMode,
        user_initiated: bool,
    },
    /// Trigger a full bidirectional sync
    FullSync,
    /// Update credentials after a refresh
    RefreshCredentials { credentials: Token },
    /// Credentials are invalid (401)
    CredentialInvalid,
}
