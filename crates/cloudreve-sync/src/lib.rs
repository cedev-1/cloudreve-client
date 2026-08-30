pub mod config;
pub mod drive;
pub mod events;
pub mod inventory;
pub mod logging;
pub mod tasks;
pub mod utils;

// The chunked uploader lives in its own crate so it can be shared with
// cloudreve-vfs without a dependency cycle. Re-exported here so every
// existing `cloudreve_sync::uploader::...` path keeps compiling.
pub use cloudreve_uploader as uploader;

// Re-export commonly used types
pub use config::{AppConfig, ConfigManager};
pub use drive::manager::{ConflictInfo, ConflictResolution, DriveInfo, DriveInfoStatus, DriveManager, StatusSummary, TaskWithProgress};
pub use drive::mounts::{Credentials, DriveConfig};
pub use events::{Event, EventBroadcaster, SummaryNotifier};
pub use logging::{LogConfig, LogGuard};

/// User agent string for HTTP requests
pub const USER_AGENT: &str = concat!("cloudreve-desktop/", env!("CARGO_PKG_VERSION"));

#[macro_use]
extern crate rust_i18n;

i18n!("../../locales");
