//! The app's data root, `~/.cloudreve`: drive list, inventory database,
//! settings, logs, server icons and on-demand caches all live under it.
//! Every path under it is built from [`app_data_root`], the one place that
//! decides where it is, so a test run can point the whole app somewhere
//! else: tests used to fill the developer's real `~/.cloudreve/vfs-cache`
//! with hundreds of drive caches nothing ever removed.
//!
//! In order, the root is:
//! - the directory a test process chose with [`redirect_for_tests`];
//! - `$CLOUDREVE_HOME`, when set and absolute (a test run, or a second
//!   profile by hand);
//! - in this crate's own unit tests, a directory under the system temp dir;
//! - `~/.cloudreve`.
//!
//! Not the Finder Sync extension's `folders.json`: the extension reads it
//! at a fixed path under the real home (see `cloudreve-finder-sync`), and it
//! is only written from inside the app bundle, never by a test.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::Result;

/// Environment variable overriding the data root (see the module doc).
pub const HOME_ENV: &str = "CLOUDREVE_HOME";

static TEST_ROOT: OnceLock<PathBuf> = OnceLock::new();

/// Where the app keeps its data (see the module doc for the order).
pub fn app_data_root() -> Result<PathBuf> {
    if let Some(root) = TEST_ROOT.get() {
        return Ok(root.clone());
    }
    if let Some(root) = std::env::var_os(HOME_ENV).map(PathBuf::from).filter(|p| p.is_absolute()) {
        return Ok(root);
    }
    #[cfg(test)]
    {
        Ok(std::env::temp_dir().join("cloudreve-unit-tests"))
    }
    #[cfg(not(test))]
    {
        use anyhow::Context;
        let home = dirs::home_dir().context("failed to resolve the user's home directory")?;
        Ok(home.join(".cloudreve"))
    }
}

/// Points [`app_data_root`] at `root` for the rest of this process. For
/// test harnesses: the first call wins (tests of one binary run in
/// parallel and share the process), later calls get that first root back.
/// Never set by the app itself.
pub fn redirect_for_tests(root: &Path) -> PathBuf {
    TEST_ROOT.get_or_init(|| root.to_path_buf()).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_tests_never_resolve_the_real_home() {
        let root = app_data_root().unwrap();
        let home = dirs::home_dir().unwrap();
        assert!(!root.starts_with(home.join(".cloudreve")), "unit tests write into {}", root.display());
    }
}
