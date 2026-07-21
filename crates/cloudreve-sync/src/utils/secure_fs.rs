//! Helpers for persisting sensitive data (OAuth tokens, credentials) with
//! restrictive filesystem permissions so other local users cannot read them.

use anyhow::{Context, Result};
use std::path::Path;

/// Write `contents` to `path`, ensuring the file is only readable and writable
/// by the current user (mode `0600` on Unix).
///
/// The permissions are applied on every write so a file created before this
/// hardening (with the default `0644` umask) is tightened on the next save.
pub fn write_private<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> Result<()> {
    let path = path.as_ref();
    std::fs::write(path, contents)
        .with_context(|| format!("Failed to write file {}", path.display()))?;
    restrict_file(path)?;
    Ok(())
}

/// Restrict an existing file to owner-only read/write (`0600`) on Unix.
/// No-op on non-Unix platforms.
pub fn restrict_file<P: AsRef<Path>>(path: P) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let path = path.as_ref();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to set permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn credential_file_is_written_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("drives.json");
        write_private(&file, b"{\"token\":\"secret\"}").unwrap();
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "credential file must not be group/world readable");
    }

    #[test]
    fn rewriting_tightens_existing_world_readable_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("drives.json");
        std::fs::write(&file, b"old").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&file, b"new").unwrap();
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}

/// Restrict a directory to owner-only access (`0700`) on Unix so its contents
/// (config, credentials) are not enumerable by other local users.
/// No-op on non-Unix platforms.
pub fn restrict_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let path = path.as_ref();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("Failed to set permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
