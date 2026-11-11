//! Disk space guards.
//!
//! A `FullMirror` drive mirrors the whole remote drive locally, so a remote
//! larger than the local volume would otherwise fill the disk until the
//! machine breaks — that is what [`fits_on_volume`]/[`available_space_for`]
//! below guard against, via `drive::sync::full_sync`'s pre-flight check.
//!
//! An `OnDemand` drive (phase 4) never mirrors anything: its on-disk
//! footprint is a bounded block cache instead, sized by
//! `drive::vfs_mode::effective_cache_cap` (D3) using [`available_space_for`]
//! and [`RESERVED_BYTES`] from this same module, but against a different
//! formula (`min(default cap, max(floor, available − reserve))`) — a small
//! cache is a degraded-but-safe outcome for that mode, not the
//! disk-filling failure mode this module's `fits_on_volume` exists to
//! prevent for a full mirror.

use std::path::Path;

/// Bytes deliberately left free on the volume so the OS and other apps keep
/// working even when the sync folder is close to filling the disk.
pub const RESERVED_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

/// Whether `required_bytes` can be written while keeping [`RESERVED_BYTES`]
/// free. Pure, so the decision is testable without a real volume.
pub fn fits_on_volume(required_bytes: u64, available_bytes: u64) -> bool {
    available_bytes.saturating_sub(RESERVED_BYTES) >= required_bytes
}

/// Space available to the current (non-privileged) user on the volume holding
/// `path`. `path` need not exist yet — the nearest existing ancestor is used.
pub fn available_space_for(path: &Path) -> std::io::Result<u64> {
    let base = path.ancestors().find(|p| p.exists()).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no existing ancestor for {}", path.display()),
        )
    })?;
    fs4::available_space(base)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GB: u64 = 1024 * 1024 * 1024;

    // These assertions are deliberately written with concrete byte counts
    // rather than in terms of RESERVED_BYTES: expressing them with the
    // constant would make them pass for *any* reserve, including none at all,
    // and the reserve is the whole point of the guard.

    #[test]
    fn a_file_leaving_a_comfortable_margin_fits() {
        // 10 GB volume, 5 GB file → ~5 GB still free afterwards.
        assert!(fits_on_volume(5 * GB, 10 * GB));
    }

    #[test]
    fn a_file_that_would_leave_the_volume_nearly_full_is_refused() {
        // 500 GB volume, 499.5 GB file: it technically fits, but would leave
        // half a gigabyte free — not enough headroom for the OS.
        let volume = 500 * GB;
        assert!(!fits_on_volume(volume - GB / 2, volume));
    }

    #[test]
    fn at_least_one_gigabyte_is_always_kept_free() {
        // Exactly at the boundary: a 4 GB volume must not accept more than 3 GB.
        assert!(fits_on_volume(3 * GB, 4 * GB));
        assert!(!fits_on_volume(3 * GB + 1, 4 * GB));
    }

    #[test]
    fn nothing_fits_on_a_volume_smaller_than_the_reserve() {
        // A 100 MB volume can never satisfy a 1 GiB reserve.
        assert!(!fits_on_volume(1, 100 * 1024 * 1024));
    }

    #[test]
    fn empty_file_fits_on_a_full_volume() {
        assert!(fits_on_volume(0, 0));
    }

    #[test]
    fn available_space_falls_back_to_the_nearest_existing_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("a/b/c/not-downloaded-yet.bin");

        let space = available_space_for(&missing).unwrap();

        assert!(space > 0, "temp volume should report some available space");
    }
}
