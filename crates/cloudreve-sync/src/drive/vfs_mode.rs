//! On-demand VFS orchestration for one drive (phase 4, task 4): the
//! per-drive cache directory layout, the effective on-disk cache cap (D3),
//! and the mount/unmount seam [`Mount`](crate::drive::mounts::Mount) goes
//! through for its on-demand lifecycle branches (D2/D5).
//!
//! ## The injection seam
//!
//! `cloudreve-sync`'s own integration tests build a real `Mount` against a
//! real (temp-dir) `sync_path`, but must never actually attach a real NFS/
//! FUSE mount at the OS level — that would need root/OS mount privileges
//! this test suite must not require, and is already end-to-end proven by
//! `cloudreve-vfs`'s own `tests/mounted_{macos,linux}.rs` (re-proven again
//! at the plan's level-3 checklist gate). [`attach`]/[`detach`] are the
//! ONLY two functions in this module that ever call into
//! `cloudreve_vfs::mount` — both take an `Option<&Arc<MountTestHook>>`,
//! and when a test installs one (`Mount::install_vfs_mount_hook_for_tests`,
//! called BEFORE `start()`/`pause()`/a resume), the real OS call is skipped
//! entirely and the request is recorded on the hook instead
//! ([`MountSeamCall`]). Everything else in this module — cache-dir layout,
//! the D3 cap formula, and [`build_vfs`] (which constructs the `Vfs`
//! facade itself: local block-cache/draft-store I/O only, no OS mount) —
//! runs identically in tests and in production, so only the genuinely
//! OS-touching sliver is ever faked.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use cloudreve_vfs::mount::{MountedVfs, cleanup_stale_mount, mount as os_mount};
use cloudreve_vfs::vfs::{DEFAULT_CACHE_MAX_BYTES, Vfs, VfsEvent};

use crate::utils::disk_space::{RESERVED_BYTES, available_space_for};

/// Floor the effective cache cap never drops below, regardless of how
/// little space remains on the volume (D3) — small enough to still be a
/// real constraint on a nearly-full disk, large enough that on-demand
/// reads don't thrash the cache on every file switch.
pub const CACHE_FLOOR_BYTES: u64 = 512 * 1024 * 1024; // 512 MiB

/// Per-drive on-demand cache directory: `~/.cloudreve/vfs-cache/<drive_id>/`.
/// Distinct from `DriveManager`'s own `~/.cloudreve` config dir (drive
/// credentials) and from a `FullMirror` drive's `sync_path` (a real mirror
/// on disk) — this is purely `Vfs::new`'s block-cache/draft-store root plus
/// the mount-identity marker `cleanup_stale_mount` reads.
pub fn cache_dir_for(drive_id: &str) -> Result<PathBuf> {
    let home = dirs::home_dir().context("failed to resolve the user's home directory")?;
    Ok(home.join(".cloudreve").join("vfs-cache").join(drive_id))
}

/// Pure D3 formula: `min(default, max(floor, available − reserve))`.
/// Factored out from [`effective_cache_cap`] so it can be pinned by a unit
/// test against concrete byte counts, independent of any real filesystem
/// call — see this module's tests for why those numbers are never
/// expressed in terms of the constants under test.
fn clamp_cache_cap(available_bytes: u64) -> u64 {
    let after_reserve = available_bytes.saturating_sub(RESERVED_BYTES);
    after_reserve.clamp(CACHE_FLOOR_BYTES, DEFAULT_CACHE_MAX_BYTES)
}

/// Effective on-disk cache cap for the volume holding `cache_dir` (D3).
/// Falls back to the unclamped default on a disk-space query failure — an
/// unreadable volume must not block the mount outright over a cap it can
/// still pick a safe (if optimistic) value for; the real mount attempt
/// that follows will fail loudly on its own if the volume is genuinely
/// unusable.
pub fn effective_cache_cap(cache_dir: &Path) -> u64 {
    match available_space_for(cache_dir) {
        Ok(available) => clamp_cache_cap(available),
        Err(err) => {
            tracing::warn!(
                target: "drive::vfs_mode",
                cache_dir = %cache_dir.display(),
                %err,
                "failed to query available disk space for the on-demand cache; using the default cap"
            );
            DEFAULT_CACHE_MAX_BYTES
        }
    }
}

/// Whether `cap` reflects D3's clamp having kicked in (below the
/// unclamped default) — callers use this to decide whether to warn the
/// user that their effective cache is smaller than the default.
pub fn is_clamped(cap: u64) -> bool {
    cap < DEFAULT_CACHE_MAX_BYTES
}

/// Ensures `mountpoint` exists and is empty. An on-demand drive's
/// mountpoint is a pure OS attach target, never a mirror — unlike a
/// `FullMirror` drive's `sync_path`, nothing is ever written there by this
/// crate directly, so anything already present is either leftover from a
/// mode switch or a user mistake; either way, mounting on top of it would
/// silently shadow those files rather than surface the conflict.
///
/// Callers MUST run [`cleanup_stale_mount`] first (this is [`attach`]'s
/// job, not this function's — see its doc): a stale NFS/FUSE mount left
/// over from a crash makes this function's `read_dir` either list the
/// remote's old listing (looking "not empty" when the directory would be a
/// plain empty dir once unmounted) or hang/error against a dead backend.
/// Checking emptiness before pre-cleaning defeats crash recovery — the
/// review finding this ordering fixes. Private: not meant to be called
/// from outside [`attach`] any more (it used to be called eagerly by
/// `Mount::start_on_demand`/`remount_on_demand`, before pre-clean ever
/// ran — that was the bug).
fn ensure_mountpoint_ready(mountpoint: &Path) -> Result<()> {
    std::fs::create_dir_all(mountpoint).with_context(|| {
        format!("failed to create the on-demand mountpoint {}", mountpoint.display())
    })?;
    let has_entries = std::fs::read_dir(mountpoint)
        .with_context(|| format!("failed to inspect the on-demand mountpoint {}", mountpoint.display()))?
        .next()
        .is_some();
    if has_entries {
        anyhow::bail!(
            "on-demand mountpoint {} is not empty — move its contents aside before enabling \
             on-demand mode for this drive",
            mountpoint.display()
        );
    }
    Ok(())
}

/// Constructs the on-demand `Vfs` facade for one drive. Real local I/O only
/// (block cache, draft store under `cache_dir`) — runs identically in
/// tests and in production; only [`attach`]/[`detach`] below ever touch
/// the OS.
pub fn build_vfs(
    cr_client: Arc<cloudreve_api::Client>,
    remote_path: String,
    cache_dir: &Path,
    cache_max_bytes: u64,
) -> Result<(Arc<Vfs>, mpsc::UnboundedReceiver<VfsEvent>)> {
    std::fs::create_dir_all(cache_dir).with_context(|| {
        format!("failed to create the on-demand cache directory {}", cache_dir.display())
    })?;
    let (vfs, events) = Vfs::new(cr_client, remote_path, cache_dir, cache_max_bytes)
        .context("failed to initialize the on-demand vfs")?;
    Ok((Arc::new(vfs), events))
}

/// One request this crate made toward attaching or detaching an on-demand
/// mount at the OS level — what [`MountTestHook`] records. `cache_max_bytes`
/// is carried on `Mount` even though the OS attach call itself doesn't need
/// it (it was already baked into the `Vfs` by [`build_vfs`]) because it's
/// meaningful metadata about what the request was for, and D3's unit tests
/// aside, this is the only place the two ever travel together for a test to
/// observe.
///
/// `PreClean` (review finding 4) is recorded alongside the REAL
/// `cleanup_stale_mount` call — that call is never faked, hooked or not
/// (see [`attach`]'s doc for why it's safe to always run for real) — purely
/// so a test can observe that it happened, and in particular that it
/// happened BEFORE the mountpoint's emptiness was ever checked: under the
/// bug this fixed, a non-empty mountpoint made `attach` bail before
/// `cleanup_stale_mount` ever ran, so `PreClean` would never appear in
/// [`MountTestHook::calls`] at all in that case. See
/// `attach_pre_cleans_before_checking_the_mountpoint_is_empty` below.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountSeamCall {
    PreClean { mountpoint: PathBuf },
    Mount { mountpoint: PathBuf, cache_max_bytes: u64 },
    Unmount { mountpoint: PathBuf },
}

/// Test-only substitute for the real `cloudreve_vfs::mount` calls
/// [`attach`]/[`detach`] make — see this module's top doc for the seam's
/// design. Install one per-`Mount` via
/// `Mount::install_vfs_mount_hook_for_tests` before `start()`/`pause()`/a
/// resume for it to take effect on that call; `None` (every production
/// path) means the real OS mount/unmount runs.
#[doc(hidden)]
#[derive(Default)]
pub struct MountTestHook {
    calls: std::sync::Mutex<Vec<MountSeamCall>>,
}

impl MountTestHook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, call: MountSeamCall) {
        self.calls.lock().expect("MountTestHook mutex poisoned").push(call);
    }

    /// Every call recorded so far, in order.
    pub fn calls(&self) -> Vec<MountSeamCall> {
        self.calls.lock().expect("MountTestHook mutex poisoned").clone()
    }

    pub fn mount_count(&self) -> usize {
        self.calls().iter().filter(|c| matches!(c, MountSeamCall::Mount { .. })).count()
    }

    pub fn unmount_count(&self) -> usize {
        self.calls().iter().filter(|c| matches!(c, MountSeamCall::Unmount { .. })).count()
    }

    pub fn pre_clean_count(&self) -> usize {
        self.calls().iter().filter(|c| matches!(c, MountSeamCall::PreClean { .. })).count()
    }
}

/// Attaches `vfs` at `mountpoint` — the OS-level half of the on-demand
/// lifecycle. Used both for the initial mount and for a resume-after-pause
/// remount (same `vfs` instance, re-attached — see
/// `Mount::remount_on_demand`).
///
/// Order matters (review finding 4 — D5's crash recovery): pre-clean via
/// [`cleanup_stale_mount`] runs FIRST, unconditionally — never hook-gated,
/// hooked or not — and ONLY THEN is the mountpoint's emptiness checked
/// ([`ensure_mountpoint_ready`]). Running the checks in the OPPOSITE order
/// (as this function used to) defeats crash recovery entirely: a stale
/// mount left over from an unclean shutdown makes the emptiness check
/// either bail on the remote's old listing or hang/error against the dead
/// backend, before `cleanup_stale_mount` — the thing that exists
/// specifically to recover from that — ever gets a chance to run.
///
/// `cleanup_stale_mount` itself is always the REAL call, even under test:
/// it only ever lists the OS's own mount table (`/sbin/mount` on macOS,
/// `/proc/mounts` on Linux) and, if nothing matches `mountpoint`, is a
/// harmless read-only no-op — exactly what happens against every test's
/// plain, never-really-mounted temp directory. Only the genuinely
/// privileged, OS-mutating step — the actual attach via [`os_mount`] — is
/// ever faked by [`MountTestHook`], and only once the mountpoint is
/// confirmed empty.
pub async fn attach(
    vfs: Arc<Vfs>,
    mountpoint: &Path,
    volume_name: &str,
    cache_dir: &Path,
    cache_max_bytes: u64,
    hook: Option<&Arc<MountTestHook>>,
) -> Result<Option<MountedVfs>> {
    if let Some(hook) = hook {
        hook.record(MountSeamCall::PreClean { mountpoint: mountpoint.to_path_buf() });
    }
    if let Err(err) = cleanup_stale_mount(mountpoint, cache_dir).await {
        tracing::warn!(
            target: "drive::vfs_mode",
            mountpoint = %mountpoint.display(),
            ?err,
            "vfs mode: pre-clean failed, attempting to mount anyway"
        );
    }

    ensure_mountpoint_ready(mountpoint)?;

    if let Some(hook) = hook {
        hook.record(MountSeamCall::Mount { mountpoint: mountpoint.to_path_buf(), cache_max_bytes });
        return Ok(None);
    }

    let mounted = os_mount(vfs, mountpoint, volume_name, cache_dir)
        .await
        .with_context(|| format!("failed to mount the on-demand vfs at {}", mountpoint.display()))?;
    Ok(Some(mounted))
}

/// Detaches an on-demand mount (D5: pause/shutdown/delete). `mounted` is
/// `None` both under test (the hook already intercepted the matching
/// [`attach`], so nothing real was ever attached) and legitimately in
/// production if there was nothing to tear down.
pub async fn detach(
    mounted: Option<MountedVfs>,
    mountpoint: &Path,
    hook: Option<&Arc<MountTestHook>>,
) -> Result<()> {
    if let Some(hook) = hook {
        hook.record(MountSeamCall::Unmount { mountpoint: mountpoint.to_path_buf() });
        return Ok(());
    }
    if let Some(mounted) = mounted {
        mounted.unmount().await.with_context(|| {
            format!("failed to unmount the on-demand vfs at {}", mountpoint.display())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    // Deliberately concrete numbers throughout, never expressed in terms of
    // `RESERVED_BYTES`/`CACHE_FLOOR_BYTES`/`DEFAULT_CACHE_MAX_BYTES`: doing
    // so would make these pass for *any* value of those constants,
    // including degenerate ones, and defeat the point of pinning the D3
    // formula.

    /// 3 GiB free, 1 GiB reserve -> 2 GiB cap (comfortably between the
    /// floor and the default, so neither clamp fires).
    #[test]
    fn three_gib_free_yields_a_two_gib_cap() {
        assert_eq!(clamp_cache_cap(3 * GIB), 2 * GIB);
    }

    /// 100 TiB free -> capped at the 10 GiB default; a huge volume must
    /// never hand the on-demand cache an effectively unbounded budget.
    #[test]
    fn a_huge_volume_is_capped_at_the_default() {
        assert_eq!(clamp_cache_cap(100 * TIB), DEFAULT_CACHE_MAX_BYTES);
        assert_eq!(DEFAULT_CACHE_MAX_BYTES, 10 * GIB);
    }

    /// 700 MiB free is less than the 1 GiB reserve, so the naive
    /// subtraction would go negative; the 512 MiB floor kicks in instead
    /// of collapsing to zero (which would make the on-demand cache
    /// useless).
    #[test]
    fn a_nearly_full_volume_is_held_at_the_floor() {
        assert_eq!(clamp_cache_cap(700 * MIB), CACHE_FLOOR_BYTES);
        assert_eq!(CACHE_FLOOR_BYTES, 512 * MIB);
    }

    /// Exactly at the reserve boundary: nothing left after the reserve, so
    /// the floor applies.
    #[test]
    fn exactly_the_reserve_amount_free_is_held_at_the_floor() {
        assert_eq!(clamp_cache_cap(RESERVED_BYTES), CACHE_FLOOR_BYTES);
    }

    /// A cap equal to the default is NOT considered clamped — only a cap
    /// strictly below it reflects the D3 clamp having actually fired.
    #[test]
    fn the_unclamped_default_is_not_reported_as_clamped() {
        assert!(!is_clamped(DEFAULT_CACHE_MAX_BYTES));
    }

    #[test]
    fn a_floored_cap_is_reported_as_clamped() {
        assert!(is_clamped(CACHE_FLOOR_BYTES));
    }

    #[test]
    fn mount_test_hook_counts_each_call_kind_independently() {
        let hook = MountTestHook::new();
        assert_eq!(hook.mount_count(), 0);
        assert_eq!(hook.unmount_count(), 0);

        hook.record(MountSeamCall::Mount {
            mountpoint: PathBuf::from("/tmp/a"),
            cache_max_bytes: GIB,
        });
        hook.record(MountSeamCall::Unmount { mountpoint: PathBuf::from("/tmp/a") });
        hook.record(MountSeamCall::Mount {
            mountpoint: PathBuf::from("/tmp/a"),
            cache_max_bytes: GIB,
        });

        assert_eq!(hook.mount_count(), 2);
        assert_eq!(hook.unmount_count(), 1);
        assert_eq!(hook.calls().len(), 3);
    }

    #[test]
    fn ensure_mountpoint_ready_creates_a_missing_directory() {
        let base = tempfile::tempdir().unwrap();
        let mountpoint = base.path().join("mnt");
        assert!(!mountpoint.exists());

        ensure_mountpoint_ready(&mountpoint).unwrap();

        assert!(mountpoint.is_dir());
    }

    #[test]
    fn ensure_mountpoint_ready_accepts_an_existing_empty_directory() {
        let base = tempfile::tempdir().unwrap();
        ensure_mountpoint_ready(base.path()).unwrap();
    }

    #[test]
    fn ensure_mountpoint_ready_refuses_a_non_empty_directory() {
        let base = tempfile::tempdir().unwrap();
        std::fs::write(base.path().join("leftover.txt"), b"hi").unwrap();

        let err = ensure_mountpoint_ready(base.path()).unwrap_err();
        assert!(err.to_string().contains("not empty"));
    }

    /// Review finding 4: `attach` must run `cleanup_stale_mount` BEFORE it
    /// ever checks the mountpoint's emptiness — the opposite order defeats
    /// crash recovery (a stale mount left over from a crash would make the
    /// emptiness check bail, or hang/error, before pre-clean ever runs).
    ///
    /// Pinned WITHOUT a real stale mount (this crate's tests never touch
    /// the OS — see the module doc): the mountpoint here is plain and
    /// non-empty, so `cleanup_stale_mount` finds nothing to clean and is a
    /// no-op either way — but ONLY under the correct (pre-clean-first)
    /// order does it get a chance to RUN at all before `attach` bails.
    /// Under the bug this fixes (empty-check first), `attach` would return
    /// before `cleanup_stale_mount` — and so `PreClean` — was ever reached,
    /// so `hook.pre_clean_count()` would stay `0`. This is the "unit-level
    /// ordering assertion" the review's finding 4 asked for when the real
    /// crash-recovery scenario itself isn't reachable from this crate's
    /// seam (that scenario is `cloudreve-vfs`'s own
    /// `tests/mounted_{macos,linux}.rs`).
    #[tokio::test]
    async fn attach_pre_cleans_before_checking_the_mountpoint_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mountpoint = dir.path().join("mnt");
        std::fs::create_dir_all(&mountpoint).unwrap();
        std::fs::write(mountpoint.join("leftover.txt"), b"stale").unwrap();
        let cache_dir = dir.path().join("cache");

        let client = Arc::new(cloudreve_api::Client::new(cloudreve_api::ClientConfig::new(
            "http://127.0.0.1:1",
        )));
        let (vfs, _events) =
            build_vfs(client, "cloudreve://my/sync".to_string(), &cache_dir, DEFAULT_CACHE_MAX_BYTES)
                .expect("build the vfs facade");

        let hook = MountTestHook::new();
        let result =
            attach(vfs, &mountpoint, "Test", &cache_dir, DEFAULT_CACHE_MAX_BYTES, Some(&hook)).await;
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!("a non-empty mountpoint must refuse to mount"),
        };
        assert!(err.to_string().contains("not empty"), "unexpected error: {err}");

        assert_eq!(
            hook.pre_clean_count(),
            1,
            "cleanup_stale_mount must be attempted BEFORE attach bails on a non-empty \
             mountpoint — otherwise a stale mount left over from a crash could never be \
             recovered (review finding 4)"
        );
        // The OS mount itself must never be attempted once the emptiness
        // check failed.
        assert_eq!(hook.mount_count(), 0);
    }
}
