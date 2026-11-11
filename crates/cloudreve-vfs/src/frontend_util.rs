//! Attr/errno mapping shared by the NFS (macOS) and FUSE (Linux) frontends
//! (phase 3, D2/D7).
//!
//! Deliberately ignorant of both `nfs3_server`'s and `fuser`'s own types:
//! everything here is a plain number or a small struct either ecosystem can
//! convert on its own. Each adapter owns the actual conversion into its
//! protocol's native shape — see `nfs.rs`'s `to_fattr3`/`to_nfsstat3` for the
//! nfs3-specific half.

use crate::tree::NodeAttr;
use crate::vfs::{
    CrossOverlayRenameError, DirNotEmptyError, JunkNameError, JunkTooBigError, RenameBusyError, RenameOntoDirectoryError,
    StaleHandleError, UnlinkBusyError,
};

/// Portable classification of a facade error/outcome, independent of any
/// frontend's own error enum. D2's mapping table, minus the lookup-miss case:
/// a lookup/getattr miss is the facade's `Ok(None)`, never an
/// `anyhow::Error`, so [`classify_error`] never produces [`NotFound`] itself
/// — each adapter constructs it directly at its own `lookup`/`getattr` call
/// site instead, so it still flows through the SAME final per-protocol
/// conversion as every other variant (this is what makes mutating that one
/// conversion function break both the not-found and the stale-handle
/// assertions at once — see `nfs.rs`'s `to_nfsstat3`).
///
/// [`NotFound`]: FrontendErrno::NotFound
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontendErrno {
    /// No such file or directory.
    NotFound,
    /// [`StaleHandleError`]: the handle's draft was uploaded and removed
    /// while the handle stayed open.
    Stale,
    /// The facade's EEXIST-marker create error: an entry already exists
    /// under that name.
    Exist,
    /// [`RenameBusyError`] (a handle is currently open on, or a live draft
    /// sits on, the entry being renamed — either its source or its
    /// destination) or [`UnlinkBusyError`] (a handle is currently open on
    /// the file being deleted). Both classify identically: the split
    /// between the two typed errors is about naming which OPERATION was
    /// refused, not a different frontend-visible outcome (see
    /// `UnlinkBusyError`'s own doc for why it's a separate type at all).
    Busy,
    /// [`DirNotEmptyError`]: a non-empty directory was asked to be removed.
    /// This facade never does a recursive delete.
    NotEmpty,
    /// [`RenameOntoDirectoryError`] (phase 4 task 3, R1): a drafted FILE
    /// source was renamed onto a destination name that resolves to an
    /// existing DIRECTORY. Both real kernels already refuse this shape
    /// themselves before the facade ever sees it (`EISDIR` on a plain
    /// `rename(2)` over a directory), so this variant is defense-in-depth
    /// for a facade-level caller, not something either mounted frontend has
    /// been observed to actually need to produce.
    IsDir,
    /// A name reserved for local macOS metadata (`crate::junk`):
    /// [`JunkNameError`].
    Access,
    /// A local-only junk file renamed onto a real name:
    /// [`CrossOverlayRenameError`]. EXDEV makes `mv`/Finder copy instead.
    CrossDevice,
    /// The local disk holding drafts and cached blocks is full (ENOSPC
    /// anywhere in the error chain).
    NoSpace,
    /// A local-only junk file past its size cap ([`JunkTooBigError`]).
    FileTooBig,
    /// Everything else. The anyhow causal chain is logged at debug so a
    /// real bug stays diagnosable without ever leaking `anyhow::Error`
    /// details across the frontend boundary.
    Io,
}

/// D2: classifies an `anyhow::Error` returned by any `Vfs` call into a
/// protocol-agnostic errno. Keyed on typed errors first
/// ([`StaleHandleError`], [`RenameBusyError`], [`UnlinkBusyError`],
/// [`DirNotEmptyError`] — phase 4 added the last two), then on the one
/// message marker the facade guarantees (`Vfs::create`'s `"EEXIST: ..."`
/// prefix — see its doc); everything else falls through to
/// [`FrontendErrno::Io`].
pub fn classify_error(err: &anyhow::Error) -> FrontendErrno {
    if err.downcast_ref::<StaleHandleError>().is_some() {
        return FrontendErrno::Stale;
    }
    if err.downcast_ref::<RenameBusyError>().is_some() {
        return FrontendErrno::Busy;
    }
    if err.downcast_ref::<UnlinkBusyError>().is_some() {
        return FrontendErrno::Busy;
    }
    if err.downcast_ref::<DirNotEmptyError>().is_some() {
        return FrontendErrno::NotEmpty;
    }
    if err.downcast_ref::<RenameOntoDirectoryError>().is_some() {
        return FrontendErrno::IsDir;
    }
    if err.downcast_ref::<JunkNameError>().is_some() {
        return FrontendErrno::Access;
    }
    if err.downcast_ref::<CrossOverlayRenameError>().is_some() {
        return FrontendErrno::CrossDevice;
    }
    if err.downcast_ref::<JunkTooBigError>().is_some() {
        return FrontendErrno::FileTooBig;
    }
    if err.to_string().starts_with("EEXIST") {
        return FrontendErrno::Exist;
    }
    let disk_full = err.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().and_then(|io| io.raw_os_error()) == Some(libc::ENOSPC)
    });
    if disk_full {
        return FrontendErrno::NoSpace;
    }
    // An app gets a bare EIO: the cause must be in the log. Once a minute
    // per distinct message (it carries the path), not per RPC.
    let message = format!("{err:#}");
    if unmapped_log_limiter().allow(&message, std::time::Duration::from_secs(60)) {
        tracing::warn!(error = %message, "vfs frontend: an operation failed, the app gets an i/o error");
    }
    FrontendErrno::Io
}

fn unmapped_log_limiter() -> &'static crate::ratelimit::RateLimiter {
    static LIMITER: std::sync::OnceLock<crate::ratelimit::RateLimiter> = std::sync::OnceLock::new();
    LIMITER.get_or_init(Default::default)
}

/// Closes a handle a frontend opened for one call. A failure (persisting
/// the queued state on a full disk — the upload is armed anyway) is
/// logged, never surfaced: the call it served already has its answer.
pub async fn close_logged(vfs: &crate::vfs::Vfs, handle: crate::vfs::FileHandle) {
    if let Err(err) = vfs.close(handle).await {
        tracing::warn!(drive = %vfs.drive_label(), error = %format!("{err:#}"), "vfs: closing a handle failed");
    }
}

/// D7: unix permission bits for a node — files are `0o600`, directories
/// `0o700`: owner-only, matching what the NFS server enforces (it refuses
/// every uid but this user's and root's, see `nfs::start_server`). Fixed,
/// not configurable: the facade has no per-file permission bit of its own
/// to report instead.
pub fn to_unix_mode(is_dir: bool) -> u32 {
    if is_dir {
        0o700
    } else {
        0o600
    }
}

/// Portable attribute view both frontends convert into their own protocol's
/// attr struct (D7). `uid`/`gid` are the process's effective ids — this
/// facade has no per-file ownership model of its own, so every entry is
/// reported as owned by whoever is running the mount. All three timestamps
/// come from the SAME `mtime_secs` (D7: "atime is a lie the spec accepts —
/// NFS couples them anyway").
#[derive(Debug, Clone, Copy)]
pub struct FrontendAttr {
    pub fileid: u64,
    pub is_dir: bool,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub mtime_secs: i64,
    pub atime_secs: i64,
    pub ctime_secs: i64,
    /// Sub-second part shared by all three times ([`NodeAttr::mtime_nsecs`]).
    pub time_nsecs: u32,
}

/// Builds a [`FrontendAttr`] for `fileid` from the facade's own [`NodeAttr`]
/// (already draft-overlaid by whichever `Vfs` call produced it).
pub fn to_frontend_attr(fileid: u64, attr: &NodeAttr) -> FrontendAttr {
    FrontendAttr {
        fileid,
        is_dir: attr.is_dir,
        size: attr.size,
        mode: to_unix_mode(attr.is_dir),
        uid: process_uid(),
        gid: process_gid(),
        nlink: 1,
        mtime_secs: attr.mtime_secs,
        atime_secs: attr.mtime_secs,
        ctime_secs: attr.mtime_secs,
        time_nsecs: attr.mtime_nsecs,
    }
}

/// The effective uid of the process running the mount — see
/// [`FrontendAttr`]'s doc.
pub fn process_uid() -> u32 {
    // SAFETY: `geteuid` takes no arguments, performs no pointer access, and
    // cannot fail.
    unsafe { libc::geteuid() }
}

/// The effective gid of the process running the mount — see
/// [`FrontendAttr`]'s doc.
pub fn process_gid() -> u32 {
    // SAFETY: `getegid` takes no arguments, performs no pointer access, and
    // cannot fail.
    unsafe { libc::getegid() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The disk holding the drafts and the cache is full: the app writing
    /// the file must get "no space left on device" (it can tell the user
    /// what is wrong), not a generic I/O error.
    #[test]
    fn a_full_disk_classifies_as_no_space_even_behind_context() {
        let io = std::io::Error::from_raw_os_error(libc::ENOSPC);
        let err = anyhow::Error::new(io).context("failed to write draft data").context("write");
        assert_eq!(classify_error(&err), FrontendErrno::NoSpace);
    }

    #[test]
    fn other_io_errors_stay_generic() {
        let io = std::io::Error::from_raw_os_error(libc::EBADF);
        let err = anyhow::Error::new(io).context("failed to write draft data");
        assert_eq!(classify_error(&err), FrontendErrno::Io);
    }

    #[test]
    fn stale_handle_error_classifies_as_stale() {
        let err = anyhow::Error::new(StaleHandleError { remote_path: "x".into() });
        assert_eq!(classify_error(&err), FrontendErrno::Stale);
    }

    #[test]
    fn rename_busy_error_classifies_as_busy() {
        let err = anyhow::Error::new(RenameBusyError { remote_path: "x".into() });
        assert_eq!(classify_error(&err), FrontendErrno::Busy);
    }

    #[test]
    fn unlink_busy_error_classifies_as_busy() {
        let err = anyhow::Error::new(UnlinkBusyError { remote_path: "x".into() });
        assert_eq!(classify_error(&err), FrontendErrno::Busy);
    }

    #[test]
    fn dir_not_empty_error_classifies_as_not_empty() {
        let err = anyhow::Error::new(DirNotEmptyError { remote_path: "x".into() });
        assert_eq!(classify_error(&err), FrontendErrno::NotEmpty);
    }

    #[test]
    fn rename_onto_directory_error_classifies_as_is_dir() {
        let err = anyhow::Error::new(RenameOntoDirectoryError { remote_path: "x".into() });
        assert_eq!(classify_error(&err), FrontendErrno::IsDir);
    }

    #[test]
    fn a_junk_name_error_classifies_as_access() {
        let err = anyhow::Error::new(JunkNameError { name: ".Trashes".into() });
        assert_eq!(classify_error(&err), FrontendErrno::Access);
    }

    #[test]
    fn a_cross_overlay_rename_classifies_as_cross_device() {
        let err = anyhow::Error::new(CrossOverlayRenameError { remote_path: "x".into() });
        assert_eq!(classify_error(&err), FrontendErrno::CrossDevice);
    }

    #[test]
    fn the_create_eexist_marker_classifies_as_exist() {
        let err =
            anyhow::anyhow!("EEXIST: an entry named \"dup.txt\" already exists in this directory");
        assert_eq!(classify_error(&err), FrontendErrno::Exist);
    }

    #[test]
    fn an_unrecognized_error_classifies_as_io() {
        let err = anyhow::anyhow!("some transient network failure");
        assert_eq!(classify_error(&err), FrontendErrno::Io);
    }

    /// Owner-only: the cloud files are this user's alone. The NFS server
    /// already refuses other uids; the modes make Finder and `ls -l` say so,
    /// and let the kernel refuse from its attribute cache too.
    #[test]
    fn nodes_are_owner_only() {
        assert_eq!(to_unix_mode(true), 0o700);
        assert_eq!(to_unix_mode(false), 0o600);
    }

    #[test]
    fn frontend_attr_overlays_the_node_attr_fields() {
        let attr = NodeAttr {
            name: "a.txt".into(),
            remote_path: "cloudreve://x/a.txt".into(),
            size: 42,
            mtime_secs: 1_700_000_000,
            mtime_nsecs: 0,
            is_dir: false,
            etag: "e1".into(),
        };
        let fa = to_frontend_attr(7, &attr);
        assert_eq!(fa.fileid, 7);
        assert_eq!(fa.size, 42);
        assert_eq!(fa.mode, 0o600);
        assert_eq!(fa.mtime_secs, 1_700_000_000);
        assert_eq!(fa.atime_secs, fa.mtime_secs);
        assert_eq!(fa.ctime_secs, fa.mtime_secs);
        assert_eq!(fa.nlink, 1);
    }
}
