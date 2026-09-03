//! Attr/errno mapping shared by the NFS (macOS) and FUSE (Linux) frontends
//! (phase 3, D2/D7).
//!
//! Deliberately ignorant of both `nfs3_server`'s and `fuser`'s own types:
//! everything here is a plain number or a small struct either ecosystem can
//! convert on its own. Each adapter owns the actual conversion into its
//! protocol's native shape — see `nfs.rs`'s `to_fattr3`/`to_nfsstat3` for the
//! nfs3-specific half.

use crate::tree::NodeAttr;
use crate::vfs::{DirNotEmptyError, RenameBusyError, StaleHandleError, UnlinkBusyError};

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
    if err.to_string().starts_with("EEXIST") {
        return FrontendErrno::Exist;
    }
    tracing::debug!(chain = ?err, "vfs frontend: unmapped error, reporting a generic i/o failure");
    FrontendErrno::Io
}

/// D7: unix permission bits for a node — files are `0o644`, directories
/// `0o755`. Fixed, not configurable: the facade has no per-file permission
/// bit of its own to report instead.
pub fn to_unix_mode(is_dir: bool) -> u32 {
    if is_dir {
        0o755
    } else {
        0o644
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

    #[test]
    fn directories_and_files_get_distinct_modes() {
        assert_eq!(to_unix_mode(true), 0o755);
        assert_eq!(to_unix_mode(false), 0o644);
    }

    #[test]
    fn frontend_attr_overlays_the_node_attr_fields() {
        let attr = NodeAttr {
            name: "a.txt".into(),
            remote_path: "cloudreve://x/a.txt".into(),
            size: 42,
            mtime_secs: 1_700_000_000,
            is_dir: false,
            etag: "e1".into(),
        };
        let fa = to_frontend_attr(7, &attr);
        assert_eq!(fa.fileid, 7);
        assert_eq!(fa.size, 42);
        assert_eq!(fa.mode, 0o644);
        assert_eq!(fa.mtime_secs, 1_700_000_000);
        assert_eq!(fa.atime_secs, fa.mtime_secs);
        assert_eq!(fa.ctime_secs, fa.mtime_secs);
        assert_eq!(fa.nlink, 1);
    }
}
