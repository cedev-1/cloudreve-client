//! NFSv3 adapter over the [`Vfs`] facade (phase 3, D4).
//!
//! `VfsNfs` implements `nfs3_server`'s [`NfsReadFileSystem`]/[`NfsFileSystem`]
//! traits (crate version pinned in `Cargo.toml`: `nfs3_server = "0.11"`,
//! resolved to `0.11.0` — see the phase-3 task report for how that was
//! verified). This module owns no socket and mounts nothing: `mount.rs`
//! (Task 3) is the only place that ever binds a port or shells out to
//! `mount_nfs`. Every test in `tests/nfs_adapter.rs` drives this trait
//! object directly.
//!
//! ## Handle model (D4)
//!
//! NFSv3 has no OPEN/CLOSE RPC at all — every trait method below (`read`,
//! `write`, `setattr`, …) receives just a `fileid`-shaped handle and must do
//! whatever it needs to do in that one call. That happens to be EXACTLY the
//! "open-per-operation" lifecycle D4 asks to start with, so there is no
//! separate lifecycle decision to make here: each method that touches file
//! content opens a fresh facade [`FileHandle`](crate::vfs::FileHandle),
//! does its work, and closes it again before returning — the draft/cache
//! layers underneath make that cheap (see `Vfs::open`'s doc: a path with an
//! active draft needs no download URL at all).
//!
//! One thing worth stating plainly: no facade handle here ever survives
//! BETWEEN two RPCs — each is opened and closed within the one call that
//! created it. That does NOT make the handle-lifecycle error paths
//! unreachable, though: `nfs3_server` spawns an independent tokio task per
//! incoming RPC (`rpcwire.rs`), so two RPCs on the same file genuinely CAN
//! run concurrently — e.g. a slow WRITE (its facade handle open for the
//! duration of that one call) overlapping a RENAME on the same path is
//! enough to legitimately trip `Vfs::rename`'s `is_subtree_open` guard and
//! return [`RenameBusyError`](crate::vfs::RenameBusyError)/`JUKEBOX`; the
//! narrower [`StaleHandleError`](crate::vfs::StaleHandleError) race (a
//! read overlapping the exact instant its draft's upload removes it) is
//! possible too, just tighter. What's actually true is narrower than
//! "unreachable": there is no DETERMINISTIC way to drive that overlap
//! through the trait object in a test (it depends on the tokio scheduler
//! racing two concurrently-dispatched RPCs), so both mappings are pinned by
//! direct unit tests (constructing the typed error and asserting the mapped
//! `nfsstat3`, see the `tests` module below) rather than an end-to-end
//! repro. Task 4's FUSE adapter, whose handles persist across calls by
//! design (not just by scheduler luck), makes both paths reachable and
//! testable end-to-end.
//!
//! ## Fileid / handle type
//!
//! `nfs3_server`'s handle type is a trait (`vfs::FileHandle`), not a bare
//! `fileid3` — the crate ships an 8-byte implementation,
//! [`FileHandleU64`], which this adapter
//! uses directly: `FileHandleU64::new(NodeId.0)`. `nfs3_server` wraps it in
//! its own opaque `nfs_fh3` (an 8-byte server generation number + our 8
//! bytes) before it ever reaches the wire, which is where the protocol's
//! OWN stale-handle detection (a handle from before a server restart) lives
//! — orthogonal to, and layered on top of, this facade's `StaleHandleError`.
//!
//! ## readdir cookies (D4)
//!
//! `readdirplus` takes one snapshot per call (`Vfs::readdir`) and hands out
//! a cookie equal to `1 + <the entry's position in that snapshot>`; cookie
//! `0` means "start of directory". Resuming with cookie `C` on the NEXT call
//! starts a FRESH snapshot at index `C` — correct as long as nothing
//! reordered the directory between the two calls. A concurrent
//! create/delete/rename elsewhere in the tree can reshuffle positions
//! between two paged calls (accepted per D4: a client that notices a
//! gap/duplicate just re-reads the directory, and NFSv3 gives no stronger
//! guarantee here without a server-side directory-generation cookie
//! verifier, which this server does not implement — see
//! `FileHandleConverter`'s doc in `nfs3_server` for the analogous handle
//! generation number, which is a different mechanism).

use std::sync::Arc;

use nfs3_server::nfs3_types::nfs3::{
    createverf3, fattr3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
    stable_how, Nfs3Option,
};
use nfs3_server::vfs::{
    DirEntryPlus, FileHandleU64, NextResult, NfsFileSystem, NfsReadFileSystem, ReadDirPlusIterator,
};

use crate::frontend_util::{classify_error, to_frontend_attr, FrontendAttr, FrontendErrno};
use crate::tree::{NodeAttr, NodeId};
use crate::vfs::Vfs;

/// NFSv3 adapter over [`Vfs`]. See the module doc for the handle lifecycle
/// and cookie scheme.
pub struct VfsNfs {
    vfs: Arc<Vfs>,
}

impl VfsNfs {
    pub fn new(vfs: Arc<Vfs>) -> Self {
        Self { vfs }
    }
}

impl NfsReadFileSystem for VfsNfs {
    type Handle = FileHandleU64;

    fn root_dir(&self) -> FileHandleU64 {
        FileHandleU64::new(self.vfs.tree().root().0)
    }

    async fn lookup(
        &self,
        dirid: &FileHandleU64,
        filename: &filename3<'_>,
    ) -> Result<FileHandleU64, nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        match self.vfs.lookup(parent, &name).await {
            Ok(Some((id, _attr))) => Ok(FileHandleU64::new(id.0)),
            // Lookup miss: the facade's `Ok(None)`, not an error — routed
            // through the SAME `to_nfsstat3` conversion as everything else
            // (see the module doc and `FrontendErrno::NotFound`'s doc).
            Ok(None) => Err(to_nfsstat3(FrontendErrno::NotFound)),
            Err(err) => Err(to_errno(&err)),
        }
    }

    async fn getattr(&self, id: &FileHandleU64) -> Result<fattr3, nfsstat3> {
        let node = NodeId(id.as_u64());
        match self.vfs.getattr(node).await {
            Ok(Some(attr)) => Ok(to_fattr3(&to_frontend_attr(node.0, &attr))),
            Ok(None) => Err(to_nfsstat3(FrontendErrno::NotFound)),
            Err(err) => Err(to_errno(&err)),
        }
    }

    async fn read(
        &self,
        id: &FileHandleU64,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let node = NodeId(id.as_u64());
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;

        // Open-per-operation (D4, see the module doc): this handle never
        // outlives this one call.
        let handle = self.vfs.open(node).await.map_err(|e| to_errno(&e))?;
        let result = self.vfs.read(handle, offset, count).await;
        // Best-effort close: a failed read still releases the cache retain
        // this open() took. `close`'s own error (handle already gone) would
        // only ever fire if something else raced this exact handle id,
        // which nothing can — it is local to this call.
        let _ = self.vfs.close(handle).await;
        let data = result.map_err(|e| to_errno(&e))?;
        let eof = offset.saturating_add(data.len() as u64) >= attr.size;
        Ok((data.to_vec(), eof))
    }

    async fn readdirplus(
        &self,
        dirid: &FileHandleU64,
        cookie: u64,
    ) -> Result<impl ReadDirPlusIterator<FileHandleU64>, nfsstat3> {
        let dir = NodeId(dirid.as_u64());
        let listing = self.vfs.readdir(dir).await.map_err(|e| to_errno(&e))?;
        let start = cookie as usize;
        if start > listing.len() {
            // Per the module doc: a directory reshuffle between two paged
            // calls can put a previously valid cookie past the end of the
            // fresh snapshot.
            return Err(nfsstat3::NFS3ERR_BAD_COOKIE);
        }
        Ok(VfsReadDirIter { listing, index: start })
    }

    async fn readlink(&self, _id: &FileHandleU64) -> Result<nfspath3<'_>, nfsstat3> {
        // The facade has no symlink concept at all.
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

impl NfsFileSystem for VfsNfs {
    async fn setattr(&self, id: &FileHandleU64, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let node = NodeId(id.as_u64());
        // D4: the only `sattr3` field this facade can act on is `size`
        // (truncate) — mode/uid/gid/atime/mtime have no server-side
        // counterpart to persist them to, so a client setting them sees the
        // call succeed (matching D7's fixed attrs) without silently
        // pretending to honor a value it can't keep.
        if let Nfs3Option::Some(size) = setattr.size {
            let handle = self.vfs.open(node).await.map_err(|e| to_errno(&e))?;
            let result = self.vfs.truncate(handle, size).await;
            let _ = self.vfs.close(handle).await;
            result.map_err(|e| to_errno(&e))?;
        }
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        Ok(to_fattr3(&to_frontend_attr(node.0, &attr)))
    }

    async fn write(
        &self,
        id: &FileHandleU64,
        offset: u64,
        data: &[u8],
        _stable: stable_how,
    ) -> Result<(fattr3, stable_how), nfsstat3> {
        let node = NodeId(id.as_u64());
        let handle = self.vfs.open(node).await.map_err(|e| to_errno(&e))?;
        let result = self.vfs.write(handle, offset, data).await;
        let _ = self.vfs.close(handle).await;
        result.map_err(|e| to_errno(&e))?;
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        // `Vfs::write` lands synchronously in the local draft store before
        // this returns — FILE_SYNC is an honest description of that, not an
        // aspirational one, regardless of what the client requested.
        Ok((to_fattr3(&to_frontend_attr(node.0, &attr)), stable_how::FILE_SYNC))
    }

    async fn create(
        &self,
        dirid: &FileHandleU64,
        filename: &filename3<'_>,
        _attr: sattr3,
    ) -> Result<(FileHandleU64, fattr3), nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        // The `sattr3` the client may pass (mode, etc.) is ignored — see
        // `setattr`'s doc: the facade has nowhere to persist it, and D7
        // fixes every entry's reported mode regardless.
        let (node, handle) = self.vfs.create(parent, &name).await.map_err(|e| to_errno(&e))?;
        let _ = self.vfs.close(handle).await;
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        Ok((FileHandleU64::new(node.0), to_fattr3(&to_frontend_attr(node.0, &attr))))
    }

    async fn create_exclusive(
        &self,
        dirid: &FileHandleU64,
        filename: &filename3<'_>,
        _createverf: createverf3,
    ) -> Result<FileHandleU64, nfsstat3> {
        // The facade has no separate exclusive-create primitive and nowhere
        // to persist a verifier to dedupe a retried request against.
        // `Vfs::create`'s own EEXIST guard already gives exclusive create's
        // essential guarantee (never silently overwrite an existing name),
        // so this just reuses it; the verifier is accepted and ignored.
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        let (node, handle) = self.vfs.create(parent, &name).await.map_err(|e| to_errno(&e))?;
        let _ = self.vfs.close(handle).await;
        Ok(FileHandleU64::new(node.0))
    }

    async fn mkdir(
        &self,
        dirid: &FileHandleU64,
        dirname: &filename3<'_>,
    ) -> Result<(FileHandleU64, fattr3), nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(dirname)?;
        let node = self.vfs.mkdir(parent, &name).await.map_err(|e| to_errno(&e))?;
        let attr = self
            .vfs
            .getattr(node)
            .await
            .map_err(|e| to_errno(&e))?
            .ok_or_else(|| to_nfsstat3(FrontendErrno::NotFound))?;
        Ok((FileHandleU64::new(node.0), to_fattr3(&to_frontend_attr(node.0, &attr))))
    }

    async fn remove(&self, dirid: &FileHandleU64, filename: &filename3<'_>) -> Result<(), nfsstat3> {
        let parent = NodeId(dirid.as_u64());
        let name = name_str(filename)?;
        self.vfs.unlink(parent, &name).await.map_err(|e| to_errno(&e))
    }

    async fn rename<'a>(
        &self,
        from_dirid: &FileHandleU64,
        from_filename: &filename3<'a>,
        to_dirid: &FileHandleU64,
        to_filename: &filename3<'a>,
    ) -> Result<(), nfsstat3> {
        let from_parent = NodeId(from_dirid.as_u64());
        let to_parent = NodeId(to_dirid.as_u64());
        let from_name = name_str(from_filename)?;
        let to_name = name_str(to_filename)?;
        self.vfs
            .rename(from_parent, &from_name, to_parent, &to_name)
            .await
            .map_err(|e| to_errno(&e))
    }

    async fn symlink<'a>(
        &self,
        _dirid: &FileHandleU64,
        _linkname: &filename3<'a>,
        _symlink: &nfspath3<'a>,
        _attr: &sattr3,
    ) -> Result<(FileHandleU64, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn commit(&self, _id: &FileHandleU64, _offset: u64, _count: u32) -> Result<(), nfsstat3> {
        // Every write already lands synchronously in the draft store before
        // `write` returns (see its own doc) — there is nothing left here to
        // flush.
        Ok(())
    }
}

/// Iterates one directory snapshot for `readdirplus` — see the module doc
/// for the cookie scheme.
struct VfsReadDirIter {
    listing: Vec<(NodeId, NodeAttr)>,
    index: usize,
}

impl ReadDirPlusIterator<FileHandleU64> for VfsReadDirIter {
    async fn next(&mut self) -> NextResult<DirEntryPlus<FileHandleU64>> {
        if self.index >= self.listing.len() {
            return NextResult::Eof;
        }
        let (id, attr) = &self.listing[self.index];
        let entry = DirEntryPlus {
            fileid: id.0,
            name: attr.name.clone().into_bytes().into(),
            cookie: (self.index + 1) as u64,
            name_attributes: Some(to_fattr3(&to_frontend_attr(id.0, attr))),
            name_handle: Some(FileHandleU64::new(id.0)),
        };
        self.index += 1;
        NextResult::Ok(entry)
    }
}

/// Decodes an NFS wire filename into a `String`, refusing anything that
/// isn't valid UTF-8 — the facade's `remote_path`s are always UTF-8 (they
/// round-trip through `serde_json`), so a non-UTF-8 name could never
/// resolve to anything anyway.
fn name_str(name: &filename3<'_>) -> Result<String, nfsstat3> {
    std::str::from_utf8(name.as_ref())
        .map(str::to_string)
        .map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

/// The nfs3-specific half of D2's mapping: turns the portable
/// [`FrontendErrno`] into `nfs3_server`'s own `nfsstat3`. The ONE place this
/// adapter turns any classified outcome (including a literal
/// [`FrontendErrno::NotFound`] constructed at a lookup/getattr-miss call
/// site, not just an [`classify_error`]'d `anyhow::Error`) into a wire code —
/// see the module doc's mutation-testing note.
fn to_nfsstat3(errno: FrontendErrno) -> nfsstat3 {
    match errno {
        FrontendErrno::NotFound => nfsstat3::NFS3ERR_NOENT,
        FrontendErrno::Stale => nfsstat3::NFS3ERR_STALE,
        FrontendErrno::Exist => nfsstat3::NFS3ERR_EXIST,
        // D2 judgment call (documented per its directive to "judge" and
        // disclose): NFSv3 has no direct "handle busy" code. `JUKEBOX`
        // ("the server can't complete this right now, retry the request")
        // is the closest fit semantically — the client is expected to retry,
        // which is exactly what closing the busy handle and retrying does
        // for `RenameBusyError`. `ACCES` was the other candidate but would
        // misleadingly suggest a permissions problem, which this isn't.
        // Reachable through THIS adapter despite handles being per-RPC (see
        // the module doc): `nfs3_server` dispatches each incoming RPC on
        // its own tokio task, so a slow WRITE's facade handle can still be
        // open when a concurrent RENAME on the same path arrives and trips
        // `Vfs::rename`'s `is_subtree_open` guard. No deterministic test drives
        // that overlap through the trait object (it depends on the tokio
        // scheduler), so this mapping is pinned by a direct unit test
        // instead — real AND end-to-end-testable once the FUSE adapter
        // (Task 4) lands, since `fuser` handles persist across calls by
        // design rather than by scheduler timing.
        FrontendErrno::Busy => nfsstat3::NFS3ERR_JUKEBOX,
        // D2 (phase 4, deliverable D): NFSv3's direct, purpose-built code
        // for exactly this condition — a real REMOVE/RMDIR-equivalent
        // refusal, unlike `Busy`'s judgment-call stand-in above.
        FrontendErrno::NotEmpty => nfsstat3::NFS3ERR_NOTEMPTY,
        FrontendErrno::Io => nfsstat3::NFS3ERR_IO,
    }
}

/// Classifies then converts an `anyhow::Error` from any `Vfs` call in one
/// step.
fn to_errno(err: &anyhow::Error) -> nfsstat3 {
    to_nfsstat3(classify_error(err))
}

/// Converts the portable [`FrontendAttr`] into `nfs3_server`'s `fattr3`.
fn to_fattr3(attr: &FrontendAttr) -> fattr3 {
    fattr3 {
        type_: if attr.is_dir { ftype3::NF3DIR } else { ftype3::NF3REG },
        mode: attr.mode,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        used: attr.size,
        rdev: specdata3::default(),
        fsid: 0,
        fileid: attr.fileid,
        atime: to_nfstime3(attr.atime_secs),
        mtime: to_nfstime3(attr.mtime_secs),
        ctime: to_nfstime3(attr.ctime_secs),
    }
}

/// `NodeAttr::mtime_secs` is a signed unix timestamp; `nfstime3::seconds` is
/// an unsigned 32-bit one. Clamped rather than wrapped: a negative or
/// far-future timestamp (neither of which a well-behaved server should ever
/// send) becomes the nearest representable bound instead of silently
/// aliasing to an unrelated time.
fn to_nfstime3(secs: i64) -> nfstime3 {
    let seconds = u32::try_from(secs).unwrap_or(if secs < 0 { 0 } else { u32::MAX });
    nfstime3 { seconds, nseconds: 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::{DirNotEmptyError, RenameBusyError, StaleHandleError, UnlinkBusyError};

    #[test]
    fn not_found_maps_to_noent() {
        assert_eq!(to_nfsstat3(FrontendErrno::NotFound), nfsstat3::NFS3ERR_NOENT);
    }

    #[test]
    fn stale_handle_error_maps_to_stale() {
        let err = anyhow::Error::new(StaleHandleError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_STALE);
    }

    #[test]
    fn rename_busy_error_maps_to_jukebox() {
        let err = anyhow::Error::new(RenameBusyError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_JUKEBOX);
    }

    #[test]
    fn unlink_busy_error_maps_to_jukebox() {
        let err = anyhow::Error::new(UnlinkBusyError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_JUKEBOX);
    }

    #[test]
    fn dir_not_empty_error_maps_to_notempty() {
        let err = anyhow::Error::new(DirNotEmptyError { remote_path: "x".into() });
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_NOTEMPTY);
    }

    #[test]
    fn eexist_marker_maps_to_exist() {
        let err = anyhow::anyhow!("EEXIST: an entry named \"dup.txt\" already exists");
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_EXIST);
    }

    #[test]
    fn an_unrecognized_error_maps_to_io() {
        let err = anyhow::anyhow!("some transient failure");
        assert_eq!(to_errno(&err), nfsstat3::NFS3ERR_IO);
    }

    #[test]
    fn negative_mtime_clamps_to_zero() {
        assert_eq!(to_nfstime3(-5).seconds, 0);
    }
}
