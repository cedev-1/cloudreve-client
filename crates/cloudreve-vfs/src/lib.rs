//! On-demand virtual filesystem for Cloudreve drives.
//!
//! One brain, two plugs: `vfs` is the facade holding all logic; the NFS
//! (macOS) and FUSE (Linux) frontends added in phase 3 are thin adapters
//! over it. Nothing in this crate mounts anything.

pub mod cache;
pub mod frontend_util;
// macOS-only for now (Task 3): `mount.rs` shells out to `mount_nfs`/`umount`/
// `diskutil`, all macOS-specific. Task 4 adds the Linux/FUSE branch into the
// SAME file and will need to relax this gate (declare unconditionally, or
// per-OS) — see `mount.rs`'s module doc.
#[cfg(target_os = "macos")]
pub mod mount;
pub mod nfs;
pub mod tree;
pub mod vfs;
pub mod writeback;
