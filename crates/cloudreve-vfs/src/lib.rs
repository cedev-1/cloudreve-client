//! On-demand virtual filesystem for Cloudreve drives.
//!
//! One brain, two plugs: `vfs` is the facade holding all logic; the NFS
//! (macOS) and FUSE (Linux) frontends added in phase 3 are thin adapters
//! over it. Nothing in this crate mounts anything.

pub mod cache;
pub mod frontend_util;
// Linux-only (Task 4): implements `fuser::Filesystem` over the facade,
// mounted by `mount.rs`'s `linux_impl` branch. Never compiled on this
// (macOS) development machine — see `fuse.rs`'s module doc.
#[cfg(target_os = "linux")]
pub mod fuse;
// Declared unconditionally as of Task 4: `mount.rs` now cfg-gates its two
// platform branches PER ITEM internally (macOS's `mount_nfs`/`umount`/
// `diskutil` shell-outs vs. Linux's `fuser` mount) rather than gating the
// whole module — see its own doc.
pub mod mount;
pub mod nfs;
pub mod tree;
pub mod vfs;
pub mod writeback;
