//! On-demand virtual filesystem for Cloudreve drives.
//!
//! One brain, two plugs: `vfs` is the facade holding all logic; the NFS
//! (macOS) and FUSE (Linux) frontends added in phase 3 are thin adapters
//! over it. Nothing in this crate mounts anything.

pub mod cache;
pub mod tree;
pub mod vfs;
