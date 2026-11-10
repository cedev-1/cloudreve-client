//! Cloudreve fork: who may use this server (see `CLOUDREVE-FORK.md`).
//!
//! Upstream answers every caller, AUTH_NULL included, and hands the root
//! handle to any MNT. A [`CallerPolicy`] set on the listener restricts that:
//! - every call except the NULL ping must carry AUTH_UNIX credentials with
//!   a uid in `allowed_uids` (MNT gets `MNT3ERR_ACCES`, any other NFSv3
//!   procedure `NFS3ERR_ACCES` — the kernel client turns both into EACCES
//!   for the calling process, which is exactly what another account
//!   reaching a mount should see);
//! - with [`CallerPolicy::single_mount`], only the FIRST successful MNT is
//!   honoured. AUTH_UNIX is a claim any local process can forge, so the uid
//!   check alone cannot stop a raw TCP client; refusing every MNT after the
//!   one the mount itself made keeps the root handle inside the kernel, and
//!   a random handle generation (`bind_with_generation`) makes the other
//!   handles unguessable.

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use nfs3_types::xdr_codec::{Pack, Result};

#[derive(Debug)]
pub struct CallerPolicy {
    allowed_uids: Vec<u32>,
    single_mount: bool,
    mounted: AtomicBool,
}

impl CallerPolicy {
    /// Only AUTH_UNIX callers whose uid is in `allowed_uids` are served.
    #[must_use]
    pub const fn new(allowed_uids: Vec<u32>) -> Self {
        Self { allowed_uids, single_mount: false, mounted: AtomicBool::new(false) }
    }

    /// Honour only the first successful MNT; refuse every later one.
    #[must_use]
    pub const fn single_mount(mut self) -> Self {
        self.single_mount = true;
        self
    }

    pub(crate) fn allows(&self, uid: Option<u32>) -> bool {
        uid.is_some_and(|uid| self.allowed_uids.contains(&uid))
    }

    /// Whether this MNT may get the root handle: always without
    /// `single_mount`, otherwise only the first time.
    pub(crate) fn claim_mount(&self) -> bool {
        !self.single_mount
            || self
                .mounted
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// `NFS3ERR_ACCES` (13) for NFSv3 procedure `proc_`: the status followed by
/// the procedure's `resfail` body with every optional attribute absent —
/// one zero word per `post_op_attr`, two per `wcc_data` (RFC 1813).
pub(crate) struct NfsAccessDenied {
    zero_words: usize,
}

impl NfsAccessDenied {
    pub(crate) const fn for_proc(proc_: u32) -> Self {
        let zero_words = match proc_ {
            // GETATTR: void
            1 => 0,
            // SETATTR, WRITE, CREATE, MKDIR, SYMLINK, MKNOD, REMOVE, RMDIR,
            // COMMIT: wcc_data
            2 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 21 => 2,
            // RENAME: fromdir_wcc + todir_wcc
            14 => 4,
            // LINK: post_op_attr + wcc_data
            15 => 3,
            // LOOKUP, ACCESS, READLINK, READ, READDIR, READDIRPLUS, FSSTAT,
            // FSINFO, PATHCONF: one post_op_attr
            _ => 1,
        };
        Self { zero_words }
    }
}

impl Pack for NfsAccessDenied {
    fn packed_size(&self) -> usize {
        4 + 4 * self.zero_words
    }

    fn pack(&self, out: &mut impl Write) -> Result<usize> {
        let mut n = 13u32.pack(out)?;
        for _ in 0..self.zero_words {
            n += 0u32.pack(out)?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_listed_unix_uids_are_allowed() {
        let p = CallerPolicy::new(vec![501, 0]);
        assert!(p.allows(Some(501)));
        assert!(p.allows(Some(0)));
        assert!(!p.allows(Some(502)));
        assert!(!p.allows(None));
    }

    #[test]
    fn single_mount_honours_only_the_first_claim() {
        let p = CallerPolicy::new(vec![501]).single_mount();
        assert!(p.claim_mount());
        assert!(!p.claim_mount());
        let open = CallerPolicy::new(vec![501]);
        assert!(open.claim_mount());
        assert!(open.claim_mount());
    }
}
