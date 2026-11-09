//! The AppleDouble sidecar macOS keeps a file's extended attributes in on a
//! file system without native xattrs: on this NFSv3 mount the kernel's
//! fallback (`vfs_xattr.c`) writes them to `._<name>` next to the file, and
//! for the mount ROOT to `._.` inside the root itself (measured on the
//! macos-latest CI runner: `xattr -w com.apple.FinderInfo` on the mounted
//! root made the overlay hold `._.`, and nothing else).
//!
//! The root's sidecar is what carries its custom-icon bit, so the drive
//! folder's logo depends on it, and it must already exist when the client
//! first lists the root: once it has, Apple's NFS client refuses to create
//! a `._` name the listing lacks (`crate::nudge::NUDGE_NAME`). Measured on
//! CI: after a listing of a root without `._.`, setting its FinderInfo
//! failed with EPERM and the bit read as absent — and Finder lists a drive
//! folder the moment it shows it. So the app seeds this file before
//! mounting (`Vfs::put_local_only`). Once it exists it stays hidden like
//! any junk: measured on CI, a seeded `._.` missing from the listings is
//! still read and rewritten after one.

/// The root's sidecar name, see the module doc.
pub const ROOT_SIDECAR_NAME: &str = "._.";

/// The kernel's whole file for a sidecar holding nothing but `finder_info`:
/// byte for byte what macOS writes on the first `setxattr` of
/// `com.apple.FinderInfo` (`create_xattrfile` in `vfs_xattr.c`), so the
/// kernel reads and updates it like one of its own. 4096 bytes, big-endian:
/// the AppleDouble header with two entries (FinderInfo, resource fork), the
/// 32 FinderInfo bytes, the empty extended-attribute header (`ATTR`), and
/// at 0xEE2 the empty resource fork every such file carries.
pub fn finder_info_sidecar(finder_info: [u8; 32]) -> Vec<u8> {
    const RSRC_OFFSET: usize = 0xEE2;
    const RSRC_LEN: usize = 0x11E;
    let mut file = vec![0u8; RSRC_OFFSET + RSRC_LEN];
    let put = |file: &mut Vec<u8>, at: usize, bytes: &[u8]| file[at..at + bytes.len()].copy_from_slice(bytes);

    put(&mut file, 0x00, &0x0005_1607u32.to_be_bytes()); // AppleDouble magic
    put(&mut file, 0x04, &0x0002_0000u32.to_be_bytes()); // version 2
    put(&mut file, 0x08, b"Mac OS X        ");
    put(&mut file, 0x18, &2u16.to_be_bytes());
    // Entry 9, FinderInfo: the 32 bytes plus the attribute area after them.
    put(&mut file, 0x1A, &9u32.to_be_bytes());
    put(&mut file, 0x1E, &0x32u32.to_be_bytes());
    put(&mut file, 0x22, &0xEB0u32.to_be_bytes());
    // Entry 2, resource fork.
    put(&mut file, 0x26, &2u32.to_be_bytes());
    put(&mut file, 0x2A, &(RSRC_OFFSET as u32).to_be_bytes());
    put(&mut file, 0x2E, &(RSRC_LEN as u32).to_be_bytes());
    put(&mut file, 0x32, &finder_info);
    // 2 bytes of padding, then the attribute header: no attributes.
    put(&mut file, 0x54, b"ATTR");
    put(&mut file, 0x5C, &(RSRC_OFFSET as u32).to_be_bytes()); // total size
    put(&mut file, 0x60, &0x78u32.to_be_bytes()); // data start

    // The empty resource fork: header, text, then the map at +0x100.
    let rsrc_header = [0x100u32, 0x100, 0, 0x1E].map(u32::to_be_bytes).concat();
    put(&mut file, RSRC_OFFSET, &rsrc_header);
    put(&mut file, RSRC_OFFSET + 0x10, b"This resource fork intentionally left blank   ");
    put(&mut file, RSRC_OFFSET + 0x100, &rsrc_header);
    // Next-map handle and file ref (0), attributes (0), type list at 0x1C,
    // name list at 0x1E, no types (-1).
    put(&mut file, RSRC_OFFSET + 0x118, &[0x00, 0x1C, 0x00, 0x1E, 0xFF, 0xFF]);
    file
}

/// `finder_info` with the custom-icon bit (kHasCustomIcon, 0x0400 in the
/// Finder flags at bytes 8-9) set or cleared, everything else kept.
pub fn with_custom_icon(mut finder_info: [u8; 32], on: bool) -> [u8; 32] {
    if on {
        finder_info[8] |= 0x04;
    } else {
        finder_info[8] &= !0x04;
    }
    finder_info
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.split_whitespace().collect();
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    /// Against the file macOS itself wrote: `xattr -wx com.apple.FinderInfo
    /// 0000000000000000 0400...` on a mount root — the first 128 bytes as the
    /// CI runner's NFS mount stored them, the resource fork as a FAT RAM
    /// volume's root `._.` held it (macOS 27.0.1, `xxd`).
    #[test]
    fn the_sidecar_is_the_file_the_kernel_writes() {
        let mut fi = [0u8; 32];
        fi[8] = 0x04;
        let file = finder_info_sidecar(fi);

        let head = hex(
            "00051607000200004d6163204f53205820202020202020200002000000090000003200000eb00000000200000ee2\
             0000011e00000000000000000400000000000000000000000000000000000000000000000000415454520000000000000ee2\
             0000007800000000000000000000000000000000000000000000000000000000",
        );
        let rsrc = hex(
            "0000010000000100000000000000001e\
             5468697320726573 6f7572636520666f726b20696e74656e74696f6e616c6c79206c65667420626c616e6b202020",
        );
        let map = hex("00000100 00000100 00000000 0000001e 00000000 0000 0000 001c 001e ffff");

        assert_eq!(file.len(), 4096);
        assert_eq!(&file[..head.len()], &head[..]);
        assert_eq!(&file[0xEE2..0xEE2 + rsrc.len()], &rsrc[..]);
        assert!(file[0xEE2 + rsrc.len()..0xFE2].iter().all(|b| *b == 0));
        assert_eq!(&file[0xFE2..], &map[..]);
        assert!(file[head.len()..0xEE2].iter().all(|b| *b == 0));
    }

    #[test]
    fn the_custom_icon_bit_is_set_and_cleared_without_touching_the_rest() {
        let mut fi = [0u8; 32];
        fi[..4].copy_from_slice(b"fold");
        fi[8] = 0x40;
        let on = with_custom_icon(fi, true);
        assert_eq!(on[8], 0x44);
        assert_eq!(&on[..4], b"fold");
        assert_eq!(with_custom_icon(on, false), fi);
    }
}
