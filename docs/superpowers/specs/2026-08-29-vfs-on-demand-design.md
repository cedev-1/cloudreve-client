# On-demand virtual drive (VFS) — design

**Date:** 2026-08-29
**Target:** v1.1, developed on a dedicated branch, merged only when complete
**Status:** approved design, pre-implementation

## 1. Goal

Give each drive an optional **on-demand mode**: instead of mirroring every file
to disk, the chosen folder becomes a mounted virtual drive. The full tree is
visible immediately; a file's bytes are downloaded only when something opens
it, cached locally, and edits are uploaded back automatically. The experience
Windows users get from OneDrive/Dropbox — within what macOS and Linux allow
without a kernel extension and without any separate install.

**Non-goals for v1.1**

- Finder sync badges and a "Free up space" context menu (needs a File
  Provider / FinderSync app extension; possible later, see §9).
- Windows support (no Windows builds ship today).
- File locking / collaborative-edit protection (NFS mounts run `nolocks`;
  conflict copies cover the failure mode instead).
- Replacing mirror mode. Mirror stays the default and is untouched.

## 2. Why this shape (research summary, Aug 2026)

Three agents researched how every comparable product does this. Full details
in the conversation; the facts that fix the architecture:

- **macFUSE (kext)** requires a hand-installed .pkg and Recovery-mode
  approval on Apple Silicon. Every OSS project is fleeing it. Rejected.
- **FUSE-T** is kext-less but closed-source, one-author, and **requires a
  commercial license to bundle**. The Rust `fuser` crate cannot talk to it
  (open issue since 2024). Rejected.
- **File Provider** is what Dropbox/Google/OneDrive/SeaDrive/Nextcloud use.
  No restricted entitlement — but it demands a Swift appex built by Xcode and
  hand-injected into the bundle; **no Tauri app has ever shipped one**, and
  Nextcloud's tracker shows the ongoing cost. Deferred, not rejected (§9).
- **In-process NFS server mounted on 127.0.0.1** is the proven kext-free,
  zero-install pattern: rclone built `nfsmount` exactly for this ("installing
  FUSE is very cumbersome" — their docs), XetHub and Hugging Face ran the
  `nfsserve` crate in production. Plain-user mount, no sudo. **Chosen for
  macOS.**
- **FUSE via `fuser`** is native and painless on Linux (fusermount3 is
  everywhere). **Chosen for Linux.**
- An **rclone sidecar** (OpenList Desktop pattern) was considered and
  rejected: +50 MB binary, WebDAV backend, credentials in an external
  config, no control over UX/errors.

Known trade-offs accepted with the NFS approach: the volume appears as a
network drive (ejectable, no badges), atime/mtime are coupled, no byte-range
locks, and a crashed app leaves a stale mount (cleaned at startup, §6).

## 3. Architecture

One brain, two plugs.

```
crates/
  cloudreve-api        existing HTTP client — unchanged
  cloudreve-sync       existing mirror engine — unchanged
  cloudreve-vfs        NEW
    src/
      tree.rs          virtual tree: inode <-> remote path, metadata, TTL, SSE invalidation
      cache.rs         block read-cache on disk + LRU accounting
      writeback.rs     open-for-write drafts + upload queue on close
      vfs.rs           facade: lookup/readdir/open/read/write/close/rename/mkdir/unlink
      nfs.rs           macOS frontend: nfs3_server impl -> mount_nfs 127.0.0.1
      fuse.rs          Linux frontend: fuser::Filesystem impl
```

`vfs.rs` holds ALL logic. `nfs.rs` and `fuse.rs` are thin adapters (~300
lines each) translating their protocol onto the facade. The facade is
testable without mounting anything (§7).

Dependencies added: `nfs3_server` (BSD-3, maintained — fork-and-own is the
fallback if it stalls) on macOS, `fuser` on Linux.

## 4. Read path

- **Tree.** On mount, list the drive root via `cloudreve-api`. Subdirectories
  list lazily on first `readdir` — no recursive walk, a 500k-file drive
  mounts instantly. Each entry keeps remote path, size, etag, updated_at and
  a locally-assigned stable inode. Listings carry a short TTL (a few
  seconds); the existing SSE stream invalidates entries eagerly (a remote
  change refreshes the entry and drops its cached blocks via the etag).
- **Reads.** `read(offset, len)` is split into 1 MiB blocks. Cached block →
  served from disk. Missing block → ranged download (`Range: bytes=…`
  against the same download URL the mirror engine already obtains), plus a
  few blocks of readahead. A 2 GB video starts playing in about a second.
- **Range support: VERIFIED 2026-08-29** (plan step 0) against the real
  instance (local-storage backend, URLs served by the server itself):
  `Accept-Ranges: bytes`, exact `206` slices mid-file and open-ended
  (`bytes=a-`), spec-compliant `416` past EOF. S3-style backends honor Range
  natively on presigned URLs. **Decision: no whole-file fallback is built.**
  Two operational notes for the implementation: send a real `User-Agent`
  (the probe's default python UA was 403'd by the instance's WAF), and treat
  a `416` as the EOF signal it is.
- **Disk cache.** `~/Library/Application Support/cloudreve.desktop/vfs-cache/<drive-id>/`
  (Linux: the XDG equivalent). One sparse file per remote file plus a small
  present-blocks table keyed by etag — etag change invalidates the whole
  entry. Per-drive max size, **default 10 GiB**, LRU eviction of closed
  files. Reuses the existing `disk_space.rs` guard so the cache never eats
  the volume.

## 5. Write path

- **First `write()`** flips the file into a local draft inside the cache. If
  the file existed and the write is partial, missing ranges are downloaded
  first (POSIX semantics). While open, everything is local and fast.
- **Last `close()`** queues the draft into a write-back uploader (same
  pattern as the existing `TaskQueue`: retries, backoff, progress visible in
  the dashboard). A ~2 s debounce absorbs editors that save-close-reopen.
  The file stays readable from the cache during upload.
- **Conflicts.** If the remote etag no longer matches the etag the draft
  started from, never overwrite: upload as `name (conflict YYYY-MM-DD).ext`
  and toast the user. Same philosophy as mirror mode — never lose a byte.
- **Offline / failed upload.** The draft stays cached, flagged pending,
  retried on reconnection (existing offline_waiting mechanics). Pending
  drafts are **exempt from LRU eviction** — data not yet uploaded is never
  discarded.
- **rename / mkdir / unlink** call the API synchronously and invalidate the
  affected tree entries.

## 6. Frontends and app integration

**macOS (NFS).**
- `nfs3_server` bound to `127.0.0.1:<random port>`, then
  `mount_nfs -o nolocks,tcp,port=…,mountport=…` onto the drive's folder.
  Plain user, nothing to install. Volume named after the drive.
- Mounted when the app starts (for drives in on-demand mode), unmounted
  cleanly on quit.
- **Stale-mount cleanup at startup**: if the app crashed, force-unmount the
  mountpoint (`umount -f` / `diskutil unmount force`) before remounting.
  This is the known trap of the pattern; handled from day one.

**Linux (FUSE).**
- `fuser` mount through fusermount3. Same callbacks onto the same facade.
- If `/dev/fuse` is missing (headless server), fail with a clear UI error,
  never a crash.

**App integration.**
- `DriveConfig` gains `mode: Mirror | OnDemand` and `cache_max_mb`.
- In OnDemand mode, `Mount` starts the VFS and does NOT start the fs watcher
  or `full_sync`; SSE stays on (feeds tree invalidation).
- Add-drive UI offers the mode choice; drive settings expose cache size.
- Write-back uploads appear as normal tasks in the dashboard. New i18n keys
  in all 11 locales.

## 7. Error handling

- Network loss mid-read: cached blocks keep serving; uncached reads fail
  with EIO after a bounded retry — no infinite hangs under Finder.
- API auth expiry: same refresh path the mirror engine uses (shared client).
- Server errors on ranged download: retry with backoff, then EIO.
- Cache corruption (missing/truncated block file): treated as a miss,
  re-downloaded.
- Unmount failures on quit: logged, retried with force on next startup (§6).

## 8. Testing (merge gate)

**Level 1 — facade, nothing mounted (bulk of coverage).**
`TestEnv`-style harness with wiremock. Call `open/read/write/close` directly;
assert bytes, emitted HTTP requests (including exact `Range` headers), cache
contents on disk. Scenarios: cache hit/miss, readahead, etag invalidation,
LRU eviction sparing pending drafts, write-back, debounce, conflict rename,
offline retry. Deterministic, cross-platform, mutation-verifiable — same
discipline as the ignore-patterns work.

**Level 2 — end-to-end, actually mounted.**
- macOS: start the NFS server, mount a temp dir, drive it with ordinary
  `std::fs` through the mountpoint, assert against wiremock. User-level
  mount → runs on GitHub macOS runners.
- Linux: same through a real FUSE mount (`/dev/fuse` is available on ubuntu
  runners).
- Crash test: kill the serving process, verify the next startup cleans the
  stale mount and remounts.

**Level 3 — manual checklist before merge.**
Real Finder (Quick Look, folder copies, eject), 2 GB video streaming,
LibreOffice save cycle, network cut mid-read, quit/relaunch.

**Merge criteria:** levels 1 and 2 green in CI on both OSes + level 3
checklist done by hand. TDD throughout: every behavior lands test-first and
is mutation-verified.

## 9. Future (explicitly out of scope)

- **File Provider appex** (Finder badges, dataless files, CloudStorage):
  the Nextcloud pattern proves Swift appex + XPC + Developer ID works
  outside the App Store, but no Tauri precedent exists and the maintenance
  cost is high. Revisit for 2.x if demand justifies it.
- **FSKit backend**: network filesystems only on macOS 26+, and it is an
  appex too. Watch, don't build.
- Windows via Cloud Files API, if Windows builds ever ship.
