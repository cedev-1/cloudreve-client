# On-demand drives: architecture

How "Files on demand" works inside the client. For what users see, read the
[README](../README.md#files-on-demand). This page describes the code as it is.

## The idea

An on-demand drive is a virtual filesystem mounted on the drive folder. Every
file and folder of the cloud is listed, but bytes are downloaded only when a
program reads them, and kept in a local block cache. Writes go to local
*drafts* and are uploaded a moment after the file is closed.

There is no macFUSE and no File Provider extension (the app is not signed):

| OS | How the folder is mounted |
|---|---|
| macOS | An NFSv3 server inside the app, on `127.0.0.1` (`nfs3_server` crate), mounted with the system's `/sbin/mount_nfs`. |
| Linux | FUSE through the `fuser` crate (pure Rust, no libfuse link); mounting goes through the system's setuid `fusermount3`. |

"One brain, two plugs": all behavior lives in one facade (`Vfs`); the NFS and
FUSE adapters only translate requests and errors.

## Crates and modules

```
crates/cloudreve-vfs/        the virtual filesystem (no app knowledge)
  vfs.rs           Vfs facade: lookup/readdir/open/read/write/create/
                   mkdir/rename/unlink/close, the junk overlay, busy rules
  tree.rs          lazy directory tree: NodeIds (stable inodes), listings
                   cached LISTING_TTL (5 s), invalidation, Unicode matching
  cache.rs         1 MiB block cache per (path, etag), LRU, pinned while open
  writeback.rs     DraftStore (on-disk drafts) + WriteBackQueue (debounced,
                   one-at-a-time uploads, retries, conflict copies)
  junk.rs          OS/app metadata names: local-only or refused
  nfs.rs           macOS adapter (NFSv3 procedures -> Vfs)
  fuse.rs          Linux adapter (fuser::Filesystem -> Vfs), cfg(linux)
  frontend_util.rs shared attributes and errno classification
  mount.rs         mount/unmount per OS, stale-mount detection and cleanup
  nudge.rs         makes open Finder/Nautilus windows refresh

crates/cloudreve-sync/       the app side
  drive/vfs_mode.rs   cache dir, cache size, attach/detach, Linux FUSE
                      preflight, mount error classification + messages
  drive/mounts.rs     Mount: DriveMode branches in start/pause/resume/
                      shutdown/delete, VfsEvent pump -> counters + toasts
  drive/manager/      DriveManager: load/add/resume/remove, status summary
  drive/remote_events.rs  server events (SSE) -> tree invalidation, retries
```

## Reading

1. `lookup`/`readdir` list a folder from the server the first time and keep
   the listing for `LISTING_TTL`; a server event for that folder forgets it
   early. A failed listing (offline) keeps serving the last known content.
2. `open` asks the server for a signed download URL (deferred to the first
   read that needs the network if that fails, so cached files open offline).
3. `read` serves 1 MiB blocks from `cache.rs`; missing runs are fetched with
   ranged GETs (plus readahead). A server that ignores `Range` and answers
   `200` with the whole file is handled by slicing.
4. Blocks are keyed by etag: a new version on the server never serves old
   bytes. The cache is capped (10 GiB, clamped down on a nearly full disk,
   512 MiB floor); open files are pinned; drafts are never evicted.

On macOS a read slower than the NFS soft timeout answers `NFS3ERR_JUKEBOX`
so the kernel retries instead of failing (`nfs.rs`). On Linux, read-side
requests run as tasks, so one slow download never blocks the mount.

## Writing

```
write -> draft (Editing) --close--> Pending --2 s debounce--> Uploading
           ^                                                    |
           |                         success: draft removed  <--+
           +-- reopen within the debounce cancels the upload    |
                                     failure: Pending, retried <-+
```

- A draft is a full local copy (`~/.cloudreve/vfs-cache/<drive>/drafts/`) plus
  `draft.json` (path, base etag, state). Partial writes to an existing file
  first materialize its current content.
- On restart, every draft is queued again (`Editing` and `Uploading` are
  demoted to `Pending`): nothing written is lost by quitting or crashing.
- Before uploading, the server's current etag is compared to the draft's base
  etag. If someone else changed the file, the draft is uploaded as a
  *conflicted copy* next to it (`ConflictSaved` event, notification). Nothing
  is ever overwritten.
- Failed uploads (3 attempts) are parked and retried on their own after 30 s,
  then with a doubling delay capped at 10 min, and on every server reconnect.
- Deleting a file whose upload is running lets the upload finish, then deletes
  what it created (the file never comes back). Renaming it waits up to
  `BUSY_GRACE` (1.5 s) for the upload, then answers busy.
- `rename` onto an existing file (every editor's atomic save) deletes the
  destination then renames; a draft follows its file.

## Names and metadata

- `junk.rs` decides per name. **Local-only** (works, kept in memory, never
  uploaded, hidden from listings): `._*` AppleDouble sidecars (macOS stores
  xattrs and Finder tags there on NFS), `.DS_Store`, `.VolumeIcon.icns`,
  `Icon\r`, KDE's `.directory`, LibreOffice `.~lock.*#`, MS Office `~$*`, the
  app's own `.cloudreve-refresh`. **Refused** (EACCES): volume folders such as
  `.Trashes`, `.Spotlight-V100`, `.TemporaryItems`, `.fseventsd`, Linux
  `.Trash`/`.Trash-<uid>`, `__MACOSX`. Refusing the trash folders makes Finder
  and Nautilus offer to delete immediately instead of moving files into a
  hidden folder of the cloud.
- Temp files editors rename onto the real name (`.goutputstream-*`,
  `~WRL*.tmp`, `lu*.tmp`) are ordinary files: junk can never be renamed onto
  a real name.
- Unicode: macOS sends decomposed names (NFD). Lookups match names equal
  under NFC, and names the client creates are sent to the server in NFC.

## Mount lifecycle

- `vfs_mode::attach`: clean a stale mount first, then require an empty folder,
  then (Linux) check `/dev/fuse` and `fusermount3`, then mount.
- A marker `<cache>/mount.port` records the owner (macOS: the NFS port;
  Linux: the pid). `cleanup_stale_mount` only force-unmounts a mount whose
  owner is gone — never another running instance's. On Linux a mount that
  answers ENOTCONN is stale even without a marker; `/proc/mounts` escapes
  (`\040` for a space) are decoded; the unmount ladder ends with
  `fusermount3 -uz` (lazy), which works for a regular user on a busy mount.
- macOS mount options: `nobrowse` (no Desktop icon), `locallocks`,
  `actimeo=1`, `nonegnamecache`, soft mount with short timeouts; Spotlight is
  kept off with `.metadata_never_index`.
- Pause and quit unmount; remove also deletes the cache (after giving running
  uploads up to 20 s). The Linux FUSE session unmounts on drop.

### When mounting fails

`classify_mount_error` maps the error to a `MountProblem` (folder not empty,
FUSE missing, permission denied, stale mount, other) and
`mount_problem_message` turns it into a localized sentence that says what to
do (`locales/app.yaml`, `mountError*`).

- Adding a drive: the add window shows that sentence and stays on the last
  step (the sign-in is kept), nothing is saved.
- At launch: the drive is **kept** (listed and saved in `drives.json`) with
  `status: mount_failed` and the sentence in `DriveInfo::mount_error` and
  `StatusSummary::mount_errors`; a notification is shown. Its `Vfs` still runs,
  so drafts from the last session keep uploading. "Retry" (`resume_sync`)
  remounts and clears the error.

## Refreshing open windows

The NFS client and FUSE kernel cache attributes for 1 s, so a remote change
shows up on the next look. But Finder and Nautilus only re-read a folder when
they get an event: on a remote change the app creates and removes a probe file
(`.cloudreve-refresh`, local-only) in that folder through the mount itself,
which fires FSEvents/kqueue (macOS) or inotify (Linux) in the file manager.

## Status shown to the user

- `StatusSummary::pending_uploads[drive]`: files whose changes are not on the
  server yet (all drafts, including parked failures).
- `DriveInfo::unsynced_files`: same count, used to warn before removing a drive.
- Toasts: conflict copy created, upload given up for good, cache limited by
  low disk space, drive could not be opened.

## Tests

| Suite | What it covers | Where it runs |
|---|---|---|
| `cloudreve-vfs` unit + `tests/*.rs` (wiremock server) | the facade end to end: reads, drafts, uploads, conflicts, renames, junk, Unicode, offline | everywhere |
| `tests/mounted_macos.rs`, `mounted_macos_apps.rs` | real NFS mount: Finder-like copies with xattrs, locks, soft timeouts, refresh, crash recovery, NSDocument safe save, Move to Trash, Finder tags (through Foundation, via `tests/support/macos_file_apis.swift`) | macOS CI (local runs need the Network Volumes permission) |
| `tests/mounted_linux.rs`, `mounted_linux_desktop.rs` | real FUSE mount: read/write, EBUSY, refresh, desktop trash, office locks, gedit-style save, `renameat2` no-replace, free space, slow download vs browsing, a real SIGKILL crash | Linux CI |
| `cloudreve-sync` (`on_demand_*.rs`, `manager` unit tests) | app wiring: mount seam, failure at launch kept + retry, status counts, SSE | everywhere |
| `cloudreve-desktop --lib` | the Tauri command boundary and JSON shapes the UI reads | everywhere |

Running the Linux mounted suites on a Mac: Docker with FUSE,

```sh
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined \
  -v "$PWD":/src -w /src rust:1-bookworm bash -c \
  'apt-get update && apt-get install -y fuse3 libssl-dev pkg-config && useradd -m t && \
   su t -c "PATH=$PATH CARGO_HOME=/tmp/cargo CARGO_TARGET_DIR=/tmp/target cargo test -p cloudreve-vfs --test mounted_linux --test mounted_linux_desktop"'
```

## Known limits

- Extended attributes and Finder tags are kept in memory only (forgotten at
  quit); permissions, owners and times set by programs are not stored.
- No symlinks, hard links or special files on the drive.
- The quota shown by the OS is a fixed 1 TiB, not the cloud's real quota.
- Renaming a folder that contains unsaved changes is refused (busy) until they
  are uploaded.
- An open file deleted on the server keeps its old content for that handle.
- Changing the mode of an existing drive is not supported (remove and add it
  again).
