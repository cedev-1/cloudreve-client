# VFS Phase 3 — Mount Frontends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The VFS becomes a real drive: an in-process NFS server mounted on
127.0.0.1 (macOS) and a FUSE mount (Linux), with stale-mount cleanup, RAII
unmount, error mapping to errno, real end-to-end tests through `std::fs`, and
CI running them on both OSes.

**Architecture:** One brain, two plugs — the phase-1/2 `Vfs` facade is the
brain; `nfs.rs` and `fuse.rs` are thin adapters sharing a platform-neutral
`frontend_util.rs` (attr/error mapping, readdir cookies). A `mount.rs` module
owns the lifecycle: spawn server → OS mount → RAII unmount → startup cleanup
of stale mounts. Spec §2/§6 fixed the mechanisms; research (2026-08-29)
validated them (rclone `nfsmount` pattern; `fuser` on Linux).

**Tech Stack:** Rust, tokio, `nfs3_server` (BSD-3 — the maintained fork of
xetdata/nfsserve), `fuser` (Linux-only target dep), wiremock, GitHub Actions
(macos + ubuntu runners).

**Spec:** `docs/superpowers/specs/2026-08-29-vfs-on-demand-design.md` (§6
frontends + §8 level-2 tests = this phase's scope). Debts binding this plan:
"Phase 3 MUST include" in `docs/superpowers/plans/2026-08-29-vfs-master-index.md`.

## Global Constraints

- **NEVER run `cargo fmt`.** Match surrounding style by hand.
- **Claude never commits.** "Commit" steps = stage with `git add`, STOP, the
  user commits personally.
- Branch `feat/vfs-on-demand`; work in `crates/cloudreve-vfs` + `.github/workflows/ci.yml` (Task 5 only).
- TDD + mutation-verification per task, tests behavioral through real entry
  points, fixtures independent of implementation constants.
- Never touch or read the stray `desktop/` directory.
- **Platform reality (be honest about it):** the dev machine is macOS. Linux
  FUSE code is `#[cfg(target_os = "linux")]` and CANNOT be executed (or even
  compiled) locally — it is validated by CI (Task 5). Every claim about the
  Linux path must say "pending CI", never "verified", until the ubuntu job is
  green.
- Zero new clippy warnings owned by `cloudreve-vfs` (pre-existing ones live in
  cloudreve-api/cloudreve-uploader only).
- Phase-2 test suites (65 tests) are the regression net — green untouched.

## Design decisions (tasks reference these)

- **D1 — one public entry:**
  `mount::mount(vfs: Arc<Vfs>, mountpoint: &Path, volume_name: &str) -> Result<MountedVfs>`.
  `volume_name` is advisory: on Linux it becomes fuser's `fsname`/`subtype`
  option; on macOS, plain `mount_nfs` has no volname option — the Finder shows
  the mountpoint's directory name, so phase 4 should name the mount DIRECTORY
  after the drive (document this on the function).
  `MountedVfs::unmount(self) -> Result<()>` is the clean path; `Drop` does a
  best-effort unmount (logged, never panics). Platform dispatch inside via
  `cfg`; unsupported OS → clear error. (`Vfs` moves behind `Arc` at the mount
  boundary — adapters and the facade share it; the facade's `&self` API
  already supports this.)
- **D2 — errno mapping lives in ONE place** (`frontend_util.rs`), consumed by
  both adapters: `StaleHandleError` → ESTALE; the facade's EEXIST-create error
  → EEXIST; the new rename-busy error (D3) → EBUSY; lookup-miss → ENOENT;
  everything else → EIO (with the anyhow chain logged at debug). The mapping
  is keyed on typed errors where they exist and on the two distinct message
  markers the facade already guarantees — if a marker proves fragile, add a
  typed error to vfs.rs instead (disclose).
- **D3 — the phase-2 BLOCKER dies at the facade:** `Vfs::rename` refuses when
  any open handle exists on the source path (a typed `RenameBusyError`,
  EBUSY). One guard in vfs.rs covers both frontends; frontends never need to
  serialize anything themselves. (Documented at `Vfs::rename`, replacing the
  phase-2 limitation note.)
- **D4 — NFS specifics:** implement `nfs3_server`'s filesystem trait
  (`NFSFileSystem` in the fork — read its docs.rs and the crate's demo fs
  first; pin the crate version in the report). `fileid3` = `NodeId.0` (already
  stable u64, root = 1). readdir cookies = the entry's position index over the
  facade's `readdir` snapshot (the trait's contract: cookie 0 = start;
  re-listing between calls may reshuffle — acceptable, Finder re-reads).
  Capabilities: read-write. Server binds `127.0.0.1:0` (ephemeral port).
  macOS mount command (rclone's pattern):
  `/sbin/mount_nfs -o nolocks,tcp,soft,port=<P>,mountport=<P> 127.0.0.1:/ <mountpoint>`
  — flags verified live during Task 3 (adjust and record what the OS
  actually accepted). Unmount: `/sbin/umount <mountpoint>`, escalating to
  `umount -f` then `diskutil unmount force` on failure.
- **D5 — stale-mount cleanup:** before mounting, if `mountpoint` is already an
  NFS mount (parse `mount` output for the path), force-unmount it first —
  that's the crash-recovery path the spec demands (§6). Tested by killing the
  server task and re-mounting (Task 3).
- **D6 — FUSE specifics:** `fuser::Filesystem` is a sync trait; the adapter
  holds a `tokio::runtime::Handle` and `block_on`s facade calls (fuser runs
  its own callback threads — no executor deadlock; say so in a comment).
  Mount via `fuser::mount2` with `AutoUnmount` + `AllowOther` NOT set (single
  user). Missing `/dev/fuse` → the mount() entry returns a clear error
  (spec §6: "fail with a clear UI error, never a crash").
- **D7 — attr mapping** (`frontend_util.rs`): `NodeAttr` → (uid,gid) = process
  euid/egid; mode 0o644 files / 0o755 dirs; nlink 1; times = `mtime_secs` for
  mtime/ctime/atime (atime is a lie the spec accepts — NFS couples them
  anyway); size from the facade (draft overlay included).

---

### Task 1: Phase-2 debt burn-down (three small TDD cycles, one dispatch)

**Files:**
- Modify: `crates/cloudreve-vfs/src/vfs.rs`, `crates/cloudreve-vfs/src/writeback.rs`
- Test: `crates/cloudreve-vfs/tests/write_back.rs`, `tests/namespace_ops.rs`, unit tests in `vfs.rs`/`writeback.rs`

**Interfaces:**
- Produces: `pub struct RenameBusyError { pub remote_path: String }` (typed,
  displayed + downcastable like `StaleHandleError` — copy that idiom);
  conflict names gain a uniqueness suffix; `Vfs::new` sweeps `cache_dir/tmp/`.

Cycle A — **rename-with-open-handle → EBUSY (the blocker, D3).**
- [ ] Test `renaming_a_file_with_an_open_handle_is_refused` (namespace_ops.rs):
  open a handle on an existing file, call `vfs.rename` on it → Err downcasting
  to `RenameBusyError`; close the handle; rename again → Ok, listing shows the
  new name. RED first (today the rename silently proceeds).
- [ ] Implement: the facade tracks open handles per remote_path already
  (open_files map — check the exact structure); rename checks it under the
  same lock that open uses (no TOCTOU: take the per-path draft lock or the
  open_files lock consistently — walk open()'s locking and match it).
  Replace the phase-2 limitation rustdoc with the new contract.
- [ ] Mutation: skip the guard → test red; revert green.

Cycle B — **same-day conflict-name collision.**
- [ ] Test `two_conflicts_on_the_same_day_get_distinct_names` (write_back.rs):
  drive the conflict path twice for the same file on the same (mocked-clock?
  no — same real day, deterministic) day; assert BOTH uploads landed under
  DIFFERENT names, both containing "(conflict ". RED first (today the second
  upload targets the same name with overwrite=false and parks forever).
- [ ] Implement: on conflict-name collision (the upload's EEXIST-class error,
  or proactively: probe the listing), append ` 2`, ` 3`, … before the
  extension until free (bounded at 100 with a hard error — comment why).
  Choose probe-vs-retry based on what the mock/server actually returns;
  document the choice.
- [ ] Mutation: always use the bare conflict name → test red; revert.

Cycle C — **materialization tmp sweep.**
- [ ] Unit test `leftover_materialization_temps_are_swept_at_startup`
  (vfs.rs tests or a small integration test): plant a stray file under
  `cache_dir/tmp/`, construct a Vfs, assert the file is gone (and a fresh
  materialization still works — no over-deletion of the dir itself). RED.
- [ ] Implement: `Vfs::new` removes `cache_dir/tmp/*` (contents only), with
  the why-comment (mid-download failures leak temps; safe because temps are
  only referenced within a single materialize call).
- [ ] Mutation: skip the sweep → red; revert.

- [ ] Whole crate green, zero new clippy warnings, stage `git add crates/cloudreve-vfs`.
- [ ] Commit (BY THE USER): `fix: burn down the phase-2 write-path debts`

---

### Task 2: NFS adapter over the facade

**Files:**
- Create: `crates/cloudreve-vfs/src/frontend_util.rs` (attr/errno mapping — D2/D7)
- Modify: `crates/cloudreve-vfs/src/nfs.rs` (placeholder → adapter), `src/lib.rs` (declare frontend_util), `Cargo.toml` (+ `nfs3_server`, latest 0.11.x — pin what resolves and record it)
- Test: `crates/cloudreve-vfs/tests/nfs_adapter.rs` (new — drives the TRAIT directly, cross-platform, wiremock env; NO mounting here)

**Interfaces:**
- Consumes: the whole Vfs facade (open/read/write/truncate/create/close,
  readdir/lookup/getattr, mkdir/unlink/rename; `RenameBusyError`,
  `StaleHandleError`).
- Produces: `pub struct VfsNfs { ... }` implementing `nfs3_server`'s
  filesystem trait; `VfsNfs::new(vfs: Arc<Vfs>) -> Self`;
  `frontend_util::{to_unix_mode, map_errno(err: &anyhow::Error) -> i32-or-enum, attr helpers}`
  (exact signatures set by what the two trait ecosystems need — the ONE rule:
  fuse.rs must be able to reuse them unchanged; keep them fuser/nfs3-agnostic,
  returning plain numbers/structs both can convert).

STUDY FIRST (the trait's exact shape is authoritative, not this plan): the
`nfs3_server` docs.rs + its example filesystem; note how it expresses
lookup/getattr/read/write/create/mkdir/remove/rename/readdir/setattr,
its fileid/cookie/fattr types, and its error type (NFS3ERR_*). Map per D4/D7;
setattr-with-size = `Vfs::truncate` (open a handle if the protocol arrives
without one — check how the demo handles it); write = open-if-needed +
`Vfs::write` (track protocol-handle ↔ FileHandle in a map keyed by fileid,
closing on... NFS3 is stateless — decide the handle lifecycle: open lazily
per operation and close immediately, OR keep an LRU of open facade handles
with a idle-close timer. START with open-per-op + close (simplest, correct —
the draft/cache layers make it cheap); note the perf follow-up if measurable.)

- [ ] **Tests first, RED:** `tests/nfs_adapter.rs` drives the trait object
  directly (no mount): root readdir lists the mocked files with sizes;
  lookup miss → NFS3ERR_NOENT; read(offset,len) returns the exact slice
  (ranged download visible via `download_requests`); write+commit round-trip
  lands in a draft and (after close/settle) uploads (reuse the phase-2 idle
  helper); create/mkdir/remove/rename map through (API hits recorded);
  rename of an open... (skip — protocol has no open, D3 guard is
  facade-level and tested in Task 1); setattr size=0 truncates; getattr on a
  drafted file reports the draft size; `StaleHandleError` → NFS3ERR_STALE
  and the EBUSY rename → the closest NFS3 error (JUDGE from the error set —
  likely NFS3ERR_ACCES or _JUKEBOX; document the choice).
- [ ] Implement `frontend_util.rs` + `nfs.rs` until GREEN; whole crate green.
- [ ] Mutations: (a) errno mapping returns EIO for everything → the NOENT and
  STALE assertions fail; (b) readdir cookie ignores the offset (always from
  0) → a paged-readdir test fails (write one: list a 300-entry dir through
  two cookie'd calls, assert no duplicates/no gaps — 300 entries via a
  loop-built mock listing). Revert each, green.
- [ ] Stage. Commit (BY THE USER): `feat: nfs adapter over the vfs facade`

---

### Task 3: macOS mount lifecycle + mounted E2E

**Files:**
- Create: `crates/cloudreve-vfs/src/mount.rs` (D1/D4/D5), `crates/cloudreve-vfs/tests/mounted_macos.rs` (`#![cfg(target_os = "macos")]`)
- Modify: `src/lib.rs` (declare mount)

**Interfaces:**
- Consumes: `VfsNfs` (Task 2).
- Produces: `mount::mount(vfs: Arc<Vfs>, mountpoint: &Path, volume_name: &str) -> Result<MountedVfs>`;
  `MountedVfs::unmount(self) -> Result<()>`; `mount::cleanup_stale_mount(mountpoint: &Path) -> Result<bool>`
  (public — phase 4 calls it at startup; returns whether something was cleaned).

- [ ] **E2E tests first (they are the spec):**

```rust
// tests/mounted_macos.rs — real mount_nfs against the wiremock-backed Vfs.
#![cfg(target_os = "macos")]
mod common;

/// The whole feature, through the OS: Finder-equivalent std::fs calls.
#[tokio::test(flavor = "multi_thread")]
async fn a_mounted_drive_lists_reads_and_writes_through_std_fs() {
    let env = common::VfsTestEnv::new().await;
    env.set_remote_files(vec![common::remote_file("hello.txt", 11, "e1")]).await;
    env.serve_file_content("hello.txt", b"hello world").await;
    env.expect_uploads().await;

    let (vfs, _events) = /* Vfs::new over env, as in write_back.rs */;
    let mp = tempfile::tempdir().unwrap();
    let mounted = cloudreve_vfs::mount::mount(std::sync::Arc::new(vfs), mp.path(), "CloudreveTest").unwrap();

    // list
    let names: Vec<_> = std::fs::read_dir(mp.path()).unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
    assert!(names.contains(&"hello.txt".to_string()));
    // on-demand read
    assert_eq!(std::fs::read(mp.path().join("hello.txt")).unwrap(), b"hello world");
    // write-through
    std::fs::write(mp.path().join("new.txt"), b"created through the mount").unwrap();
    // give write-back its debounce (test override) then idle
    /* settle via the env's writeback idle pattern */
    assert_eq!(env.uploaded_content("new.txt").as_deref(), Some(&b"created through the mount"[..]));

    mounted.unmount().unwrap();
    assert!(std::fs::read_dir(mp.path()).unwrap().next().is_none(), "unmounted dir is empty again");
}

/// Crash recovery: a dead server leaves a stale mount; cleanup revives the dir.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_mount_from_a_crashed_server_is_cleaned_and_remountable() {
    /* mount; abort the server task WITHOUT unmounting (MountedVfs::leak_for_tests()
       or std::mem::forget + direct handle abort — expose a #[doc(hidden)] test hook
       and justify it); assert cleanup_stale_mount(mp)==true; mount again; a read works. */
}
```

  (Exact settle/velocity details: reuse write_back.rs's debounce override and
  idle helpers; the test file spells them out fully at implementation time —
  the assertions above are the contract.) RED first: `mount` doesn't exist.
- [ ] Implement `mount.rs`: spawn the `nfs3_server` on 127.0.0.1:0, capture
  the port, run the D4 mount command, wrap in `MountedVfs` (server task
  handle + mountpoint), `unmount` = umount escalation chain + server abort,
  `cleanup_stale_mount` per D5. `Drop` = best-effort.
- [ ] GREEN locally (this IS the macOS machine), whole crate green, zero new
  clippy warnings. These tests are slow (real mounts) — mark them `#[ignore]`?
  NO — keep them normal but serialize with a file-based or static mutex if
  parallel mounts collide on ports/dirs (tempdirs isolate dirs; ports are
  ephemeral — likely fine; observe and disclose).
- [ ] Mutation: break `cleanup_stale_mount` into a no-op → the crash test
  fails; revert. And: make `unmount` skip the OS umount (server abort only) →
  the first test's final empty-dir assert fails; revert.
- [ ] Stage (`git add crates/cloudreve-vfs`). Commit (BY THE USER): `feat: mount the vfs over local nfs on macos`

---

### Task 4: FUSE adapter + Linux mount (validated by CI, not locally)

**Files:**
- Modify: `crates/cloudreve-vfs/src/fuse.rs` (placeholder → adapter), `src/mount.rs` (linux branch), `Cargo.toml` (`[target.'cfg(target_os = "linux")'.dependencies] fuser = <latest, pin what resolves>`), `src/lib.rs` (cfg-gate the fuse module)
- Test: `crates/cloudreve-vfs/tests/mounted_linux.rs` (`#![cfg(target_os = "linux")]`) — mirrors Task 3's two E2E tests through `std::fs`, plus a `/dev/fuse`-missing check if expressible.

**Interfaces:**
- Consumes: `frontend_util` (Task 2), the facade, `mount::MountedVfs` shape (Task 3).
- Produces: `VfsFuse` implementing `fuser::Filesystem` (D6: Handle + block_on),
  wired into `mount::mount`'s linux branch (`fuser::mount2`, spawn on a
  dedicated thread, unmount via the fuser session drop + fusermount -u
  escalation).

- [ ] Write the adapter + linux mount branch + the E2E test file. On THIS
  machine you can only `cargo check`/`cargo test` the non-linux surface —
  the linux code will not even compile locally. Discipline: keep the adapter
  a thin translation of the SAME facade calls Task 2's NFS tests already pin
  (that is the cross-platform validation story); every fuser-specific line
  must be justifiable by the fuser docs you read (cite the doc items in
  comments where the API is non-obvious, e.g. reply.entry TTLs — use short
  TTLs matching LISTING_TTL).
- [ ] `cargo test -p cloudreve-vfs` (mac surface) green; zero new clippy
  warnings on the mac surface. State PLAINLY in the report that the linux
  path is compile-and-behavior UNVERIFIED until Task 5's CI runs — no
  "verified" claims.
- [ ] Stage. Commit (BY THE USER): `feat: fuse adapter and linux mount branch (ci-validated)`

---

### Task 5: CI — both OSes, mounted tests

**Files:**
- Modify: `.github/workflows/ci.yml` (currently ONE ubuntu job running `cargo test -p cloudreve-sync -p cloudreve-api` — it doesn't even build cloudreve-vfs today)

**Interfaces:** none (infra).

- [ ] Extend the ubuntu job's test command to `-p cloudreve-sync -p cloudreve-api -p cloudreve-uploader -p cloudreve-vfs` (mounted_linux tests are cfg-linux and will run here; install fuse3 first: `sudo apt-get install -y fuse3` and confirm `/dev/fuse` exists on the runner — GitHub-hosted runners have it).
- [ ] Add a `macos-latest` job: checkout, rust toolchain (mirror the ubuntu steps), `cargo test -p cloudreve-vfs` (the mounted_macos tests run here; mount_nfs needs no privileges — that was the whole point).
- [ ] Keep the workflow's existing style (read it fully first; match step naming/caching conventions).
- [ ] Validation: this cannot be tested locally. Stage the workflow change; after the USER commits AND pushes, watch the run (`gh run watch` / `gh run list`) and iterate: any CI failure on the linux path is Task 4's real test cycle — treat each red run as a RED, fix, re-stage, user re-commits. Budget for 2-3 iterations; record each failure+fix in the report.
- [ ] Commit (BY THE USER): `ci: run the vfs suites and mounted tests on macos and ubuntu`

---

### Task 6: Phase gate

- [ ] `cargo test --workspace` green locally; BOTH CI jobs green on the pushed branch (paste run URLs in the ledger).
- [ ] Coverage audit: spec §6 bullets + §8 level-2 bullets + every "Phase 3 MUST include" debt line map to passing tests (the same-day-conflict and tmp-sweep debts → Task 1; rename blocker → Task 1; stale-mount cleanup + crash test → Tasks 3/4).
- [ ] Update the master index: phase 3 row → done with counts + CI links; strike phase-3 debts; carry discoveries into phase 4's list.
- [ ] Stage docs. Commit (BY THE USER): `docs: mark vfs phase 3 complete`
- [ ] Final whole-phase review (most capable model) over the phase-3 range + ONE fix wave + scoped re-review, as in phases 1-2. Then write the phase-4 plan.
