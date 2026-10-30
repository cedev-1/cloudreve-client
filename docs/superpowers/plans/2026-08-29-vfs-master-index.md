# On-demand VFS — master plan index

**Spec:** `docs/superpowers/specs/2026-08-29-vfs-on-demand-design.md`
**Branch:** all work happens on `feat/vfs-on-demand`; nothing merges to `main`
until every phase is done and the level-3 manual checklist passes.

The feature ships as four phase plans. Each phase produces working, tested
software on its own; a phase plan is written in full detail when the previous
phase completes (carrying its learnings), never before.

| Phase | Plan file | Delivers | Merge-blocking tests |
|---|---|---|---|
| 1 | `2026-08-29-vfs-phase1-read-core.md` — **DONE 2026-08-30** | `cloudreve-vfs` crate: lazy tree (TTL+invalidation), pin-aware block cache (etag, LRU, crash self-heal), ranged-read facade with readahead, cap enforced end-to-end. Range verified on the real instance (no fallback needed). | 19 tests (10 unit + 9 integration), all mutation-verified; workspace 196/0; clippy vfs = 0 |
| 2 | `2026-08-30-vfs-phase2-write-path.md` — **DONE 2026-08-31** | `cloudreve-uploader` extracted (shared crate); refcounted pins; drafts + debounced write-back with conflict copies, bounded retry, stuck-state recovery; restart survival; mkdir/unlink/rename. All three phase-1 debts closed. | 61 crate tests (all mutation-verified); workspace 240/0; clippy vfs-owned = 0 |
| 3 | `2026-09-02-vfs-phase3-frontends.md` — **DONE 2026-09-02** | Phase-2 debt burn-down (rename EBUSY guard incl. directory-descendant prefix matching, conflict-name uniqueness, tmp sweep); NFS adapter (`nfs3_server`, per-RPC handles) + macOS `mount_nfs` lifecycle with stale-mount cleanup; FUSE adapter (`fuser 0.15`, persistent handles, real EBUSY/ESTALE) + Linux mount branch; CI runs the mounted suites on BOTH OS runners (macos run green unprivileged; ubuntu green with fuse3). | 99 crate tests (3 real macOS mounts local, 4 real FUSE mounts on CI); workspace 278/0; CI both jobs green: https://github.com/cedev-1/cloudreve-client/actions/runs/33665943802 |
| 4 | `vfs-phase4-integration.md` (to write) | `DriveConfig.mode`, `Mount` wiring, SSE invalidation hookup, effective cache cap bounded by free disk (`disk_space.rs`), add-drive UI + settings + dashboard tasks, 11 locales | Full workspace green + level-3 manual checklist |

Phase order is strict: 2 needs 1's facade, 3 needs a working facade to mount,
4 needs everything.

## Debts carried out of phases 1-2 (binding on later phase plans)

Phase-1 debts: ALL CLOSED in phase 2 (refcounted pins T2, prune-on-relist T4,
LRU stamp redesign T3).

- **Phase 3 debts: ALL CLOSED 2026-09-02** (rename-with-open-handle → EBUSY
  guard incl. directory-descendant prefix matching in the final fix wave;
  same-day conflict collision → uniqueness suffix; tmp sweep → in `Vfs::new`).
  Torn-at-original-path stays a documented residual (no byte loss; converges
  on any re-upload).
- **Phase 4 MUST include (carried from the phase-3 final review triage —
  verbatim obligations for the phase-4 plan):**
  - Directory-rename descendant residual: the EBUSY guard's descendant check
    is check-then-act across per-path lock keys — a descendant open/create in
    flight during an ancestor rename can still slip through (~one HTTP
    round-trip window). Full fix = hierarchical/namespace locking.
    Documented at `Vfs::rename`.
  - Editor-save patterns BEFORE the level-3 manual checklist:
    unlink-of-open-file resurrection, and rename-onto-an-open-destination
    (atomic-save idiom: write tmp, rename over the target).
  - NOTEMPTY mapping / recursive-rmdir decision at the facade.
  - Deadlines on prod mount/umount shell-outs (Command::output inline in
    async context today).
  - `mount()` defensive pre-clean (call `cleanup_stale_mount` before
    mounting, per D5's original intent) + start the stale-cleanup escalation
    at `umount -f` (halves worst-case startup latency).
  - Two-app-instances false positive in stale-mount detection.
  - Bridge fuser's `log`-crate output into `tracing`.
  - Optional: re-resolve `open()`'s attr under `open_lock` (today a lost
    race fails loudly — acceptable, but re-resolving removes the failure).
- **Phase 4 MUST include (from phase 2):** a test pinning `invalidate_path` called with a
  DIRECTORY's own path BEFORE wiring SSE — including the case where the dir's
  own `children`/`listed_at` must be cleared (a direct readdir within TTL still
  serves the old list today); the two-handles-post-rewrite etag ping-pong (disk
  entry keyed by path-hash only); and reconnect wiring notes: concurrent
  `retry_pending_uploads` callers can duplicate stranded-recovery enqueues
  (harmless, wasteful), and a panic unwinding through an upload leaks an
  `in_flight` entry (recovery net blind for that path until restart).
  Also: the VfsEvent channel is NOT reconstruction-complete — UploadQueued has
  no terminal counterpart when a reopen cancels the timer, an unlink drops a
  queued draft, or a rename migrates it; the phase-4 dashboard design must
  either add terminal events (Cancelled/Dropped/Renamed) or poll DraftStore
  state instead of folding events alone.
