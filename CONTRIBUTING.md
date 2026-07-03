# Contributing to Cloudreve Desktop Client

Thanks for your interest in contributing! This document explains how to set up your environment and submit changes.

## Project overview

Desktop sync client for [Cloudreve](https://cloudreve.org) (macOS & Linux), built with [Tauri](https://tauri.app):

| Directory | Description |
|---|---|
| `ui/` | React frontend (Vite + TypeScript + MUI) |
| `src-tauri/` | Tauri shell — IPC commands, tray, windows |
| `crates/cloudreve-sync/` | Sync engine — drives, tasks, conflict handling, SQLite inventory |
| `crates/cloudreve-api/` | Cloudreve API client |

## Development setup

### With Nix (recommended)

The repository ships a `flake.nix` providing all dependencies (rustup, pkg-config, openssl, yarn, cargo-tauri):

```sh
nix develop
```

If you use [direnv](https://direnv.net), an `.envrc` is already present — just run `direnv allow` once and the shell loads automatically.

### Manual setup

You will need:

- [Rust](https://rustup.rs) (stable toolchain)
- [Node.js](https://nodejs.org) + [Yarn](https://yarnpkg.com)
- [cargo-tauri](https://tauri.app/start/prerequisites/) CLI and the Tauri platform prerequisites for your OS

### Building and running

```sh
# Install frontend dependencies
cd ui && yarn install && cd ..

# Run in development mode (starts Vite + Tauri)
cargo tauri dev

# Debug build (runs `tsc -b && vite build` first)
cargo tauri build --debug

# Quick Rust-only check
cargo check -p cloudreve-sync -p cloudreve-desktop

# Lint the frontend
cd ui && yarn lint
```

### Running the tests

```sh
# All tests (unit + integration)
cargo test -p cloudreve-sync

# Only the sync integration scenarios
cargo test -p cloudreve-sync --test sync_scenarios

# Only unit tests (inventory, conflict lifecycle, fs events...)
cargo test -p cloudreve-sync --lib

# A single test by name
cargo test -p cloudreve-sync both_sides_modified
```

Tests are **behavior-driven**: they describe real user situations (edits on both sides, deletions, same-size edits...) and assert the outcome the user cares about — no silent data loss, conflicts surfaced instead of overwritten, deletions never propagated to the server.

The integration tests in `crates/cloudreve-sync/tests/` run against a real `Mount`, a real SQLite inventory, and real files on disk; only the Cloudreve API is faked with a [wiremock](https://docs.rs/wiremock) HTTP server (see the `TestEnv` harness in `tests/common/mod.rs`). No network access or real server is needed.

If your change touches the sync engine, add a scenario to `tests/sync_scenarios.rs` describing the user-visible behavior — reuse `TestEnv` (`write_local`, `track_synced`, `set_remote_files`, `full_sync`, `all_tasks`...).

## How to contribute

### 1. Fork and branch

1. **Fork** the repository on GitHub.
2. Create a branch from `main` with a descriptive name:

```
feat/conflict-resolution-ui
fix/reauth-loop-on-scope-error
docs/update-readme
```

Use the `<type>/<short-description>` pattern, with the same types as conventional commits (see below).

### 2. Commit messages — Conventional Commits

We follow [Conventional Commits](https://www.conventionalcommits.org):

```
<type>(<optional scope>): <description>
```

Common types:

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `chore:` — maintenance (dependencies, release bumps, tooling)
- `test:` — adding or fixing tests

Examples from this repository:

```
feat: detect sync conflicts and prevent silent overwrites
fix: retry upload with overwrite when file already exists on server
chore(release): bump version to 0.2.0-beta.2
```

Keep commits focused: one logical change per commit.

### 3. Translations

The UI is translated in 11 languages under `ui/public/locales/<lng>/common.json`. If your change adds user-facing strings:

- Add the key to **all** locale files (de, en-US, es, fr, it, ja, ko, pl, ru, zh-CN, zh-TW), keeping the same key position in each file.
- Use the `t("key", "English fallback")` pattern in components.

### 4. Before opening a pull request

- Make sure `cargo test -p cloudreve-sync` passes.
- Make sure `cargo check` passes and the frontend builds (`cargo tauri build --debug`).
- Run `yarn lint` in `ui/` for frontend changes.
- Rebase on the latest `main` if needed.

### 5. Open the pull request

- Target the `main` branch.
- Describe **what** the change does and **why**.
- Link related issues (`Fixes #123`).
- Screenshots are appreciated for UI changes.

## Reporting bugs

Open an issue with:

- Your OS and version (macOS / Linux distribution)
- Cloudreve **server** version
- Client version
- Steps to reproduce and relevant logs

## Questions

Feel free to open a discussion or an issue if anything is unclear. Thanks for contributing!
