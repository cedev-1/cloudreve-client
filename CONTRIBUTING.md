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
