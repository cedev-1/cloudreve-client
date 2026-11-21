# Cloudreve MacOS & Linux Desktop Client

> [!IMPORTANT]
> This client is still young — grab the [latest release](https://github.com/cedev-1/cloudreve-client/releases/latest).

<img width="689" height="586" alt="image" src="https://github.com/user-attachments/assets/a199526f-c6ab-467b-8f3d-f8e1330f91ef" />

#### New AUR package

See [cloudreve-client](https://aur.archlinux.org/packages/cloudreve-client) (Maintainer [Loooooooooong](https://github.com/Loong-OvO))

#### Nix

Flake with a package, home-manager, NixOS and nix-darwin modules: see [docs/nix.md](docs/nix.md).

Desktop sync client for [Cloudreve](https://cloudreve.org) - fork from [cloudreve windows client](https://github.com/cloudreve/desktop).

## Files on demand (Beta)

> [!CAUTION]
> Files on demand is in **Beta**: it may have bugs, use it at your own risk and keep a backup of important files.

When you add a drive, you choose between **Download everything** (every file is copied to your computer and kept in sync) and **Files on demand** (all files show up right away but use no disk space until you open them). The choice cannot be changed afterwards.

See **[docs/files-on-demand.md](docs/files-on-demand.md)** for how it behaves, requirements (FUSE on Linux), deleting and renaming, and troubleshooting.

> [!WARNING]
> Cloudreve server version **4.16.1** has a bug where OAuth scope enforcement blocks desktop clients from accessing `/user/me` and other user endpoints ([details](https://github.com/cloudreve/cloudreve/issues/3471)). Please upgrade to **4.17.0**.

## License

MIT, see [LICENSE](LICENSE). This project is a fork of [cloudreve/desktop](https://github.com/cloudreve/desktop) (MIT, Copyright (c) 2025 Cloudreve).
