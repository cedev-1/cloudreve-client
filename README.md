# Cloudreve MacOS & Linux Desktop Client

> [!IMPORTANT]
> This client is still young — grab the [latest release](https://github.com/cedev-1/cloudreve-client/releases/latest).

#### New AUR package

See [cloudreve-client](https://aur.archlinux.org/packages/cloudreve-client) (Maintainer [Loooooooooong](https://github.com/Loong-OvO))

#### Nix

Flake with a package, home-manager, NixOS and nix-darwin modules: see [docs/nix.md](docs/nix.md).

Desktop sync client for [Cloudreve](https://cloudreve.org) - fork from [cloudreve windows client](https://github.com/cloudreve/desktop).

## Files on demand (Beta)

> [!CAUTION]
> Files on demand is in **Beta**: it may have bugs, use it at your own risk and keep a backup of important files.

When you add a drive, you choose how its files are kept on your computer:

- **Download everything** — every file is copied to your computer and kept in sync. Files open without internet, but use as much disk space as your cloud.
- **Files on demand** (Beta) — all your files and folders show up in the drive folder right away, but they use no disk space until you open them. Opening a file downloads it; files you opened recently stay on your computer, so they open instantly next time.

The choice is made when you add the drive and cannot be changed afterwards. To switch, remove the drive and add it again (your files stay in the cloud).

### What to expect

- **Opening a file** for the first time needs an internet connection. Large files start opening before they are fully downloaded (video and audio players can start right away).
- **Saving or adding a file** works like on any folder. The upload starts a couple of seconds after the app finishes writing. The menu bar / tray popup shows how many files are waiting to upload.
- **Offline**: folders you already browsed and files you already opened stay available. Changes you make are kept and uploaded as soon as the connection is back, also after quitting or restarting the app.
- **Changes made elsewhere** (web interface, another computer) appear in the folder within a few seconds.
- **Same file changed in two places**: nothing is overwritten. Your version is uploaded next to the other one as a *conflicted copy*, and you get a notification.
- **Deleting a file** in the drive folder deletes it from your cloud. The drive has no trash of its own: on macOS, Finder offers *Delete Immediately*; on Linux, the file manager offers to delete permanently (the cloud's own trash still applies if your Cloudreve server has one).
- **Cache**: opened files are kept in `~/.cloudreve/vfs-cache/` (at most 10 GB, less on a nearly full disk); the oldest are removed first. Files with changes not uploaded yet are never removed.
- **Finder tags and other file metadata** (macOS) and file-manager view settings are kept on this computer only and are forgotten when the app quits.
- **Removing the drive** empties the folder on this computer; your files stay in the cloud. If some changes are not uploaded yet, the app warns you first.

### Deleting and renaming in a "Download everything" drive

This follows the drive's **Delete and rename in the cloud too** switch, on by default when you add a drive and changeable at any time in **Settings › Drives**. Drives added with an older version keep it off until you turn it on: an update never starts deleting in your cloud by itself. When it is off, nothing you delete here is deleted from your cloud, and a renamed file is uploaded again under its new name. When it is on:

- **Deleting** a file or folder deletes it from your cloud too, into the Cloudreve trash: restore it from the web interface if it was a mistake. Deleted while the app was closed or offline, it is deleted at the next sync.
- **Renaming or moving** a file or folder inside the drive renames or moves it in your cloud (no second copy under the new name). A rename made while the app was closed is applied as a delete of the old name plus an upload of the new one; the old version stays in the trash.
- **Changed elsewhere first**: if a file you deleted here was changed on the server since it was last synced, the server's version wins: it is downloaded back instead of deleted.
- **Many files at once**: deleting 200 files or more at once, or more than half of the drive, looks like an accident. Nothing is deleted from your cloud and you get a notification; if it was intended, delete them on the web interface.
- Nothing is deleted from your cloud while the drive is paused (deletions made meanwhile are sent when it resumes), nor when the sync folder itself is missing (an unplugged disk, a folder deleted or moved away). Ignored files (system junk, your ignore patterns) never cause a deletion.

### Requirements

- **macOS**: nothing to install. The drive is served by the app itself and mounted with the system's network file system support. The first time, macOS may ask whether Cloudreve can access files on a network volume: click **Allow**.
- **Linux**: FUSE 3 (the `fuse3` package). It is already installed on most desktops; otherwise:
  - Ubuntu / Debian: `sudo apt install fuse3`
  - Fedora: `sudo dnf install fuse3`
  - Arch: `sudo pacman -S fuse3`

  The `.deb` and `.rpm` packages pull it in automatically. Your user needs access to `/dev/fuse` (the default on desktop distributions).

### Troubleshooting

When a drive can't be opened, the app says why and what to do, in the popup and in **Settings › Drives**, with a **Retry** button. The usual causes:

| Message | What to do |
|---|---|
| The folder already contains files | Files on demand needs an empty folder. Move its contents elsewhere (or pick another folder), then click Retry. |
| Files on demand needs FUSE | Linux only: install the `fuse3` package (see above), then click Retry. |
| macOS blocked Cloudreve | Open **System Settings › Privacy & Security › Files and Folders**, allow Cloudreve to access **Network Volumes**, then click Retry. |
| The folder is still held by an earlier session | The app was closed abruptly and the old drive is still attached. Close any window or terminal open in that folder, then click Retry. If it keeps happening, restart your computer. |

The drive folder shows as broken or "Transport endpoint is not connected" (Linux) after a crash: just start the app again, it cleans this up by itself.

Detailed logs are in `~/.cloudreve/logs` (**Settings › Open log folder**) — include them when reporting a problem.

For developers: see [docs/vfs-architecture.md](docs/vfs-architecture.md).

<img width="689" height="586" alt="image" src="https://github.com/user-attachments/assets/a199526f-c6ab-467b-8f3d-f8e1330f91ef" />

> [!WARNING]
> Cloudreve server version **4.16.1** has a bug where OAuth scope enforcement blocks desktop clients from accessing `/user/me` and other user endpoints ([details](https://github.com/cloudreve/cloudreve/issues/3471)). Please upgrade to **4.17.0**.

## License

MIT, see [LICENSE](LICENSE). This project is a fork of [cloudreve/desktop](https://github.com/cloudreve/desktop) (MIT, Copyright (c) 2025 Cloudreve).
