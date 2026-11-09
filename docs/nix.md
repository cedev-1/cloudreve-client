# Nix

The repository is a flake. It provides the client as a package for Linux and macOS, plus
modules for home-manager, NixOS and nix-darwin.

| Output | Content |
|---|---|
| `packages.<system>.cloudreve-client` (also `default`) | The app. Linux: `bin/cloudreve-desktop`, desktop entry, icons. macOS: `Applications/Cloudreve.app` and `bin/cloudreve-desktop`. |
| `overlays.default` | Adds `pkgs.cloudreve-client`. |
| `homeManagerModules.default` | `programs.cloudreve-client`: package, autostart, `cloudreve://` handler, declarative settings. |
| `nixosModules.default` | `programs.cloudreve-client`: system-wide install, FUSE, optional autostart for every user. |
| `darwinModules.default` | `programs.cloudreve-client`: install into `/Applications/Nix Apps`, optional launchd agent. |
| `apps.<system>.default` | `nix run` entry point. |
| `checks.<system>.*` | Package build, settings merge tests, module tests, NixOS VM test (x86_64-linux). |

Systems: `x86_64-linux`, `aarch64-linux`, `aarch64-darwin`. nixpkgs unstable no longer supports
Intel Macs (`x86_64-darwin`); see [Intel Macs](#intel-macs).

## Try it

```sh
nix run github:cedev-1/cloudreve-client
```

## Install

### Profile (any Linux distribution or macOS)

```sh
nix profile add github:cedev-1/cloudreve-client
```

On macOS the app is at `~/.nix-profile/Applications/Cloudreve.app`. Finder and Spotlight don't
index that folder, and the browser sign-in needs an app LaunchServices knows about, so either
launch it once with `open ~/.nix-profile/Applications/Cloudreve.app` after each update, or use
nix-darwin / home-manager below.

### NixOS

```nix
# flake.nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    cloudreve-client = {
      url = "github:cedev-1/cloudreve-client";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, cloudreve-client, ... }: {
    nixosConfigurations.my-machine = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./configuration.nix
        cloudreve-client.nixosModules.default
        {
          programs.cloudreve-client = {
            enable = true;
            # autostart = true;  # every user, with the graphical session
          };
        }
      ];
    };
  };
}
```

### nix-darwin

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    nix-darwin = {
      url = "github:nix-darwin/nix-darwin";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    cloudreve-client = {
      url = "github:cedev-1/cloudreve-client";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nix-darwin, cloudreve-client, ... }: {
    darwinConfigurations.my-mac = nix-darwin.lib.darwinSystem {
      modules = [
        cloudreve-client.darwinModules.default
        {
          nixpkgs.hostPlatform = "aarch64-darwin";
          system.primaryUser = "alice";
          programs.cloudreve-client = {
            enable = true;
            autostart = true;
          };
        }
      ];
    };
  };
}
```

The app lands in `/Applications/Nix Apps/Cloudreve.app` and is registered with LaunchServices
for `system.primaryUser` at each activation.

### home-manager

As a NixOS / nix-darwin module (`home-manager.users.<name>`) or standalone:

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    home-manager = {
      url = "github:nix-community/home-manager";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    cloudreve-client = {
      url = "github:cedev-1/cloudreve-client";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, home-manager, cloudreve-client, ... }: {
    homeConfigurations.alice = home-manager.lib.homeManagerConfiguration {
      pkgs = nixpkgs.legacyPackages.x86_64-linux; # or aarch64-darwin
      modules = [
        cloudreve-client.homeManagerModules.default
        {
          home.username = "alice";
          home.homeDirectory = "/home/alice";
          home.stateVersion = "25.11";

          programs.cloudreve-client = {
            enable = true;
            autostart = true;
            settings = {
              language = "fr";
              log_level = "info";
              notify_file_conflict = true;
              heartbeat_interval_secs = 30;
            };
          };

          # Linux: let home-manager write mimeapps.list so cloudreve:// opens the app.
          xdg.mimeApps.enable = true;
        }
      ];
    };
  };
}
```

On macOS keep `targets.darwin.copyApps.enable` on (the default from `home.stateVersion = "25.11"`):
the app is then copied to `~/Applications/Home Manager Apps`, where Spotlight finds it and where
the module registers it with LaunchServices.

### Overlay

```nix
nixpkgs.overlays = [ cloudreve-client.overlays.default ];
environment.systemPackages = [ pkgs.cloudreve-client ];
```

## home-manager options

| Option | Type | Default | Effect |
|---|---|---|---|
| `programs.cloudreve-client.enable` | bool | `false` | Install the app. |
| `programs.cloudreve-client.package` | package | this flake's | Package to use. |
| `programs.cloudreve-client.autostart` | bool | `false` | Linux: systemd user service on `systemdTarget`, `Restart=on-failure`. macOS: launchd agent, `RunAtLoad`, relaunched only after a crash. Quitting from the tray stops it until the next login. |
| `programs.cloudreve-client.systemdTarget` | string | `"graphical-session.target"` | Target the service is bound to (Linux). |
| `programs.cloudreve-client.restartOnSettingsChange` | bool | `true` | Restart the autostart service/agent when the managed settings change. |
| `programs.cloudreve-client.registerUrlHandler` | bool | `true` | Make the app the `cloudreve://` handler (browser sign-in). Linux: `xdg.mimeApps.defaultApplications`. macOS: `lsregister` at activation. |
| `programs.cloudreve-client.settings` | attrs (JSON) | `{ }` | Settings merged into `~/.cloudreve/config.json`, see below. |

`settings` has typed keys. They all default to `null`, meaning "not managed": the app's default
applies and the setting stays editable in the UI.

| Key | Type | App default | Effect |
|---|---|---|---|
| `language` | `"en-US"`, `"zh-CN"`, `"zh-TW"`, `"ja"`, `"de"`, `"fr"`, `"es"`, `"ko"`, `"ru"`, `"pl"`, `"it"` or `"system"` | `"system"` | UI and notification language. |
| `notify_credential_expired` | bool | `true` | Notify when a drive must be signed in again. |
| `notify_file_conflict` | bool | `true` | Notify on file conflicts. |
| `fast_popup_launch` | bool | `true` | Keep the tray popup alive for faster opening. |
| `heartbeat_interval_secs` | int, 10-120 | `15` | Seconds between server connectivity checks. |
| `log_to_file` | bool | `true` | Write logs to `~/.cloudreve/logs`. |
| `log_level` | `"trace"` ... `"error"` | `"debug"` | Log verbosity. |
| `log_max_files` | positive int | `5` | Rotated log files kept. |

Any other key is written as-is (freeform), for settings added to the app later. The app drops keys
it doesn't know the next time it saves its config.

NixOS module: `enable`, `package`, `fuse` (default `true`: enables `programs.fuse`, the setuid
`fusermount3` that on-demand drives need) and `autostart`. nix-darwin module: `enable`, `package`,
`autostart`. Settings are per user, so they are only in the home-manager module.

## How settings management works

The app keeps its settings in `~/.cloudreve/config.json` and rewrites the whole file each time a
setting changes in its Settings window. A read-only file linked from the Nix store would break
that, so the module does not link the file. At each activation it merges instead:

- every key set in `programs.cloudreve-client.settings` is written over the current file;
- every other key is left untouched;
- the file keeps the app's permissions (`600`, directory `700`) and is replaced atomically, and
  only when something changed;
- a file the app couldn't parse is moved to `config.json.invalid-<timestamp>` and rebuilt.

So values set in Nix win at each activation, and everything else stays editable in the UI. A
managed value changed in the UI holds until the next activation.

The app reads the file only at startup. When `autostart` is on, the module restarts the service
(Linux) or reloads the agent (macOS) when the managed settings change, after the merge. Otherwise,
quit and relaunch the app after `home-manager switch`. A running app that still has the old values
in memory would write them back the next time a setting is changed in the UI.

### Drives stay manual

Drives (`~/.cloudreve/drives.json`) hold the server URL together with the OAuth tokens the app
gets at sign-in, and the app has no notion of a drive that exists before its first sign-in.
Declaring drives would mean tokens in the world-readable Nix store, or app changes to support
pre-declared drives waiting for a sign-in. Add drives from the app: tray icon, "Add drive".

## Updates

The Nix build has the built-in updater turned off at compile time (`CLOUDREVE_PACKAGE_MANAGED=1`,
see `src-tauri/src/packaging.rs`): no update check, no "check for updates" button, no update
settings. Update the flake input instead:

```sh
nix flake update cloudreve-client   # then rebuild / switch
nix profile upgrade cloudreve-client
```

The same build hides the app's own "Launch at startup" toggle: it would register the current
store path as a login item, which disappears after an update and garbage collection. Use the
modules' `autostart` option, or your desktop's autostart settings with `cloudreve-desktop`.

## Sign-in and the `cloudreve://` link

Adding a drive opens the server's sign-in page in the browser, which hands the credentials back
through a `cloudreve://` link. If the browser doesn't hand it to the app, the sign-in never
finishes.

- **Linux.** The package's `Cloudreve.desktop` declares `MimeType=x-scheme-handler/cloudreve`
  (the build fails otherwise). NixOS and home-manager index it in the MIME cache; with
  `xdg.mimeApps.enable = true` the home-manager module also sets it as the default handler. The Nix
  build doesn't let the app write its own `~/.local/share/applications/*-handler.desktop`, which
  would point to an unwrapped store binary. Check with
  `xdg-mime query default x-scheme-handler/cloudreve` (expect `Cloudreve.desktop`).
- **macOS.** LaunchServices maps URL schemes to app bundles it knows. The modules register the
  bundle at its stable location (`/Applications/Nix Apps` or `~/Applications/Home Manager Apps`)
  at each activation. If an older copy of Cloudreve (e.g. a `.dmg` install in `/Applications`)
  is also registered, macOS may open that one instead. Remove it, or re-register the one you use:

  ```sh
  /System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister \
    -f "$HOME/Applications/Home Manager Apps/Cloudreve.app"
  ```

## On-demand drives (Linux)

On-demand drives (files downloaded when opened, not mirrored) mount through FUSE on Linux and need
a setuid `fusermount3`. The NixOS module enables `programs.fuse` for that (`fuse = true`). On other
distributions install your distribution's `fuse3` package. The package's own `fusermount3` is
only a last-resort fallback at the end of `PATH`, since it can't be setuid. macOS needs nothing
extra.

## Intel Macs

nixpkgs unstable (26.11) dropped `x86_64-darwin`, so the flake doesn't build for it. The package
itself is not tied to unstable: apply `overlays.default` to a `nixpkgs-26.05-darwin` and install
`pkgs.cloudreve-client`. This is untested.

## Troubleshooting

- **Logs**: `~/.cloudreve/logs` (or `journalctl --user -u cloudreve-client` with `autostart` on Linux).
- **No tray icon on GNOME**: install the AppIndicator extension
  (`gnomeExtensions.appindicator`). The package already provides `libayatana-appindicator`.
- **Service doesn't start (Linux)**: some window managers don't start `graphical-session.target`.
  Set `systemdTarget` to the one your session uses (e.g. `hyprland-session.target`).
- **"Unknown setting" after an upgrade**: freeform keys the app no longer knows are dropped by
  the app. Remove them from `settings`.
- **Settings reverted**: a key set in Nix wins at each activation. Remove it from `settings` to
  manage it from the UI again.

## Development

```sh
nix develop               # dev shell (rustup, yarn, cargo-tauri)
nix build .#cloudreve-client
nix flake check           # package + module tests for this system
nix fmt                   # format nix files
```

When `ui/yarn.lock` changes, update the `fetchYarnDeps` hash in `nix/package.nix`: set it to
`lib.fakeHash`, run `nix build .#ui` and copy the hash it prints. `Cargo.lock` is read directly and
needs no hash.
