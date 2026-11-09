{
  self,
  pkgs,
  lib,
  home-manager,
  nix-darwin,
}:

let
  inherit (pkgs.stdenv.hostPlatform) isLinux isDarwin system;

  package = self.packages.${system}.cloudreve-client;

  # Stand-in for the app in module tests: they check the wiring, not the app,
  # and must not need the full Tauri build to evaluate.
  fakeApp = pkgs.writeShellScriptBin "cloudreve-desktop" "exit 0";

  hmConfig =
    module:
    (home-manager.lib.homeManagerConfiguration {
      inherit pkgs;
      modules = [
        self.homeManagerModules.default
        {
          home.username = "alice";
          home.homeDirectory = if isDarwin then "/Users/alice" else "/home/alice";
          home.stateVersion = "25.11";
          programs.cloudreve-client.package = fakeApp;
        }
        module
      ];
    }).config;

  managed = hmConfig {
    programs.cloudreve-client = {
      enable = true;
      autostart = true;
      settings = {
        language = "fr";
        log_level = "warn";
        heartbeat_interval_secs = 30;
        notify_file_conflict = false;
        # freeform: a key the module has no typed option for
        notify_connection_status = false;
      };
    };
  };

  systemLanguage = hmConfig {
    programs.cloudreve-client = {
      enable = true;
      settings.language = "system";
    };
  };

  unmanaged = hmConfig { programs.cloudreve-client.enable = true; };

  # The activation snippet exactly as home-manager runs it (`run` is the
  # activation helper that honours dry-run; here it just runs).
  activationScript =
    cfg:
    pkgs.writeShellScript "activation" ''
      set -euo pipefail
      run() { "$@"; }
      ${cfg.home.activation.cloudreveClientSettings.data}
    '';

  check =
    name: assertions:
    let
      failed = lib.filter (a: !a.ok) assertions;
    in
    pkgs.runCommand "cloudreve-client-${name}" { } (
      if failed == [ ] then
        "touch $out"
      else
        ''
          echo 'failed assertions:' >&2
          ${lib.concatMapStrings (a: "echo ${lib.escapeShellArg "  - ${a.msg}"} >&2\n") failed}
          exit 1
        ''
    );

  # Body of `pub struct AppConfig { ... }` (no regex: brace escapes differ
  # between the darwin and Linux regex engines).
  appConfigFields = lib.head (
    lib.splitString "\n}" (
      lib.elemAt (lib.splitString "pub struct AppConfig {" (builtins.readFile ../../crates/cloudreve-sync/src/config.rs)) 1
    )
  );

  typedKeys = lib.attrNames (
    lib.filterAttrs (n: _: !(lib.hasPrefix "_" n)) (
      (lib.evalModules {
        modules = [
          {
            options.s = (import ../modules/settings.nix { inherit lib pkgs; }).option;
          }
        ];
      }).options.s.type.getSubOptions
        [ ]
    )
  );
in
{
  inherit package;

  # The merge script against real files: what the activation does to an
  # existing, a missing and a corrupt config.
  merge-config =
    pkgs.runCommand "cloudreve-client-merge-config-test"
      {
        nativeBuildInputs = [
          pkgs.jq
          package.passthru.merge-config
        ];
      }
      ''
        set -euo pipefail
        fail() { echo "FAIL: $*" >&2; exit 1; }
        export HOME=$PWD/home

        echo '{"language":"fr","log_level":"warn"}' > managed.json

        # 1. no config yet: created with the managed keys, private permissions
        cloudreve-client-merge-config managed.json
        cfg=$HOME/.cloudreve/config.json
        [ "$(jq -r .language "$cfg")" = fr ] || fail "language not written to a new config"
        [ "$(jq -r 'keys | join(",")' "$cfg")" = "language,log_level" ] || fail "new config has extra keys: $(cat "$cfg")"
        [ "$(stat -c %a "$cfg")" = 600 ] || fail "config mode is $(stat -c %a "$cfg"), expected 600"
        [ "$(stat -c %a "$HOME/.cloudreve")" = 700 ] || fail "config dir mode is $(stat -c %a "$HOME/.cloudreve")"

        # 2. existing config edited in the UI: managed keys win, the rest is kept
        echo '{"language":"de","log_level":"debug","log_max_files":7,"fast_popup_launch":false}' > "$cfg"
        cloudreve-client-merge-config managed.json "$cfg"
        [ "$(jq -r .language "$cfg")" = fr ] || fail "managed language did not win"
        [ "$(jq -r .log_level "$cfg")" = warn ] || fail "managed log_level did not win"
        [ "$(jq -r .log_max_files "$cfg")" = 7 ] || fail "unmanaged log_max_files was lost"
        [ "$(jq -r .fast_popup_launch "$cfg")" = false ] || fail "unmanaged fast_popup_launch was lost"

        # 3. nothing to change: the file is not rewritten
        touch -d '2001-01-01' "$cfg"
        cloudreve-client-merge-config managed.json "$cfg"
        [ "$(stat -c %Y "$cfg")" = "$(date -d 2001-01-01 +%s)" ] || fail "an up-to-date config was rewritten"

        # 4. corrupt config: moved aside, rebuilt from the managed keys
        echo '{ not json' > "$cfg"
        cloudreve-client-merge-config managed.json "$cfg"
        [ "$(jq -r .language "$cfg")" = fr ] || fail "corrupt config not rebuilt"
        ls "$cfg".invalid-* >/dev/null || fail "corrupt config not backed up"
        grep -q 'not json' "$cfg".invalid-* || fail "backup does not hold the corrupt content"

        # 5. a managed null (language = "system") is written as null
        echo '{"language":null}' > managed-null.json
        cloudreve-client-merge-config managed-null.json "$cfg"
        [ "$(jq -c .language "$cfg")" = null ] || fail "language null not written"

        touch $out
      '';

  # Each typed option must be a real AppConfig field, or the app silently
  # ignores it (serde skips unknown keys).
  settings-match-app = check "settings-match-app" (
    map (key: {
      ok = lib.hasInfix "pub ${key}: " appConfigFields;
      msg = "settings.${key} is not a field of AppConfig in crates/cloudreve-sync/src/config.rs";
    }) typedKeys
    ++ [
      {
        ok = lib.length typedKeys >= 8;
        msg = "expected the typed settings, found ${toString typedKeys}";
      }
    ]
  );

  # The home-manager module end to end: the activation it generates, run on a
  # real file, plus the autostart wiring for this platform.
  home-manager-module =
    pkgs.runCommand "cloudreve-client-hm-test" { nativeBuildInputs = [ pkgs.jq ]; }
      ''
        set -euo pipefail
        fail() { echo "FAIL: $*" >&2; exit 1; }

        export HOME=$PWD/home
        mkdir -p $HOME/.cloudreve
        echo '{"language":"de","log_max_files":7,"fast_popup_launch":false}' > $HOME/.cloudreve/config.json

        ${activationScript managed}
        cfg=$HOME/.cloudreve/config.json
        jq -e '.language == "fr" and .log_level == "warn" and .heartbeat_interval_secs == 30
               and .notify_file_conflict == false and .notify_connection_status == false' "$cfg" \
          || fail "managed settings not applied: $(cat "$cfg")"
        jq -e '.log_max_files == 7 and .fast_popup_launch == false' "$cfg" \
          || fail "unmanaged settings lost: $(cat "$cfg")"
        jq -e 'has("log_to_file") | not' "$cfg" || fail "an unset option was written: $(cat "$cfg")"

        ${activationScript systemLanguage}
        jq -e '.language == null' "$cfg" || fail "language = system not mapped to null: $(cat "$cfg")"

        touch $out
      '';

  home-manager-wiring = check "home-manager-wiring" (
    [
      {
        ok = !(unmanaged.home.activation ? cloudreveClientSettings);
        msg = "no settings set, yet an activation step touches config.json";
      }
      {
        ok = lib.elem fakeApp managed.home.packages;
        msg = "the package is not installed in home.packages";
      }
      {
        ok =
          lib.elem "reloadSystemd" managed.home.activation.cloudreveClientSettings.before
          && lib.elem "setupLaunchAgents" managed.home.activation.cloudreveClientSettings.before;
        msg = "settings must be merged before services/agents are restarted";
      }
    ]
    ++ lib.optionals isLinux [
      {
        ok =
          managed.systemd.user.services.cloudreve-client.Service.ExecStart
          == "${fakeApp}/bin/cloudreve-desktop";
        msg = "systemd service does not run the package";
      }
      {
        ok =
          managed.systemd.user.services.cloudreve-client.Install.WantedBy == [ "graphical-session.target" ];
        msg = "systemd service not attached to graphical-session.target";
      }
      {
        ok = managed.systemd.user.services.cloudreve-client.Unit.X-Restart-Triggers != [ ];
        msg = "service is not restarted when managed settings change";
      }
      {
        ok = !(unmanaged.systemd.user.services ? cloudreve-client);
        msg = "autostart off, yet a service is installed";
      }
      {
        ok =
          managed.xdg.mimeApps.defaultApplications."x-scheme-handler/cloudreve" == [ "Cloudreve.desktop" ];
        msg = "cloudreve:// is not mapped to the packaged desktop entry";
      }
    ]
    ++ lib.optionals isDarwin [
      {
        ok =
          managed.launchd.agents.cloudreve-client.config.ProgramArguments == [
            "${fakeApp}/Applications/Cloudreve.app/Contents/MacOS/cloudreve-desktop"
          ];
        msg = "launchd agent does not run the app bundle";
      }
      {
        ok =
          managed.launchd.agents.cloudreve-client.config.RunAtLoad
          && managed.launchd.agents.cloudreve-client.config.KeepAlive.SuccessfulExit == false;
        msg = "launchd agent must start at login and relaunch only after a crash";
      }
      {
        ok = managed.launchd.agents.cloudreve-client.config.EnvironmentVariables ? CLOUDREVE_NIX_SETTINGS;
        msg = "agent plist does not change with the managed settings (no reload)";
      }
      {
        ok =
          !(unmanaged.launchd.agents ? cloudreve-client) || !unmanaged.launchd.agents.cloudreve-client.enable;
        msg = "autostart off, yet a launchd agent is installed";
      }
      {
        ok = lib.hasInfix "/Users/alice/Applications/Home Manager Apps/Cloudreve.app" managed.home.activation.cloudreveClientUrlHandler.data;
        msg = "lsregister must target the stable copy in ~/Applications/Home Manager Apps";
      }
      {
        ok = managed.warnings == [ ];
        msg = "unexpected warnings: ${toString managed.warnings}";
      }
    ]
  );
}
// lib.optionalAttrs isLinux {
  nixos-module =
    let
      nixos =
        (lib.nixosSystem {
          inherit system;
          modules = [
            self.nixosModules.default
            {
              programs.cloudreve-client = {
                enable = true;
                package = fakeApp;
                autostart = true;
              };
              boot.loader.grub.enable = false;
              fileSystems."/" = {
                device = "/dev/null";
                fsType = "ext4";
              };
              system.stateVersion = "25.11";
            }
          ];
        }).config;
    in
    check "nixos-module" [
      {
        ok = lib.elem fakeApp nixos.environment.systemPackages;
        msg = "package not installed system-wide";
      }
      {
        ok = nixos.programs.fuse.enable && nixos.security.wrappers ? fusermount3;
        msg = "fusermount3 setuid wrapper missing (on-demand drives need it)";
      }
      {
        ok = nixos.systemd.user.services.cloudreve-client.wantedBy == [ "graphical-session.target" ];
        msg = "autostart user service not attached to graphical-session.target";
      }
    ];
}
// lib.optionalAttrs (system == "x86_64-linux") {
  # Boots a NixOS VM with the system module and home-manager: the settings are
  # merged at boot (home-manager-alice.service), the real package is installed
  # and its desktop entry is indexed as the cloudreve:// handler.
  nixos-vm = pkgs.testers.runNixOSTest {
    name = "cloudreve-client";
    nodes.machine = {
      imports = [
        self.nixosModules.default
        home-manager.nixosModules.home-manager
      ];
      environment.systemPackages = [ pkgs.jq ];
      programs.cloudreve-client.enable = true;
      users.users.alice = {
        isNormalUser = true;
        uid = 1000;
      };
      home-manager.users.alice = {
        imports = [ self.homeManagerModules.default ];
        home.stateVersion = "25.11";
        programs.cloudreve-client = {
          enable = true;
          autostart = true;
          settings = {
            language = "fr";
            log_level = "warn";
          };
        };
      };
    };
    testScript = ''
      machine.wait_for_unit("home-manager-alice.service")
      cfg = machine.succeed("cat /home/alice/.cloudreve/config.json")
      print(cfg)
      machine.succeed("jq -e '.language == \"fr\" and .log_level == \"warn\"' /home/alice/.cloudreve/config.json")
      machine.succeed("stat -c %U:%a /home/alice/.cloudreve/config.json | grep -qx alice:600")
      machine.succeed("test -e /home/alice/.config/systemd/user/cloudreve-client.service")
      machine.succeed("grep -q 'x-scheme-handler/cloudreve' /run/current-system/sw/share/applications/mimeinfo.cache")
      machine.succeed("test -u /run/wrappers/bin/fusermount3")
      machine.succeed("test -x /run/current-system/sw/bin/cloudreve-desktop")
    '';
  };
}
// lib.optionalAttrs isDarwin {
  darwin-module =
    let
      darwin =
        (nix-darwin.lib.darwinSystem {
          modules = [
            self.darwinModules.default
            {
              nixpkgs.hostPlatform = system;
              system.stateVersion = 6;
              system.primaryUser = "alice";
              programs.cloudreve-client = {
                enable = true;
                package = fakeApp;
                autostart = true;
              };
            }
          ];
        }).config;
    in
    check "darwin-module" [
      {
        ok = lib.elem fakeApp darwin.environment.systemPackages;
        msg = "package not installed (and so not copied into /Applications/Nix Apps)";
      }
      {
        ok =
          darwin.launchd.user.agents.cloudreve-client.serviceConfig.ProgramArguments == [
            "/Applications/Nix Apps/Cloudreve.app/Contents/MacOS/cloudreve-desktop"
          ];
        msg = "launchd agent does not run the app from /Applications/Nix Apps";
      }
      {
        ok =
          lib.hasInfix "sudo -u alice" darwin.system.activationScripts.postActivation.text
          && lib.hasInfix "lsregister -f '/Applications/Nix Apps/Cloudreve.app'" darwin.system.activationScripts.postActivation.text;
        msg = "the bundle is not registered with LaunchServices for the primary user";
      }
    ];
}
