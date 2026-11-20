{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:

let
  inherit (lib)
    mkEnableOption
    mkIf
    mkMerge
    mkOption
    optional
    types
    ;

  cfg = config.programs.cloudreve-client;
  settingsLib = import ./settings.nix { inherit lib pkgs; };

  inherit (pkgs.stdenv.hostPlatform) isDarwin isLinux;

  defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.cloudreve-client;

  managedSettings = settingsLib.render cfg.settings;
  managedFile = settingsLib.file "cloudreve-client-settings.json" managedSettings;
  hasManagedSettings = managedSettings != { };

  merge = "${
    cfg.package.passthru.merge-config or (pkgs.callPackage ../merge-config.nix { })
  }/bin/cloudreve-client-merge-config";

  # Where LaunchServices should find the app on macOS: the stable copy/link
  # home-manager maintains, or the store path when neither is enabled.
  darwinApp =
    let
      darwinCfg = config.targets.darwin or { };
      copy = darwinCfg.copyApps or { enable = false; };
      link = darwinCfg.linkApps or { enable = false; };
    in
    if copy.enable then
      "${config.home.homeDirectory}/${copy.directory}/Cloudreve.app"
    else if link.enable then
      "${config.home.homeDirectory}/${link.directory}/Cloudreve.app"
    else
      "${cfg.package}/Applications/Cloudreve.app";

  lsregister = "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister";

  # Restart the autostarted app when the managed settings change: it only reads
  # config.json at startup and rewrites it whole on the next UI change.
  restartTrigger = lib.optional (cfg.restartOnSettingsChange && hasManagedSettings) managedFile;
in
{
  meta.maintainers = [ ];

  options.programs.cloudreve-client = {
    enable = mkEnableOption "the Cloudreve desktop sync client";

    package = mkOption {
      type = types.package;
      default = defaultPackage;
      defaultText = lib.literalExpression "cloudreve-client.packages.\${system}.cloudreve-client";
      description = "The cloudreve-client package to use.";
    };

    autostart = mkOption {
      type = types.bool;
      default = false;
      example = true;
      description = ''
        Start the client with the graphical session.

        - Linux: a systemd user service bound to {option}`programs.cloudreve-client.systemdTarget`,
          restarted on failure (quitting from the tray menu stops it until the next login).
        - macOS: a launchd user agent (`RunAtLoad`), relaunched only if it crashes.

        The app's own "Launch at startup" toggle is hidden in the Nix build: it would
        register the current store path, which disappears after an update and garbage
        collection.
      '';
    };

    systemdTarget = mkOption {
      type = types.str;
      default = "graphical-session.target";
      example = "hyprland-session.target";
      description = "systemd user target the autostart service is attached to (Linux only).";
    };

    restartOnSettingsChange = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Restart the autostart service/agent when the managed
        {option}`programs.cloudreve-client.settings` change, so the running app picks them up
        (it reads its config only at startup, and would otherwise write its old in-memory
        values back on the next change made in the UI). Running syncs resume after the restart.
      '';
    };

    registerUrlHandler = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Make this package the handler of `cloudreve://` URLs, which the browser sign-in
        (OAuth) uses to hand the credentials back to the app.

        - Linux: sets `x-scheme-handler/cloudreve` in {option}`xdg.mimeApps.defaultApplications`
          (effective when {option}`xdg.mimeApps.enable` is on; otherwise the packaged
          {file}`Cloudreve.desktop`, which declares the scheme, is picked by the desktop's MIME cache).
        - macOS: registers the app bundle with LaunchServices at activation (`lsregister`),
          using the copy in {file}`~/Applications/Home Manager Apps` when
          {option}`targets.darwin.copyApps` / {option}`targets.darwin.linkApps` is enabled.
      '';
    };

    settings = settingsLib.option;
  };

  config = mkIf cfg.enable (mkMerge [
    {
      home.packages = [ cfg.package ];

      warnings = optional (isDarwin && cfg.registerUrlHandler && lib.hasPrefix "/nix/store" darwinApp) (
        "programs.cloudreve-client: neither targets.darwin.copyApps nor targets.darwin.linkApps is enabled, "
        + "so cloudreve:// is registered on the store path, which stops working after an update and garbage collection. "
        + "Enable targets.darwin.copyApps (default since home.stateVersion 25.11)."
      );

      assertions = [
        {
          assertion = isLinux || isDarwin;
          message = "programs.cloudreve-client is only supported on Linux and macOS.";
        }
      ];

      # Runs before systemd units are (re)started and launchd agents are
      # reloaded, so a restarted app reads the merged file.
      home.activation.cloudreveClientSettings = mkIf hasManagedSettings (
        lib.hm.dag.entryBetween [ "reloadSystemd" "setupLaunchAgents" ] [ "writeBoundary" ] ''
          run ${merge} ${managedFile} "$HOME/.cloudreve/config.json"
        ''
      );
    }

    (mkIf (isLinux && cfg.registerUrlHandler) {
      xdg.mimeApps.defaultApplications."x-scheme-handler/cloudreve" = [ "Cloudreve.desktop" ];
    })

    (mkIf (isLinux && cfg.autostart) {
      systemd.user.services.cloudreve-client = {
        Unit = {
          Description = "Cloudreve desktop sync client";
          Documentation = [ "https://github.com/cedev-1/cloudreve-client" ];
          PartOf = [ cfg.systemdTarget ];
          After = [
            cfg.systemdTarget
            "tray.target"
          ];
          X-Restart-Triggers = restartTrigger;
        };
        Service = {
          ExecStart = lib.getExe cfg.package;
          Restart = "on-failure";
          RestartSec = 5;
        };
        Install.WantedBy = [ cfg.systemdTarget ];
      };
    })

    (mkIf (isDarwin && cfg.autostart) {
      launchd.agents.cloudreve-client = {
        enable = true;
        config = {
          ProgramArguments = [ "${cfg.package}/Applications/Cloudreve.app/Contents/MacOS/cloudreve-desktop" ];
          RunAtLoad = true;
          # Quitting from the tray exits 0 and stays quit; a crash is relaunched.
          KeepAlive.SuccessfulExit = false;
          ProcessType = "Interactive";
          LimitLoadToSessionType = [ "Aqua" ];
          # A changed plist makes home-manager reload (restart) the agent.
          EnvironmentVariables = lib.optionalAttrs (restartTrigger != [ ]) {
            CLOUDREVE_NIX_SETTINGS = "${managedFile}";
          };
        };
      };
    })

    (mkIf (isDarwin && cfg.registerUrlHandler) {
      home.activation.cloudreveClientUrlHandler =
        lib.hm.dag.entryAfter
          [
            "writeBoundary"
            "linkGeneration"
            "copyApps"
          ]
          ''
            if [ -d ${lib.escapeShellArg darwinApp} ]; then
              run ${lsregister} -f ${lib.escapeShellArg darwinApp}
            fi
          '';
    })
  ]);
}
