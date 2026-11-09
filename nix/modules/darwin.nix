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
    mkOption
    types
    ;
  cfg = config.programs.cloudreve-client;

  # nix-darwin copies/links the apps of environment.systemPackages here.
  app = "/Applications/Nix Apps/Cloudreve.app";
  lsregister = "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister";
in
{
  options.programs.cloudreve-client = {
    enable = mkEnableOption "the Cloudreve desktop sync client, installed in /Applications/Nix Apps";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.cloudreve-client;
      defaultText = lib.literalExpression "cloudreve-client.packages.\${system}.cloudreve-client";
      description = "The cloudreve-client package to use.";
    };

    autostart = mkOption {
      type = types.bool;
      default = false;
      description = ''
        Start the client at login for every user (launchd user agent, `RunAtLoad`,
        relaunched only if it crashes). Settings management is available in the
        home-manager module.
      '';
    };
  };

  config = mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];

    # The OAuth sign-in comes back through cloudreve://. LaunchServices keeps a
    # per-user database and activation runs as root, so register the bundle at
    # its stable location for the primary user (other users get it on first
    # launch from /Applications).
    system.activationScripts.postActivation.text = lib.mkIf (config.system.primaryUser != null) ''
      if [ -d ${lib.escapeShellArg app} ]; then
        sudo -u ${lib.escapeShellArg config.system.primaryUser} ${lsregister} -f ${lib.escapeShellArg app} || true
      fi
    '';

    launchd.user.agents.cloudreve-client = mkIf cfg.autostart {
      serviceConfig = {
        ProgramArguments = [ "${app}/Contents/MacOS/cloudreve-desktop" ];
        RunAtLoad = true;
        KeepAlive.SuccessfulExit = false;
        ProcessType = "Interactive";
        LimitLoadToSessionType = [ "Aqua" ];
      };
    };
  };
}
