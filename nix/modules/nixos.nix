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
in
{
  meta.maintainers = [ ];

  options.programs.cloudreve-client = {
    enable = mkEnableOption "the Cloudreve desktop sync client, installed system-wide";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.cloudreve-client;
      defaultText = lib.literalExpression "cloudreve-client.packages.\${system}.cloudreve-client";
      description = "The cloudreve-client package to use.";
    };

    fuse = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Enable {option}`programs.fuse`, which installs the setuid `fusermount3` wrapper.
        On-demand drives (files fetched when opened instead of mirrored) mount through
        FUSE on Linux and cannot work without it. Mirror drives don't need it.
      '';
    };

    autostart = mkOption {
      type = types.bool;
      default = false;
      description = ''
        Start the client for every user with their graphical session (systemd user
        service on `graphical-session.target`, restarted on failure). Per-user control
        and settings management are available in the home-manager module instead.
      '';
    };
  };

  config = mkIf cfg.enable {
    # The package's .desktop entry declares x-scheme-handler/cloudreve; NixOS
    # indexes it in the MIME cache of the system profile (sign-in callback).
    environment.systemPackages = [ cfg.package ];

    programs.fuse.enable = mkIf cfg.fuse (lib.mkDefault true);

    systemd.user.services.cloudreve-client = mkIf cfg.autostart {
      description = "Cloudreve desktop sync client";
      documentation = [ "https://github.com/cedev-1/cloudreve-client" ];
      partOf = [ "graphical-session.target" ];
      after = [ "graphical-session.target" ];
      wantedBy = [ "graphical-session.target" ];
      serviceConfig = {
        ExecStart = lib.getExe cfg.package;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };
  };
}
