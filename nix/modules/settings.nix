# Declarative app settings, shared by the modules and the tests.
#
# Keys are the exact field names of `AppConfig` in
# crates/cloudreve-sync/src/config.rs, which the app persists as
# ~/.cloudreve/config.json. Every typed option defaults to `null`, meaning
# "not managed": the app default applies and the setting stays editable in the
# UI. A non-null value is written into config.json at each activation (see
# ../merge-config.sh) and wins over whatever the UI saved before.
{ lib, pkgs }:

let
  inherit (lib) mkOption types literalExpression;

  jsonFormat = pkgs.formats.json { };

  # The languages the app ships (ui/src/i18n.ts, locales/app.yaml).
  languages = [
    "en-US"
    "zh-CN"
    "zh-TW"
    "ja"
    "de"
    "fr"
    "es"
    "ko"
    "ru"
    "pl"
    "it"
  ];

  managed =
    type: appDefault: description:
    mkOption {
      type = types.nullOr type;
      default = null;
      description = ''
        ${description}

        App default: `${appDefault}`. `null` leaves the setting to the app and its UI.
      '';
    };
in
{
  inherit languages;

  option = mkOption {
    default = { };
    example = literalExpression ''
      {
        language = "fr";
        log_level = "info";
        notify_file_conflict = true;
        heartbeat_interval_secs = 30;
      }
    '';
    description = ''
      Settings written into {file}`~/.cloudreve/config.json`.

      The file stays owned and writable by the app. At each activation, every
      option set here (non-`null`) is merged over the current file; keys not
      set here are left untouched, so they remain editable in the app's
      Settings window. Values set in Nix win again at the next activation.

      The app reads the file at startup: a change applies when the app
      restarts, which the module triggers for the autostart service.

      Unknown keys are accepted (freeform) and written as-is, for settings this
      module does not know yet. The app drops keys it does not know the next
      time it saves.
    '';
    type = types.submodule {
      freeformType = jsonFormat.type;
      options = {
        language = managed (types.either (types.enum languages) (types.enum [ "system" ])) "system" ''
          Interface and notification language. `"system"` follows the OS locale.
        '';

        notify_credential_expired =
          managed types.bool "true"
            "Show a notification when a drive's credentials expire and it must be signed in again.";

        notify_file_conflict =
          managed types.bool "true"
            "Show a notification when a file conflict is detected.";

        fast_popup_launch =
          managed types.bool "true"
            "Keep the tray popup window alive (hidden instead of closed) so it opens faster, at the cost of memory.";

        heartbeat_interval_secs =
          managed (types.ints.between 10 120) "15"
            "Seconds between server connectivity checks (the app clamps this to 10-120).";

        log_to_file = managed types.bool "true" "Write logs to {file}`~/.cloudreve/logs`.";

        log_level = managed (types.enum [
          "trace"
          "debug"
          "info"
          "warn"
          "error"
        ]) "debug" "Log verbosity.";

        log_max_files = managed types.ints.positive "5" "Number of rotated log files kept.";
      };
    };
  };

  # The JSON object to merge: unmanaged (null) options dropped, `"system"`
  # language mapped to the app's own representation (JSON null).
  render =
    settings:
    let
      set = lib.filterAttrs (_: v: v != null) settings;
    in
    set // lib.optionalAttrs ((set.language or null) == "system") { language = null; };

  file = name: settings: jsonFormat.generate name settings;
}
