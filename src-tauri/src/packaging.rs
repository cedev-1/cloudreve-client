//! Build-time switch for builds installed by a package manager (Nix, distro
//! packages, AUR...).
//!
//! Such builds live in a read-only, versioned location (e.g. `/nix/store/...`).
//! Three runtime integrations of the regular release builds then do harm:
//!
//! - the Tauri updater would download a release over a package it cannot
//!   replace, and nag about versions the package manager has not shipped yet;
//! - the autostart toggle writes the *current* executable path into a login
//!   item, which points at a store path that disappears after the next update
//!   and garbage collection;
//! - on Linux, the deep-link plugin writes its own `*-handler.desktop` pointing
//!   at `current_exe()`, i.e. the unwrapped binary without the environment the
//!   package wrapper sets, and makes it the default `cloudreve://` handler over
//!   the packaged `.desktop` entry, which already declares the scheme.
//!
//! Set `CLOUDREVE_PACKAGE_MANAGED=1` in the environment of `cargo build` /
//! `cargo tauri build` to turn them off. Updates, autostart and the URL
//! handler are then the package's (or its modules') responsibility.

/// Whether this binary was built for a package manager.
pub fn package_managed() -> bool {
    flag_enabled(option_env!("CLOUDREVE_PACKAGE_MANAGED"))
}

/// Interprets the build-time flag. Unset, empty, `0` and `false` all mean a
/// regular release build, so exporting the variable empty can't flip it.
fn flag_enabled(value: Option<&str>) -> bool {
    match value.map(str::trim) {
        None | Some("") | Some("0") => false,
        Some(v) => !v.eq_ignore_ascii_case("false"),
    }
}

/// Whether the startup update check may run. A package-managed build never
/// checks, whatever the user's `check_updates_on_startup` setting says: the
/// setting may predate the switch to the packaged build.
pub fn should_check_for_updates(package_managed: bool, check_on_startup: bool) -> bool {
    !package_managed && check_on_startup
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_release_build_without_the_flag_is_not_package_managed() {
        assert!(!flag_enabled(None));
    }

    #[test]
    fn the_nix_build_flag_marks_the_build_as_package_managed() {
        assert!(flag_enabled(Some("1")));
        assert!(flag_enabled(Some("true")));
        assert!(flag_enabled(Some("nix")));
    }

    #[test]
    fn an_empty_or_negative_flag_keeps_the_release_behaviour() {
        for value in ["", "  ", "0", "false", "FALSE"] {
            assert!(!flag_enabled(Some(value)), "`{value}` must not disable the updater");
        }
    }

    #[test]
    fn a_package_managed_build_never_checks_for_updates() {
        assert!(!should_check_for_updates(true, true));
        assert!(!should_check_for_updates(true, false));
    }

    #[test]
    fn a_release_build_follows_the_user_setting() {
        assert!(should_check_for_updates(false, true));
        assert!(!should_check_for_updates(false, false));
    }
}
