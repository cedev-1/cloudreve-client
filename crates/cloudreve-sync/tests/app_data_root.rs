//! Where the app keeps its data: `$CLOUDREVE_HOME` when set, else
//! `~/.cloudreve`, and a test harness can point it at its own directory.
//! One test in its own binary: it changes the process environment.

use cloudreve_sync::utils::app_dir::{HOME_ENV, app_data_root, redirect_for_tests};

#[test]
fn the_data_root_follows_cloudreve_home_then_a_test_redirect() {
    let profile = tempfile::tempdir().unwrap();
    // SAFETY: the only test of this binary; nothing else reads the
    // environment concurrently.
    unsafe { std::env::set_var(HOME_ENV, profile.path()) };
    assert_eq!(app_data_root().unwrap(), profile.path());

    // A relative value is not a location: ignored, not resolved against
    // whatever the working directory happens to be.
    unsafe { std::env::set_var(HOME_ENV, "relative/dir") };
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from).unwrap();
    assert_eq!(app_data_root().unwrap(), home.join(".cloudreve"));

    let harness = tempfile::tempdir().unwrap();
    assert_eq!(redirect_for_tests(harness.path()), harness.path());
    assert_eq!(app_data_root().unwrap(), harness.path());
    // First redirect wins: tests of one binary share it.
    let other = tempfile::tempdir().unwrap();
    assert_eq!(redirect_for_tests(other.path()), harness.path());
    assert_eq!(app_data_root().unwrap(), harness.path());
}
