//! `fuser`, the Linux FUSE frontend of an on-demand drive, logs through the
//! `log` facade, not `tracing`. Its mount failures must land in the same log
//! file as everything else — and installing that bridge must not fail at
//! every launch (an explicit `LogTracer::init()` on top of the one
//! `tracing-subscriber` already installs used to log a warning each time).
//!
//! Own binary: logging is process-global.

use cloudreve_sync::LogConfig;

#[test]
fn log_crate_records_reach_the_log_file() {
    let dir = tempfile::tempdir().unwrap();
    let guard = cloudreve_sync::logging::init_logging(LogConfig {
        log_dir: dir.path().to_path_buf(),
        file_prefix: "bridge".to_string(),
        max_files: 2,
        log_to_file: true,
        log_level: "info".to_string(),
    })
    .expect("logging initializes");

    log::error!(target: "fuser::session", "mount failed: fusermount3 exited with 1");
    drop(guard);

    let text: String = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
        .collect();
    assert!(text.contains("mount failed: fusermount3 exited with 1"), "log file: {text}");
    assert!(!text.contains('\u{1b}'), "the file holds no color escapes: {text:?}");
}
