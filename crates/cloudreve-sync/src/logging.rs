use anyhow::{Context, Result};
use std::path::PathBuf;
use chrono::Datelike;
use std::sync::OnceLock;
use tracing_subscriber::{
    EnvFilter,
    fmt,
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::config::{ConfigManager, LogLevel};

/// Configuration for the logging system
pub struct LogConfig {
    /// Directory where log files will be stored
    pub log_dir: PathBuf,
    /// Prefix for log file names
    pub file_prefix: String,
    /// Maximum number of log files to keep (rotation)
    pub max_files: usize,
    /// Whether to write logs to file
    pub log_to_file: bool,
    /// Log level filter string
    pub log_level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        let log_dir = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".cloudreve")
            .join("logs");

        Self {
            log_dir,
            file_prefix: "cloudreve-sync".to_string(),
            max_files: 5,
            log_to_file: true,
            log_level: "info".to_string(),
        }
    }
}

impl LogConfig {
    /// Create LogConfig from ConfigManager settings
    pub fn from_config_manager() -> Self {
        if let Some(config_manager) = ConfigManager::try_get() {
            let config = config_manager.get_config();
            Self {
                log_dir: ConfigManager::get_log_dir(),
                file_prefix: "cloudreve-sync".to_string(),
                max_files: config.log_max_files,
                log_to_file: config.log_to_file,
                log_level: config.log_level.as_str().to_string(),
            }
        } else {
            Self::default()
        }
    }
}

/// Global flag for whether file logging is enabled
static FILE_LOGGING_ENABLED: OnceLock<std::sync::RwLock<bool>> = OnceLock::new();

/// Initialize the logging system with both file and stdout output
///
/// This sets up:
/// - File logging with rotation (max 5 files by default)
/// - Stdout logging with colors
/// - Component-specific log targets (api, drive, events, sync)
/// - Configurable log levels via RUST_LOG environment variable
///
/// # Log Targets
/// - `api` - API requests and responses
/// - `api::health` - Health check endpoints
/// - `api::drives` - Drive management operations
/// - `api::sync` - Sync operations
/// - `api::sse` - Server-Sent Events
/// - `api::error` - API error responses
/// - `drive` - DriveManager operations
/// - `events` - Event broadcasting
/// - `main` - Application lifecycle
///
/// # Example
/// ```bash
/// # Set log level for all components
/// RUST_LOG=debug cargo run
///
/// # Set different levels for different components
/// RUST_LOG=api=debug,drive=info,events=trace cargo run
///
/// # Show only specific component
/// RUST_LOG=api::drives=debug cargo run
/// ```
pub fn init_logging(config: LogConfig) -> Result<LogGuard> {
    // Ensure log directory exists
    std::fs::create_dir_all(&config.log_dir).context("Failed to create log directory")?;

    // Store file logging state
    FILE_LOGGING_ENABLED.get_or_init(|| std::sync::RwLock::new(config.log_to_file));

    // RUST_LOG, when set, replaces the whole filter (dependency caps included).
    let rust_log_set = std::env::var_os("RUST_LOG").is_some();
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(filter_directives(&config.log_level)));

    // The file is what users send with a bug report: plain text, no color
    // escapes. Its size is bounded by `max_files` days of at most
    // `DAILY_LOG_CAP_BYTES` each (`SizeCappedWriter`).
    let (file_layer, worker_guard) = if config.log_to_file {
        let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix(&config.file_prefix)
            .filename_suffix("log")
            .max_log_files(config.max_files)
            .build(&config.log_dir)
            .context("Failed to create file appender")?;
        // Same day boundary as the appender's own rotation (UTC dates).
        let capped = SizeCappedWriter::new(file_appender, DAILY_LOG_CAP_BYTES, || {
            chrono::Utc::now().date_naive().num_days_from_ce() as u32
        });
        let (non_blocking_file, worker_guard) = tracing_appender::non_blocking(capped);
        let layer = fmt::layer()
            .compact()
            .with_writer(non_blocking_file)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_ansi(false);
        (Some(layer), worker_guard)
    } else {
        // A guard is still needed for `LogGuard`'s shape.
        let (non_blocking_sink, worker_guard) = tracing_appender::non_blocking(std::io::sink());
        drop(non_blocking_sink);
        (None, worker_guard)
    };

    // Nobody reads the stdout of a GUI app launched from the Dock or a
    // desktop entry: only a development build, or someone who asked for
    // logs on the terminal with RUST_LOG, gets them there.
    let stdout_layer = (cfg!(debug_assertions) || rust_log_set || !config.log_to_file).then(|| {
        fmt::layer()
            .compact()
            .with_target(true)
            .with_thread_ids(false)
            .with_line_number(true)
            .with_ansi(true)
    });

    // `try_init` also installs the `log` -> `tracing` bridge (the default
    // `tracing-log` feature of tracing-subscriber), so crates logging
    // through the `log` facade (fuser) reach these same layers.
    tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .with(stdout_layer)
        .init();

    tracing::info!(
        target: "main",
        log_dir = %config.log_dir.display(),
        max_files = config.max_files,
        log_to_file = config.log_to_file,
        log_level = %config.log_level,
        "Logging system initialized"
    );

    Ok(LogGuard {
        _worker_guard: worker_guard,
    })
}

/// Upper bound on one day's log file. A loop logging the same failure over
/// and over (a server answering every upload with an error, a dead mount
/// polled by Finder) must not fill the disk: past this, the rest of the day
/// is dropped. With the default `log_max_files` that bounds the whole log
/// directory to a few hundred MiB.
const DAILY_LOG_CAP_BYTES: u64 = 64 * 1024 * 1024;

/// The filter used when RUST_LOG is not set: the user's level for the app,
/// with the dependencies that log every request capped. `nfs3_server` logs
/// each NFS call at debug and each MOUNT call at info; `hyper`/`reqwest`
/// each connection; `globset` each compiled pattern; the Tauri runtime each
/// window event.
fn filter_directives(user_level: &str) -> String {
    format!(
        "{user_level},nfs3_server=warn,nfs3_server::mount_handlers=error,hyper_util=warn,hyper=warn,\
         reqwest=warn,globset=warn,tauri_runtime_wry=warn,fuser=warn"
    )
}

/// A writer that stops forwarding once `cap` bytes were written on the
/// current day (as told by `day`), writes one line saying so, and resumes
/// when the day changes — i.e. when the rolling appender behind it starts
/// a new file.
struct SizeCappedWriter<W, D> {
    inner: W,
    cap: u64,
    day: D,
    current_day: u32,
    written: u64,
    announced: bool,
}

impl<W: std::io::Write, D: Fn() -> u32> SizeCappedWriter<W, D> {
    fn new(inner: W, cap: u64, day: D) -> Self {
        let current_day = day();
        Self { inner, cap, day, current_day, written: 0, announced: false }
    }
}

impl<W: std::io::Write, D: Fn() -> u32> std::io::Write for SizeCappedWriter<W, D> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let today = (self.day)();
        if today != self.current_day {
            self.current_day = today;
            self.written = 0;
            self.announced = false;
        }
        if self.written >= self.cap {
            if !self.announced {
                self.announced = true;
                self.inner.write_all(b"--- daily log size limit reached, dropping further lines until tomorrow ---\n")?;
            }
            // Report success: the logging pipeline must never fail over this.
            return Ok(buf.len());
        }
        self.inner.write_all(buf)?;
        self.written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Update the log level setting (note: requires restart to take effect)
pub fn set_log_level(level: LogLevel) -> Result<()> {
    // The log level change is persisted to config but requires restart
    // because tracing-subscriber doesn't support runtime filter changes easily
    // when combined with multiple layers
    tracing::info!(target: "config", level = level.as_str(), "Log level setting updated (restart required)");
    Ok(())
}

/// Get the current file logging enabled state
pub fn is_file_logging_enabled() -> bool {
    FILE_LOGGING_ENABLED
        .get()
        .and_then(|lock| lock.read().ok())
        .map(|v| *v)
        .unwrap_or(true)
}

/// Guard that ensures logs are flushed before exit
/// This wraps the WorkerGuard from tracing_appender which MUST be kept alive
/// for the entire application lifetime to ensure file logging works properly
pub struct LogGuard {
    _worker_guard: tracing_appender::non_blocking::WorkerGuard,
}

impl Drop for LogGuard {
    fn drop(&mut self) {
        tracing::info!(target: "main", "Flushing logs before shutdown");
        // WorkerGuard will be dropped here, flushing remaining logs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_default_log_config() {
        let config = LogConfig::default();
        assert_eq!(config.file_prefix, "cloudreve-sync");
        assert_eq!(config.max_files, 5);
        assert!(config.log_to_file);
        assert_eq!(config.log_level, "info");
    }

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl Buf {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    /// What a subscriber built on `filter` lets through.
    fn emitted_under(filter: &str, emit: impl FnOnce()) -> String {
        let buf = Buf(Arc::new(Mutex::new(Vec::new())));
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(filter))
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        buf.text()
    }

    /// Even at debug, the per-request chatter of the NFS server and the HTTP
    /// stack stays out: a single Finder window listing a folder produced
    /// thousands of such lines and drowned everything the app itself said.
    #[test]
    fn dependency_chatter_is_kept_out_even_at_debug() {
        let out = emitted_under(&filter_directives("debug"), || {
            tracing::debug!(target: "nfs3_server::rpcwire", "nfs request");
            tracing::warn!(target: "nfs3_server::mount_handlers", "mount request");
            tracing::debug!(target: "hyper_util::client", "pooled connection");
            tracing::debug!(target: "reqwest::connect", "connecting");
            tracing::debug!(target: "drive::sync", "app debug line");
            tracing::warn!(target: "nfs3_server::rpcwire", "nfs warning");
        });
        assert!(out.contains("app debug line"), "{out}");
        assert!(out.contains("nfs warning"), "a dependency's warnings still get through: {out}");
        for noise in ["nfs request", "mount request", "pooled connection", "connecting"] {
            assert!(!out.contains(noise), "{noise:?} leaked: {out}");
        }
    }

    #[test]
    fn the_chosen_level_still_applies_to_the_app() {
        let out = emitted_under(&filter_directives("warn"), || {
            tracing::info!(target: "drive::sync", "routine");
            tracing::warn!(target: "drive::sync", "trouble");
        });
        assert!(!out.contains("routine") && out.contains("trouble"), "{out}");
    }

    /// A runaway loop logging the same failure must not fill the disk: past
    /// the daily cap, lines are dropped (after one line saying so) until the
    /// next day's file starts.
    #[test]
    fn a_day_of_logs_stops_growing_past_its_cap() {
        let sink = Buf(Arc::new(Mutex::new(Vec::new())));
        let day = Arc::new(Mutex::new(1u32));
        let clock = day.clone();
        let mut w = SizeCappedWriter::new(sink.clone(), 1000, move || *clock.lock().unwrap());

        let line = [b'x'; 99];
        for _ in 0..50 {
            w.write_all(&line).unwrap();
            w.write_all(b"\n").unwrap();
        }
        let first_day = sink.text();
        assert!(first_day.len() < 1200, "wrote {} bytes on a 1000-byte cap", first_day.len());
        assert!(first_day.contains("log size limit"), "the cut is announced");

        *day.lock().unwrap() = 2;
        w.write_all(b"next day\n").unwrap();
        assert!(sink.text().ends_with("next day\n"), "a new day writes again");
    }
}
