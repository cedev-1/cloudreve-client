//! Gitignore-style pattern matching for sync ignore rules.
//!
//! This module provides an `IgnoreMatcher` that can match file paths against
//! gitignore-style patterns. Patterns are relative to the sync root path,
//! and input paths are expected to be absolute paths.

use anyhow::{Context, Result};
use globset::{Glob, GlobBuilder, GlobSet, GlobSetBuilder};
use std::path::{Path, PathBuf};

/// Patterns always applied, on top of whatever the user configured.
///
/// These files are written by the OS or by an editor without the user ever
/// asking for them, and are recreated as fast as they are removed — Finder
/// drops a `.DS_Store` in every folder it merely displays. Syncing them means
/// an upload, an SSE echo and a download attempt for each one, on top of
/// polluting the server with files that mean nothing on another machine.
const DEFAULT_FILE_PATTERNS: &[&str] = &[
    // Office / editor temporaries
    "~*",
    ".~lock.*",
    "*.swp",
    "*.swx",
    "*~",
    // macOS
    ".DS_Store",
    "._*",
    ".localized",
    // Linux desktops
    ".directory",
    ".nfs*",
    // Windows
    "Thumbs.db",
    "ehthumbs.db",
    "desktop.ini",
];

/// Junk *directories*. The walker reports the files inside them rather than the
/// directory itself, so each one is ignored both by name and by subtree.
const DEFAULT_JUNK_DIRS: &[&str] = &[
    // macOS
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    ".TemporaryItems",
    ".DocumentRevisions-V100",
    ".AppleDouble",
    ".AppleDB",
    ".AppleDesktop",
    // Linux desktops
    ".Trash-*",
    // Windows
    "$RECYCLE.BIN",
    "System Volume Information",
];

/// The built-in patterns, for display in the settings UI.
///
/// They are always on and are not part of the user's own list, so without this
/// they are invisible: a file silently not syncing then looks like a bug.
pub fn default_patterns() -> Vec<String> {
    DEFAULT_FILE_PATTERNS
        .iter()
        .chain(DEFAULT_JUNK_DIRS)
                .map(|p| (*p).to_string())
        .collect()
}

/// Build one of the built-in junk patterns.
///
/// Case-insensitive, unlike the user's own patterns: nobody types these names,
/// the OS does, and it is not consistent about the case it picks. Explorer has
/// written both `Thumbs.db` and `thumbs.db`, and a `.DS_Store` that has been
/// through a case-insensitive volume can come back as `.ds_store`.
/// Turn one gitignore-style line into the glob actually compiled, or `None`
/// for lines that carry no rule (blank lines, `#` comments).
///
/// - Patterns without '/' match anywhere in the path
/// - Patterns starting with '/' are anchored to root
fn user_glob_pattern(pattern: &str) -> Option<String> {
    let pattern = pattern.trim();
    if pattern.is_empty() || pattern.starts_with('#') {
        return None;
    }
    let glob = if pattern.contains('/') || pattern.contains('\\') {
        // Normalize path separators to forward slashes for glob matching
        let normalized = pattern.replace('\\', "/");
        match normalized.strip_prefix('/') {
            // Anchored pattern - remove leading '/' and match from start
            Some(anchored) => anchored.to_string(),
            // Match anywhere in the path
            None => format!("**/{}", normalized),
        }
    } else {
        // Simple filename pattern - match anywhere
        format!("**/{}", pattern)
    };
    Some(glob)
}

/// Refuse a list holding an unparseable pattern, naming the offending line.
///
/// Loading is tolerant — a typo in the saved config must not switch the
/// defaults off, so `new` warns and skips. Saving is not: the user is right
/// there in the dialog to fix the line, and accepting it silently would
/// display a rule that does nothing.
pub fn validate_patterns(patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        if let Some(glob) = user_glob_pattern(pattern) {
            Glob::new(&glob)
                .map(|_| ())
                .with_context(|| format!("invalid pattern `{}`", pattern.trim()))?;
        }
    }
    Ok(())
}

/// `literal_separator` keeps `*` from running across `/`: without it,
/// `**/~*` does not mean "files starting with `~`" but "everything under any
/// folder starting with `~`" — a user's `~archive/` would silently stop
/// syncing. Junk *directories* get their subtree via an explicit `/**` rule.
fn junk_glob(pattern: &str) -> Result<Glob> {
    GlobBuilder::new(pattern)
        .case_insensitive(true)
        .literal_separator(true)
        .build()
        .with_context(|| format!("Failed to build built-in ignore pattern `{pattern}`"))
}

/// A wrapper around `GlobSet` for matching ignore patterns (gitignore-style).
///
/// The matcher stores the sync root path and automatically strips it from
/// absolute paths before matching against the patterns.
#[derive(Debug, Clone)]
pub struct IgnoreMatcher {
    globset: GlobSet,
    /// Original patterns for debugging/logging
    patterns: Vec<String>,
    /// The sync root path - patterns are relative to this path
    sync_root: PathBuf,
}

impl IgnoreMatcher {
    /// Build an IgnoreMatcher from a list of gitignore-style patterns.
    ///
    /// # Arguments
    /// * `patterns` - List of gitignore-style patterns
    /// * `sync_root` - The sync root path. All patterns are relative to this path,
    ///                 and input paths will have this prefix stripped before matching.
    ///
    /// # Pattern Syntax
    /// - `*.log` - Matches any file ending with `.log` anywhere in the tree
    /// - `temp/` - Matches any directory named `temp` anywhere
    /// - `/build` - Matches `build` only at the sync root level
    /// - `docs/*.md` - Matches `.md` files in any `docs` directory
    /// - `#comment` - Lines starting with `#` are treated as comments
    pub fn new(patterns: &[String], sync_root: PathBuf) -> Result<Self> {
        let mut builder = GlobSetBuilder::new();

        for pattern in patterns {
            let Some(glob_pattern) = user_glob_pattern(pattern) else {
                // Skip empty lines and comments (gitignore-style)
                continue;
            };

            // A typo in one pattern only costs that pattern: dropping the whole
            // filter would silently take the defaults down with it.
            match Glob::new(&glob_pattern) {
                Ok(glob) => builder.add(glob),
                Err(e) => {
                    tracing::warn!(
                        target: "drive::ignore",
                        pattern = %pattern,
                        error = %e,
                        "Skipping invalid ignore pattern"
                    );
                    continue;
                }
            };
        }

        for name in DEFAULT_FILE_PATTERNS {
            builder.add(junk_glob(&format!("**/{name}"))?);
        }
        for dir in DEFAULT_JUNK_DIRS {
            builder.add(junk_glob(&format!("**/{dir}"))?);
            builder.add(junk_glob(&format!("**/{dir}/**"))?);
        }

        let globset = builder
            .build()
            .context("Failed to build ignore pattern matcher")?;

        Ok(Self {
            globset,
            patterns: patterns.to_vec(),
            sync_root,
        })
    }

    /// Create an empty matcher that matches nothing.
    ///
    /// # Arguments
    /// * `sync_root` - The sync root path (still required for consistency)
    pub fn empty(sync_root: PathBuf) -> Self {
        Self {
            globset: GlobSet::empty(),
            patterns: Vec::new(),
            sync_root,
        }
    }

    /// Check if an absolute path matches any of the ignore patterns.
    ///
    /// The path will have the sync root prefix stripped before matching.
    /// If the path is not under the sync root, it will not match any patterns.
    ///
    /// # Arguments
    /// * `path` - The absolute path to check
    ///
    /// # Returns
    /// `true` if the path matches any ignore pattern, `false` otherwise
    pub fn is_match<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();

        // Try to get the relative path from sync root
        let relative_path = match path.strip_prefix(&self.sync_root) {
            Ok(rel) => rel,
            Err(_) => {
                // Path is not under sync root, cannot match
                return false;
            }
        };

        // Convert to forward slashes for consistent matching across platforms
        let normalized = relative_path.to_string_lossy().replace('\\', "/");

        self.globset.is_match(&normalized)
    }

    /// Check if a path (given as relative path from sync root) matches any patterns.
    ///
    /// Use this when you already have a relative path.
    ///
    /// # Arguments
    /// * `relative_path` - Path relative to sync root
    ///
    /// # Returns
    /// `true` if the path matches any ignore pattern, `false` otherwise
    pub fn is_match_relative<P: AsRef<Path>>(&self, relative_path: P) -> bool {
        let normalized = relative_path.as_ref().to_string_lossy().replace('\\', "/");

        self.globset.is_match(&normalized)
    }

    /// Check if a filename (without path) matches any of the ignore patterns.
    ///
    /// This is useful for quick checks on just the filename.
    /// Note: This only matches patterns that don't contain path separators.
    ///
    /// # Arguments
    /// * `filename` - The filename to check (without path)
    ///
    /// # Returns
    /// `true` if the filename matches any ignore pattern, `false` otherwise
    pub fn is_match_filename(&self, filename: &str) -> bool {
        self.globset.is_match(filename)
    }

    /// Get the original patterns for debugging/logging.
    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    /// Get the sync root path.
    pub fn sync_root(&self) -> &Path {
        &self.sync_root
    }

    /// Check if the matcher has any patterns.
    pub fn is_empty(&self) -> bool {
        self.globset.is_empty()
    }

    /// Get the number of patterns.
    pub fn len(&self) -> usize {
        self.globset.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sync_root() -> PathBuf {
        PathBuf::from("/Users/test/sync")
    }

    #[test]
    fn test_simple_pattern() {
        let sync_root = test_sync_root();
        let patterns = vec!["*.log".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(matcher.is_match(sync_root.join("debug.log")));
        assert!(matcher.is_match(sync_root.join("subdir/error.log")));
        assert!(!matcher.is_match(sync_root.join("readme.txt")));
    }

    #[test]
    fn test_anchored_pattern() {
        let sync_root = test_sync_root();
        let patterns = vec!["/build".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(matcher.is_match(sync_root.join("build")));
        assert!(!matcher.is_match(sync_root.join("src/build")));
    }

    #[test]
    fn test_directory_pattern() {
        let sync_root = test_sync_root();
        let patterns = vec!["node_modules".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(matcher.is_match(sync_root.join("node_modules")));
        assert!(matcher.is_match(sync_root.join("project/node_modules")));
    }

    #[test]
    fn test_path_pattern() {
        let sync_root = test_sync_root();
        let patterns = vec!["docs/*.md".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(matcher.is_match(sync_root.join("docs/readme.md")));
        assert!(matcher.is_match(sync_root.join("project/docs/api.md")));
        assert!(!matcher.is_match(sync_root.join("readme.md")));
    }

    #[test]
    fn test_comment_and_empty_lines() {
        let sync_root = test_sync_root();
        let patterns = vec![
            "# This is a comment".to_string(),
            "".to_string(),
            "  ".to_string(),
            "*.tmp".to_string(),
        ];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        let defaults_only = IgnoreMatcher::new(&[], sync_root.clone()).unwrap();
        assert_eq!(
            matcher.len(),
            defaults_only.len() + 1,
            "only *.tmp should have counted as a pattern"
        );
        assert!(matcher.is_match(sync_root.join("file.tmp")));
        assert!(!matcher.is_match(sync_root.join("This is a comment")));
    }

    /// Paths the systems themselves produce, written down from what macOS,
    /// Windows and the Linux desktops actually put on disk — deliberately not
    /// read off the constants above, so that deleting a rule shows up here as a
    /// failure rather than as a silently shrinking list.
    ///
    /// Most of this junk lives *inside* a directory: the walker reports the
    /// shards under `.Spotlight-V100/`, never the directory itself.
    const REAL_WORLD_JUNK: &[&str] = &[
        // macOS
        ".DS_Store",
        "._report.pdf",
        ".localized",
        ".Spotlight-V100/Store-V2/abc/0.indexHead",
        ".Trashes/501/deleted.txt",
        ".fseventsd/0000000000123456",
        ".TemporaryItems/folders.501/x",
        ".DocumentRevisions-V100/PerUID/501/db",
        ".AppleDouble/report.pdf",
        ".AppleDB/index",
        ".AppleDesktop/x",
        // Linux desktops
        ".directory",
        ".nfs0000000012345678",
        ".Trash-1000/files/old.txt",
        // Windows
        "Thumbs.db",
        "ehthumbs.db",
        "desktop.ini",
        "$RECYCLE.BIN/S-1-5-21/x.dat",
        "System Volume Information/tracking.log",
        // Office / editors
        "~$budget.xlsx",
        ".~lock.budget.ods#",
        ".notes.txt.swp",
        ".notes.txt.swx",
        "notes.txt~",
    ];

    /// Junk written by the OS behind the user's back must never reach the
    /// server. These files are recreated constantly (Finder writes `.DS_Store`
    /// on every folder it displays), so without this every browse turns into an
    /// upload, an SSE echo and a download attempt.
    #[test]
    fn os_junk_is_ignored_without_the_user_configuring_anything() {
        let sync_root = test_sync_root();
        let matcher = IgnoreMatcher::new(&[], sync_root.clone()).unwrap();

        for junk in REAL_WORLD_JUNK {
            assert!(
                matcher.is_match(sync_root.join(junk)),
                "{junk} should be ignored at the sync root"
            );
            assert!(
                matcher.is_match(sync_root.join("photos/2024").join(junk)),
                "{junk} should be ignored in nested folders too"
            );
        }
    }

    /// The defaults must not swallow files the user actually cares about.
    #[test]
    fn ordinary_files_are_not_caught_by_the_default_patterns() {
        let sync_root = test_sync_root();
        let matcher = IgnoreMatcher::new(&[], sync_root.clone()).unwrap();

        for kept in [
            "report.pdf",
            "DS_Store.txt",
            "my.desktop.ini.backup",
            "Trashes.md",
            "directory.json",
            "swap.swift",
            ".gitignore",
            ".env",
        ] {
            assert!(
                !matcher.is_match(sync_root.join(kept)),
                "{kept} is a legitimate file and must be synced"
            );
        }
    }

    /// A *folder* the user named `~archive` or `._design` is not junk — the
    /// junk patterns describe file names, and a glob `*` that is allowed to
    /// run across `/` turns `**/~*` into "everything under any folder starting
    /// with `~`", silently unsyncing the whole subtree in both directions.
    #[test]
    fn a_real_folder_starting_like_junk_does_not_swallow_its_contents() {
        let sync_root = test_sync_root();
        let matcher = IgnoreMatcher::new(&[], sync_root.clone()).unwrap();

        for kept in [
            "~archive/report.docx",
            "~backup/2024/photos.zip",
            "._design/logo.png",
            ".nfs-mounts/data.csv",
            ".~lock.projects/notes.txt",
        ] {
            assert!(
                !matcher.is_match(sync_root.join(kept)),
                "{kept} lives in a user folder and must be synced"
            );
        }
    }

    /// Windows and macOS both write these names in whatever case they feel like:
    /// Explorer has shipped `Thumbs.db` and `thumbs.db` over the years, and a
    /// `.DS_Store` copied through a case-insensitive volume comes back as
    /// `.ds_store`. Matching the exact spelling only would let every variant sync.
    #[test]
    fn the_default_patterns_ignore_junk_whatever_its_case() {
        let sync_root = test_sync_root();
        let matcher = IgnoreMatcher::new(&[], sync_root.clone()).unwrap();

        for junk in [
            "thumbs.db",
            "THUMBS.DB",
            ".ds_store",
            ".Ds_Store",
            "Desktop.ini",
            "DESKTOP.INI",
            "notes.SWP",
            ".spotlight-v100/Store-V2/shard",
            "$Recycle.Bin/S-1-5-21/x.dat",
            "system volume information/tracking.log",
        ] {
            assert!(
                matcher.is_match(sync_root.join(junk)),
                "{junk} is the same junk in another case and must be ignored"
            );
        }
    }

    /// The user's own patterns keep gitignore semantics: case-sensitive. Only the
    /// built-in junk list is relaxed, because only that list is about names the
    /// OS picks itself.
    #[test]
    fn the_users_own_patterns_stay_case_sensitive() {
        let sync_root = test_sync_root();
        let patterns = vec!["*.LOG".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(matcher.is_match(sync_root.join("crash.LOG")));
        assert!(
            !matcher.is_match(sync_root.join("debug.log")),
            "a user pattern must match exactly what they typed"
        );
    }

    /// A file that stops syncing with nothing in Settings to explain it is
    /// indistinguishable from a bug. Every junk path the engine blocks must be
    /// accounted for by a line the user can actually read in the dialog.
    ///
    /// The shown lines are matched here on their own — not by asking the engine
    /// again — interpreted the way the dialog tells the user to read them: a
    /// bare name blocks that name anywhere, a directory blocks its contents too.
    #[test]
    fn the_settings_list_explains_every_file_the_engine_blocks() {
        let sync_root = test_sync_root();
        let engine = IgnoreMatcher::new(&[], sync_root.clone()).unwrap();

        let mut shown = GlobSetBuilder::new();
        for line in default_patterns() {
            shown.add(junk_glob(&format!("**/{line}")).unwrap());
            shown.add(junk_glob(&format!("**/{line}/**")).unwrap());
        }
        let shown = shown.build().unwrap();

        for junk in REAL_WORLD_JUNK {
            assert!(engine.is_match(sync_root.join(junk)), "{junk} is not blocked at all");
            assert!(
                shown.is_match(junk),
                "{junk} is blocked but no line shown in Settings accounts for it"
            );
        }
    }

    /// Backstop for the rules `REAL_WORLD_JUNK` does not happen to cover: a rule
    /// added to the engine but not to the displayed list would block files with
    /// no visible cause, and nothing else would notice.
    #[test]
    fn no_rule_is_enforced_without_being_listed_in_settings() {
        let shown = default_patterns();
        assert!(!shown.is_empty(), "the built-in list must not be empty");

        for enforced in DEFAULT_FILE_PATTERNS.iter().chain(DEFAULT_JUNK_DIRS) {
            assert!(
                shown.contains(&enforced.to_string()),
                "`{enforced}` is enforced but never shown in Settings"
            );
        }
    }

    /// A typo in one user pattern must not take the whole filter down with it.
    ///
    /// Both call sites fall back to a matcher on error and neither surfaces it,
    /// so a single bad pattern used to silently disable every other rule *and*
    /// the built-in defaults — turning junk filtering off without telling anyone.
    #[test]
    fn one_invalid_pattern_does_not_disable_the_rest() {
        let sync_root = test_sync_root();
        let patterns = vec!["[unclosed".to_string(), "*.log".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(matcher.is_match(sync_root.join("debug.log")), "the valid pattern was dropped");
        assert!(matcher.is_match(sync_root.join(".DS_Store")), "the defaults were dropped");
        assert!(!matcher.is_match(sync_root.join("report.pdf")));
    }

    #[test]
    fn test_path_outside_sync_root() {
        let sync_root = test_sync_root();
        let patterns = vec!["*.log".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root.clone()).unwrap();

        assert!(!matcher.is_match("/other/path/debug.log"));
    }

    #[test]
    fn test_relative_path_matching() {
        let sync_root = test_sync_root();
        let patterns = vec!["*.log".to_string(), "/build".to_string()];
        let matcher = IgnoreMatcher::new(&patterns, sync_root).unwrap();

        assert!(matcher.is_match_relative("debug.log"));
        assert!(matcher.is_match_relative("subdir/error.log"));
        assert!(matcher.is_match_relative("build"));
        assert!(!matcher.is_match_relative("src/build"));
    }
}
