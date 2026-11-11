use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cloudreve_api::models::uri::CrUri;
use url::Url;

use crate::drive::mounts::DriveConfig;

/// Prefix of a partial download. Downloads are staged next to their destination
/// so the final move stays on one filesystem, which means the sync folder holds
/// these files while they are being written.
pub const PARTIAL_DOWNLOAD_PREFIX: &str = ".cloudreve-part-";

/// Whether `path` is a partial download staged by the client. Such a file is
/// half-written by definition and must never be scanned, tracked or uploaded.
pub fn is_partial_download(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with(PARTIAL_DOWNLOAD_PREFIX))
}

/// Turns a path the server sent, relative to the synced folder ("/a/b.txt"
/// in an SSE event, the tail of a listed entry's uri), into a relative local
/// path — or `None` if joining it to the sync folder could land anywhere but
/// under it.
///
/// Every server-supplied path goes through here: a `..` segment (sent as is,
/// or hidden in a name behind an encoded `/` that decoding turns back into a
/// separator) would otherwise resolve next to the sync folder, and the
/// download task would happily write or delete a file there — anywhere in
/// the user's home directory. `Path::starts_with` is no protection: it
/// compares components lexically, and `sync/../x` starts with `sync`.
///
/// Empty segments (leading, trailing or doubled slashes) are dropped as
/// harmless. Rejected: `.` and `..`, NUL, and a backslash (a separator on
/// Windows, where the same server data must mean the same thing). An empty
/// result (the root itself) is also `None`: it names no file.
pub fn safe_relative_path(remote_rel: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for segment in remote_rel.split('/').filter(|s| !s.is_empty()) {
        if segment == "." || segment == ".." || segment.contains('\0') || segment.contains('\\') {
            return None;
        }
        // Belt and braces: whatever the platform, one segment must be
        // exactly one plain name.
        let mut components = Path::new(segment).components();
        if !matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(_)), None)
        ) {
            return None;
        }
        out.push(segment);
    }
    if out.as_os_str().is_empty() { None } else { Some(out) }
}

/// Whether `path` names something strictly under `root`, with no `..` or
/// other non-name component after it — the check `safe_relative_path`
/// guarantees for paths built from server data, re-asserted where a local
/// file is about to be written or removed.
pub fn is_inside(root: &Path, path: &Path) -> bool {
    match path.strip_prefix(root) {
        Ok(rest) => {
            !rest.as_os_str().is_empty()
                && rest.components().all(|c| matches!(c, std::path::Component::Normal(_)))
        }
        Err(_) => false,
    }
}

pub fn local_path_to_cr_uri(path: PathBuf, root: PathBuf, remote_base: String) -> Result<CrUri> {
    let mut base = CrUri::new(&remote_base)?;

    // Strip the root from path to get the relative path
    let relative = path.strip_prefix(&root).context("Path is not under root")?;
    // `strip_prefix` is lexical: `root/../x` strips to `../x`.
    if !relative.components().all(|c| matches!(c, std::path::Component::Normal(_))) {
        anyhow::bail!("Path escapes the sync folder: {}", path.display());
    }

    // Convert to string with forward slashes (for URI compatibility)
    let relative_str = relative
        .to_str()
        .context("Path contains invalid UTF-8")?
        .replace("\\", "/");

    // Join the relative path to the base URI if not empty
    if !relative_str.is_empty() {
        base.join(&relative_str.split("/").collect::<Vec<&str>>());
    }

    Ok(base)
}

pub fn remote_path_to_local_relative_path(
    remote_path: &CrUri,
    remote_base: &CrUri,
) -> Result<PathBuf> {
    let remote_path_str = remote_path.path().clone();
    let remote_base_str = remote_base.path().clone();

    // 1. add ending slash if not presented to remote_base_str
    let remote_base_str = if !remote_base_str.ends_with('/') {
        remote_base_str + "/"
    } else {
        remote_base_str
    };

    // 2. remove remote_base_str from remote_path_str
    let relative_path = remote_path_str
        .strip_prefix(&remote_base_str)
        .context("Path is not under remote base")?;

    // 3. `path()` is percent-decoded: a single listed name like `..%2Fx`
    // comes back as `../x`. See `safe_relative_path`.
    safe_relative_path(relative_path)
        .with_context(|| format!("Remote path escapes the synced folder: {remote_path_str}"))
}

/// Generate a URL to view a folder or file online.
///
/// For folders: pass the folder path as `folder_path` and None for `open_file`
/// For files: pass the parent folder path as `folder_path` and the file path as `open_file`
pub fn view_online_url(
    folder_path: &str,
    open_file: Option<&str>,
    config: &DriveConfig,
) -> Result<String> {
    let mut base = config.instance_url.parse::<Url>()?;
    base.set_path("/home");

    {
        let mut query = base.query_pairs_mut();
        query.append_pair("path", folder_path);

        if let Some(file) = open_file {
            query.append_pair("open", file);
        }

        query.append_pair("user_hint", config.user_id.as_str());
    }

    Ok(base.to_string())
}

pub fn recycle_bin_url(config: &DriveConfig) -> Result<String> {
    let mut base = config.instance_url.parse::<Url>()?;
    base.set_path("/home");

    {
        let mut query = base.query_pairs_mut();
        query.append_pair("user_hint", config.user_id.as_str());
        query.append_pair("path", "cloudreve://trash");
    }

    Ok(base.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DriveConfig {
        DriveConfig {
            instance_url: "https://cloud.example.com".to_string(),
            user_id: "user-42".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn local_path_maps_to_remote_uri() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/home/me/sync/docs/report.txt");
        let uri = local_path_to_cr_uri(path, root, "cloudreve://my/data".to_string()).unwrap();
        assert_eq!(uri.path(), "/data/docs/report.txt");
    }

    #[test]
    fn local_path_at_root_returns_base() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/home/me/sync");
        let uri = local_path_to_cr_uri(path, root, "cloudreve://my/data".to_string()).unwrap();
        assert_eq!(uri.path(), "/data");
    }

    #[test]
    fn local_path_outside_root_errors() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/etc/passwd");
        assert!(local_path_to_cr_uri(path, root, "cloudreve://my".to_string()).is_err());
    }

    #[test]
    fn remote_path_maps_to_local_relative() {
        let base = CrUri::new("cloudreve://my/data").unwrap();
        let remote = CrUri::new("cloudreve://my/data/docs/report.txt").unwrap();
        let rel = remote_path_to_local_relative_path(&remote, &base).unwrap();
        assert_eq!(rel, PathBuf::from("docs/report.txt"));
    }

    #[test]
    fn remote_path_outside_base_errors() {
        let base = CrUri::new("cloudreve://my/data").unwrap();
        let remote = CrUri::new("cloudreve://my/other/file").unwrap();
        assert!(remote_path_to_local_relative_path(&remote, &base).is_err());
    }

    #[test]
    fn ordinary_server_paths_map_to_nested_local_paths() {
        assert_eq!(safe_relative_path("/docs/2026/report.txt"), Some(PathBuf::from("docs/2026/report.txt")));
        assert_eq!(safe_relative_path("photo..jpg"), Some(PathBuf::from("photo..jpg")));
        assert_eq!(safe_relative_path("/a//b/"), Some(PathBuf::from("a/b")));
        assert_eq!(safe_relative_path("/.config/app.ini"), Some(PathBuf::from(".config/app.ini")));
    }

    #[test]
    fn server_paths_that_climb_out_are_rejected() {
        for evil in [
            "/../planted.txt",
            "../../Library/LaunchAgents/x.plist",
            "/a/../../b",
            "/a/..",
            "/./a",
            "..",
            "/a\\..\\..\\b",
            "/a/b\0c",
        ] {
            assert_eq!(safe_relative_path(evil), None, "{evil:?} was accepted");
        }
    }

    #[test]
    fn the_root_itself_names_no_file() {
        assert_eq!(safe_relative_path("/"), None);
        assert_eq!(safe_relative_path(""), None);
    }

    #[test]
    fn a_listed_name_hiding_a_climb_behind_encoded_slashes_is_rejected() {
        let base = CrUri::new("cloudreve://my/data").unwrap();
        let remote = CrUri::new("cloudreve://my/data/..%2F..%2Fplanted.txt").unwrap();
        assert!(remote_path_to_local_relative_path(&remote, &base).is_err());
    }

    #[test]
    fn only_paths_strictly_under_the_root_are_inside() {
        let root = Path::new("/home/me/sync");
        assert!(is_inside(root, Path::new("/home/me/sync/a/b.txt")));
        assert!(!is_inside(root, Path::new("/home/me/sync/../x")));
        assert!(!is_inside(root, Path::new("/home/me/sync/a/../../x")));
        assert!(!is_inside(root, Path::new("/home/me/sync")));
        assert!(!is_inside(root, Path::new("/home/me/syncx/a")));
        assert!(!is_inside(root, Path::new("/etc/passwd")));
    }

    #[test]
    fn a_local_path_climbing_out_has_no_remote_uri() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/home/me/sync/../.ssh/id_rsa");
        assert!(local_path_to_cr_uri(path, root, "cloudreve://my".to_string()).is_err());
    }

    #[test]
    fn view_online_url_for_folder() {
        let url = view_online_url("cloudreve://my/docs", None, &config()).unwrap();
        assert!(url.starts_with("https://cloud.example.com/home?"));
        assert!(url.contains("user_hint=user-42"));
        assert!(url.contains("path=cloudreve"));
        assert!(!url.contains("open="));
    }

    #[test]
    fn view_online_url_for_file_includes_open() {
        let url = view_online_url(
            "cloudreve://my/docs",
            Some("cloudreve://my/docs/a.txt"),
            &config(),
        )
        .unwrap();
        assert!(url.contains("open=cloudreve"));
    }

    #[test]
    fn recycle_bin_url_points_at_trash() {
        let url = recycle_bin_url(&config()).unwrap();
        assert!(url.starts_with("https://cloud.example.com/home?"));
        assert!(url.contains("user_hint=user-42"));
        assert!(url.contains("trash"));
    }
}
