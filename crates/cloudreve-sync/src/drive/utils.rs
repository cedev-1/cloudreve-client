use std::path::PathBuf;

use anyhow::{Context, Result};
use cloudreve_api::models::uri::CrUri;
use url::Url;

use crate::drive::mounts::DriveConfig;

pub fn local_path_to_cr_uri(path: PathBuf, root: PathBuf, remote_base: String) -> Result<CrUri> {
    let mut base = CrUri::new(&remote_base)?;

    // Strip the root from path to get the relative path
    let relative = path.strip_prefix(&root).context("Path is not under root")?;

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

    // 3. make sure OS slash is used
    let relative_path = relative_path.replace("/", std::path::MAIN_SEPARATOR_STR);

    Ok(PathBuf::from(relative_path))
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
