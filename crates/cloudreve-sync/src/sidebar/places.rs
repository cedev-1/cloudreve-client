//! Text edits of the two Linux "places" files a file manager reads its
//! sidebar from. Pure functions over the file contents: the caller reads
//! the file, asks for an edit, and writes the result back atomically only
//! when there is one (`None` = nothing to change, so a file is never
//! rewritten — and never reformatted — just to stay the same).
//!
//! - GTK bookmarks (`$XDG_CONFIG_HOME/gtk-3.0/bookmarks`): one entry per
//!   line, `URI[ LABEL]`. Read by Nautilus (GNOME Files), Nemo, Caja,
//!   Thunar and every GTK 3/4 file chooser — GTK 4 still reads the
//!   `gtk-3.0` file, there is no `gtk-4.0/bookmarks`.
//! - KDE places (`$XDG_DATA_HOME/user-places.xbel`): XBEL XML, read by
//!   Dolphin and every KDE file dialog. Each `<bookmark>` can name a theme
//!   icon, which is how the entry gets the Cloudreve logo there.
//!
//! Entries are matched by the folder they point to, decoded from the URI,
//! never by raw string: GLib and KDE do not percent-encode exactly the
//! same characters, and an entry the user added by hand must still be
//! recognized as ours.

use std::path::{Path, PathBuf};

/// `file://` URI of an absolute path, encoded the way GLib's
/// `g_filename_to_uri` does: every byte outside the unreserved set and
/// `!$&'()*+,;=:@/` is percent-encoded (UTF-8 bytes included).
pub fn file_uri(path: &Path) -> String {
    use std::fmt::Write;
    use std::os::unix::ffi::OsStrExt;
    let mut uri = String::from("file://");
    for &b in path.as_os_str().as_bytes() {
        if b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/".contains(&b) {
            uri.push(b as char);
        } else {
            let _ = write!(uri, "%{b:02X}");
        }
    }
    uri
}

/// The folder a `file://` URI points to, or `None` for any other scheme
/// (`sftp://`, `smb://`, `recent:///`, ...) or a malformed escape. A
/// trailing slash needs no stripping: `Path` equality ignores it.
pub fn path_of_uri(uri: &str) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let encoded = uri.strip_prefix("file://")?;
    let mut bytes = Vec::with_capacity(encoded.len());
    let mut it = encoded.bytes();
    while let Some(b) = it.next() {
        if b == b'%' {
            let hex = [it.next()?, it.next()?];
            bytes.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
        } else {
            bytes.push(b);
        }
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
}

fn points_to(uri: &str, folder: &Path) -> bool {
    path_of_uri(uri).is_some_and(|p| p == folder)
}

/// The URI of one bookmarks line (everything before the first space).
fn gtk_line_uri(line: &str) -> &str {
    line.split_once(' ').map_or(line, |(uri, _)| uri)
}

/// Whether the GTK bookmarks file already lists `folder`.
pub fn gtk_contains(content: &str, folder: &Path) -> bool {
    content.lines().any(|line| points_to(gtk_line_uri(line), folder))
}

/// `content` with a bookmark for `folder` labelled `label` appended, or
/// `None` when one is already there.
pub fn gtk_add(content: &str, folder: &Path, label: &str) -> Option<String> {
    if gtk_contains(content, folder) {
        return None;
    }
    let mut out = content.to_string();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    // A line break in the label would start a bogus entry.
    let label = label.replace(['\n', '\r'], " ");
    out.push_str(&format!("{} {}\n", file_uri(folder), label.trim()));
    Some(out)
}

/// `content` without any bookmark pointing to `folder`, or `None` when
/// there is none.
pub fn gtk_remove(content: &str, folder: &Path) -> Option<String> {
    if !gtk_contains(content, folder) {
        return None;
    }
    Some(
        content
            .split_inclusive('\n')
            .filter(|line| !points_to(gtk_line_uri(line.trim_end_matches(['\n', '\r'])), folder))
            .collect(),
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// The `href` attribute of a `<bookmark ...>` start tag, unescaped.
fn href_of(tag: &str) -> Option<String> {
    let at = tag.find("href=")?;
    let rest = &tag[at + 5..];
    let quote = rest.chars().next().filter(|c| *c == '"' || *c == '\'')?;
    let value = &rest[1..];
    let end = value.find(quote)?;
    Some(xml_unescape(&value[..end]))
}

/// Byte ranges of every `<bookmark>` element pointing to `folder`, each
/// extended over its line's leading indentation and trailing newline so
/// removing it leaves no blank line behind.
fn xbel_ranges(content: &str, folder: &Path) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut from = 0;
    while let Some(rel) = content[from..].find("<bookmark") {
        let start = from + rel;
        from = start + 1;
        // `<bookmark:icon>` is metadata (a `<folder>` carries one too), not
        // an entry: written as an open tag, taking it for one would run to
        // the NEXT `</bookmark>` and skip the real bookmark in between.
        if !content[start + 9..].starts_with([' ', '\t', '\n', '\r', '>', '/']) {
            continue;
        }
        let Some(tag_len) = content[start..].find('>') else { break };
        let tag = &content[start..start + tag_len + 1];
        let end = if tag.ends_with("/>") {
            start + tag.len()
        } else {
            match content[start..].find("</bookmark>") {
                Some(close) => start + close + "</bookmark>".len(),
                None => break,
            }
        };
        if href_of(tag).is_some_and(|href| points_to(&href, folder)) {
            let line_start = content[..start].rfind('\n').map_or(0, |i| i + 1);
            let lead_is_indent = content[line_start..start].chars().all(|c| c == ' ' || c == '\t');
            let begin = if lead_is_indent { line_start } else { start };
            let finish = if lead_is_indent && content[end..].starts_with('\n') { end + 1 } else { end };
            ranges.push(begin..finish);
        }
        from = end;
    }
    ranges
}

/// Whether the XBEL places file already has a bookmark for `folder`.
pub fn xbel_contains(content: &str, folder: &Path) -> bool {
    !xbel_ranges(content, folder).is_empty()
}

/// `content` with a `<bookmark>` for `folder` (title `title`, theme icon
/// `icon`, KDE id `id`) inserted before the closing `</xbel>`, or `None`
/// when one is already there or the document has no `</xbel>` (not a
/// places file this code understands: left alone).
pub fn xbel_add(content: &str, folder: &Path, title: &str, icon: &str, id: &str) -> Option<String> {
    if xbel_contains(content, folder) {
        return None;
    }
    let close = content.rfind("</xbel>")?;
    let entry = format!(
        " <bookmark href=\"{href}\">\n  <title>{title}</title>\n  <info>\n   <metadata owner=\"http://freedesktop.org\">\n    <bookmark:icon name=\"{icon}\"/>\n   </metadata>\n   <metadata owner=\"http://www.kde.org\">\n    <ID>{id}</ID>\n    <isSystemItem>false</isSystemItem>\n   </metadata>\n  </info>\n </bookmark>\n",
        href = xml_escape(&file_uri(folder)),
        title = xml_escape(title),
        icon = xml_escape(icon),
        id = xml_escape(id),
    );
    // `</xbel>` normally starts its own line; if not, give the entry one.
    let sep = if close == 0 || content[..close].ends_with('\n') { "" } else { "\n" };
    Some(format!("{}{sep}{entry}{}", &content[..close], &content[close..]))
}

/// `content` without any `<bookmark>` element pointing to `folder`, or
/// `None` when there is none.
pub fn xbel_remove(content: &str, folder: &Path) -> Option<String> {
    let ranges = xbel_ranges(content, folder);
    if ranges.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(content.len());
    let mut at = 0;
    for r in ranges {
        out.push_str(&content[at..r.start]);
        at = r.end;
    }
    out.push_str(&content[at..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A bookmarks file as Nautilus writes it: plain entries, a labelled
    // one, a remote one, and the final newline.
    const GTK_FILE: &str = "file:///home/ana/Documents\n\
file:///home/ana/Music%20Library Music\n\
sftp://nas.local/srv/share NAS\n";

    #[test]
    fn a_path_with_spaces_and_accents_becomes_the_uri_nautilus_writes() {
        assert_eq!(file_uri(Path::new("/home/ana/My Drive")), "file:///home/ana/My%20Drive");
        assert_eq!(file_uri(Path::new("/home/ana/Clé")), "file:///home/ana/Cl%C3%A9");
        assert_eq!(file_uri(Path::new("/home/ana/a#b%c")), "file:///home/ana/a%23b%25c");
        assert_eq!(file_uri(Path::new("/home/ana/R&D (2024)")), "file:///home/ana/R&D%20(2024)");
    }

    #[test]
    fn a_file_uri_decodes_back_to_its_folder_and_other_schemes_do_not() {
        assert_eq!(path_of_uri("file:///home/ana/Cl%C3%A9"), Some(PathBuf::from("/home/ana/Clé")));
        assert_eq!(path_of_uri("file:///home/ana/Cloud/"), Some(PathBuf::from("/home/ana/Cloud")));
        // KDE leaves `(` encoded where GLib does not: same folder.
        assert_eq!(path_of_uri("file:///home/ana/R%26D%20%282024%29"), Some(PathBuf::from("/home/ana/R&D (2024)")));
        assert_eq!(path_of_uri("sftp://nas.local/srv/share"), None);
        assert_eq!(path_of_uri("file:///home/ana/bad%zz"), None);
    }

    #[test]
    fn adding_a_drive_folder_appends_one_labelled_line_and_keeps_the_rest() {
        let out = gtk_add(GTK_FILE, Path::new("/home/ana/Cloudreve"), "Cloudreve").unwrap();
        assert_eq!(out, format!("{GTK_FILE}file:///home/ana/Cloudreve Cloudreve\n"));
    }

    #[test]
    fn adding_to_a_file_without_a_final_newline_starts_a_new_line() {
        let out = gtk_add("file:///home/ana/Documents", Path::new("/home/ana/Cloud"), "Cloud").unwrap();
        assert_eq!(out, "file:///home/ana/Documents\nfile:///home/ana/Cloud Cloud\n");
    }

    #[test]
    fn adding_to_a_missing_or_empty_file_writes_just_the_entry() {
        assert_eq!(gtk_add("", Path::new("/home/ana/Cloud"), "Cloud").unwrap(), "file:///home/ana/Cloud Cloud\n");
    }

    #[test]
    fn adding_a_folder_that_is_already_bookmarked_changes_nothing() {
        assert_eq!(gtk_add(GTK_FILE, Path::new("/home/ana/Documents"), "Docs"), None);
        // Bookmarked by hand with a label and a trailing slash: still ours.
        assert_eq!(gtk_add(GTK_FILE, Path::new("/home/ana/Music Library"), "Music"), None);
        let added = gtk_add(GTK_FILE, Path::new("/home/ana/Cloud"), "Cloud").unwrap();
        assert_eq!(gtk_add(&added, Path::new("/home/ana/Cloud"), "Cloud"), None);
    }

    #[test]
    fn a_bookmarked_folder_is_found_and_a_lookalike_is_not() {
        assert!(gtk_contains(GTK_FILE, Path::new("/home/ana/Music Library")));
        assert!(!gtk_contains(GTK_FILE, Path::new("/home/ana/Music")));
        assert!(!gtk_contains(GTK_FILE, Path::new("/home/ana/Documents/Old")));
        assert!(!gtk_contains(GTK_FILE, Path::new("/srv/share")));
        // A bookmark on a folder INSIDE the drive is not the drive.
        assert!(!gtk_contains("file:///home/ana/Cloud/Photos\n", Path::new("/home/ana/Cloud")));
        assert!(!xbel_contains("<xbel>\n <bookmark href=\"file:///home/ana/Cloud/Photos\"/>\n</xbel>\n", Path::new("/home/ana/Cloud")));
    }

    #[test]
    fn removing_a_drive_folder_drops_only_its_line() {
        let out = gtk_remove(GTK_FILE, Path::new("/home/ana/Music Library")).unwrap();
        assert_eq!(out, "file:///home/ana/Documents\nsftp://nas.local/srv/share NAS\n");
    }

    #[test]
    fn removing_drops_every_duplicate_of_the_folder() {
        let doubled = "file:///home/ana/Cloud Cloud\nfile:///home/ana/Documents\nfile:///home/ana/Cloud/\n";
        assert_eq!(gtk_remove(doubled, Path::new("/home/ana/Cloud")).unwrap(), "file:///home/ana/Documents\n");
    }

    #[test]
    fn removing_a_folder_that_is_not_bookmarked_changes_nothing() {
        assert_eq!(gtk_remove(GTK_FILE, Path::new("/home/ana/Cloud")), None);
    }

    // A places file as Dolphin writes it (trimmed): the namespace
    // declarations on the root, one system place, one user place.
    const XBEL_FILE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE xbel>
<xbel xmlns:bookmark="http://www.freedesktop.org/standards/desktop-bookmarks" xmlns:mime="http://www.freedesktop.org/standards/shared-mime-info" xmlns:kdepriv="http://www.kde.org/kdepriv">
 <bookmark href="file:///home/ana">
  <title>Home</title>
  <info>
   <metadata owner="http://freedesktop.org">
    <bookmark:icon name="user-home"/>
   </metadata>
   <metadata owner="http://www.kde.org">
    <ID>1699999999/0</ID>
    <isSystemItem>true</isSystemItem>
   </metadata>
  </info>
 </bookmark>
 <bookmark href="file:///home/ana/R%26D%20%282024%29">
  <title>R&amp;D</title>
  <info>
   <metadata owner="http://www.kde.org">
    <ID>1699999999/3</ID>
   </metadata>
  </info>
 </bookmark>
</xbel>
"#;

    #[test]
    fn adding_a_drive_folder_to_kde_places_inserts_one_bookmark_with_its_icon() {
        let out = xbel_add(XBEL_FILE, Path::new("/home/ana/My Cloud"), "My <Cloud>", "cloudreve", "1700000000/7").unwrap();
        let expected_entry = r#" <bookmark href="file:///home/ana/My%20Cloud">
  <title>My &lt;Cloud&gt;</title>
  <info>
   <metadata owner="http://freedesktop.org">
    <bookmark:icon name="cloudreve"/>
   </metadata>
   <metadata owner="http://www.kde.org">
    <ID>1700000000/7</ID>
    <isSystemItem>false</isSystemItem>
   </metadata>
  </info>
 </bookmark>
"#;
        let (head, tail) = XBEL_FILE.rsplit_once("</xbel>").unwrap();
        assert_eq!(out, format!("{head}{expected_entry}</xbel>{tail}"));
        assert!(xbel_contains(&out, Path::new("/home/ana/My Cloud")));
    }

    #[test]
    fn adding_a_folder_kde_already_lists_changes_nothing() {
        assert_eq!(xbel_add(XBEL_FILE, Path::new("/home/ana/R&D (2024)"), "x", "cloudreve", "1/1"), None);
    }

    #[test]
    fn a_document_without_a_closing_xbel_tag_is_left_alone() {
        assert_eq!(xbel_add("<xbel>\n <bookmark href=\"file:///a\"/>\n", Path::new("/home/ana/Cloud"), "C", "i", "1/1"), None);
    }

    #[test]
    fn kde_places_are_matched_by_folder_not_by_prefix() {
        assert!(xbel_contains(XBEL_FILE, Path::new("/home/ana")));
        assert!(xbel_contains(XBEL_FILE, Path::new("/home/ana/R&D (2024)")));
        assert!(!xbel_contains(XBEL_FILE, Path::new("/home/an")));
        assert!(!xbel_contains(XBEL_FILE, Path::new("/home/ana/Cloud")));
    }

    #[test]
    fn removing_a_drive_folder_from_kde_places_drops_its_whole_bookmark_only() {
        let out = xbel_remove(XBEL_FILE, Path::new("/home/ana/R&D (2024)")).unwrap();
        let start = XBEL_FILE.find(" <bookmark href=\"file:///home/ana/R%26D").unwrap();
        let end = XBEL_FILE.find("</xbel>").unwrap();
        assert_eq!(out, format!("{}{}", &XBEL_FILE[..start], &XBEL_FILE[end..]));
    }

    #[test]
    fn removing_what_was_added_restores_the_original_file_byte_for_byte() {
        let added = xbel_add(XBEL_FILE, Path::new("/home/ana/Cloud"), "Cloud", "cloudreve", "1700000000/7").unwrap();
        assert_eq!(xbel_remove(&added, Path::new("/home/ana/Cloud")).unwrap(), XBEL_FILE);
        let added = gtk_add(GTK_FILE, Path::new("/home/ana/Cloud"), "Cloud").unwrap();
        assert_eq!(gtk_remove(&added, Path::new("/home/ana/Cloud")).unwrap(), GTK_FILE);
    }

    #[test]
    fn a_self_closing_kde_bookmark_is_removed_too() {
        let doc = "<xbel>\n <bookmark href='file:///home/ana/Cloud'/>\n <bookmark href=\"file:///home/ana\"/>\n</xbel>\n";
        assert_eq!(
            xbel_remove(doc, Path::new("/home/ana/Cloud")).unwrap(),
            "<xbel>\n <bookmark href=\"file:///home/ana\"/>\n</xbel>\n"
        );
    }

    #[test]
    fn a_bookmark_right_after_a_folder_icon_is_still_found() {
        let doc = "<xbel>\n <folder>\n  <info><metadata><bookmark:icon name=\"folder\"></bookmark:icon></metadata></info>\n </folder>\n <bookmark href=\"file:///home/ana/Cloud\">\n  <title>Cloud</title>\n </bookmark>\n</xbel>\n";
        assert!(xbel_contains(doc, Path::new("/home/ana/Cloud")));
        assert_eq!(
            xbel_remove(doc, Path::new("/home/ana/Cloud")).unwrap(),
            "<xbel>\n <folder>\n  <info><metadata><bookmark:icon name=\"folder\"></bookmark:icon></metadata></info>\n </folder>\n</xbel>\n"
        );
    }

    #[test]
    fn removing_a_folder_kde_does_not_list_changes_nothing() {
        assert_eq!(xbel_remove(XBEL_FILE, Path::new("/home/ana/Cloud")), None);
    }
}
