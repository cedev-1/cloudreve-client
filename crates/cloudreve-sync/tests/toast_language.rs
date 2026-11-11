//! Notifications speak the user's language, like the rest of the app: the
//! conflict and low-disk-space ones used to be hardcoded English.
//!
//! Own binary: the notifier and the locale are process-wide.

use std::path::Path;

use tokio::sync::mpsc;

#[test]
fn conflict_and_disk_space_notifications_are_translated() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    cloudreve_sync::utils::toast::init_os_notifier(tx);
    rust_i18n::set_locale("fr");

    cloudreve_sync::utils::toast::send_conflict_toast(
        "drive-1",
        Path::new("/home/ana/Cloud/budget.ods"),
        Some(Path::new("/home/ana/Cloud/budget (conflict 2026-11-10).ods")),
    );
    let (title, body) = rx.try_recv().expect("a conflict notification");
    assert_eq!(title, "Modification locale en conflit avec le distant pour le fichier suivant :");
    assert_eq!(
        body,
        "/home/ana/Cloud/budget.ods\nL'autre version a été conservée sous /home/ana/Cloud/budget (conflict 2026-11-10).ods."
    );

    cloudreve_sync::utils::toast::send_low_disk_space_toast("drive-1", "Travail", 5 * 1024 * 1024 * 1024, 1024 * 1024 * 1024);
    let (title, body) = rx.try_recv().expect("a disk space notification");
    assert_eq!(title, "Espace disque insuffisant");
    assert!(body.starts_with("Travail a besoin de "), "{body}");
}
