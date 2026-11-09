//! The Finder Sync extension (`CloudreveFinderSync.appex`), see the crate
//! doc in `lib.rs` for why it exists.
//!
//! It does one thing: keep `FIFinderSyncController.directoryURLs` equal to
//! the folder list the app writes. No badge, no menu, no toolbar item —
//! Finder then only uses it to pick the sidebar glyph of those folders.
//!
//! The same binary is also the helper app's executable (an app bundle needs
//! one to register); run from there it exits at once.

#[cfg(target_os = "macos")]
mod extension;

#[cfg(target_os = "macos")]
fn main() {
    extension::main()
}

#[cfg(not(target_os = "macos"))]
fn main() {}
