fn main() {
    // The extension's logging shim (src/oslog.c), macOS only.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rerun-if-changed=src/oslog.c");
        cc::Build::new().file("src/oslog.c").cargo_warnings(false).compile("cloudreve_finder_sync_oslog");
    }
}
