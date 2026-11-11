#!/bin/sh
# Builds target/finder-sync/CloudreveSidebar.app: the helper app holding the
# Finder Sync extension (crates/cloudreve-finder-sync), which the Tauri
# bundler then copies into Cloudreve.app/Contents/Helpers
# (tauri.macos.conf.json: build.beforeBundleCommand + bundle.macOS.files).
#
# Needs no Xcode: the extension is plain Rust, Assets.car is committed
# (compile-symbol.sh regenerates it, that one does need actool).
#
# Signed here, inside out, because the bundler signs only the main app
# (without --deep, so these signatures are kept): the extension ad-hoc with
# its sandbox entitlements, then the helper. Re-signing the extension
# without Extension.entitlements would make PlugInKit ignore it.
set -eu

[ "$(uname)" = Darwin ] || exit 0

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
target_dir=${CARGO_TARGET_DIR:-$root/target}
triple=${TAURI_ENV_TARGET_TRIPLE:-}

# Always release: it is tiny, and a debug app build must still register a
# working extension.
if [ -n "$triple" ]; then
	cargo build --manifest-path "$root/Cargo.toml" --release -p cloudreve-finder-sync --target "$triple"
	bin=$target_dir/$triple/release/cloudreve-finder-sync
else
	cargo build --manifest-path "$root/Cargo.toml" --release -p cloudreve-finder-sync
	bin=$target_dir/release/cloudreve-finder-sync
fi

[ -f "$here/Assets.car" ] || { echo "build-helper.sh: $here/Assets.car is missing (compile-symbol.sh, needs actool)" >&2; exit 1; }

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$root/Cargo.toml" | head -n 1)
[ -n "$version" ] || { echo "build-helper.sh: no workspace version in Cargo.toml" >&2; exit 1; }

app=$root/target/finder-sync/CloudreveSidebar.app
appex=$app/Contents/PlugIns/CloudreveFinderSync.appex
rm -rf "$app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources" "$appex/Contents/MacOS"

sed "s/@VERSION@/$version/g" "$here/Host-Info.plist" > "$app/Contents/Info.plist"
sed "s/@VERSION@/$version/g" "$here/Extension-Info.plist" > "$appex/Contents/Info.plist"
cp "$here/Assets.car" "$app/Contents/Resources/Assets.car"
cp "$root/src-tauri/icons/icon.icns" "$app/Contents/Resources/icon.icns"
# One binary, two roles: it runs the extension only from inside the .appex.
cp "$bin" "$appex/Contents/MacOS/CloudreveFinderSync"
cp "$bin" "$app/Contents/MacOS/CloudreveSidebar"
# codesign rejects FinderInfo / resource fork "detritus" on copied files.
xattr -cr "$app"

codesign --force --sign - --options runtime --entitlements "$here/Extension.entitlements" "$appex"
codesign --force --sign - --options runtime "$app"
codesign --verify --strict --deep "$app"
echo "build-helper.sh: built $app ($version)"
