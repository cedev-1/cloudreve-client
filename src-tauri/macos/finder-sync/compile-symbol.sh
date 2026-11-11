#!/bin/sh
# Compiles Symbols.xcassets (the sidebar glyph) into Assets.car. Needs
# Xcode's actool, which a dev machine may not have: the result is committed,
# so building the app needs no Xcode. CI runs this on every build (see the
# finder-sync job in ci.yml) and uploads the fresh Assets.car as an artifact.
#
#   sh compile-symbol.sh [output-dir]      default: next to this script
#   sh compile-symbol.sh --source-hash     print the source fingerprint
#
# Next to Assets.car goes Assets.car.source: the fingerprint of the
# Symbols.xcassets it was compiled from. CI compares it with the current
# source (no actool needed), so an edited symbol whose catalog was not
# recompiled fails the build instead of shipping the old glyph.
set -eu
here=$(cd "$(dirname "$0")" && pwd)

source_hash() {
	(cd "$here" && find Symbols.xcassets -type f ! -name .DS_Store | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -d ' ' -f 1)
}

if [ "${1:-}" = --source-hash ]; then
	source_hash
	exit 0
fi

out=${1:-$here}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
xcrun actool "$here/Symbols.xcassets" --compile "$work" --platform macosx \
	--minimum-deployment-target 11.0 --output-format human-readable-text
# The symbol must be in the catalog under the name Host-Info.plist uses.
assetutil --info "$work/Assets.car" | grep -q '"Name" : "custom.cloudreve.logo"'
mkdir -p "$out"
cp "$work/Assets.car" "$out/Assets.car"
source_hash > "$out/Assets.car.source"
echo "compile-symbol.sh: wrote $out/Assets.car"
