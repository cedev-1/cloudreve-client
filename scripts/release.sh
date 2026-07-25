#!/usr/bin/env bash
#
# Prepare a release: bump the version everywhere, commit, tag.
#
#   ./scripts/release.sh 0.4.0          -> stable, becomes "Latest", users get updated
#   ./scripts/release.sh 0.4.0-beta.1   -> prerelease, no auto-update pushed
#
# Deliberately stops before `git push`: pushing the tag publishes a GitHub release
# to everyone, and that is not a step a script should take on your behalf. The
# command to run is printed at the end.
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:-}"
VERSION="${VERSION#v}"

if [[ -z "$VERSION" ]]; then
  echo "usage: $0 <version>   e.g. 0.4.0  or  0.4.0-beta.1" >&2
  exit 1
fi

# Loose semver. The suffix is what the release workflow reads to decide whether
# this is a prerelease, so a typo there silently ships a beta as stable.
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]]; then
  echo "error: '$VERSION' is not a semver version (X.Y.Z or X.Y.Z-suffix)" >&2
  exit 1
fi

TAG="v$VERSION"

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "error: working tree is dirty — commit or stash first" >&2
  git status --short >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "error: tag $TAG already exists" >&2
  exit 1
fi

# Two files carry the version. tauri.conf.json is the bundle version the updater
# compares; the workspace one feeds CARGO_PKG_VERSION, which is baked into the
# User-Agent sent to every Cloudreve server.
jq --arg v "$VERSION" '.version = $v' src-tauri/tauri.conf.json > src-tauri/tauri.conf.json.tmp
mv src-tauri/tauri.conf.json.tmp src-tauri/tauri.conf.json

sed -i.bak -E "s/^version = \".*\"/version = \"$VERSION\"/" Cargo.toml
rm Cargo.toml.bak

# A sed that matched nothing leaves a stale version behind and would ship a
# release lying about which build it is.
grep -q "^version = \"$VERSION\"\$" Cargo.toml \
  || { echo "error: failed to patch the version in Cargo.toml" >&2; exit 1; }
[[ "$(jq -r .version src-tauri/tauri.conf.json)" == "$VERSION" ]] \
  || { echo "error: failed to patch the version in tauri.conf.json" >&2; exit 1; }

# Cargo.lock pins the workspace members by version too. Left alone it would still
# name the previous release, so the tag would ship a lockfile contradicting it.
cargo update --workspace --offline -q

git add Cargo.toml Cargo.lock src-tauri/tauri.conf.json
git commit -m "chore(release): $TAG"
git tag "$TAG"

if [[ "$VERSION" == *-* ]]; then
  KIND="prerelease — will NOT be offered to users as an update"
else
  KIND="stable — will become \"Latest\" and be offered to every user"
fi

echo
echo "Prepared $TAG ($KIND)."
echo "Nothing is published yet. To release:"
echo
echo "    git push origin $(git rev-parse --abbrev-ref HEAD) $TAG"
echo
