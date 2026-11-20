#!/usr/bin/env bash
# Starts a throwaway Cloudreve server for the real-server e2e suites
# (crates/*/tests/real_server_*.rs) and creates the admin account they log
# in with. Prints the env vars to export.
#
#   scripts/e2e-server.sh <data-dir> [port]
#
# The release is pinned and its checksum checked: the suites test against
# a known server, not whatever "latest" is today.
set -euo pipefail

VERSION=4.19.1
DATA_DIR=$(mkdir -p "$1" && cd "$1" && pwd)
PORT=${2:-5212}
EMAIL=${CLOUDREVE_E2E_EMAIL:-admin@e2e.test}
PASSWORD=${CLOUDREVE_E2E_PASSWORD:-e2e-password-123}

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) ASSET=darwin_arm64 SHA=a67577da69a77fd11246fc663604b6edabf3392149db764243bfcf496f581620 ;;
  Linux-x86_64) ASSET=linux_amd64 SHA=287a1f59adb1249af15cd550b191d2fa71132bfd125fead8578f20152c4b99de ;;
  Linux-aarch64) ASSET=linux_arm64 SHA=f55d77e0e5eb8bc699a9a6bb1216b0ee3afa3bfe8d7c9d405ca9845339ae7b0e ;;
  *) echo "no pinned Cloudreve build for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

TARBALL="$DATA_DIR/cloudreve_${VERSION}_${ASSET}.tar.gz"
if [ ! -x "$DATA_DIR/cloudreve" ]; then
  curl -fsSL -o "$TARBALL" \
    "https://github.com/cloudreve/cloudreve/releases/download/${VERSION}/cloudreve_${VERSION}_${ASSET}.tar.gz"
  if command -v sha256sum >/dev/null; then GOT=$(sha256sum "$TARBALL" | cut -d' ' -f1)
  else GOT=$(shasum -a 256 "$TARBALL" | cut -d' ' -f1); fi
  if [ "$GOT" != "$SHA" ]; then echo "checksum mismatch for $TARBALL: $GOT" >&2; exit 1; fi
  tar -xzf "$TARBALL" -C "$DATA_DIR" cloudreve
fi

[ -f "$DATA_DIR/conf.ini" ] || printf '[System]\nMode = master\nListen = 127.0.0.1:%s\n' "$PORT" > "$DATA_DIR/conf.ini"
# -w: the database and uploads live next to conf.ini, not next to the binary.
(cd "$DATA_DIR" && exec nohup ./cloudreve -w --conf "$DATA_DIR/conf.ini" > "$DATA_DIR/server.log" 2>&1) &
echo $! > "$DATA_DIR/server.pid"

URL="http://127.0.0.1:$PORT"
for _ in $(seq 1 60); do
  curl -fs "$URL/api/v4/site/ping" >/dev/null && break
  sleep 1
done
curl -fs "$URL/api/v4/site/ping" >/dev/null || { tail -n 50 "$DATA_DIR/server.log" >&2; exit 1; }

# The first account registered on a fresh server is its admin. Already
# there on a second run: the login below is what matters.
curl -fs -X POST "$URL/api/v4/user" -H 'content-type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\",\"language\":\"en-US\"}" >/dev/null || true
LOGIN=$(curl -fs -X POST "$URL/api/v4/session/token" -H 'content-type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}")
case "$LOGIN" in *'"code":0'*) ;; *) echo "login failed: $LOGIN" >&2; exit 1 ;; esac

echo "CLOUDREVE_E2E_URL=$URL"
echo "CLOUDREVE_E2E_EMAIL=$EMAIL"
echo "CLOUDREVE_E2E_PASSWORD=$PASSWORD"
