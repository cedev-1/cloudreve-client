# Merge declaratively managed settings into the Cloudreve client config.
#
# Usage: cloudreve-client-merge-config MANAGED_JSON [CONFIG_JSON]
#
#   MANAGED_JSON  JSON object with the keys Nix manages (read-only, store path)
#   CONFIG_JSON   the app's own config, default ~/.cloudreve/config.json
#
# The app owns CONFIG_JSON: it reads it at startup and rewrites the whole file
# whenever a setting changes in the UI. A read-only symlink would break that,
# so instead every key of MANAGED_JSON is written over the current file and
# every other key is kept: Nix wins for what it manages, the UI stays free for
# the rest. The file is replaced atomically and only when something changes.
#
# The app refuses to start its settings layer on a file it can't parse; such a
# file is moved aside (CONFIG_JSON.invalid-<epoch>) and rebuilt from the
# managed keys, the app filling the rest with its defaults.

managed=${1:?usage: cloudreve-client-merge-config MANAGED_JSON [CONFIG_JSON]}
target=${2:-$HOME/.cloudreve/config.json}

if ! jq -e 'type == "object"' "$managed" >/dev/null; then
  echo "cloudreve-client: $managed is not a JSON object" >&2
  exit 1
fi

dir=$(dirname -- "$target")
mkdir -p -- "$dir"
# Same permissions the app enforces (crates/cloudreve-sync/src/utils/secure_fs.rs):
# the directory also holds drive credentials.
chmod 700 -- "$dir"

current='{}'
if [ -e "$target" ]; then
  if jq -e 'type == "object"' "$target" >/dev/null 2>&1; then
    current=$(cat -- "$target")
  else
    backup="$target.invalid-$(date +%s)"
    mv -- "$target" "$backup"
    echo "cloudreve-client: $target was not a valid JSON object, moved to $backup" >&2
  fi
fi

merged=$(jq --slurpfile managed "$managed" '. * $managed[0]' <<<"$current")

if [ -e "$target" ] && jq -e --argjson merged "$merged" '. == $merged' "$target" >/dev/null; then
  exit 0
fi

tmp=$(mktemp "$dir/.config.json.XXXXXX")
trap 'rm -f -- "$tmp"' EXIT
printf '%s\n' "$merged" >"$tmp"
chmod 600 -- "$tmp"
mv -f -- "$tmp" "$target"
trap - EXIT
