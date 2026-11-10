#!/bin/sh
# One signing policy for build artifacts and their final placement on macOS.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
usage_error() {
  printf 'error: %s\n' "$1" >&2
  printf 'usage: %s sign|verify|place binary routing [destination]\n' "$0" >&2
  exit 2
}
[ "$#" -ge 3 ] || usage_error "operation, binary and module kind are required"
operation=$1
binary=$2
kind=$3
[ -n "$binary" ] || usage_error "binary path must not be empty"
case "$kind" in routing) ;; *) usage_error "unsupported module kind: '$kind'" ;; esac
# Always pass an explicit identifier: codesign otherwise derives one from the
# file name, and a temporary name would change the identity that keychain
# items and privacy grants are tied to once modules carry a team signature.
identifier="ck-striatum"
sign() {
  codesign -f -s - -o runtime --identifier "$identifier" "$1"
}
case "$operation" in
  sign) sign "$binary" ;;
  verify) exec python3 "$ROOT/script/verify-module-signing.py" "$binary" "$kind" ;;
  place)
    [ "$#" -ge 4 ] && [ -n "$4" ] || usage_error "place requires a destination"
    dest=$4
    mkdir -p "$(dirname "$dest")"
    scratch=$(mktemp -d "$(dirname "$dest")/.module-placement.XXXXXX")
    installed=0
    accepted=0
    existed=0
    finished=0
    rollback() {
      status=$?
      if [ "$status" -eq 0 ] && [ "$finished" -ne 1 ]; then
        status=1
      fi
      if [ "$installed" = 1 ] && [ "$accepted" = 0 ]; then
        if [ "$existed" = 1 ]; then mv -f "$scratch/old" "$dest"; else rm -f "$dest"; fi
        echo "REFUSED: placed binary failed gate; previous binary restored (no restart)" >&2
      fi
      rm -rf "$scratch"
      exit "$status"
    }
    trap rollback EXIT
    trap 'exit 1' INT TERM
    staged="$scratch/ckdev-${identifier#ck-}"
    cp "$binary" "$staged"
    sign "$staged"
    if [ -e "$dest" ]; then cp -p "$dest" "$scratch/old"; existed=1; fi
    mv -f "$staged" "$dest"
    installed=1
    # Re-sign at the final path and inspect that path, not the staging artifact.
    sign "$dest"
    python3 "$ROOT/script/verify-module-signing.py" "$dest" "$kind"
    accepted=1
    finished=1
    ;;
  *) usage_error "unsupported operation: '$operation'" ;;
esac
