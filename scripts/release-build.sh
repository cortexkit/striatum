#!/usr/bin/env bash
# Stamp ck-striatum with the source commit so the packaged binary can report it.
set -euo pipefail

cd "$(dirname "$0")/.."

revision=$(git rev-parse HEAD)
# A dirty tree cannot be identified by a commit. Leave dev builds unstamped
# rather than claiming the resulting binary came from an unchanged tree.
dirty=$(git status --porcelain)
if [[ -n "$dirty" ]]; then
    echo "refusing to stamp a dirty tree: the built bytes would not match $revision" >&2
    echo "commit or stash first, or use a plain cargo build for a dev binary" >&2
    exit 1
fi

echo "building at $revision"
CK_BUILD_GIT_SHA="$revision" CK_BUILD_GIT_DIRTY=false \
    cargo build --locked --release -p striatum-module --bin ck-striatum

after_revision=$(git rev-parse HEAD)
dirty=$(git status --porcelain)
if [[ "$after_revision" != "$revision" || -n "$dirty" ]]; then
    echo "refusing to report provenance: the checkout changed during the build (expected clean $revision; HEAD is $after_revision)" >&2
    exit 1
fi

# Use a hard link so the probe runs the same executable without appearing as
# a second production ck-striatum process on a developer machine.
probe_dir=$(mktemp -d)
# macOS Bash 3.2 can report success on an unbound variable with an EXIT trap.
# Require the script to reach its last step before accepting a zero status.
finished=0
on_exit() {
    local rc=$?
    rm -rf "$probe_dir" || true
    if [[ "$rc" -eq 0 && "$finished" -ne 1 ]]; then
        echo "release-build.sh: stopped before its last step; treating this run as failed" >&2
        rc=1
    fi
    exit "$rc"
}
trap on_exit EXIT

suffix=""
python_cmd=python3
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) suffix=.exe; python_cmd=python ;;
esac
artifact_dir="${CARGO_TARGET_DIR:-target}/release"
binary="ck-striatum$suffix"
probe="$probe_dir/ckdev-striatum$suffix"
ln "$artifact_dir/$binary" "$probe" 2>/dev/null || cp "$artifact_dir/$binary" "$probe"
if ! provenance=$("$probe" --print-manifest | "$python_cmd" -c '
import json, sys
manifest = json.load(sys.stdin)["manifest"]
version = manifest["module_version"]
revision = manifest["provenance"].get("build_git_sha")
if revision != sys.argv[1]:
    sys.exit(f"ck-striatum reports revision {revision!r}, expected {sys.argv[1]}")
print(f"ck-striatum {version} ({revision})")
' "$revision"); then
    echo "release-build.sh: ck-striatum manifest probe failed" >&2
    exit 1
fi
printf '\n%s\n\nsha256:\n' "$provenance"

# macOS ships shasum; Linux and Git Bash can use sha256sum.
(
    cd "$artifact_dir"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$binary"
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$binary"
    else
        echo "no sha256 tool found (looked for shasum, sha256sum)" >&2
        exit 1
    fi
)
finished=1
