#!/bin/sh
# Build, validate, sign and place ck-striatum, then verify the
# supervisor generation, running inode, boot ledger and health on macOS.
# Usage: script/deploy-module.sh [routing]
# DEPLOY_PREBUILT=<binary> skips tests/build but requires known build provenance.
# DEPLOY_SKIP_QUICK_GATE=1 skips tests when this exact tree was already verified.
set -eu

BIN_DIR="$HOME/.local/share/cortexkit/bin"
CONN="$HOME/.local/share/cortexkit/run/subc-connection.json"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

case "${1:-routing}" in
  routing) NAME="ck-striatum"; MODULE="striatum" ;;
  *) echo "usage: $0 [routing]" >&2; exit 2 ;;
esac

DATA_DIR="$HOME/.local/share/cortexkit/$MODULE"
DEST="$BIN_DIR/$NAME"

PREBUILT="${DEPLOY_PREBUILT:-}"
# The commit the candidate binary embeds when it was built elsewhere; empty
# when this script builds the checkout.
CANDIDATE_SHA=""
if [ -n "$PREBUILT" ]; then
  [ -f "$PREBUILT" ] && [ -x "$PREBUILT" ] || { echo "REFUSED: DEPLOY_PREBUILT $PREBUILT is not an executable file" >&2; exit 2; }
  CANDIDATE_SHA="$("$PREBUILT" --print-manifest 2>/dev/null | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin)["manifest"]["provenance"]["build_git_sha"])
except (ValueError, KeyError, TypeError):
    pass
' || true)"
  git -C "$REPO_ROOT" cat-file -e "$CANDIDATE_SHA^{commit}" 2>/dev/null \
    || { echo "REFUSED: $PREBUILT embeds no build commit this checkout knows (got '$CANDIDATE_SHA')" >&2; exit 2; }
  echo "placing prebuilt $NAME built from $CANDIDATE_SHA; quick gate and build skipped" >&2
elif [ "${DEPLOY_SKIP_QUICK_GATE:-0}" = "1" ]; then
  # For internal modules whose targeted tests already passed on this tree.
  echo "quick gate skipped: DEPLOY_SKIP_QUICK_GATE=1" >&2
else
  (cd "$REPO_ROOT" && bash scripts/rust-test-gate.sh)
fi

# Build the candidate here unless an explicit prebuilt binary was supplied.
# Matching inodes proves placement, not freshness: a stale build can pass every
# restart check. Cargo must select the candidate from the current source tree.
#
# The build itself reports where it wrote the binary, and that report is the
# only source we install from. A hardcoded target/release is a guess about the
# layout that CARGO_TARGET_DIR, a `[build] target-dir` in any cargo config, or a
# --target triple all invalidate; with an old binary sitting in the default
# directory, the guess installs THOSE bytes and every gate below (inode, boot,
# health) passes without deploying the current sources.
BUILD_LOG="$(mktemp "${TMPDIR:-/tmp}/deploy-module-build.XXXXXX")"
finished=0
cleanup() {
  status=$?
  if [ "$status" -eq 0 ] && [ "$finished" -ne 1 ]; then
    status=1
  fi
  rm -f "$BUILD_LOG"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [ -n "$PREBUILT" ]; then
  SRC="$PREBUILT"
else
PKG="striatum-module"
(
  cd "$REPO_ROOT"
  # Deployment attests this checkout, not any caller-supplied build environment.
  CK_BUILD_GIT_SHA="$(git rev-parse HEAD)"
  TRACKED_STATUS="$(git status --porcelain --untracked-files=no)"
  if [ -n "$TRACKED_STATUS" ]; then
    CK_BUILD_GIT_DIRTY=true
  else
    CK_BUILD_GIT_DIRTY=false
  fi
  export CK_BUILD_GIT_SHA CK_BUILD_GIT_DIRTY
  cargo build --locked --release -p "$PKG" --message-format=json-render-diagnostics
) > "$BUILD_LOG"
SRC="$(DEPLOY_BIN_NAME="$NAME" python3 - "$BUILD_LOG" <<'PY'
import json, os, sys

wanted = os.environ["DEPLOY_BIN_NAME"]
found = ""
with open(sys.argv[1]) as handle:
    for line in handle:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if record.get("reason") != "compiler-artifact":
            continue
        executable = record.get("executable") or ""
        if executable and os.path.basename(executable) == wanted:
            found = executable
print(found)
PY
)"
[ -n "$SRC" ] || { echo "cargo reported no executable named $NAME in its build output (build failed?)" >&2; exit 1; }
fi
[ -f "$SRC" ] || { echo "cargo named $SRC but nothing is there (build failed?)" >&2; exit 1; }

# Validate immediately before replacing the serving binary.
# A live-configuration validation failure must not consume supervisor retries.
if ! "$SRC" --check-config; then
  echo "REFUSED: $NAME rejected the live configuration; installed binary and supervisor left unchanged" >&2
  exit 9
fi

CK="$HOME/.local/share/cortexkit/bin/ck"
if [ "${DEPLOY_NO_RESTART:-0}" != "1" ]; then
  # The supervisor status reports a spawn generation, not a process PID. Capture
  # that value before placement so the later PID lookup is only trusted after a
  # genuinely newer process generation has been observed.
  supervisor_generation() {
    "$CK" --subc "$CONN" module status "$MODULE" --json 2>/dev/null | python3 -c '
import json, sys
try:
    module = json.load(sys.stdin)["module"]
    generation = module["spawn_generation"]
except (AttributeError, KeyError, TypeError, ValueError):
    sys.exit(1)
if isinstance(generation, int) and not isinstance(generation, bool) and generation >= 0:
    print(generation)
else:
    sys.exit(1)
'
  }
  OLD_GENERATION="$(supervisor_generation)" || {
    echo "REFUSED: supervisor did not report the current $MODULE spawn generation" >&2
    exit 10
  }
fi

# The signing gate restores the previous file on refusal before any restart.
sh "$REPO_ROOT/script/module-signing.sh" place "$SRC" routing "$DEST"
# This exec is the final production placement, not a staged smoke copy: DEST
# is already in ~/.local/share/cortexkit/bin, so its ck-* name is intentional.
# Finish Gatekeeper's first-launch assessment before the supervisor starts it.
"$DEST" --version >/dev/null 2>&1 || true
# DEPLOY_NO_RESTART=1 places the signed, warm-exec'd binary for the next daemon
# start (a planned machine reboot) and leaves the running process alone. Nothing
# below applies: there is no new process yet to verify. Verify the boot after the restart
# by hand: running inode == disk inode, boot-strikes ledger clean, health ok.
if [ "${DEPLOY_NO_RESTART:-0}" = "1" ]; then
  echo "placed $NAME without restart: disk inode $(stat -f '%i' "$DEST"); verify inode, boot ledger and health after the next start"
  finished=1
  exit 0
fi
# Absolute path: the operator PATH carries this bin dir, but scheduled jobs and
# noninteractive tool shells may not. A missing probe leaves the module running the
# OLD inode with the NEW file on disk - the exact state the inode check exists
# to catch.
SUBC_PROBE="$HOME/.local/share/cortexkit/bin/subc-probe"
"$SUBC_PROBE" --subc "$CONN" --supervisor-restart "$MODULE" >/dev/null

DISK_INODE="$(stat -f '%i' "$DEST")"
PID=""
RUNNING_INODE=none
GENERATION_ADVANCED=0
MULTIPLE_PROCESSES=0
for _ in $(seq 1 12); do
  CURRENT_GENERATION="$(supervisor_generation || true)"
  if [ -n "$CURRENT_GENERATION" ] && [ "$CURRENT_GENERATION" -gt "$OLD_GENERATION" ]; then
    GENERATION_ADVANCED=1
    # Count only processes whose executable is the module binary. pgrep -f alone
    # also matches any shell whose command line merely mentions the path (a
    # watcher running `ls .../bin/$NAME` failed a good deploy on 2026-11-01).
    PIDS="$(pgrep -f "bin/$NAME" | while read -r candidate; do
      case "$(ps -o comm= -p "$candidate" 2>/dev/null)" in (*/bin/"$NAME") echo "$candidate" ;; esac
    done || true)"
    PID_COUNT="$(printf '%s\n' "$PIDS" | awk 'NF { count++ } END { print count+0 }')"
    # The old process can still be draining for a few seconds after the new one
    # spawns, so two matches are expected briefly. Keep polling; only fail if
    # more than one process still matches when the wait runs out.
    MULTIPLE_PROCESSES=0
    if [ "$PID_COUNT" -gt 1 ]; then
      MULTIPLE_PROCESSES=1
    elif [ "$PID_COUNT" -eq 1 ]; then
      PID="$PIDS"
      RUNNING_INODE="$(lsof -p "$PID" 2>/dev/null | awk -v n="$NAME" '$4=="txt" && index($0,n){print $NF; exit}' | xargs stat -f '%i' 2>/dev/null || echo none)"
      [ "$RUNNING_INODE" = "$DISK_INODE" ] && break
    fi
  fi
  sleep 5
done
[ "$GENERATION_ADVANCED" = "1" ] || { echo "VERIFY FAILED: supervisor spawn generation did not advance after restarting $NAME" >&2; exit 4; }
[ "$MULTIPLE_PROCESSES" = "0" ] || { echo "VERIFY FAILED: multiple processes still match the restarted $NAME binary path after 60 s" >&2; exit 4; }
[ -n "$PID" ] || { echo "VERIFY FAILED: supervisor reported a new $NAME spawn, but no unique process matched its binary path" >&2; exit 4; }
if [ "$RUNNING_INODE" != "$DISK_INODE" ]; then
  echo "VERIFY FAILED: running inode $RUNNING_INODE != disk $DISK_INODE" >&2
  exit 4
fi

# The inode proves the new file is the one executing; it says nothing about
# whether it BOOTED. On 2026-09-10 the module tripped its boot breaker twice in
# the first two seconds and served reads only for 2.5 hours while this script
# had printed "verified". Two boot facts close that gap: the breaker ledger the
# module writes beside its store, and the health the supervisor reports.
LEDGER="$DATA_DIR/boot-strikes.json"
if [ -f "$LEDGER" ]; then
  TRIPPED="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d.get("breaker_tripped_at_ms") is not None)' "$LEDGER" 2>/dev/null || echo unknown)"
  if [ "$TRIPPED" != "False" ]; then
    echo "VERIFY FAILED: $MODULE boot breaker ledger is tripped (or unreadable: $TRIPPED); the module is serving reads only. Ledger:" >&2
    cat "$LEDGER" >&2; echo >&2
    exit 5
  fi
fi
# The supervisor probes health on its own cadence; right after a restart the row
# reads "unknown" until the first probe lands. Wait for a real reading (up to a
# minute) before judging it.
HEALTH=unknown
for _ in $(seq 1 12); do
  HEALTH="$("$CK" --subc "$CONN" module list --json 2>/dev/null \
    | python3 -c 'import json,sys; m=[x for x in json.load(sys.stdin).get("modules",[]) if x.get("module_id")==sys.argv[1]]; print(m[0].get("health","absent") if m else "absent")' "$MODULE" 2>/dev/null || echo unknown)"
  [ "$HEALTH" != "unknown" ] && break
  sleep 5
done
if [ "$HEALTH" != "ok" ]; then
  echo "VERIFY FAILED: supervisor reports $MODULE health=$HEALTH after restart (detail in ~/.local/share/cortexkit/run/subc.log)" >&2
  exit 5
fi
echo "deployed $NAME: inode $DISK_INODE verified, boot clean, health ok"

finished=1
