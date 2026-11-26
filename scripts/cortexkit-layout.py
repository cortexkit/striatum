#!/usr/bin/env python3
"""Reject tracked CortexKit files outside the shared project layout."""

import re
import subprocess
import sys


ALLOWED = re.compile(
    rb"^\.cortexkit/(?:release-notes/|skills/|rules/|WORKER\.md$|[a-z0-9-]+\.jsonc$)"
)


def main():
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z", "--", ".cortexkit"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as error:
        print(f"BLIND: git ls-files failed: {error}", file=sys.stderr)
        return 2
    if result.returncode:
        print("BLIND: git ls-files failed", file=sys.stderr)
        sys.stderr.buffer.write(result.stderr)
        return 2

    bad = [path for path in result.stdout.split(b"\0") if path and not ALLOWED.match(path)]
    if bad:
        sys.stderr.buffer.write(b"tracked outside the .cortexkit allowlist:\n")
        for path in bad:
            sys.stderr.buffer.write(path + b"\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
