#!/usr/bin/env python3
"""Keep raw current_exe references confined to reviewed, non-self-spawning uses.

Test subprocesses must use crate::test_exe::current_test_exe so a Cargo binary on a removable drive is staged
internally before exec. Scan every Rust
source, including untracked files and imports/aliases, rather than guessing
which modules Cargo compiles only for tests. Existing production and path-only
uses are recorded by enclosing function, count, and reason in the allowlist.
"""

from __future__ import annotations

import re
import runpy
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST = Path(__file__).with_suffix(".allow")
RUST = runpy.run_path(str(Path(__file__).with_name("rust-include-boundary.py")))
REFERENCE = re.compile(r"\bcurrent_exe\b")
FUNCTION = re.compile(r"\bfn\s+([A-Za-z_]\w*)\b[^;{]*\{")
REMEDY = "use crate::test_exe::current_test_exe() before re-executing a test program"


def read_allowlist(path=ALLOWLIST):
    allowed = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != 4:
            raise ValueError(f"{path}:{number}: expected path, function, count, and reason separated by tabs")
        source, function, count, reason = fields
        key = (source, function)
        if key in allowed or not source or not function or not count.isdecimal() or int(count) < 1 or not reason.strip():
            raise ValueError(f"{path}:{number}: expected a unique function, positive count, and nonempty reason")
        allowed[key] = int(count)
    return allowed


def scan(root=ROOT, allowed=None):
    if allowed is None:
        allowed = read_allowlist()
    observed = Counter()
    errors = []
    checked = 0
    for path in sorted((root / "crates").rglob("*.rs")):
        checked += 1
        source = path.read_text()
        masked = RUST["mask_non_code"](source)
        pairs = RUST["delimiter_pairs"](masked)
        functions = [
            (match.start(), pairs[match.end() - 1], match.group(1))
            for match in FUNCTION.finditer(masked)
            if match.end() - 1 in pairs
        ]
        relative = path.relative_to(root).as_posix()
        for reference in REFERENCE.finditer(masked):
            enclosing = [item for item in functions if item[0] <= reference.start() < item[1]]
            function = max(enclosing, default=(0, 0, "<module>"))[2]
            key = (relative, function)
            observed[key] += 1
            if observed[key] > allowed.get(key, 0):
                line = source.count("\n", 0, reference.start()) + 1
                errors.append(f"{relative}:{line} ({function}): {REMEDY}")
    for key, count in allowed.items():
        if observed[key] < count:
            errors.append(f"{key[0]} ({key[1]}): stale current_exe allowance; review or remove it")
    return checked, errors


def main():
    try:
        checked, errors = scan()
    except (OSError, ValueError) as error:
        print(f"test self-exec gate failed: {error}", file=sys.stderr)
        return 1
    if errors:
        print("test self-exec gate failed:", file=sys.stderr)
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"test self-exec: ok ({checked} Rust files checked; all raw current_exe uses reviewed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
