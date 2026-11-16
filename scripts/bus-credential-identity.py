#!/usr/bin/env python3
"""Keep ck-bus authentication on the daemon's stamped module identity."""
from pathlib import Path
import re
import runpy
import sys

ROOT = Path(__file__).resolve().parent.parent
structural_source = runpy.run_path(str(ROOT / "scripts/unreachable-modules.py"))["structural_source"]


def violations(text):
    code = structural_source(text)
    return [code.count("\n", 0, match.start()) + 1
            for match in re.finditer(r"\bconsumer_identity\b", code)]


def main():
    path = ROOT / "crates/striatum-module/src/quota_bus.rs"
    errors = violations(path.read_text())
    for line in errors:
        print(f"{path.relative_to(ROOT)}:{line}: ck-bus calls must use daemon-stamped identity", file=sys.stderr)
    if errors:
        return 1
    print("bus credential identity: checked quota_bus.rs; no consumer_identity override")
    return 0


if __name__ == "__main__":
    sys.exit(main())
