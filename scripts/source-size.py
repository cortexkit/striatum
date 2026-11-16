#!/usr/bin/env python3
"""Keep hand-written Rust sources below the line limit or their frozen size."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
LIMIT = 5000
GENERATED_MARKERS = ("@generated", "automatically generated", "auto-generated", "do not edit")


def exempt(path, source):
    parts = path.parts
    name = path.name
    if ("tests" in parts or "fixtures" in parts or "fixture" in parts
            or name == "tests.rs" or name.endswith("_tests.rs")
            or name in ("fixture.rs", "fixtures.rs")
            or name.endswith(("_fixture.rs", "_fixtures.rs"))):
        return True
    # Generated files conventionally identify themselves in their header.
    header = "\n".join(source.splitlines()[:10]).lower()
    return any(marker in header for marker in GENERATED_MARKERS)


def violations(root=ROOT):
    allow = {}
    for line in (root / "scripts/source-size.allow").read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            path, count = line.split()
            if path in allow:
                raise ValueError(f"duplicate allow entry: {path}")
            allow[path] = int(count)

    errors, notices = [], []
    seen = set()
    exempt_counts = {}
    for path in sorted((root / "crates").rglob("*.rs")):
        relative = path.relative_to(root).as_posix()
        source = path.read_text()
        count = len(source.splitlines())
        if exempt(path.relative_to(root), source):
            exempt_counts[relative] = count
            continue
        seen.add(relative)
        if relative in allow:
            recorded = allow[relative]
            if count <= LIMIT:
                errors.append(f"{relative}: {count} lines, limit {LIMIT}; remove stale allow entry (recorded {recorded})")
            elif count > recorded:
                errors.append(f"{relative}: {count} lines, limit {LIMIT}; exceeds recorded count {recorded}")
            elif count < recorded:
                notices.append(f"{relative}: {count} lines, limit {LIMIT}; lower recorded count from {recorded}")
        elif count > LIMIT:
            errors.append(f"{relative}: {count} lines, limit {LIMIT}; not in allow list")

    for relative in sorted(allow.keys() - seen):
        if relative in exempt_counts:
            errors.append(f"{relative}: {exempt_counts[relative]} lines, limit {LIMIT}; allow entry is exempt (recorded {allow[relative]})")
        else:
            errors.append(f"{relative}: 0 lines, limit {LIMIT}; allow entry is missing (recorded {allow[relative]})")
    return errors, notices


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT
    try:
        errors, notices = violations(root)
    except (OSError, ValueError) as error:
        print(f"source size gate failed: {error}", file=sys.stderr)
        return 1
    for notice in notices:
        print(f"source size notice: {notice}")
    if errors:
        print("source size gate failed:", file=sys.stderr)
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("source size: all hand-written Rust files meet their limits")
    return 0


if __name__ == "__main__":
    sys.exit(main())
