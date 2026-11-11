#!/usr/bin/env python3
"""Keep production SQLite transaction construction behind the store policy."""

import re
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = runpy.run_path(str(ROOT / "scripts/unreachable-modules.py"))["production_source"]
RUST = runpy.run_path(str(ROOT / "scripts/rust-include-boundary.py"))
HELPER = "crates/striatum-store/src/transactions.rs"
ALLOWLIST = ROOT / "scripts/read-only-transactions.allow"
OPENING = re.compile(
    r"\b(?:unchecked_transaction|transaction|transaction_with_behavior)\s*\("
    r"|\bTransaction\s*::\s*(?:new|new_unchecked)\s*\("
)
FUNCTION = re.compile(r"\bfn\s+(\w+)\b[^;{]*\{")


def transaction_sites(source: str) -> list[tuple[str, int]]:
    code = PRODUCTION_SOURCE(source)
    test_ranges = RUST["test_only_ranges"](source, code, RUST["delimiter_pairs"](code))
    functions = []
    for function in FUNCTION.finditer(code):
        depth = 1
        end = function.end()
        while end < len(code) and depth:
            depth += (code[end] == "{") - (code[end] == "}")
            end += 1
        functions.append((function.start(), end, function.group(1)))
    sites = []
    for opening in OPENING.finditer(code):
        if RUST["in_ranges"](opening.start(), test_ranges):
            continue
        owner = next(
            (name for start, end, name in reversed(functions) if start <= opening.start() < end),
            "<outside-function>",
        )
        sites.append((owner, source.count("\n", 0, opening.start()) + 1))
    return sites


def read_allowlist(path: Path) -> dict[str, str]:
    allowed = {}
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        site, separator, reason = line.partition(" # ")
        if not separator or not reason.strip() or site in allowed:
            raise ValueError(f"invalid read-only transaction allowance: {line}")
        allowed[site] = reason
    return allowed


def scan(root: Path, allowed: dict[str, str]) -> tuple[int, list[str]]:
    used = set()
    errors = []
    count = 0
    for path in sorted((root / "crates").glob("*/src/**/*.rs")):
        relative = path.relative_to(root).as_posix()
        if path.name in {"tests.rs", "test.rs"} or path.stem.endswith(("_tests", "_test")):
            continue
        sites = transaction_sites(path.read_text())
        count += len(sites)
        for function, line in sites:
            site = f"{relative}::{function}"
            if relative == HELPER and function == "write_transaction":
                continue
            if site in allowed and site not in used:
                used.add(site)
            else:
                errors.append(f"{relative}:{line}: use write_transaction; only audited read-only transactions may be allowlisted ({function})")
    errors.extend(f"stale read-only transaction allowance: {site}" for site in sorted(allowed.keys() - used))
    return count, errors


def main() -> int:
    count, errors = scan(ROOT, read_allowlist(ALLOWLIST))
    for error in errors:
        print(error)
    print(f"write-transactions: checked {count} transaction openings; {len(errors)} violations")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
