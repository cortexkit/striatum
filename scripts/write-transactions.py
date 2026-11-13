#!/usr/bin/env python3
"""Keep production SQLite transaction construction behind the store policy."""

import re
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULES = runpy.run_path(str(ROOT / "scripts/unreachable-modules.py"))
PRODUCTION_SOURCE = MODULES["production_source"]
RUST = runpy.run_path(str(ROOT / "scripts/rust-include-boundary.py"))
HELPER = "crates/striatum-store/src/transactions.rs"
ALLOWLIST = ROOT / "scripts/read-only-transactions.allow"
ACCOUNT_ENTRIES = ROOT / "scripts/account-writer-entry-points.txt"
PROJECTION = "crates/striatum-store/src/evaluation_snapshot.rs::projection_command"
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


def read_account_entries(path: Path) -> set[str]:
    entries = set()
    projections = []
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        if line.startswith("@projection "):
            projections.append(line.removeprefix("@projection "))
        elif not re.fullmatch(r"\w+", line) or line in entries:
            raise ValueError(f"invalid account writer entry: {line}")
        else:
            entries.add(line)
    if projections != [PROJECTION]:
        raise ValueError("exactly the private SQL projection allowance is required")
    return entries


def account_writer_violations(root: Path, entries: set[str]) -> list[str]:
    """Distinguish store entry points from methods on a command's AccountTx."""
    source_root = root / "crates/striatum-module/src"
    files = sorted(source_root.rglob("*.rs"))
    projection_file = root / PROJECTION.split("::")[0]
    if projection_file.exists():
        files.append(projection_file)
    declarations = {}
    test_files = set()
    globals_ = MODULES["declarations_in"].__globals__
    previous_root = globals_["ROOT"]
    globals_["ROOT"] = root
    try:
        for path in files:
            declared, test_only = MODULES["declarations_in"](path)
            declarations[path.resolve()] = declared
            if test_only:
                test_files.add(path.resolve())
            source = path.read_text()
            masked = RUST["mask_non_code"](source)
            line_offsets = [0]
            for line in source.splitlines(keepends=True):
                line_offsets.append(line_offsets[-1] + len(line))
            opening_for = {end: start for start, end in RUST["delimiter_pairs"](masked).items()}
            for declaration in declared:
                start = line_offsets[declaration.line - 1]
                attributes = [masked[start:line_offsets[declaration.line]]]
                cursor = start
                # Only attributes attached to this declaration apply to it;
                # the previous module's cfg must not hide a live sibling.
                while cursor:
                    end = cursor - 1
                    while end >= 0 and masked[end].isspace():
                        end -= 1
                    if end < 0 or masked[end] != "]":
                        break
                    opening = opening_for.get(end)
                    if opening is None or opening == 0 or masked[opening - 1] != "#":
                        break
                    attributes.append(masked[opening - 1:end + 1])
                    cursor = opening - 1
                attributes = "\n".join(attributes)
                if declaration.target and re.search(r"#\[\s*cfg\s*\(\s*all\s*\(\s*test\b", attributes):
                    test_files.add(declaration.target)
    finally:
        globals_["ROOT"] = previous_root
    included = MODULES["production_subtree"](source_root / "main.rs", declarations, test_files)
    declared_files = {d.target for ds in declarations.values() for d in ds if d.target}
    errors = []
    calls = re.compile(r"(?:\.|::)\s*(" + "|".join(sorted(entries)) + r")\s*\(")
    for path in files:
        relative = path.relative_to(root).as_posix()
        if relative == "crates/striatum-module/src/account_writer.rs" or path.resolve() in test_files:
            continue
        if path.resolve() in declared_files and path.resolve() not in included:
            continue
        source = path.read_text()
        code = PRODUCTION_SOURCE(source)
        pairs = RUST["delimiter_pairs"](code)
        test_ranges = RUST["test_only_ranges"](source, code, pairs)
        tx_scopes = []
        functions = []
        # A typed helper receives only the transaction connection. The same
        # method name on that value is not a call to the live store wrapper.
        for function in FUNCTION.finditer(code):
            header = code[function.start():function.end()]
            names = re.findall(r"\b(\w+)\s*:\s*&\s*(?:[\w:]+::)?AccountTx\b", header)
            end = pairs.get(function.end() - 1, function.end())
            functions.append((function.start(), end, function[1]))
            tx_scopes.extend((function.end(), end, name) for name in names)
        command = re.compile(r"\baccount_writer::(?:execute|repair)\s*\([^|;]*?\bmove\s*\|\s*(\w+)\s*\|\s*\{")
        for closure in command.finditer(code):
            tx_scopes.append((closure.end(), pairs.get(closure.end() - 1, closure.end()), closure[1]))
        for call in calls.finditer(code):
            if RUST["in_ranges"](call.start(), test_ranges):
                continue
            receiver = re.search(r"(\w+)\s*$", code[:call.start()])
            if receiver and receiver[1] == "account_writer" and code[call.start():].startswith("::"):
                continue
            if receiver and any(start <= call.start() < end and receiver[1] == name for start, end, name in tx_scopes):
                continue
            owner = next((name for start, end, name in reversed(functions) if start <= call.start() < end), "<outside-function>")
            if f"{relative}::{owner}" == PROJECTION:
                continue
            line = source.count("\n", 0, call.start()) + 1
            errors.append(f"{relative}:{line}: {call[1]} must be submitted from account_writer.rs")
    return errors


def scan(root: Path, allowed: dict[str, str]) -> tuple[int, list[str]]:
    used = set()
    errors = []
    count = 0
    for path in sorted((root / "crates").glob("*/src/**/*.rs")):
        relative = path.relative_to(root).as_posix()
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
    entries = read_account_entries(ACCOUNT_ENTRIES)
    errors.extend(account_writer_violations(ROOT, entries))
    for error in errors:
        print(error)
    print(f"write-transactions: checked {count} transaction openings; {len(errors)} violations")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
