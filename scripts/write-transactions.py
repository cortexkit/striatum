#!/usr/bin/env python3
"""Keep production SQLite transaction construction behind the store policy."""

import re
import runpy
from bisect import bisect_left
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULES = runpy.run_path(str(ROOT / "scripts/unreachable-modules.py"))
PRODUCTION_SOURCE = MODULES["production_source"]
RUST = runpy.run_path(str(ROOT / "scripts/rust-include-boundary.py"))
HELPER = "crates/striatum-store/src/transactions.rs"
ALLOWLIST = ROOT / "scripts/read-only-transactions.allow"
ACCOUNT_ENTRIES = ROOT / "scripts/account-writer-entry-points.txt"
FROZEN_NAMES = ROOT / "scripts/frozen-block-names.txt"
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
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        if not re.fullmatch(r"\w+", line) or line in entries:
            raise ValueError(f"invalid account writer entry: {line}")
        else:
            entries.add(line)
    return entries


def account_writer_violations(root: Path, entries: set[str]) -> list[str]:
    """Distinguish store entry points from methods on a command's AccountTx."""
    source_root = root / "crates/striatum-module/src"
    files = sorted(source_root.rglob("*.rs"))
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
        # A typed helper receives only the transaction connection, either as
        # the AccountTx itself or as the module's AccountCommand over it. The
        # same method name on that value is not a call to the live store
        # wrapper.
        for function in FUNCTION.finditer(code):
            header = code[function.start():function.end()]
            names = re.findall(r"\b(\w+)\s*:\s*&\s*(?:[\w:]+::)?(?:AccountTx|AccountCommand)\b", header)
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
            line = source.count("\n", 0, call.start()) + 1
            errors.append(f"{relative}:{line}: {call[1]} must be submitted from account_writer.rs")
    return errors


def read_frozen_names(path: Path) -> tuple[set[str], dict[str, set[str]]]:
    relations: set[str] = set()
    columns: dict[str, set[str]] = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        if len(fields) == 2 and fields[0] in {"table", "view", "trigger", "removed_trigger"}:
            relations.add(fields[1])
        elif len(fields) == 3 and fields[0] == "column" and fields[2] in {"null", "0"}:
            table, dot, column = fields[1].partition(".")
            if not dot or not table or not column:
                raise ValueError(f"{path}:{number}: invalid frozen column")
            columns.setdefault(table, set()).add(column)
        else:
            raise ValueError(f"{path}:{number}: invalid frozen name")
    return relations, columns


def rust_literals(source: str) -> list[tuple[int, str]]:
    """Read actual string tokens, not string-shaped text in comments or chars."""
    literals = []
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index)
            index = len(source) if end < 0 else end + 1
            continue
        if source.startswith("/*", index):
            index += 2
            depth = 1
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        start = index
        raw = MODULES["RAW_STRING_PREFIX"].match(source, index)
        if raw and (index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_")):
            closing = '"' + raw.group("hashes")
            end = source.find(closing, raw.end())
            if end < 0:
                break
            literals.append((start, source[raw.end():end]))
            index = end + len(closing)
            continue
        quote = index + 1 if source.startswith('b"', index) else index
        if source[quote:quote + 1] == '"':
            index = quote + 1
            while index < len(source):
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    break
                else:
                    index += 1
            text = source[quote + 1:index]
            escapes = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", '"': '"', "'": "'", "\\": "\\"}
            def unescape(match: re.Match[str]) -> str:
                value = match[0][1:]
                if value.startswith(("\n", "\r")):
                    return ""
                if value.startswith("u{"):
                    return chr(int(value[2:-1].replace("_", ""), 16))
                if value.startswith("x"):
                    return chr(int(value[1:], 16))
                return escapes[value]
            text = re.sub(r"\\(?:\r?\n\s*|u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|[nrt0\"'\\])", unescape, text)
            literals.append((start, text))
            index += 1
            continue
        if source[index] == "'":
            end = MODULES["_char_literal_end"](source, index)
            if end is not None:
                index = end
                continue
        index += 1
    return literals


SQL_TOKEN = re.compile(
    r"(?P<comment>--[^\n]*(?:\n|$)|/\*[\s\S]*?\*/)"
    r"|(?P<string>'(?:''|[^'])*')"
    r'|(?P<quoted>"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\])'
    r"|(?P<word>[A-Za-z_][A-Za-z_0-9]*)|(?P<symbol>[^\s])"
)
SQL_CLAUSES = {"where", "group", "order", "limit", "having", "union", "except", "intersect", "returning", "set", "values"}
SQL_KEYWORDS = SQL_CLAUSES | {"as", "on", "join", "left", "right", "inner", "outer", "cross", "full", "natural", "from", "into", "select", "update", "delete", "insert", "replace", "indexed", "not", "using", "do", "conflict", "begin", "end"}


def frozen_sql_names(sql: str, rules: tuple[set[str], dict[str, set[str]]]) -> list[str]:
    """Associate frozen columns with their relation, not a joined block alias."""
    tokens = []
    for match in SQL_TOKEN.finditer(sql):
        kind = match.lastgroup
        value = match[0]
        if kind == "comment":
            continue
        if kind in {"quoted", "string"}:
            quote = value[0]
            value = value[1:-1].replace(quote * 2, quote)
        tokens.append((kind, value.lower()))
    if not tokens or tokens[0][1] not in {"select", "with", "insert", "replace", "update", "delete", "create", "drop", "alter", "from", "join", "pragma"}:
        return []
    frozen, columns = rules
    aliases: dict[str, set[str]] = {}
    relations = set()
    relation_tokens = set()

    def relation_at(index: int) -> None:
        if [value for _, value in tokens[index:index + 3]] == ["if", "not", "exists"]:
            index += 3
        if index >= len(tokens) or tokens[index][0] not in {"word", "quoted", "string"} or tokens[index][1] in SQL_KEYWORDS:
            return
        relation_tokens.add(index)
        table = tokens[index][1]
        if index + 2 < len(tokens) and tokens[index + 1][1] == ".":
            index += 2
            relation_tokens.add(index)
            table = tokens[index][1]
        relations.add(table)
        aliases.setdefault(table, set()).add(table)
        index += 1
        if index < len(tokens) and tokens[index][1] == "as":
            index += 1
        if index < len(tokens) and tokens[index][0] in {"word", "quoted", "string"} and tokens[index][1] not in SQL_KEYWORDS:
            aliases.setdefault(tokens[index][1], set()).add(table)
            relation_tokens.add(index)

    depth = 0
    from_depths = set()
    for index, (kind, value) in enumerate(tokens):
        if value == "(":
            depth += 1
        elif value == ")":
            from_depths.discard(depth)
            depth -= 1
        elif kind != "word" and value != ",":
            continue
        elif value in SQL_CLAUSES:
            from_depths.discard(depth)
        elif value == "from":
            from_depths.add(depth)
            relation_at(index + 1)
        elif value in {"join", "update", "into", "table", "trigger", "references"} or (value == "," and depth in from_depths):
            relation_at(index + 1)
    violations = {f"frozen relation {name}" for name in relations & frozen}
    for index, (kind, value) in enumerate(tokens):
        if value == "*" and index and tokens[index - 1][1] in {"select", "distinct", "all", ",", "."}:
            owners = aliases.get(tokens[index - 2][1], set()) if tokens[index - 1][1] == "." and index >= 2 else relations
            violations.update(f"frozen wildcard {table}.*" for table in owners if table in columns)
        if kind not in {"word", "quoted"} or index in relation_tokens:
            continue
        if index >= 2 and tokens[index - 1][1] == ".":
            owners = aliases.get(tokens[index - 2][1], set())
        else:
            owners = relations
        for table in owners:
            if value in columns.get(table, set()):
                violations.add(f"frozen column {table}.{value}")
    return sorted(violations)


def frozen_sql_sites(source: str, rules: tuple[set[str], dict[str, set[str]]]) -> tuple[int, list[tuple[int, str]]]:
    code = MODULES["structural_source"](source)
    production = PRODUCTION_SOURCE(source)
    if not production:
        return 0, []
    test_ranges = RUST["test_only_ranges"](source, code, RUST["delimiter_pairs"](code))
    anchors = [index for index, char in enumerate(code) if not char.isspace()]
    errors = []
    count = 0
    for position, sql in rust_literals(source):
        anchor_index = bisect_left(anchors, position) - 1
        # A literal's preceding assignment/call/body/comma token belongs to
        # the same attributed item. The shared production mask removes that
        # token along with a cfg(test/test-support) item's entire body.
        if anchor_index < 0 or production[anchors[anchor_index]].isspace() or RUST["in_ranges"](position, test_ranges):
            continue
        violations = frozen_sql_names(sql, rules)
        count += 1
        errors.extend((source.count("\n", 0, position) + 1, error) for error in violations)
    # A SQL statement split across literal concat!/format! arguments is still
    # one statement. Resolve literal arguments before associating its columns.
    for macro in re.finditer(r"\b(concat|format)\s*!\s*\(", code):
        if production[macro.start()].isspace() or RUST["in_ranges"](macro.start(), test_ranges):
            continue
        closing = RUST["matching_delimiter"](code, macro.end() - 1, "(", ")")
        if closing is None:
            continue
        arguments = RUST["split_arguments"](source[macro.end():closing])
        decoded = []
        for argument in arguments:
            literals = rust_literals(argument)
            if MODULES["structural_source"](argument).strip() or len(literals) != 1:
                break
            decoded.append(literals[0][1])
        else:
            try:
                sql = "".join(decoded) if macro[1] == "concat" else decoded[0].format(*decoded[1:])
            except (IndexError, KeyError, ValueError):
                continue
            errors.extend((source.count("\n", 0, macro.start()) + 1, error) for error in frozen_sql_names(sql, rules))
    return count, sorted(set(errors))


def frozen_sql_violations(root: Path, rules: tuple[set[str], dict[str, set[str]]]) -> tuple[int, list[str]]:
    files = sorted((root / "crates").glob("*/src/**/*.rs"))
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
    finally:
        globals_["ROOT"] = previous_root
    roots = [path for path in files if path.name in {"main.rs", "lib.rs"} or path.parent.name == "bin"]
    production = set().union(*(MODULES["production_subtree"](path, declarations, test_files) for path in roots))
    queue = [declaration.target for declared in declarations.values() for declaration in declared if not declaration.production_eligible and declaration.target is not None]
    while queue:
        path = queue.pop().resolve()
        if path in test_files:
            continue
        test_files.add(path)
        queue.extend(declaration.target for declaration in declarations.get(path, []) if declaration.target is not None)
    count, errors = 0, []
    for path in files:
        if path.resolve() in test_files - production:
            continue
        found, sites = frozen_sql_sites(path.read_text(), rules)
        count += found
        errors.extend(f"{path.relative_to(root).as_posix()}:{line}: {error}" for line, error in sites)
    return count, errors


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
    literals, frozen_errors = frozen_sql_violations(ROOT, read_frozen_names(FROZEN_NAMES))
    errors.extend(frozen_errors)
    for error in errors:
        print(error)
    print(f"write-transactions: checked {count} transaction openings and {literals} production string literals; {len(errors)} violations")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
