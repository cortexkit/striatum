#!/usr/bin/env python3
"""Reject production Rust includes that escape the repository's Rust source roots."""

from __future__ import annotations

import ast
import re
import sys
from bisect import bisect_right
from pathlib import Path

MACRO_NAME = re.compile(r"\b(include_str|include_bytes|include)\b")
MACRO_RULES = re.compile(r"\bmacro_rules\s*!\s*([A-Za-z_]\w*)\s*\{")
METAVARIABLE = re.compile(r"\$([A-Za-z_]\w*)\s*:\s*[a-z]+")
INTEGER_LITERAL = re.compile(r"\d[\d_]*")
TEST_ATTRIBUTE = re.compile(r"#\s*\[\s*(?:cfg\s*\(\s*test\s*\)|test)\s*\]")
# A module file whose leading inner attributes include `#![cfg(test)]` is never
# compiled outside tests. Only the file's leading attributes count: the same
# attribute opening an inline `mod { }` block covers that block alone.
FILE_TEST_ATTRIBUTE = re.compile(
    r"\A(?:\s*#\s*!\s*\[[^\]]*\])*?\s*#\s*!\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]"
)
RAW_STRING_START = re.compile(r'(?:br|r)(#+)?"')
GENERATED = object()


def mask_non_code(source: str) -> str:
    """Blank comments and literals while preserving offsets and line breaks."""
    masked = list(source)
    index = 0
    length = len(source)

    def blank(start: int, end: int) -> None:
        for position in range(start, end):
            if source[position] not in "\r\n":
                masked[position] = " "

    while index < length:
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            if end < 0:
                end = length
            blank(index, end)
            index = end
            continue
        if source.startswith("/*", index):
            start = index
            index += 2
            depth = 1
            while index < length and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            blank(start, index)
            continue

        # Rust raw and byte-raw strings may contain arbitrary punctuation.
        raw_prefix = RAW_STRING_START.match(source, index)
        if raw_prefix:
            hashes = raw_prefix.group(1) or ""
            start = index
            index = raw_prefix.end()
            closing = '"' + hashes
            end = source.find(closing, index)
            index = length if end < 0 else end + len(closing)
            blank(start, index)
            continue

        # Normal and byte strings, with escaped quotes and backslashes.
        quote_index = index + 1 if source.startswith('b"', index) else index
        if quote_index < length and source[quote_index] == '"':
            start = index
            index = quote_index + 1
            while index < length:
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            blank(start, min(index, length))
            continue

        # Character literals are skipped without mistaking lifetimes for quotes.
        if source[index] == "'":
            closing = index + 1
            if closing < length and source[closing] == "\\":
                closing += 2
            else:
                closing += 1
            if closing < length and source[closing] == "'":
                blank(index, closing + 1)
                index = closing + 1
                continue

        index += 1

    return "".join(masked)


def delimiter_pairs(masked: str) -> dict[int, int]:
    opening_to_closing = {"(": ")", "[": "]", "{": "}"}
    closing_to_opening = {value: key for key, value in opening_to_closing.items()}
    stack = []
    pairs = {}
    for index, char in enumerate(masked):
        if char in opening_to_closing:
            stack.append((char, index))
        elif char in closing_to_opening and stack and stack[-1][0] == closing_to_opening[char]:
            _, opening = stack.pop()
            pairs[opening] = index
    return pairs


def matching_delimiter(masked: str, opening: int, left: str, right: str) -> int | None:
    depth = 0
    for index in range(opening, len(masked)):
        if masked[index] == left:
            depth += 1
        elif masked[index] == right:
            depth -= 1
            if depth == 0:
                return index
    return None


def test_only_ranges(
    source: str, masked: str, pairs: dict[int, int]
) -> list[tuple[int, int]]:
    ranges = []
    item_delimiters = [index for index, char in enumerate(masked) if char in "{;"]
    for attribute in TEST_ATTRIBUTE.finditer(masked):
        tail = attribute.end()
        delimiter_index = bisect_right(item_delimiters, tail)
        if delimiter_index == len(item_delimiters):
            end = tail
        else:
            delimiter = item_delimiters[delimiter_index]
            if masked[delimiter] == ";":
                end = delimiter + 1
            else:
                closing = pairs.get(delimiter)
                end = closing + 1 if closing is not None else len(source)
        if end > attribute.start():
            ranges.append((attribute.start(), end))

    merged = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def in_ranges(position: int, ranges: list[tuple[int, int]]) -> bool:
    low, high = 0, len(ranges)
    while low < high:
        middle = (low + high) // 2
        if ranges[middle][0] <= position:
            low = middle + 1
        else:
            high = middle
    index = low - 1
    return index >= 0 and position < ranges[index][1]


def rust_string(value: str) -> str | None:
    value = value.strip()
    raw = re.fullmatch(r'r(#+)?"(.*?)"\1', value, re.DOTALL)
    if raw:
        return raw.group(2)
    if value.startswith('"'):
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return None
        return parsed if isinstance(parsed, str) else None
    return None


def split_arguments(expression: str) -> list[str]:
    masked = mask_non_code(expression)
    pieces = []
    start = 0
    depth = 0
    for index, char in enumerate(masked):
        if char in "([{":
            depth += 1
        elif char in ")]}" and depth:
            depth -= 1
        elif char == "," and depth == 0:
            pieces.append(expression[start:index].strip())
            start = index + 1
    if expression[start:].strip():
        pieces.append(expression[start:].strip())
    return pieces


def included_path(expression: str, crate_root: Path) -> str | object | None:
    expression = expression.strip()
    while expression.startswith("(") and expression.endswith(")"):
        inner = expression[1:-1].strip()
        if matching_delimiter(mask_non_code(expression), 0, "(", ")") != len(expression) - 1:
            break
        expression = inner

    literal = rust_string(expression)
    if literal is not None:
        return literal

    macro = re.fullmatch(r"(concat|env)!\s*\((.*)\)", expression, re.DOTALL)
    if not macro:
        return None
    name, arguments = macro.groups()
    parts = split_arguments(arguments)
    if name == "env" and len(parts) == 1:
        variable = rust_string(parts[0])
        if variable == "CARGO_MANIFEST_DIR":
            return str(crate_root)
        if variable == "OUT_DIR":
            # Cargo creates OUT_DIR artifacts during the build; they are not source-tree inputs.
            return GENERATED
        return None
    if name == "concat":
        # concat! also stringifies integer literals, such as a version number
        # passed to a macro_rules! include.
        values = [
            part if INTEGER_LITERAL.fullmatch(part) else included_path(part, crate_root)
            for part in parts
        ]
        if any(value is None for value in values):
            return None
        if any(value is GENERATED for value in values):
            return GENERATED
        return "".join(str(value) for value in values)
    return None


def macro_definitions(
    source: str, masked: str, pairs: dict[int, int]
) -> list[tuple[str, int, int, list[str] | None, bool]]:
    """Each macro_rules! as (name, body start, body end, names, repeated literal).

    Single-arm macros map arguments by position. A single comma-separated
    literal repetition maps each argument separately. Other matcher shapes
    list None, so an include that uses their metavariables fails closed.
    """
    definitions = []
    for match in MACRO_RULES.finditer(masked):
        body_start = match.end() - 1
        body_end = pairs.get(body_start)
        if body_end is None:
            continue
        metavariables = None
        repeated_literal = False
        matcher = body_start + 1
        while matcher < body_end and masked[matcher].isspace():
            matcher += 1
        matcher_end = pairs.get(matcher) if masked[matcher] in "([{" else None
        if matcher_end is not None:
            arrow = re.match(r"\s*=>\s*", masked[matcher_end + 1 : body_end])
            transcriber = matcher_end + 1 + arrow.end() if arrow else None
            transcriber_end = pairs.get(transcriber) if transcriber is not None else None
            matcher_text = source[matcher + 1 : matcher_end]
            if (
                transcriber_end is not None
                and masked[transcriber_end + 1 : body_end].strip() in ("", ";")
            ):
                if "$(" not in matcher_text:
                    metavariables = METAVARIABLE.findall(matcher_text)
                else:
                    repetition = re.fullmatch(
                        r"\s*\$\(\s*\$([A-Za-z_]\w*)\s*:\s*literal\s*\)\s*,\s*\+"
                        r"\s*(?:\$\(\s*,\s*\)\s*\?)?\s*",
                        matcher_text,
                    )
                    if repetition:
                        metavariables = [repetition.group(1)]
                        repeated_literal = True
        definitions.append((match.group(1), body_start, body_end, metavariables, repeated_literal))
    return definitions


def macro_invocations(
    name: str, source: str, masked: str, pairs: dict[int, int], body: tuple[int, int]
) -> list[tuple[int, list[str]]]:
    """(line, arguments) for each invocation of `name` outside its own body."""
    invocations = []
    for match in re.finditer(rf"\b{re.escape(name)}\s*!\s*[(\[{{]", masked):
        if body[0] <= match.start() <= body[1]:
            continue
        opening = match.end() - 1
        closing = pairs.get(opening)
        if closing is None:
            continue
        line = source.count("\n", 0, match.start()) + 1
        invocations.append((line, split_arguments(source[opening + 1 : closing])))
    return invocations


def expand_metavariables(
    expression: str, metavariables: list[str], arguments: list[str]
) -> str:
    values = dict(zip(metavariables, arguments))
    return re.sub(
        r"\$([A-Za-z_]\w*)",
        lambda found: values.get(found.group(1), found.group(0)),
        expression,
    )


def crate_root_for(source_path: Path, crates_root: Path) -> Path | None:
    for parent in (source_path.parent, *source_path.parents):
        if parent == crates_root.parent:
            break
        if (parent / "Cargo.toml").is_file():
            return parent
    return None


def is_test_file(source_path: Path, crates_root: Path) -> bool:
    if "tests" in source_path.relative_to(crates_root).parts:
        return True
    name = source_path.name
    return name in {"tests.rs", "test.rs"} or name.endswith(("_test.rs", "_tests.rs"))


def scan(root: Path) -> tuple[int, list[str]]:
    root = root.resolve()
    crates_root = root / "crates"
    allowed_roots = (crates_root.resolve(), (root / "test-vectors").resolve())
    errors: list[str] = []
    source_count = 0

    for source_path in sorted(crates_root.rglob("*.rs")):
        if not source_path.is_file() or is_test_file(source_path, crates_root):
            continue
        source_count += 1
        source = source_path.read_text(encoding="utf-8")
        masked = mask_non_code(source)
        if FILE_TEST_ATTRIBUTE.match(masked):
            continue
        pairs = delimiter_pairs(masked)
        test_ranges = test_only_ranges(source, masked, pairs)
        definitions = macro_definitions(source, masked, pairs)
        crate_root = crate_root_for(source_path, crates_root)
        if crate_root is None:
            errors.append(f"{source_path.relative_to(root)}: could not find its Cargo.toml")
            continue

        for match in MACRO_NAME.finditer(masked):
            start = match.start()
            cursor = match.end()
            while cursor < len(masked) and masked[cursor].isspace():
                cursor += 1
            if cursor >= len(masked) or masked[cursor] != "!":
                continue
            cursor += 1
            while cursor < len(masked) and masked[cursor].isspace():
                cursor += 1
            if cursor >= len(masked) or masked[cursor] != "(":
                continue
            closing = pairs.get(cursor)
            if closing is None or in_ranges(start, test_ranges):
                continue

            expression = source[cursor + 1 : closing]
            relative_source = source_path.relative_to(root)
            line = source.count("\n", 0, start) + 1
            location = f"{relative_source}:{line}"

            # An include inside a macro_rules! body that uses the macro's
            # metavariables names its file only at each invocation, so every
            # invocation in this file is expanded and checked; a macro with no
            # such invocation, or a shape whose arguments cannot be mapped,
            # fails closed below as an unresolvable path.
            candidates = [(location, expression)]
            enclosing = [
                definition for definition in definitions
                if definition[1] < start < definition[2]
            ]
            if enclosing and "$" in mask_non_code(expression):
                name, body_start, body_end, metavariables, repeated_literal = enclosing[-1]
                invocations = macro_invocations(name, source, masked, pairs, (body_start, body_end))
                if repeated_literal:
                    # Check every literal in the filename list, not just its first
                    # entry; a later filename can escape the allowed source roots.
                    if all(arguments and all(rust_string(arg) is not None for arg in arguments)
                           for _, arguments in invocations):
                        invocations = [(line, [arg]) for line, args in invocations for arg in args]
                    else:
                        invocations = []
                if metavariables is not None and invocations and all(
                    len(arguments) == len(metavariables) for _, arguments in invocations
                ):
                    candidates = [
                        (
                            f"{location} (via {name}! at line {invocation_line})",
                            expand_metavariables(expression, metavariables, arguments),
                        )
                        for invocation_line, arguments in invocations
                    ]

            for where, candidate in candidates:
                path_text = included_path(candidate, crate_root)
                if path_text is GENERATED:
                    continue
                if path_text is None:
                    errors.append(f"{where}: cannot resolve production {match.group(1)}! path")
                    continue
                if not isinstance(path_text, str):
                    continue

                resolved = Path(path_text)
                if not resolved.is_absolute():
                    resolved = source_path.parent / resolved
                resolved = resolved.resolve(strict=False)
                if not any(resolved == allowed or allowed in resolved.parents for allowed in allowed_roots):
                    errors.append(
                        f"{where}: {match.group(1)}! resolves outside crates/ and test-vectors/: {resolved}"
                    )

    return source_count, errors


def violations(root: Path) -> list[str]:
    return scan(root)[1]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    source_count, errors = scan(root)
    if errors:
        for error in errors:
            print(f"rust include boundary: {error}", file=sys.stderr)
        print(f"rust include boundary: {len(errors)} violation(s) in {source_count} production Rust files", file=sys.stderr)
        return 1
    print(f"rust include boundary: ok ({source_count} production Rust files checked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
