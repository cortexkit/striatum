#!/usr/bin/env python3
"""Rename routing consumers without reformatting an operator's JSONC settings."""

import argparse
from collections import Counter
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import sys
from typing import Any


@dataclass
class Node:
    value: Any
    start: int
    end: int
    members: list = field(default_factory=list)
    items: list = field(default_factory=list)


class Jsonc:
    """Keep token offsets so edits can leave whitespace and comments untouched."""

    trivia = re.compile(r"(?:\s+|//[^\r\n]*|/\*[\s\S]*?\*/)*")

    def __init__(self, source):
        self.source = source
        self.pos = 0
        self.tokens = []
        self.root = self.parse_value()
        self.skip()
        if self.pos != len(source):
            raise ValueError(f"unexpected content at offset {self.pos}")

    def skip(self):
        match = self.trivia.match(self.source, self.pos)
        assert match is not None
        self.pos = match.end()

    def punctuation(self, expected):
        self.skip()
        start = self.pos
        if not self.source.startswith(expected, start):
            raise ValueError(f"expected {expected!r} at offset {start}")
        self.pos += len(expected)
        self.tokens.append((start, self.pos))
        return (start, self.pos)

    def peek(self, text):
        self.skip()
        return self.source.startswith(text, self.pos)

    def parse_value(self):
        self.skip()
        start = self.pos
        if self.peek("{"):
            self.punctuation("{")
            node = Node({}, start, 0)
            while not self.peek("}"):
                key = self.parse_value()
                if not isinstance(key.value, str):
                    raise ValueError("object keys must be strings")
                if key.value in node.value:
                    raise ValueError(f"duplicate object key: {key.value}")
                self.punctuation(":")
                value = self.parse_value()
                comma = self.punctuation(",") if self.peek(",") else None
                node.members.append((key, value, comma))
                node.value[key.value] = value.value
                if comma is None:
                    break
            self.punctuation("}")
        elif self.peek("["):
            self.punctuation("[")
            node = Node([], start, 0)
            while not self.peek("]"):
                value = self.parse_value()
                node.items.append(value)
                node.value.append(value.value)
                if not self.peek(","):
                    break
                self.punctuation(",")
            self.punctuation("]")
        else:
            value, self.pos = json.JSONDecoder().raw_decode(self.source, self.pos)
            self.tokens.append((start, self.pos))
            node = Node(value, start, self.pos)
        node.end = self.pos
        return node


RENAMES = {
    "mason": ("prefrontal-core.mason",),
    "sidekick": ("prefrontal-core.sidekick",),
    "athena": ("prefrontal-core.athena",),
    "athena.gather": ("prefrontal-core.athena.gather",),
    "synthesizer": ("prefrontal-core.athena.synthesizer",),
    "lab": ("prefrontal-core.lab",),
    "core": ("prefrontal-core.wake",),
    "codemode": ("basal.codemode",),
    "flow": ("basal.flow",),
    "mc_summary": ("magic-context.summary",),
    "mc_dream": ("magic-context.dream",),
    "mc_dream_classify": ("magic-context.dream.classify-memories",),
    "mc_dream_curate": ("magic-context.dream.curate",),
    "mc_dream_compress_cues": ("magic-context.dream.compress-cues",),
    "mc_dream_review_user": ("magic-context.dream.review-user-memories",),
    "mc_dream_map": ("magic-context.dream.map-memories",),
    "mc_dream_primers": ("magic-context.dream.refresh-primers",),
    "mc_dream_retrospective": ("magic-context.dream.reflect-events",),
    "mc_dream_verify": ("magic-context.dream.verify", "magic-context.dream.verify-broad"),
    "mc_dream_docs": (),
    "lab_planner": (),
}
UNCHANGED = {"head", "classifier", "media.smoke", "runners"}
UNCHANGED.update(name for names in RENAMES.values() for name in names)
RULES = {"only_for", "not_for", "pin_only_for"}


def destinations(name):
    if name in RENAMES:
        return RENAMES[name]
    if name in UNCHANGED:
        return (name,)
    raise ValueError(f"unknown consumer name: {name}")


def positions(path):
    """Only the current settings grammar's consumer keys and rule lists count."""
    if path == ("consumers",):
        return "keys"
    if len(path) == 3 and path[0] in {"models", "providers"} and path[2] in RULES:
        return "list"
    return None


def structural_rename(value, path=()):
    kind = positions(path)
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            names = destinations(key) if kind == "keys" else (key,)
            for name in names:
                if name in result:
                    raise ValueError(f"consumer rename would collide at {name!r}")
                result[name] = structural_rename(child, path + (key,))
        return result
    if isinstance(value, list):
        if kind == "list":
            return [name for item in value for name in destinations(item)]
        return [structural_rename(item, path + (index,)) for index, item in enumerate(value)]
    return value


def convert(source):
    parsed = Jsonc(source)
    counts, unknown = Counter(), set()
    edits = []

    def names(node):
        if not isinstance(node.value, str):
            raise ValueError("consumer names must be strings")
        counts[node.value] += 1
        try:
            return destinations(node.value)
        except ValueError:
            unknown.add(node.value)
            return (node.value,)

    def erase(start, end):
        # Remove syntax tokens, not trivia: comments survive even when the
        # retired consumer entry they accompanied is removed.
        edits.extend((a, b, "") for a, b in parsed.tokens if start <= a and b <= end)

    def walk(node, path=()):
        kind = positions(path)
        if kind == "keys":
            retained = []
            for index, (key, child, comma) in enumerate(node.members):
                targets = names(key)
                if not targets:
                    erase(key.start, child.end)
                else:
                    retained.append(index)
                    if targets != (key.value,):
                        edits.append((key.start, key.end, json.dumps(targets[0])))
                    if len(targets) > 1:
                        # The second verify task needs a copy of the policy,
                        # but not a second copy of the operator's comments.
                        addition = ", " + ", ".join(
                            json.dumps(name) + ": " + json.dumps(child.value, ensure_ascii=False)
                            for name in targets[1:]
                        )
                        edits.append((child.end, child.end, addition))
            for index, (_, _, comma) in enumerate(node.members):
                if comma and (index not in retained or not any(i > index for i in retained)):
                    # A trailing comma on a retained member can stay, but a
                    # removed last member cannot leave two adjacent commas.
                    if index not in retained:
                        edits.append((*comma, ""))
            return
        if kind == "list":
            retained = []
            for index, item in enumerate(node.items):
                targets = names(item)
                if targets:
                    retained.append(index)
                    if targets != (item.value,):
                        edits.append((item.start, item.end, ", ".join(map(json.dumps, targets))))
                else:
                    erase(item.start, item.end)
            # Array commas are token spans, so removing a name preserves nearby
            # comments just like removing a consumer entry does.
            commas = [(a, b) for a, b in parsed.tokens
                      if node.start < a < node.end and source[a:b] == ","]
            for index, comma in enumerate(commas):
                if index not in retained:
                    edits.append((*comma, ""))
            return
        for key, child, _ in node.members:
            walk(child, path + (key.value,))
        for index, child in enumerate(node.items):
            walk(child, path + (index,))

    walk(parsed.root)
    if unknown:
        return None, counts, sorted(unknown)
    expected = structural_rename(parsed.root.value)
    result = source
    for start, end, replacement in sorted(edits, reverse=True):
        result = result[:start] + replacement + result[end:]
    if Jsonc(result).root.value != expected:
        raise ValueError("converted JSONC differs from the structural consumer rename")
    return result, counts, []


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("settings", type=Path)
    parser.add_argument("--write", metavar="NEW_PATH", type=Path,
                        help="write a verified copy to a new file (default: dry run)")
    args = parser.parse_args(argv)
    try:
        # Byte decoding avoids newline translation, including on CRLF files.
        source = args.settings.read_bytes().decode("utf-8")
        converted, counts, unknown = convert(source)
        for name, count in sorted(counts.items()):
            target = ", ".join(RENAMES.get(name, (name,))) or "(dropped)"
            print(f"{name}: {count} -> {target}")
        if unknown:
            print("unknown consumer names: " + ", ".join(unknown), file=sys.stderr)
            return 1
        assert converted is not None
        if args.write:
            if args.write.resolve() == args.settings.resolve():
                raise ValueError("--write must name a new path, never the input file")
            # Exclusive creation also refuses an existing destination or symlink.
            with args.write.open("xb") as output:
                output.write(converted.encode("utf-8"))
            print(f"wrote verified copy: {args.write}")
        else:
            print("dry run; no file written")
        return 0
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
