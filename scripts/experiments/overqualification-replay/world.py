#!/usr/bin/env python3
"""Write the replay's routing world: today's operator fence and multipliers from
~/.config/cortexkit/striatum.jsonc (under $XDG_CONFIG_HOME when set), the
subscription classification the
recorded decisions show per substrate, and the routes that won at least once
under each recorded operator-policy revision (re-admitted in that revision,
because a route that won was demonstrably not fenced then)."""

import json
import os
import re
import sys
from collections import defaultdict

HOME = os.path.expanduser("~")
CONFIG = os.path.join(os.environ.get("XDG_CONFIG_HOME") or f"{HOME}/.config", "cortexkit/striatum.jsonc")
DECISIONS = sys.argv[1] if len(sys.argv) > 1 else "/tmp/overq/decisions.jsonl"
OUT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/overq/world.json"


def strip_jsonc(text):
    out, i, n, in_str = [], 0, len(text), False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\":
                out.append(text[i + 1])
                i += 2
                continue
            if c == '"':
                in_str = False
        elif c == '"':
            in_str = True
            out.append(c)
        elif text.startswith("//", i):
            while i < n and text[i] != "\n":
                i += 1
            continue
        elif text.startswith("/*", i):
            i = text.index("*/", i) + 2
            continue
        else:
            out.append(c)
        i += 1
    s = "".join(out)
    return re.sub(r",(\s*[}\]])", r"\1", s)


# Striatum's routing file keeps these keys at the top level; the prefrontal
# file this kit was written for nested them under `model_routing`.
document = json.loads(strip_jsonc(open(CONFIG).read()))
cfg = document.get("model_routing", document)
speed = {k: v["speed_multiplier"] for k, v in (cfg.get("models") or {}).items() if "speed_multiplier" in v}

epoch_winners = defaultdict(set)
for line in open(DECISIONS):
    d = json.loads(line)
    if d.get("kind") == "single" and d.get("policy") and d.get("winner"):
        epoch_winners[d["policy"]].add(d["winner"])

world = {
    # Absent from the file when the operator excludes nothing or prices no
    # subscription; see the README for how today's exclusions are spelled.
    "exclude": cfg.get("exclude", []),
    "subscription_multipliers": cfg.get("subscription_multipliers", {}),
    "speed_multipliers": speed,
    # OpenCode auth marks anthropic, openai, google and xai as oauth; the operator
    # list negates xai and re-adds four grok routes and names the plan providers.
    "subscription_providers": [
        "anthropic", "openai", "google", "kimi-for-coding", "kimi-code-plan-global",
        "ollama-cloud", "alibaba-token-plan",
    ],
    "subscription_routes": ["xai/grok-build-0.1", "xai/grok-4.5", "xai/grok-4.6", "xai/grok-4.7"],
    # Pi decisions record anthropic and openai winners as metered.
    "metered_on_pi": ["anthropic", "openai"],
    "epoch_winners": {k: sorted(v) for k, v in epoch_winners.items()},
}
json.dump(world, open(OUT, "w"), indent=1)
print(f"{len(world['exclude'])} exclude entries, {len(epoch_winners)} policy revisions -> {OUT}", file=sys.stderr)
