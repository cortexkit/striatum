#!/usr/bin/env python3
"""Review an offline max-vs-expiry replay; never edit the source stores."""

import argparse
import json
import os
import time
from contextlib import closing
from pathlib import Path
import sqlite3

from analyze import analyze, read_results
from replay import replay
from report import render_audit, render_sweep

DAY_MS = 86_400_000
DEFAULT_ROOT = Path.home() / ".local/share/cortexkit/striatum"


def read_only(path):
    # as_uri escapes literal '?' and '#' in filenames before adding mode=ro.
    connection = sqlite3.connect(Path(path).expanduser().absolute().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--out", required=True, type=Path, help="report directory")
    parser.add_argument("--results", type=Path, help="analyze client response JSONL instead of reading the stores")
    args = parser.parse_args(argv)
    if args.results is not None:
        manifest = json.loads((args.out / "manifest.json").read_text(encoding="utf-8"))
        result = analyze(manifest, read_results(args.results))
        text = render_sweep(manifest, result)
        write_outputs(args.out, {"sweep.md": text, "sweep.json": json_text(result)})
        print(text, end="")
        return
    if args.days <= 0:
        parser.error("--days must be positive")
    try:
        end = int(os.environ.get("PACING_WINDOW_END_MS", time.time_ns() // 1_000_000))
    except ValueError:
        parser.error("PACING_WINDOW_END_MS must be an integer Unix millisecond timestamp")
    store = os.environ.get("PACING_ROUTING_STORE", DEFAULT_ROOT / "store.db")
    history = os.environ.get("PACING_HISTORY_STORE", DEFAULT_ROOT / "history.db")
    with closing(read_only(store)) as routing, closing(read_only(history)) as readings:
        # The stores are separate snapshots. Pin the time bound and keep each
        # store's own queries on a consistent SQLite snapshot.
        routing.execute("BEGIN")
        readings.execute("BEGIN")
        manifest, requests = replay(routing, readings, end - args.days * DAY_MS, end)

    text = render_audit(manifest, len(requests))
    write_outputs(args.out, {"audit.md": text, "manifest.json": json_text(manifest),
                             "requests.jsonl": "".join(json.dumps(row, allow_nan=False) + "\n" for row in requests)})
    print(text, end="")


def json_text(value):
    return json.dumps(value, indent=2, allow_nan=False) + "\n"


def write_outputs(path, outputs):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name in outputs:
        out = path / name
        if out.exists() or out.is_symlink():
            raise FileExistsError(f"refusing to overwrite {out}")
    for name, content in outputs.items():
        fd = os.open(path / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(content)


if __name__ == "__main__":
    main()
