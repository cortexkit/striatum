# Over-qualification replay (calibration kit)

Offline calibration of the over-qualification term in route selection. It
re-runs recorded routing decisions through `striatum_core`'s own selection,
first with no penalty (the baseline, which should reproduce the recorded
winner) and then once per grid point of a proposed penalty, and reports how
often and where the winner would move.

Ported from the prefrontal repository's
`script/experiments/overqualification-replay/`. Paths now point at the
striatum store, and `replay.rs` builds against `striatum_core` and
`striatum_store` (it needed one new input field, `target_is_head`, set to
`false` because the study replays worker decisions).

## What it reads

Every reader opens the live stores **read-only** (`mode=ro`) and never writes
to them:

- `~/.local/share/cortexkit/striatum/store.db`: `model_route_decisions`
  (`extract.py`; override with `OVERQ_ROUTING_STORE`).
- `~/.local/share/cortexkit/prefrontal-core/store.db`: core's `manager_task`
  and `manager_task_evaluation`, for each mason launch's host list and settle
  outcome (`extract.py`, `analyze.py`, `mason_tokens.py`).
- `~/.local/share/opencode/opencode.db`: OpenCode session token totals
  (`mason_tokens.py`).
- `~/.config/cortexkit/striatum.jsonc` (under `$XDG_CONFIG_HOME` when set):
  today's multipliers (`world.py`).

`replay.rs` is the exception: it opens a store with the routing store library,
which applies migrations, so give it a **copy** (`/tmp/overq/store.db` in
`calibrate.py`), never the live file.

## Full-detail rows only

Routing retention keeps a decision's full scoring detail (every candidate and
every capacity's quota signal) for two days, then compacts it. A deterministic
1-in-20 sample keeps full detail for the whole 30-day decision window: a
decision is in the sample when the FNV-1a 64-bit hash of its id's UTF-8 bytes
is a multiple of 20 (`retention.model_route_decisions_full_detail_sample_one_in`
in the routing file). `extract.py` writes only rows with
`detail_stripped_at IS NULL`, that is, the sample plus the last two days, and
marks each with `full_detail_sample`. Compacted rows still feed the abundance
time series, which uses only the explanation lines every row keeps. The
default window is the last 30 days ending now; pin `OVERQ_WINDOW_END_MS` (and
`OVERQ_WINDOW_DAYS`) to rerun a report over the same rows.

## Running it

```sh
mkdir -p /tmp/overq
python3 extract.py /tmp/overq/decisions.jsonl
python3 world.py /tmp/overq/decisions.jsonl /tmp/overq/world.json
python3 mason_tokens.py /tmp/overq/decisions.jsonl /tmp/overq/mason_tokens.json
# A private copy of the store for the replay (routing may stay running; the
# backup API takes a consistent snapshot):
sqlite3 -readonly ~/.local/share/cortexkit/striatum/store.db ".backup /tmp/overq/store.db"
cp replay.rs ../../../crates/striatum-store/examples/overq_replay.rs
cargo build --release -p striatum-store --example overq_replay
BIN=../../../target/release/examples/overq_replay
python3 calibrate.py "$BIN"          # fits per-revision operator fences into world.json
"$BIN" /tmp/overq/store.db /tmp/overq/decisions.jsonl /tmp/overq/world.json /tmp/overq/replay.jsonl
python3 analyze.py > /tmp/overq/report.md
```

Remove the copied example afterwards; it is not part of the build.
`test_archive_routing.py` tests `extract.py`'s paged reads of core's archived
routing payloads (`python3 -m unittest test_archive_routing` from this
directory).

## Known gaps

- Striatum's routing file states operator exclusions per consumer and provider
  rather than as one top-level `exclude` list. `world.py` does not translate
  them, so the world's `exclude` is empty unless set by hand; `calibrate.py`
  fits each policy revision's fence from the decisions instead.
- Decisions do not record whether the target agent was a head, so the replay
  treats every decision as a worker's.
