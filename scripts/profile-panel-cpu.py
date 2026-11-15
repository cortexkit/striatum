#!/usr/bin/env python3
"""Time panel function scopes in a disposable Linux checkout, then restore sources.

Run only through the remote build runner with --disposable-copy. CPU is process
CPU, so inclusive async scopes also include their blocking workers. Run the one
benchmark alone: concurrent tests would contaminate the attribution. Nested
scopes overlap; their times must not be added. Hot per-candidate functions get
call counters only, not timers. The uninstrumented benchmark is the baseline.
"""

import argparse
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE = "crates/striatum-module/src/"
CORE = "crates/striatum-core/src/"
BENCH = MODULE + "routing_v1/request_cpu_bench.rs"
# path, function, printed name, timed (otherwise count only)
TARGETS = [
    (MODULE + "routing_v1/panels.rs", "seat_panel", "panel.seat_panel", True),
    (MODULE + "routing_v1/panels.rs", "candidates", "panel.candidates", True),
    (MODULE + "routing_v1/select.rs", "read_with_config", "inputs.read", True),
    (MODULE + "routing_v1/select.rs", "prepare_for_sweep", "inputs.prepare", True),
    (MODULE + "routing_v1/select.rs", "prepare_selection", "seat.prepare_selection", True),
    (MODULE + "routing_v1/select.rs", "select_at_tier_attempt", "seat.tier_attempt", True),
    (MODULE + "routing_v1/select.rs", "score_input_evaluated", "seat.score_input", True),
    (MODULE + "routing_v1/select.rs", "live_profile_lookup", "profiles.lookup", True),
    (MODULE + "select.rs", "price_selection_inputs_for_sweep", "price_inputs", True),
    (MODULE + "account_selection.rs", "read_for_sweep", "classes.read", True),
    (MODULE + "account_selection.rs", "class", "classes.class", False),
    (MODULE + "account_selection.rs", "apply_eligibility", "classes.eligibility", True),
    (MODULE + "account_selection.rs", "apply_legacy_route_fences", "classes.fences", True),
    (MODULE + "account_selection.rs", "apply_configured_service_tiers", "classes.tiers", True),
    (MODULE + "account_selection.rs", "reserve_selection_probes", "classes.reserve", True),
    (CORE + "panel.rs", "select_panel_with_availability", "core.panel", True),
    (CORE + "model_router.rs", "admit_routes_with_availability", "core.admit", True),
    (CORE + "model_router.rs", "evaluate_model_route_with_concurrency", "core.evaluate", True),
    (CORE + "model_router.rs", "compute_objective", "core.objective", False),
    (CORE + "launch_policy.rs", "compile", "policy.compile", True),
    (CORE + "provider_model_spec.rs", "compile", "specs.compile", False),
    (MODULE + "select_policy.rs", "prepare_candidates", "policy.prepare_candidates", True),
    (MODULE + "select_policy.rs", "input_with_policy", "policy.input", True),
    (MODULE + "select_concurrency.rs", "capture_with_snapshot", "concurrency.capture", True),
    (MODULE + "routing_v1/select/price_answer.rs", "for_selection", "answer.price", True),
    (MODULE + "account_selection.rs", "probe", "accounts.probe", False),
    (MODULE + "account_selection.rs", "probe_eligible", "accounts.probe_eligible", False),
    (MODULE + "routing_v1/select.rs", "pin", "seat.pin", True),
    (MODULE + "routing_v1/select.rs", "account_check", "seat.account_check", True),
    (MODULE + "config_overrides/new_shape.rs", "selection_exclusions", "policy.selection_exclusions", True),
    (MODULE + "config_overrides/new_shape.rs", "route_refusal_detail_with_pin_scope", "policy.route_refusal", False),
    (MODULE + "routing_v1/select.rs", "score_candidates_evaluated", "seat.score_candidates", True),
]
STAGES = [
    (MODULE + "routing_v1/select.rs", "let mut account_blocks = HashMap::new();",
     "let subscription_keys = candidates.subscription_keys;", "seat.account_preflight"),
]
ROWS = [
    (CORE + "model_router.rs", "core.admit", "facts.len()", "core.admit_rows"),
    (MODULE + "select_policy.rs", "policy.input", "self.facts.len()", "policy.input_rows"),
]

RUST = r'''
#[doc(hidden)]
pub mod __panel_cpu_profile {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    const NAMES: &[&str] = &__NAMES__;
    static CALLS: [AtomicU64; __COUNT__] = [const { AtomicU64::new(0) }; __COUNT__];
    static CPU: [AtomicU64; __COUNT__] = [const { AtomicU64::new(0) }; __COUNT__];
    #[repr(C)]
    struct Timespec { sec: i64, nsec: i64 }
    unsafe extern "C" { fn clock_gettime(clock: i32, value: *mut Timespec) -> i32; }
    fn now() -> u64 {
        let mut t = Timespec { sec: 0, nsec: 0 };
        assert_eq!(unsafe { clock_gettime(2, &mut t) }, 0);
        t.sec as u64 * 1_000_000_000 + t.nsec as u64
    }
    pub fn count(id: usize) { CALLS[id].fetch_add(1, Relaxed); }
    pub fn rows(id: usize, n: usize) { CALLS[id].fetch_add(n as u64, Relaxed); }
    pub struct Guard { id: usize, start: u64 }
    pub fn enter(id: usize) -> Guard { count(id); Guard { id, start: now() } }
    impl Drop for Guard {
        fn drop(&mut self) { CPU[self.id].fetch_add(now() - self.start, Relaxed); }
    }
    pub fn reset() {
        for x in CALLS.iter().chain(CPU.iter()) { x.store(0, Relaxed); }
    }
    pub fn print(label: &str, samples: usize) {
        for (id, name) in NAMES.iter().enumerate() {
            println!("profile {label} {name} calls/op={:.3} inclusive_cpu_us/op={:.3}",
                CALLS[id].load(Relaxed) as f64 / samples as f64,
                CPU[id].load(Relaxed) as f64 / samples as f64 / 1000.0);
        }
    }
}
'''


def profile(root):
    paths = {path for path, _, _, _ in TARGETS + STAGES} | {BENCH, CORE + "lib.rs"}
    originals = {path: (root / path).read_text() for path in paths}
    edited = dict(originals)
    try:
        for index, (path, function, name, timed) in enumerate(TARGETS):
            source = edited[path]
            matches = list(re.finditer(r"\bfn " + function + r"\s*(?:<[^\n]*>)?\s*\(", source))
            if len(matches) != 1:
                raise RuntimeError(f"expected one {path}:{function}, found {len(matches)}")
            brace = source.index("{", matches[0].end()) + 1
            prefix = "crate" if path.startswith(CORE) else "striatum_core"
            expr = f"{prefix}::__panel_cpu_profile::"
            expr += f"enter({index})" if timed else f"count({index})"
            statement = f"let _panel_cpu_guard = {expr};" if timed else f"{expr};"
            edited[path] = source[:brace] + "\n" + statement + source[brace:]
            original_match = re.search(r"\bfn " + function + r"\s*(?:<[^\n]*>)?\s*\(", originals[path])
            if original_match is None:
                raise RuntimeError(f"missing original function: {name}")
            line = originals[path][:original_match.start()].count("\n") + 1
            print(f"instrument {name}: {path}:{line} timed={timed}", flush=True)
        for index, (path, start, end, name) in enumerate(STAGES, len(TARGETS)):
            if edited[path].count(start) != 1 or edited[path].count(end) != 1:
                raise RuntimeError(f"missing unique stage boundaries: {name}")
            edited[path] = edited[path].replace(start, start + f"\nlet panel_stage = striatum_core::__panel_cpu_profile::enter({index});")
            edited[path] = edited[path].replace(end, "drop(panel_stage);\n" + end)
        for index, (path, parent, expression, name) in enumerate(ROWS, len(TARGETS) + len(STAGES)):
            parent_id = next(i for i, row in enumerate(TARGETS) if row[2] == parent)
            prefix = "crate" if path.startswith(CORE) else "striatum_core"
            anchor = f"let _panel_cpu_guard = {prefix}::__panel_cpu_profile::enter({parent_id});"
            if edited[path].count(anchor) != 1:
                raise RuntimeError(f"missing row-counter parent: {name}")
            edited[path] = edited[path].replace(anchor, anchor + f"\n{prefix}::__panel_cpu_profile::rows({index}, {expression});")
        names = [row[2] for row in TARGETS] + [row[3] for row in STAGES + ROWS]
        names = "[" + ",".join('"' + name + '"' for name in names) + "]"
        count = len(TARGETS) + len(STAGES) + len(ROWS)
        edited[CORE + "lib.rs"] += RUST.replace("__NAMES__", names).replace("__COUNT__", str(count))
        for anchor, statement in [
            ("// The remote profiling helper resets its counters after this warmup.",
             "striatum_core::__panel_cpu_profile::reset();"),
            ("// The remote profiling helper prints inclusive CPU per call here.",
             "striatum_core::__panel_cpu_profile::print(label, samples);"),
        ]:
            if edited[BENCH].count(anchor) != 1:
                raise RuntimeError(f"missing unique benchmark hook: {anchor}")
            edited[BENCH] = edited[BENCH].replace(anchor, statement)
        for path, source in edited.items():
            (root / path).write_text(source)
        return subprocess.call([
            "cargo", "test", "--locked", "--release", "-p", "striatum-module",
            "--bin", "ck-striatum", "panel_select_request_cpu", "--", "--ignored", "--nocapture",
        ], cwd=root)
    finally:
        for path, source in originals.items():
            (root / path).write_text(source)
        print("restored all instrumented sources", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disposable-copy", action="store_true", required=True)
    args = parser.parse_args()
    if not args.disposable_copy or sys.platform != "linux":
        parser.error("requires a disposable remote Linux checkout")
    # The build runner mounts tracked sources read-only. Copy into scratch first;
    # neither successful instrumentation nor a failed run can alter that mount.
    scratch = ROOT / "target" / "scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="panel-cpu-", dir=scratch) as directory:
        root = pathlib.Path(directory) / "checkout"
        shutil.copytree(ROOT, root, ignore=shutil.ignore_patterns(
            ".git", "target", ".cortexkit", "node_modules", "__pycache__"))
        return profile(root)


if __name__ == "__main__":
    sys.exit(main())
