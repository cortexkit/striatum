"""Render historical coverage, matched pressures, and weighted selector results."""

from analyze import counts
from history import utc


def when(ms):
    return "—" if ms is None else utc(ms)


def fmt(value):
    return f"{value:.4g}" if isinstance(value, float) else "—" if value is None else str(value)


def table(header, rows):
    def line(cells):
        return "| " + " | ".join(fmt(c).replace("|", "\\|").replace("\n", " ") for c in cells) + " |"

    return [line(header), line(["---"] * len(header)), *(line(row) for row in rows), ""]


def header(manifest, title):
    scope = manifest["scope"]
    return [title, "",
            f"Traffic window: {when(scope['start_ms'])} (inclusive) to {when(scope['end_ms'])} (exclusive).",
            f"Retained quota log extent: {when(scope['quota_first_ms'])} to {when(scope['quota_last_ms'])}.", "",
            "The sweep uses today's catalog, task profiles, settings and account identities/fences over historical quota states.",
            "It compares two calls to the real selector, not the recorded winner to a new pick.", ""]


def render_audit(manifest, request_count):
    audit = manifest["audit"]
    matched = sum(row["matched"] for row in audit["days"].values())
    checked = sum(row["checked"] for row in audit["days"].values())
    points = sum(row["budget_matched"] for row in audit["days"].values())
    rate = f"{matched / checked:.1%}" if checked else "n/a"
    lines = header(manifest, "# Expiring quota pacing — preparation audit")
    lines += ["## Winner pressure audit", "",
              f"- Reconstructed max pressure: **{matched}/{checked} matched ({rate})**; {points} also matched available points.",
              f"- Tolerance: relative {audit['relative_tolerance']:g}, absolute {audit['absolute_tolerance']:g} (`math.isclose`).",
              "- Compaction keeps the winner's pricing. It does not keep alternatives' objectives, even in full-detail rows.",
              "- Budget mismatches suggest identity, reading, or usage differences; points matches with pressure mismatches isolate timing or window topology. See `manifest.json` for examples.",
              "- Check these diagnostics before interpreting the sweep. Unmatched histories are not evidence that expiry changes a pick.", ""]
    lines += table(("UTC date", "Decisions", "Score", "Full detail", "Subscription winner", "Audited", "Matched"), (
        (date, row["decisions"], row["with_score"], row["full_detail"], row["subscription_winner"],
         row["checked"], row["matched"]) for date, row in audit["days"].items()
    ))
    if audit["skipped"] or audit["diagnoses"]:
        lines += table(("Audit exclusion or diagnosis", "Count"), (
            (reason, n) for items in (audit["skipped"], audit["diagnoses"]) for reason, n in counts(items)
        ))
    cells = manifest["cells"]
    noun = "cell" if len(cells) == 1 else "cells"
    lines += ["## Sweep inputs", "",
              f"- Prepared **{request_count} requests** for {len(cells)} weighted {noun} ({sum(c['weight'] for c in cells)} decisions).",
              f"- {len(manifest['unjoined_history_accounts'])} history account keys could not be joined; see `manifest.json`.",
              "- Positive banked counts trigger two sensitivity cases: no banked resets and all saved counts as pricing capacity.", ""]
    if manifest["excluded_traffic"]:
        lines += table(("Traffic not emitted", "Decisions"), counts(manifest["excluded_traffic"]))
    lines += ["### Quota coverage by account and UTC day", ""]
    lines += table(("Historical account key", "UTC date", "Expected slots", "Usable entry", "Usable weekly entry"), (
        (r["account"], r["date"], r["slots"], r["usable_entries"], r["weekly_entries"]) for r in manifest["coverage"]
    ))
    return "\n".join(lines) + "\n"


def render_sweep(manifest, result):
    lines = header(manifest, "# Expiring quota pacing — weighted selector sweep")
    lines += [f"Responses: **{result['received_requests']}/{result['expected_requests']}** requests returned; weights are actual traffic, not extrapolated.",
              "Read `audit.md` for the pressure match rate and missing history. Cells with only partial account coverage still have today’s account roster, but lack readings for missing accounts.",
              "A changed pick means a different route/variant or priced subscription plan. Refusals are reported separately, not counted as unchanged picks.", ""]
    sensitivity = result["sensitivity"]
    lines += ["## Banked reset sensitivity", "",
              f"{len(sensitivity)} slots had positive historical saved counts. "
              f"The two cases agree for {sum(r['agree'] for r in sensitivity)} decisions by weight.", "",
              "Rows where treating all logged credits as capacity changes either pick (or missing responses prevent comparison):", ""]
    lines += table(("Slot UTC", "Weight", "Agree", "Differ", "Unresolved"), (
        (when(r["slot_ms"]), r["weight"], r["agree"], r["differ"], r["unresolved"]) for r in sensitivity
        if r["differ"] or r["unresolved"]
    ))
    lines += ["## Weekly wall proxy", "",
              "An account-day rate is the within-reset rise in observed weekly `used_percent` divided by distinct served admissions between readings. The simulation uses the median of those rates for a provider/tier.",
              "Observed headroom is the max-rule baseline proxy. Expiry subtracts the cumulative net shifted load, allocated in proportion to observed headroom across that plan's reset cohorts. Partial weeks, missing rates, coverage gaps, external traffic, and changes in admission or future prices limit the estimate.",
              "Minima are across sampled slot headroom minus shifted load through that slot, not predictions of the minute a wall would occur.", ""]
    rates = manifest["daily_rates"]
    lines += [f"- Admissions source available: {'yes' if rates['available'] else 'no'}; multi-window account-days excluded from rate fitting: {rates['ambiguous_account_days']}.", ""]
    lines += table(("Provider/tier", "Account-days", "Min pp/task", "Median pp/task", "Max pp/task"), (
        (plan, len(r["observations"]), r["min"], r["median"], r["max"]) for plan, r in rates["plans"].items()
    ))
    lines += ["### Pressure falls below 20% weekly headroom", "",
              "All reconstructed plan/slot pressures are in `manifest.json`. The projection uses the most frequently recorded reserve for the tier, or zero when not observed.", ""]
    lines += table(("UTC slot", "Provider/tier", "Banked case", "Headroom share", "Max", "Expiry"), (
        (when(slot["at_ms"]), group["label"], case, group["headroom_share"], r["max"], r["expiry"])
        for slot in manifest["slots"] for group in slot["groups"] for case, r in group["pressures"].items()
        if r["low_headroom_discount"]
    ))
    for case, scenario in result["scenarios"].items():
        lines += [f"## {'No credits counted' if case == 'none' else 'Logged credits counted as capacity'}", "",
                  f"**{scenario['changed_weight']} weighted picks change.**", ""]
        lines += table(("Result coverage", "Decision weight"), counts(scenario["status_weights"]))
        lines += table(("UTC slot date", "Prepared weight", "Paired weight", "Changed weight"), (
            (date, r["prepared"], r["paired"], r["changed"]) for date, r in scenario["by_day"].items()
        ))
        for key, heading, columns in (
            ("by_consumer", "By consumer", ("Consumer", "Count")),
            ("by_level", "By level band", ("Level band", "Count")),
            ("by_route", "By model and plan transition", ("From model", "From plan", "To model", "To plan", "Count")),
        ):
            lines += [f"### {heading}", ""] + table(columns, scenario[key])
        lines += ["### Plan weeks", ""]
        lines += table(("Provider/tier", "Reset UTC", "Slots", "Max min %", "Expiry min %", "Net extra %", "Max first wall", "Expiry first wall"), (
            (r["plan"], when(r["reset_ms"]), r["slots"], r["max_min_headroom_percent"],
             r["expiry_min_headroom_percent"], r["shifted_percent_of_cohort"], when(r["max_first_wall_ms"]),
             when(r["expiry_first_wall_ms"])) for r in scenario["plan_weeks"]
        ))
        if scenario["unallocated_shift_weights"]:
            lines += table(("No rate or matching cohort", "Absolute net shift weight by slot"),
                           counts(scenario["unallocated_shift_weights"]))
    return "\n".join(lines) + "\n"
