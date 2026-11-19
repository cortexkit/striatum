"""Prepare a weighted selector sweep and audit the historical quota reconstruction."""

from bisect import bisect_left
from collections import Counter, defaultdict
import itertools
import math
import statistics

from history import DAY, SLOT, Timeline, day, identities, number, obj
from pressure import budget

REL_TOL = 0.01
ABS_TOL = 0.0001
RULES = ("max", "expiry")
CASES = ("none", "capacity")


def consumer_name(consumer):
    if consumer and consumer.startswith("prefrontal-core."):
        return "prefrontal." + consumer.removeprefix("prefrontal-core.")
    return consumer


def label(group):
    provider, tier = group
    return f"{provider}/{tier}" if tier else f"{provider}/(unknown tier)"


class Audit:
    def __init__(self):
        self.days = defaultdict(Counter)
        self.reasons = Counter()
        self.mismatch = Counter()
        self.examples = []
        self.margins = defaultdict(Counter)

    def compare(self, decision, pricing, snapshots, by_group):
        ts = decision["created_at"]
        counts = self.days[day(ts)]
        pressure = pricing.get("planPressure")
        recorded_points = pricing.get("availablePoints")
        margin = pricing.get("keepFreePercent")
        provider = decision["selected_provider_id"]
        if not all(number(i) for i in (pressure, recorded_points, margin)):
            self.reasons["missing numeric plan budget (including unknown-capacity pricing)"] += 1
            return
        if pricing.get("pacing", "max") != "max":
            self.reasons["non-max recorded pacing"] += 1
            return
        if pricing.get("bankedResetsApplied") != 0:
            self.reasons["banked resets applied or not disclosed"] += 1
            return
        group = (provider, pricing.get("planTier"))
        self.margins[group][margin] += 1
        values = [p for p in pricing.get("planValues", []) if p.get("planTier") == group[1]]
        if len(values) > 1:
            self.reasons["multiple recorded plans share the priced tier"] += 1
            return
        accounts = [snapshots[a.key].account for a in by_group[group]
                    if snapshots.get(a.key) and snapshots[a.key].account is not None]
        if not accounts:
            self.reasons["no usable weekly history for recorded provider/tier"] += 1
            return
        counts["checked"] += 1
        estimate = budget(accounts, margin)
        pressure_ok = math.isclose(estimate.max_pressure, pressure, rel_tol=REL_TOL, abs_tol=ABS_TOL)
        points_ok = math.isclose(estimate.points, recorded_points, rel_tol=1e-6, abs_tol=1e-4)
        if pressure_ok:
            counts["matched"] += 1
        if pressure_ok and points_ok:
            counts["budget_matched"] += 1
            return

        if not points_ok:
            reason = "available points differ (identity/window coverage or cached utilization)"
        else:
            recorded_short = pricing.get("shortWindowPressure")
            if number(recorded_short) and math.isclose(recorded_short, pressure, rel_tol=REL_TOL, abs_tol=ABS_TOL):
                reason = "short-window-driven mismatch with matching points (window timing or topology)"
            else:
                reason = "weekly-driven mismatch with matching points (reset timing or account set)"
        self.mismatch[reason] += 1
        if sum(e["diagnosis"] == reason for e in self.examples) < 8:
            self.examples.append({"decision_id": decision["id"], "at_ms": ts, "plan": label(group),
                                  "recorded_pressure": pressure, "reconstructed_pressure": estimate.max_pressure,
                                  "recorded_points": recorded_points, "reconstructed_points": estimate.points,
                                  "account_count": len(accounts), "diagnosis": reason})

    def result(self):
        fields = ("decisions", "with_score", "full_detail", "subscription_winner", "checked", "matched", "budget_matched")
        return {
            "days": {date: {key: counts[key] for key in fields} for date, counts in sorted(self.days.items())},
            "relative_tolerance": REL_TOL, "absolute_tolerance": ABS_TOL,
            "skipped": dict(self.reasons), "diagnoses": dict(self.mismatch), "examples": self.examples,
        }


def replay(routing, history, start, end):
    accounts = identities(routing)
    by_group = defaultdict(list)
    for account in accounts:
        by_group[account.group].append(account)
    clock = Timeline(history, accounts, start, end)
    audit = Audit()
    coverage = defaultdict(Counter)
    requests, cells, slots = [], [], []
    excluded_traffic = Counter()
    decisions = iter(routing.execute("""
        SELECT id, created_at, decision_kind, consumer, requirements_json, selected_provider_id,
               selected_route_key, score_json, detail_stripped_at
          FROM model_route_decisions WHERE created_at >= ? AND created_at < ? ORDER BY created_at, id
    """, (start, end)))
    upcoming = next(decisions, None)
    for at in range(start, end, SLOT):
        clock.advance(at)
        snapshots = clock.snapshots(at)
        for key, snapshot in snapshots.items():
            stat = coverage[key, day(at)]
            stat["slots"] += 1
            stat["usable_entries"] += snapshot is not None
            stat["weekly_entries"] += snapshot is not None and snapshot.account is not None
        traffic = defaultdict(Counter)
        while upcoming is not None and upcoming["created_at"] < at + SLOT:
            decision, upcoming = upcoming, next(decisions, None)
            ts = decision["created_at"]
            counts = audit.days[day(ts)]
            counts["decisions"] += 1
            score = obj(decision["score_json"])
            counts["with_score"] += bool(score)
            counts["full_detail"] += bool(score) and decision["detail_stripped_at"] is None
            pricing = (score.get("objective") or {}).get("subscriptionPricing")
            if isinstance(pricing, dict) and decision["decision_kind"] == "single":
                counts["subscription_winner"] += 1
                clock.advance(ts)
                audit.compare(decision, pricing, clock.snapshots(ts), by_group)
            if decision["decision_kind"] != "single" or not decision["selected_route_key"]:
                excluded_traffic["not a successful single-model decision"] += 1
                continue
            iq = obj(decision["requirements_json"]).get("iq")
            consumer = consumer_name(decision["consumer"])
            if not consumer or not number(iq) or not 0 <= iq <= 100:
                excluded_traffic["no recorded consumer or usable IQ"] += 1
                continue
            traffic[consumer][iq] += 1
        alive = {key: value for key, value in snapshots.items() if value is not None}
        if not alive:
            excluded_traffic["slot lacks all reconstructable quota history"] += sum(map(sum_counts, traffic.values()))
        else:
            add_requests(requests, cells, traffic, alive, at)
        slots.append({"at_ms": at, "covered_accounts": len(alive), "expected_accounts": len(snapshots),
                      "traffic": sum(map(sum_counts, traffic.values())), "groups": slot_groups(by_group, alive)})
    clock.advance(end - 1)
    for slot in slots:
        for row in slot["groups"]:
            margins = audit.margins[row["provider"], row["tier"]]
            add_pressures(row, margins)
    rates = daily_rates(history, routing, accounts, clock.daily, start, end)
    extent = history.execute("SELECT min(observed_at), max(observed_at) FROM quota_reading_log").fetchone()
    return {
        "scope": {"start_ms": start, "end_ms": end, "quota_first_ms": extent[0], "quota_last_ms": extent[1]},
        "audit": audit.result(), "excluded_traffic": dict(excluded_traffic), "slots": slots, "cells": cells,
        "coverage": [{"account": key, "date": date, **stat} for (key, date), stat in sorted(coverage.items())],
        "unjoined_history_accounts": sorted(clock.unjoined), "unrecognized_window_keys": sorted(clock.unsupported_windows),
        "daily_rates": rates,
    }, requests


def sum_counts(counter):
    return sum(counter.values())


def add_requests(requests, cells, traffic, alive, at):
    banked_providers = sorted({s.entry["apiProvider"] for s in alive.values() if s.banked})
    cases = CASES if banked_providers else ("none",)
    for consumer, levels in sorted(traffic.items()):
        levels_sorted = sorted(levels)
        for offset in range(0, len(levels_sorted), 101):
            iqs = levels_sorted[offset:offset + 101]
            ids = {}
            for case, rule in itertools.product(cases, RULES):
                key = f"pacing-{len(requests):06d}"
                ids[f"{case}/{rule}"] = key
                entries = [({**s.entry, "savedResets": {"availableCount": s.banked}}
                            if s.banked and case == "capacity" else s.entry) for s in alive.values()]
                requests.append({"id": key, "method": "route.what_if", "params": {
                    "consumer": consumer, "iqs": iqs, "pacing": rule, "quota_override_at_ms": at,
                    "quota_override": entries,
                    **({"pricing_patch": {"banked_resets": dict.fromkeys(banked_providers, "capacity")}}
                       if banked_providers else {}),
                }})
            cells.extend({"slot_ms": at, "consumer": consumer, "iq": iq, "weight": levels[iq], "requests": ids}
                         for iq in iqs)


def slot_groups(by_group, alive):
    groups = []
    for (provider, tier), identities_for_plan in sorted(by_group.items(), key=lambda item: label(item[0])):
        snapshots = [alive[a.key] for a in identities_for_plan if a.key in alive and alive[a.key].account is not None]
        if not snapshots:
            continue
        cohorts = defaultdict(list)
        for snapshot in snapshots:
            cohorts[snapshot.week].append(snapshot.account)
        groups.append({"provider": provider, "tier": tier, "label": label((provider, tier)),
                       "headroom_share": sum(100 - s.account.weekly_used for s in snapshots) / (100 * len(snapshots)),
                       "cohorts": [{"reset_ms": reset, "accounts": len(accounts),
                                    "points": sum(100 - a.weekly_used for a in accounts)}
                                   for reset, accounts in sorted(cohorts.items())],
                       "_accounts": [s.account for s in snapshots]})
    return groups


def add_pressures(row, margins):
    accounts = row.pop("_accounts")
    margin = margins.most_common(1)[0][0] if margins else 0.0
    row["margin"] = margin
    row["margin_observed"] = bool(margins)
    row["pressures"] = {}
    for case in CASES:
        port = budget(accounts, margin, banked_as_capacity=case == "capacity")
        row["pressures"][case] = {"max": port.max_pressure, "expiry": port.expiry_pressure,
                                  "low_headroom_discount": (row["headroom_share"] < 0.2 and
                                                            port.expiry_pressure < port.max_pressure)}


def daily_rates(history, routing, accounts, readings, start, end):
    # Account admissions live in the history namespace after the store split.
    # A pre-split read-only export may still have them in store.db.
    database = next((db for db in (history, routing) if db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'account_decision'"
    ).fetchone()), None)
    if database is None:
        return {"available": False, "plans": {}, "ambiguous_account_days": 0}
    admissions = defaultdict(list)
    for provider, account, at in database.execute("""
        SELECT provider, account_id, min(created_at) AS at FROM account_decision
         WHERE mode='serve' AND outcome='admit' AND account_id IS NOT NULL
           AND created_at >= ? AND created_at < ?
         GROUP BY provider, account_id, coalesce(admission_id, decision_id) ORDER BY at
    """, (start, end)):
        admissions[f"{provider}/{account}"].append(at)
    account_groups = {a.key: a.group for a in accounts}
    keys = defaultdict(set)
    for account, window, date, _ in readings:
        keys[account, date].add(window)
    daily = defaultdict(lambda: [0.0, 0])
    for (account, window, date, reset), observations in readings.items():
        if len(keys[account, date]) != 1:
            continue  # A single admission count cannot identify independent weekly meters.
        unique = dict((t, used) for t, used in observations if start <= t < end)
        if len(unique) < 2:
            continue
        first, last = min(unique), max(unique)
        times = admissions[account]
        admitted = bisect_left(times, last) - bisect_left(times, first)
        rise = unique[last] - unique[first]
        if admitted and rise >= 0:
            daily[account, date][0] += rise
            daily[account, date][1] += admitted
    by_plan = defaultdict(list)
    for (account, date), (rise, count) in sorted(daily.items()):
        by_plan[label(account_groups[account])].append({"account": account, "date": day(date * DAY),
            "points": rise, "admissions": count, "points_per_admission": rise / count})
    return {"available": True, "ambiguous_account_days": sum(len(windows) > 1 for windows in keys.values()),
            "plans": {plan: {"observations": rows,
                "min": min(r["points_per_admission"] for r in rows),
                "median": statistics.median(r["points_per_admission"] for r in rows),
                "max": max(r["points_per_admission"] for r in rows)} for plan, rows in sorted(by_plan.items())}}
