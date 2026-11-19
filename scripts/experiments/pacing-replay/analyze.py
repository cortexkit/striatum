"""Join operator responses to prepared cells; no database reads in this stage."""

from collections import Counter, defaultdict
import json

from history import day, number
from replay import CASES, RULES, label


def read_results(path):
    answers = {}
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            response = json.loads(line)
            key = response.get("id") if isinstance(response, dict) else None
            if not isinstance(key, str) or not key or key in answers:
                raise ValueError(f"line {line_number}: missing or duplicate response id")
            body = response.get("result")
            if not isinstance(body, dict) or not isinstance(body.get("results"), list):
                answers[key] = None
                continue
            rows = {}
            for row in body["results"]:
                iq = row.get("iq") if isinstance(row, dict) else None
                if not number(iq) or iq in rows or "pick" not in row:
                    raise ValueError(f"line {line_number}: missing or duplicate result IQ/pick")
                rows[iq] = row["pick"]
            answers[key] = (body.get("consumer"), rows)
    return answers


def picked(value):
    if not isinstance(value, dict) or not isinstance(value.get("route"), str):
        return None
    model = value["route"]
    if value.get("variant") and not model.endswith(f":{value['variant']}"):
        model += f":{value['variant']}"
    pricing = value.get("subscription_plan")
    plan = None
    group = None
    if isinstance(pricing, dict) and pricing.get("provider"):
        group = label((pricing["provider"], pricing.get("tier")))
        plan = pricing.get("plan") or group
    return model, plan, group


def pair(answers, cell, case):
    ids = cell["requests"]
    case = case if f"{case}/max" in ids else "none"
    result = []
    for rule in RULES:
        answer = answers.get(ids[f"{case}/{rule}"])
        if answer is None:
            return "missing response or request error", None
        consumer, rows = answer
        if consumer != cell["consumer"] or cell["iq"] not in rows:
            return "response consumer or IQ did not match request", None
        result.append(picked(rows[cell["iq"]]))
    if any(pick is None for pick in result):
        return "one or both selections refused", tuple(result)
    return "paired", tuple(result)


def band(iq):
    high = max(10, 10 * ((int(iq) + 9) // 10))
    if iq > high:
        high += 10
    return "0–10" if high == 10 else f">{high - 10}–{high}"


def counts(counter):
    return [list(key if isinstance(key, tuple) else (key,)) + [n]
            for key, n in sorted(counter.items(), key=lambda item: (-item[1], str(item[0])))]


def analyze(manifest, answers):
    known = {key for cell in manifest["cells"] for key in cell["requests"].values()}
    if unexpected := answers.keys() - known:
        raise ValueError(f"{len(unexpected)} response IDs do not belong to the manifest; first: {min(unexpected)}")
    sensitivity = defaultdict(Counter)
    scenarios = {}
    for case in CASES:
        statuses, by_consumer, by_level, by_route = (Counter() for _ in range(4))
        by_day = defaultdict(Counter)
        changes_by_slot = defaultdict(Counter)
        for cell in manifest["cells"]:
            weight = cell["weight"]
            status, result = pair(answers, cell, case)
            statuses[status] += weight
            daily = by_day[day(cell["slot_ms"])]
            daily["prepared"] += weight
            if case == "capacity" and "capacity/max" in cell["requests"]:
                baseline_status, baseline_pair = pair(answers, cell, "none")
                stats = sensitivity[cell["slot_ms"]]
                stats["weight"] += weight
                if status == "paired" and baseline_status == "paired":
                    stats["agree" if result == baseline_pair else "differ"] += weight
                else:
                    stats["unresolved"] += weight
            if status != "paired" or result is None:
                continue
            daily["paired"] += weight
            old, new = result
            if old == new:
                continue
            daily["changed"] += weight
            by_consumer[cell["consumer"]] += weight
            by_level[band(cell["iq"])] += weight
            by_route[(old[0], old[1], new[0], new[1])] += weight
            if old[2] != new[2]:
                net = changes_by_slot[cell["slot_ms"]]
                if old[2] is not None:
                    net[old[2]] -= weight
                if new[2] is not None:
                    net[new[2]] += weight
        weeks, unallocated = weekly_proxy(manifest, changes_by_slot)
        scenarios[case] = {"status_weights": dict(statuses), "changed_weight": sum(by_consumer.values()),
                           "by_consumer": counts(by_consumer), "by_level": counts(by_level),
                           "by_day": {date: {key: stat[key] for key in ("prepared", "paired", "changed")}
                                      for date, stat in sorted(by_day.items())},
                           "by_route": counts(by_route), "plan_weeks": weeks, "unallocated_shift_weights": unallocated}
    return {"received_requests": len(answers), "expected_requests": len(known), "scenarios": scenarios,
            "sensitivity": [{"slot_ms": slot, **{key: stat[key] for key in ("weight", "agree", "differ", "unresolved")}}
                            for slot, stat in sorted(sensitivity.items())]}


def weekly_proxy(manifest, changes):
    rates = manifest["daily_rates"]["plans"]
    cumulative = Counter()
    weeks = {}
    unallocated = Counter()
    for slot in manifest["slots"]:
        at = slot["at_ms"]
        net = changes[at]
        groups = {group["label"]: group for group in slot["groups"]}
        for plan, delta in net.items():
            if delta and (plan not in groups or plan not in rates):
                unallocated[plan] += abs(delta)
        for plan, group in groups.items():
            cohorts = group["cohorts"]
            all_points = sum(cohort["points"] for cohort in cohorts)
            all_accounts = sum(cohort["accounts"] for cohort in cohorts)
            for cohort in cohorts:
                reset = cohort["reset_ms"]
                capacity, actual = 100.0 * cohort["accounts"], cohort["points"]
                key = plan, reset
                weight = actual / all_points if all_points else cohort["accounts"] / all_accounts
                rate = rates.get(plan, {}).get("median")
                if rate is not None:
                    cumulative[key] += net[plan] * weight * rate
                old_pct = actual / capacity * 100
                new_pct = None if rate is None else min(100.0, (actual - cumulative[key]) / capacity * 100)
                week = weeks.setdefault(key, {"plan": plan, "reset_ms": reset, "slots": 0,
                    "max_min_headroom_percent": 100.0, "expiry_min_headroom_percent": None,
                    "max_first_wall_ms": None, "expiry_first_wall_ms": None,
                    "shifted_points": None, "shifted_percent_of_cohort": None})
                week["slots"] += 1
                week["max_min_headroom_percent"] = min(week["max_min_headroom_percent"], old_pct)
                if old_pct <= 0 and week["max_first_wall_ms"] is None:
                    week["max_first_wall_ms"] = at
                if new_pct is not None:
                    previous = week["expiry_min_headroom_percent"]
                    week["expiry_min_headroom_percent"] = new_pct if previous is None else min(previous, new_pct)
                    if new_pct <= 0 and week["expiry_first_wall_ms"] is None:
                        week["expiry_first_wall_ms"] = at
                    week["shifted_points"] = cumulative[key]
                    week["shifted_percent_of_cohort"] = cumulative[key] / capacity * 100
    return [week for _, week in sorted(weeks.items())], dict(unallocated)
