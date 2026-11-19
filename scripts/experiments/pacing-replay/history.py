"""Join normalized reading history to today's account identities and window names."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import re
from typing import Optional, TypeGuard

from pressure import Account, ShortWindow

MINUTE = 60_000
DAY = 24 * 60 * MINUTE
SLOT = 2 * 60 * MINUTE
WEEKLY = 7 * 24 * 60
SLOTS = ("primary", "secondary", "tertiary")
POOLS = {"gemini-5h", "gemini-weekly", "3p-5h", "3p-weekly", "gemini-argon-wtu-weekly"}


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def day(ms):
    return utc(ms)[:10]


def number(value: object) -> TypeGuard[float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def obj(value):
    try:
        result = json.loads(value)
        return result if isinstance(result, dict) else {}
    except (TypeError, ValueError):
        return {}


def slug(title):
    return "-".join(filter(None, re.split(r"[\s./_()-]+", title.lower()))) or "window"


@dataclass(frozen=True)
class Identity:
    provider: str
    account: str
    tier: Optional[str]
    template: dict
    since: Optional[int]
    tombstone: Optional[int]

    @property
    def key(self):
        return f"{self.provider}/{self.account}"

    @property
    def group(self):
        return self.provider, self.tier

    def expected(self, at):
        return (self.since is None or self.since <= at) and (self.tombstone is None or at < self.tombstone)


def identities(store):
    row = store.execute("SELECT roster_json FROM account_vault_roster WHERE singleton = 1").fetchone()
    checkpoint = obj(row[0] if row else None)
    by_id = defaultdict(list)
    for item in checkpoint.get("identities", []):
        by_id[item.get("account_id")].append(item)
    by_credential = defaultdict(list)
    for item in (checkpoint.get("listing") or {}).get("credentials", []):
        by_credential[item.get("id")].append(item)
    result = []
    for row in store.execute("SELECT provider, account_id, entry_json, tombstoned_at FROM account_quota_reading"):
        provider, account, raw, tombstone = row
        data = obj(raw)
        if not data.get("apiProvider"):
            continue
        roster = by_id[account][0] if len(by_id[account]) == 1 else {}
        tier = roster.get("plan_tier")
        # Key-only plans have no roster identity. When the quota reading
        # identifies only a credential, use the saved credential listing.
        if tier is None and not data.get("account") and data.get("credentialRef"):
            rows = by_credential[data["credentialRef"]]
            if len(rows) == 1 and (rows[0].get("account_id") or rows[0]["id"]) == account:
                tier = rows[0].get("plan_tier")
        # A tombstoned checkpoint is still useful as a historical identity.
        result.append(Identity(data["apiProvider"], account, tier,
                               {**data, "provider": provider}, roster.get("first_seen_ms"), tombstone))
    return sorted(result, key=lambda a: a.key)


def history_join(accounts, account_key, provider):
    vendor, _, account = account_key.partition("/")
    options = [a for a in accounts if a.provider == vendor and (not account or a.account == account)]
    same_lane = [a for a in options if a.template["provider"] == provider]
    options = same_lane or options
    # With account-less reads, a provider with multiple accounts is not enough
    # evidence to assign the row to any one of them.
    if not account:
        options = [a for a in options if not a.template.get("account")]
    return options[0] if len(options) == 1 else None


class Timeline:
    """Walk observations once as decisions and slot starts advance in time."""

    def __init__(self, history, accounts, start, end):
        # Older observations fail even the last-good freshness limit.
        self.rows = iter(history.execute("""
            SELECT account_key, window_key, observed_at, used_percent, raw_used_percent,
                   resets_at, window_minutes, banked_credit_count, fetched_at,
                   is_stale, stale_since, stale_class
              FROM quota_reading_log WHERE observed_at >= ? AND observed_at < ?
             ORDER BY observed_at, account_key, window_key
        """, (start - 30 * MINUTE, end)))
        self.accounts = accounts
        self.next = next(self.rows, None)
        self.join = {}
        self.latest = defaultdict(dict)
        self.daily = defaultdict(list)
        self.unjoined = set()
        self.unsupported_windows = set()

    def advance(self, at):
        while self.next is not None and self.next["observed_at"] <= at:
            row = self.next
            self.next = next(self.rows, None)
            parts = row["window_key"].split(":", 2)
            if len(parts) != 3 or parts[0] != "codexbar":
                self.unsupported_windows.add(row["window_key"])
                continue
            _, lane, window = parts
            join_key = row["account_key"], lane
            if join_key not in self.join:
                self.join[join_key] = history_join(self.accounts, *join_key)
            account = self.join[join_key]
            if account is None:
                self.unjoined.add(row["account_key"])
                continue
            self.latest[account.key][window] = row
            fetched = row["fetched_at"]
            if weekly_candidate(window, row) and row["resets_at"] and fetched is not None and fresh(row, row["observed_at"]):
                if fetched < row["resets_at"] and number(row["used_percent"]):
                    self.daily[account.key, window, fetched // DAY, row["resets_at"]].append((fetched, row["used_percent"]))

    def snapshots(self, at):
        return {account.key: reconstruct(account, self.latest[account.key], at)
                for account in self.accounts if account.expected(at)}


def fresh(row, at):
    # subscription_economics.rs:354-365 and account_quota.rs:334-356:
    # only the known last-good class can use the longer age bound.
    fetched = row["fetched_at"]
    if fetched is None:
        return False
    age = at - fetched
    if row["is_stale"]:
        return row["stale_class"] == "upstream_failed" and -MINUTE <= age < 30 * MINUTE
    return 0 <= age <= 15 * MINUTE


def weekly_candidate(key, row):
    return key.endswith("-weekly") or (row["window_minutes"] or 0) >= WEEKLY


@dataclass
class Snapshot:
    entry: dict
    account: Optional[Account]
    week: Optional[int]
    banked: int
    weekly_key: Optional[str]


def reconstruct(identity, rows, at):
    if not rows:
        return None
    # Routing reads one producer entry, not a fresh portion of a stale entry.
    # Where history mixes source stamps, retain the most recent coherent set.
    newest = max(rows.values(), key=lambda row: (row["observed_at"], row["fetched_at"] or 0))
    rows = {key: row for key, row in rows.items()
            if (row["fetched_at"], row["is_stale"]) == (newest["fetched_at"], newest["is_stale"])}
    if not fresh(newest, at) or (newest["is_stale"] and any(
        row["resets_at"] is not None and row["resets_at"] <= at for row in rows.values()
    )):
        return None

    template = identity.template
    entry = {key: template[key] for key in ("provider", "apiProvider", "account", "credentialRef", "source")
             if template.get(key) is not None}
    if "account" not in entry and "credentialRef" not in entry:
        entry["account"] = identity.account
    entry["fetchedAt"] = utc(newest["fetched_at"])
    if newest["is_stale"]:
        entry["stale"] = {"class": newest["stale_class"], "since": newest["stale_since"]}
    usage = entry["usage"] = {}
    extra = {item.get("id") or slug(item.get("title") or ""): item
             for item in (template.get("usage") or {}).get("extraRateWindows") or []}
    order = {key: index for index, key in enumerate((*SLOTS, *extra))}
    active = []
    for key, row in sorted(rows.items(), key=lambda item: (order.get(item[0], len(order)), item[0])):
        if key not in SLOTS and key not in extra and key not in POOLS:
            continue  # The normalized log cannot recover the original scope title.
        if not number(row["used_percent"]):
            continue
        w = {"usedPercent": row["used_percent"]}
        reset, mins = row["resets_at"], row["window_minutes"]
        if number(row["raw_used_percent"]):
            w["rawUsedPercent"] = row["raw_used_percent"]
        if reset is not None:
            w["resetsAt"] = utc(reset)
        if mins is not None:
            w["windowMinutes"] = mins
        if weekly_candidate(key, row):
            w["windowKind"] = "weekly"
        if key in SLOTS:
            usage[key] = w
        else:
            scope = {"id": key, "window": w}
            if extra.get(key, {}).get("title"):
                scope["title"] = extra[key]["title"]
            usage.setdefault("extraRateWindows", []).append(scope)
        active.append((key, row, reset, mins))
    if not active:
        return None
    weekly = next((window for window in active if weekly_candidate(window[0], window[1])), None)
    banked = max(0, newest["banked_credit_count"] or 0)
    if weekly is None or weekly[2] is None or weekly[2] <= at or (weekly[3] or 0) <= 0:
        return Snapshot(entry, None, None, banked, None)

    def used(row):
        return min(100.0, max(0.0, row["raw_used_percent"] if number(row["raw_used_percent"]) else row["used_percent"]))

    _, reading, reset, mins = weekly
    short = tuple(ShortWindow(m, 100 - used(r), min(1.0, (end - at) / (m * MINUTE)))
                  for key, r, end, m in active if key != weekly[0] and m and m > 0 and end is not None and end > at)
    account = Account(used(reading), min(1.0, (reset - at) / (mins * MINUTE)),
                      any(w.points == 0 for w in short), short, banked)
    return Snapshot(entry, account, reset, banked, weekly[0])
