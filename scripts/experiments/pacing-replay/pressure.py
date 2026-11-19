"""Numeric port of subscription.rs, with a banked-capacity sensitivity case."""

from collections import defaultdict
from dataclasses import dataclass

SHORT_WINDOW_BINDING_SHARE = 0.25


@dataclass(frozen=True)
class ShortWindow:
    minutes: int
    points: float
    time_left: float


@dataclass(frozen=True)
class Account:
    weekly_used: float
    weekly_time_left: float
    short_walled: bool = False
    short_windows: tuple[ShortWindow, ...] = ()
    banked: int = 0


@dataclass(frozen=True)
class Budget:
    points: float
    max_pressure: float
    expiry_pressure: float


def plan_pressure(time_left, headroom_share):
    # crates/striatum-core/src/subscription.rs:152-158
    return min(1_000_000.0, max(0.01,
               min(1.0, max(0.0, time_left)) / max(1e-6, headroom_share) ** 2))


def budget(accounts, margin, banked_as_capacity=False):
    # subscription.rs:181-204. The recorded margin is zero for sessions
    # exempt from the worker reserve. Walled accounts still count
    # toward paid capacity and determine the latest reset time.
    if not accounts:
        return Budget(0.0, 1.0, 1.0)
    live = [a for a in accounts if not a.short_walled and a.weekly_used < 100.0]
    points = sum(max(0.0, 100.0 - a.weekly_used - margin) for a in live)
    # subscription.rs:271-305. The sensitivity case assumes all logged credits
    # are eligible; it adds them to pricing capacity, not spendable headroom.
    banked = sum(a.banked for a in accounts) if banked_as_capacity else 0
    weekly = plan_pressure(max(0.0, *(a.weekly_time_left for a in accounts)),
                           (points + banked * max(0, 100 - margin)) /
                           (100.0 * (len(accounts) + banked)))

    # subscription.rs:229-251. Test the share of the duration pool, not
    # individual accounts; exactly one quarter left is not binding.
    pools = defaultdict(list)
    for account in live:
        for window in account.short_windows:
            pools[window.minutes].append(window)
    old = new = weekly
    for windows in pools.values():
        share = sum(w.points for w in windows) / (100.0 * len(windows))
        short = plan_pressure(max(0.0, *(w.time_left for w in windows)), share)
        old = max(old, short)
        if share < SHORT_WINDOW_BINDING_SHARE:
            new = max(new, short)
    return Budget(points, old, new)
