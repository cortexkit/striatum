//! Account quota status for cache warming.
//! This endpoint answers from quota headroom without reserving or admitting work.

use super::*;

// ---------------------------------------------------------------------------
// route.account_status
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AccountStatusParams {
    pub(super) provider: String,
    pub(super) account_id: String,
}

/// A closed vocabulary; only reserve refusals carry a window identity.
#[derive(Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
enum WarmReason {
    Headroom,
    WithinReserve { window: String },
    ReadingStale,
    ReadingMissing,
    AccountUnknown,
    AccountSelectionDisabled,
}

#[derive(Serialize)]
struct StatusWindow {
    pub(super) kind: String,
    pub(super) used_percent: f64,
    pub(super) resets_at: Option<String>,
    pub(super) window_minutes: Option<i64>,
    #[serde(skip)]
    reserve_window: ApplicableWindow,
}

fn status_window(
    window: &RateWindow,
    reported_kind: Option<&str>,
    reserve_window: Option<ApplicableWindow>,
) -> StatusWindow {
    let general = if window.window_minutes
        == Some(i64::from(GeneralWindow::FiveHour.window_minutes()))
    {
        Some(("five_hour", ApplicableWindow::FiveHour))
    } else if window.window_minutes == Some(i64::from(GeneralWindow::SevenDay.window_minutes())) {
        Some(("weekly", ApplicableWindow::SevenDay))
    } else {
        None
    };
    StatusWindow {
        kind: window
            .window_kind
            .as_deref()
            .or(reported_kind)
            .or(general.map(|(kind, _)| kind))
            .unwrap_or("reported")
            .to_string(),
        used_percent: window.used_percent,
        resets_at: window.resets_at.clone(),
        window_minutes: window.window_minutes,
        reserve_window: reserve_window
            .unwrap_or_else(|| general.map_or(ApplicableWindow::SevenDay, |(_, window)| window)),
    }
}

pub(crate) fn account_status(params: &Value, ctx: &Ctx<'_>) -> OpReply {
    let parsed: AccountStatusParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_status params: {error}")),
    };
    if parsed.provider.trim().is_empty() || parsed.account_id.trim().is_empty() {
        return invalid("`provider` and `account_id` must not be empty");
    }
    let provider = normalize(&parsed.provider);
    let mut observed_at = None;
    let mut band = None;
    let mut windows = Vec::<StatusWindow>::new();
    let reason = (|| {
        if !ctx.flags.enabled {
            return WarmReason::AccountSelectionDisabled;
        }
        let Some(Ok(state)) = ctx.accounts.map(|accounts| accounts.lock()) else {
            return WarmReason::ReadingMissing;
        };
        // Like account placement, identity comes from the vault, not an
        // Insula-only reading or an email alias.
        let built = allocator_roster_for_model(&state, &provider, None, ctx.now_ms, &[]);
        let known = built.roster.as_ref().is_some_and(|roster| {
            roster
                .accounts
                .iter()
                .any(|account| account.account_id == parsed.account_id)
        });
        if !known {
            return WarmReason::AccountUnknown;
        }
        // Readings are keyed by producer (for example `codex`), while the
        // requested routing provider is the entry's apiProvider (`openai`).
        let Some(stored) = state
            .readings
            .readings
            .values()
            .filter(|reading| {
                reading.account_id == parsed.account_id
                    && reading.routing_provider() == Some(provider.as_str())
            })
            .filter(|reading| reading.tombstoned_at_ms.is_none())
            .max_by_key(|reading| {
                (
                    reading
                        .entry
                        .get("fetchedAt")
                        .and_then(Value::as_str)
                        .and_then(account_quota::parse_rfc3339_ms),
                    &reading.provider,
                )
            })
        else {
            return WarmReason::ReadingMissing;
        };
        let Ok(entry) = serde_json::from_value::<AccountEntry>(stored.entry.clone()) else {
            return WarmReason::ReadingMissing;
        };
        observed_at = entry.fetched_at.clone();
        let Some(reading) = quota_reading_for_model(&stored.entry, &stored.account_id, None, &[])
        else {
            return WarmReason::ReadingMissing;
        };
        let freshness = assess_reading(Some(&reading), ctx.now_ms, ctx.policy);
        band = freshness.age_band;
        if let Some(usage) = &entry.usage {
            // Use the allocator's topology: pooled readings ignore misleading
            // aggregate slots and infer pool durations from their producer ids.
            let reported = striatum_core::quota_extract::account_rate_windows(usage, None, &[]);
            for (_, window) in reported.iter().filter(|(slot, _)| *slot != "extra") {
                windows.push(status_window(window, None, None));
            }
            let mut pools = reported
                .iter()
                .filter(|(slot, _)| *slot == "extra")
                .map(|(_, window)| window);
            for extra in usage.extra_rate_windows.iter().flatten() {
                let Some(window) = &extra.window else {
                    return WarmReason::ReadingMissing;
                };
                let pool = striatum_core::quota_extract::is_account_pool_window(extra);
                let window = if pool {
                    pools
                        .next()
                        .expect("reported pool windows retain producer order")
                } else {
                    window
                };
                let reserve_window = if pool {
                    None
                } else if ctx.policy.scope_ids.iter().any(|scope| {
                    extra.id.as_deref() == Some(scope.scope_id.as_str())
                        && window.window_minutes == Some(i64::from(scope.window_minutes))
                }) {
                    Some(ApplicableWindow::FamilyScope)
                } else {
                    // A producer-reported pool outside routing's scope mapping
                    // gets the same conservative fallback as a general unknown.
                    Some(ApplicableWindow::SevenDay)
                };
                windows.push(status_window(
                    window,
                    extra.window_kind.as_deref().or(extra.id.as_deref()),
                    reserve_window,
                ));
            }
        }
        // The allocator marks malformed general topology as degraded. For this
        // read, it is a missing/invalid window rather than a stale observation.
        if entry.error_class.is_none()
            && entry.error.is_none()
            && reading.error_class.as_deref() == Some("invalid_general_window")
        {
            return WarmReason::ReadingMissing;
        }
        if !freshness.unknown.is_empty() {
            return WarmReason::ReadingStale;
        }
        if entry.stale.is_some()
            && windows.iter().any(|window| {
                window
                    .resets_at
                    .as_deref()
                    .and_then(account_quota::parse_rfc3339_ms)
                    .is_some_and(|reset| reset <= ctx.now_ms)
            })
        {
            return WarmReason::ReadingStale;
        }
        // Missing or malformed windows cannot be treated as unused quota.
        if windows.is_empty()
            || reading.required_windows().is_empty()
            || windows.iter().any(|window| {
                !window.used_percent.is_finite() || !(0.0..=100.0).contains(&window.used_percent)
            })
        {
            return WarmReason::ReadingMissing;
        }
        for window in &windows {
            if 100.0 - window.used_percent <= reserve_for(window.reserve_window, ctx.policy) {
                return WarmReason::WithinReserve {
                    window: window.kind.clone(),
                };
            }
        }
        WarmReason::Headroom
    })();
    OpReply::Result(json!({
        "provider": provider,
        "account_id": parsed.account_id,
        "observed_at": observed_at,
        "age_band": band,
        "windows": windows,
        "warm_ok": matches!(reason, WarmReason::Headroom),
        "reason": reason,
    }))
}
