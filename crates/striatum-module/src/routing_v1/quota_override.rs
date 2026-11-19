use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use striatum_core::{
    model_router::SelectedModelRoute, quota::ProviderUsage, subscription::SubscriptionPool,
};

use crate::account_quota::{self, AccountEntry, AccountQuotaState, SharedAccountQuota};

pub(crate) struct QuotaOverride {
    pub(crate) accounts: SharedAccountQuota,
    pub(super) windows: Vec<ProviderUsage>,
    pub(super) pools: HashMap<String, SubscriptionPool>,
    pub(super) at: i64,
    pub(super) wall_clock: i64,
}

impl QuotaOverride {
    pub(super) fn new(entries: Vec<Value>, at: i64, wall_clock: i64) -> Result<Self, String> {
        let mut state = crate::ACCOUNT_QUOTA
            .get()
            .and_then(|quota| quota.lock().ok().map(|quota| quota.clone()))
            .unwrap_or_default();
        // Keep today's accounts and their account-to-credential mappings, but
        // never keep a live quota reading for an account the override omits.
        state.readings = Default::default();
        for (index, raw) in entries.iter().enumerate() {
            let entry: AccountEntry = serde_json::from_value(raw.clone())
                .map_err(|error| format!("entry {index}: {error}"))?;
            if ![entry.account, entry.credential_ref]
                .into_iter()
                .flatten()
                .any(|value| !value.is_empty())
            {
                return Err(format!(
                    "entry {index} must name an account or credentialRef"
                ));
            }
        }
        let mut eligibility = state.clone();
        apply(&mut state, &entries, at)?;
        let pools = crate::subscription_economics::preview_pools(&state, at, wall_clock);

        // The allocator combines reading clocks with today's holds, leases and
        // fences. Translate only quota timestamps to keep equivalent reading
        // ages and time-to-reset without moving those operational clocks.
        let entries = if at == wall_clock {
            entries
        } else {
            entries
                .into_iter()
                .map(|mut entry| {
                    translate(&mut entry, wall_clock - at);
                    entry
                })
                .collect()
        };
        apply(&mut eligibility, &entries, wall_clock)?;
        let windows = entries
            .into_iter()
            .filter_map(|entry| serde_json::from_value(entry).ok())
            .collect();
        Ok(Self {
            accounts: Arc::new(Mutex::new(eligibility)),
            windows,
            pools,
            at,
            wall_clock,
        })
    }
}

pub(super) fn describe_plan(selected: &SelectedModelRoute, result: &mut Value) {
    let Some(pricing) = &selected.score.objective.subscription_pricing else {
        return;
    };
    let mut same_tier = pricing
        .plan_values
        .iter()
        .filter(|p| p.plan_tier == pricing.plan_tier);
    let plan = same_tier.next().and_then(|p| {
        same_tier
            .all(|other| other.plan == p.plan)
            .then_some(p.plan.as_deref())
            .flatten()
    });
    result["subscription_plan"] = json!({
        "provider": selected.model.provider_id,
        "plan": plan,
        "tier": pricing.plan_tier,
    });
}

fn apply(state: &mut AccountQuotaState, entries: &[Value], now: i64) -> Result<(), String> {
    let payload = serde_json::to_vec(&json!({"result":entries})).map_err(|e| e.to_string())?;
    account_quota::apply_usage_reply(state, &payload, now).map(|_| ())
}

fn translate(entry: &mut Value, delta: i64) {
    fn field(object: &mut Value, name: &str, delta: i64) {
        if let Some(value) = object.get_mut(name) {
            if let Some(at) = value.as_str().and_then(account_quota::parse_rfc3339_ms) {
                let shifted = at.saturating_add(delta);
                let whole = crate::account_selection::rfc3339(shifted);
                *value = json!(format!(
                    "{}.{:03}Z",
                    whole.trim_end_matches('Z'),
                    shifted.rem_euclid(1000)
                ));
            }
        }
    }
    field(entry, "fetchedAt", delta);
    if let Some(stale) = entry.get_mut("stale") {
        field(stale, "since", delta);
    }
    if let Some(usage) = entry.get_mut("usage") {
        for key in ["primary", "secondary", "tertiary"] {
            if let Some(window) = usage.get_mut(key) {
                field(window, "resetsAt", delta);
            }
        }
        if let Some(extra) = usage
            .get_mut("extraRateWindows")
            .and_then(Value::as_array_mut)
        {
            for scope in extra {
                if let Some(window) = scope.get_mut("window") {
                    field(window, "resetsAt", delta);
                }
            }
        }
    }
    if let Some(saved) = entry.get_mut("savedResets") {
        field(saved, "soonestExpiresAt", delta);
        for (array, expiry) in [("credits", "expiresAt"), ("kinds", "soonestExpiresAt")] {
            if let Some(rows) = saved.get_mut(array).and_then(Value::as_array_mut) {
                for row in rows {
                    field(row, expiry, delta);
                }
            }
        }
    }
}
