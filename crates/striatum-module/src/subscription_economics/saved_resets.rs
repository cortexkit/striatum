use std::{collections::BTreeSet, sync::Mutex};

use serde_json::Value;

use super::parse_rfc3339_ms;

// Reject broken readings; this is a sanity bound, not a provider quota limit.
const MAX_SAVED_RESETS_PER_ACCOUNT: u32 = 10_000;
static WARNED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

#[derive(Default)]
pub(super) struct SavedResets {
    pub(super) count: u32,
    pub(super) soonest: Option<i64>,
    pub(super) credits: Vec<i64>,
    pub(super) kinds: Vec<(i64, u32)>,
}

pub(super) fn decode(value: &Value, account_id: &str) -> SavedResets {
    checked(value).unwrap_or_else(|| {
        if WARNED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(account_id.to_owned())
        {
            crate::warn_operator(format!(
                "[striatum] ignoring absurd saved-reset count for account {account_id:?}"
            ));
        }
        SavedResets::default()
    })
}

fn checked(value: &Value) -> Option<SavedResets> {
    let count = |value: &Value| {
        if value
            .as_f64()
            .is_some_and(|count| count > f64::from(MAX_SAVED_RESETS_PER_ACCOUNT))
        {
            return None;
        }
        Some(value.as_u64().unwrap_or(0) as u32)
    };
    let expiry = |value: &Value| value.as_str().and_then(parse_rfc3339_ms);
    if let Some(kinds) = value.get("kinds") {
        let mut saved = SavedResets::default();
        for kind in kinds
            .as_array()
            .into_iter()
            .flatten()
            .filter(|kind| matches!(kind["kind"].as_str(), Some("weekly" | "full")))
        {
            let count = count(&kind["availableCount"])?;
            saved.count += count;
            if saved.count > MAX_SAVED_RESETS_PER_ACCOUNT {
                return None;
            }
            if let Some(expiry) = expiry(&kind["soonestExpiresAt"]) {
                saved.soonest = Some(saved.soonest.map_or(expiry, |time| time.min(expiry)));
                // The aggregate credit list does not identify each credit's kind.
                // Expire the whole group at the earliest expiry for that kind.
                saved.kinds.push((expiry, count));
            }
        }
        Some(saved)
    } else {
        let count = count(&value["availableCount"])?;
        let credits: Vec<_> = value["credits"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|credit| expiry(&credit["expiresAt"]))
            .collect();
        Some(SavedResets {
            count,
            soonest: expiry(&value["soonestExpiresAt"]).or_else(|| credits.iter().copied().min()),
            credits,
            ..Default::default()
        })
    }
}
