use serde::Deserialize;
use serde_json::Value;
use striatum_core::{accounts::CreditExpiry, quota::SpendBalance};

use super::{parse_rfc3339_ms, AccountQuotaState};

/// `None` means there is no usable Console credit reading, not a zero balance.
pub(crate) fn live_total_micros(
    quota: &AccountQuotaState,
    credential: &str,
    now_ms: i64,
) -> Option<i64> {
    let org = quota.credential_orgs.get(credential)?;
    let reading = quota
        .readings
        .readings
        .get(&("claude-console".into(), org.clone()))?;
    if reading.tombstoned_at_ms.is_some()
        || reading.entry.get("account").and_then(Value::as_str) != Some(org.as_str())
    {
        return None;
    }
    reading
        .entry
        .get("spend")?
        .as_array()?
        .iter()
        .filter_map(|pool| live_credit(pool, now_ms))
        .try_fold(0_i64, |total, credit| {
            total.checked_add(usd_micros(&credit.remaining)?)
        })
}

fn usd_micros(balance: &SpendBalance) -> Option<i64> {
    if balance.unit != "USD" {
        return None;
    }
    // Compare exact sums so changing the order or scale of the pools cannot
    // make an unchanged balance look like a top-up.
    let shift = 6_i32.checked_sub(balance.exponent)?;
    let scale = 10_i64.checked_pow(shift.unsigned_abs())?;
    if shift >= 0 {
        balance.minor.checked_mul(scale)
    } else {
        (balance.minor % scale == 0).then_some(balance.minor / scale)
    }
}

pub(crate) fn live_credit(pool: &Value, now_ms: i64) -> Option<CreditExpiry> {
    let expires_at_ms = parse_rfc3339_ms(pool.get("expiresAt")?.as_str()?)?;
    if expires_at_ms <= now_ms {
        return None;
    }
    let remaining = SpendBalance::deserialize(pool.get("remaining")?).ok()?;
    (!remaining.is_non_positive()).then_some(CreditExpiry {
        expires_at_ms,
        remaining,
    })
}
