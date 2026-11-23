use serde::Deserialize;
use serde_json::Value;
use striatum_core::{accounts::CreditExpiry, quota::SpendBalance};

use super::{parse_rfc3339_ms, AccountQuotaState, StoredReading};

pub(crate) fn org_credit_reading<'a, I>(
    quota: &AccountQuotaState,
    credential: &str,
    readings_for_org: impl FnOnce(&str) -> I,
) -> Option<&'a Value>
where
    I: IntoIterator<Item = &'a StoredReading>,
{
    let org = quota.credential_orgs.get(credential)?;
    // Trust the reading's reported org, not just the cache key. If more than
    // one reading claims the org, there is no basis to pick one balance.
    let mut matching = readings_for_org(org).into_iter().filter(|reading| {
        reading.tombstoned_at_ms.is_none()
            && reading.account_id == *org
            && reading.entry.get("account").and_then(Value::as_str) == Some(org.as_str())
    });
    let reading = matching.next()?;
    matching.next().is_none().then_some(&reading.entry)
}

/// `None` means there is no usable Console credit reading, not a zero balance.
pub(crate) fn live_total_micros(
    quota: &AccountQuotaState,
    credential: &str,
    now_ms: i64,
) -> Option<i64> {
    let reading = org_credit_reading(quota, credential, |_| quota.readings.readings.values())?;
    live_reading_total_micros(reading, now_ms)
}

pub(crate) fn live_reading_total_micros(reading: &Value, now_ms: i64) -> Option<i64> {
    reading
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
