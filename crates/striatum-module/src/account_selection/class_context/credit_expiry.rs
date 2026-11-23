use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;
use striatum_core::accounts::{CreditExpiry, Roster};
use striatum_core::quota::SpendBalance;

use super::ClassContext;
use crate::account_quota::parse_rfc3339_ms;

impl ClassContext {
    pub(in crate::account_selection) fn expiring_org_credits(
        &self,
        roster: &Roster,
    ) -> BTreeMap<String, CreditExpiry> {
        // The quota cache and admission roster can be snapshotted on different
        // sides of a re-list. Do not apply links from an older view to new keys.
        if self.quota.credential_orgs.is_empty()
            || !self
                .quota
                .roster
                .as_ref()
                .is_some_and(|listed| listed.view == roster.view_digest)
        {
            return BTreeMap::new();
        }
        roster
            .accounts
            .iter()
            .flat_map(|account| &account.credentials)
            .filter_map(|credential| {
                let id = &credential.credential_id;
                let org = self.quota.credential_orgs.get(id)?;
                let readings = self.reading_index().accounts.get(&org.id)?;
                let credit = readings
                    .iter()
                    .filter_map(|record| {
                        let reading = &record.reading;
                        let vendor = reading.routing_provider()?;
                        if reading.entry.get("account")?.as_str()? != org.id
                            || !org.vendors.iter().any(|served| served == vendor)
                        {
                            return None;
                        }
                        reading.entry.get("spend")?.as_array()
                    })
                    .flatten()
                    .filter_map(|pool| live_credit(pool, self.now_ms))
                    .min_by_key(|credit| credit.expires_at_ms)?;
                Some((id.clone(), credit))
            })
            .collect()
    }
}

fn live_credit(pool: &Value, now_ms: i64) -> Option<CreditExpiry> {
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
