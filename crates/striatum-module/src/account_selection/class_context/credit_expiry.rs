use std::collections::BTreeMap;

use serde_json::Value;
use striatum_core::accounts::{CreditExpiry, Roster};

use super::ClassContext;
use crate::account_quota::console_credit::{live_credit, org_credit_reading};

impl ClassContext {
    pub(super) fn org_credit_reading(&self, credential: &str) -> Option<&Value> {
        org_credit_reading(&self.quota, credential, |org| {
            self.reading_index()
                .accounts
                .get(org)
                .into_iter()
                .flatten()
                .map(|record| &record.reading)
        })
    }

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
                let credit = self
                    .org_credit_reading(id)?
                    .get("spend")?
                    .as_array()?
                    .iter()
                    .filter_map(|pool| live_credit(pool, self.now_ms))
                    .min_by_key(|credit| credit.expires_at_ms)?;
                Some((id.clone(), credit))
            })
            .collect()
    }
}
