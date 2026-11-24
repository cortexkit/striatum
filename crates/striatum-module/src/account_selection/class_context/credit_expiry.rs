use std::collections::BTreeMap;

use serde_json::Value;
use striatum_core::accounts::{CreditExpiry, Roster};

use super::ClassContext;
use crate::account_quota::console_credit::{live_credit, org_credit_reading};
use crate::account_quota::parse_rfc3339_ms;

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

    pub(in crate::account_selection) fn legacy_billing_credit_probe(
        &self,
        origin: &striatum_store::FenceRecheck,
    ) -> bool {
        if !origin.is_untried_legacy_billing_wall()
            || !self.credentials.listing_version().is_some_and(|(view, _)| {
                self.quota
                    .roster
                    .as_ref()
                    .is_some_and(|roster| roster.view == view)
            })
        {
            return false;
        }
        let Some(reading) = origin
            .credential_id
            .as_deref()
            .and_then(|id| self.org_credit_reading(id))
        else {
            return false;
        };
        // With no refusal-time balance, a newer positive reading justifies one
        // test, not clearing evidence from a request that the provider refused.
        reading
            .get("fetchedAt")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_ms)
            .is_some_and(|at| at > origin.evidence_at)
            && crate::account_quota::console_credit::live_reading_total_micros(reading, self.now_ms)
                .is_some_and(|total| total > 0)
    }

    pub(in crate::account_selection) fn mark_early_billing_probes(
        &self,
        origins: &mut [striatum_store::FenceRecheck],
    ) {
        for origin in origins {
            origin.early_billing_probe =
                origin.due_at() > self.now_ms && self.legacy_billing_credit_probe(origin);
        }
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
