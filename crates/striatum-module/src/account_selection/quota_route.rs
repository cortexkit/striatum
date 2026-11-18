use std::collections::{BTreeMap, BTreeSet};

use cortexkit_claustrum_client::ListedCredential;
use striatum_core::quota::quota_route_join_for_source;
use striatum_core::quota_extract::requires_account_pool;
use striatum_core::ModelFact;

use super::ClassContext;
use crate::account_quota::{AccountQuotaState, StoredReading};

pub(super) type VendorlessQuota = BTreeMap<String, String>;

pub(super) fn vendorless_quota(
    quota: &AccountQuotaState,
    catalog: &[ModelFact],
) -> VendorlessQuota {
    // Only the catalog can identify the provider of these pools. A synthetic
    // route alias must not make an otherwise unambiguous join ambiguous.
    let sources: BTreeSet<_> = quota
        .readings
        .readings
        .values()
        .filter(|reading| {
            reading.tombstoned_at_ms.is_none() && reading.routing_provider().is_none()
        })
        .map(|reading| reading.provider.as_str())
        .collect();
    sources
        .into_iter()
        .filter_map(|source| {
            let join = quota_route_join_for_source(source, None, catalog)?;
            join.antigravity_family_only
                .then(|| (source.to_owned(), join.provider_id.to_owned()))
        })
        .collect()
}

impl ClassContext {
    pub(super) fn quota_serves(
        &self,
        reading: &StoredReading,
        row: &ListedCredential,
        fact: &ModelFact,
        capacity: &str,
    ) -> bool {
        if let Some(vendor) = reading.routing_provider() {
            return vendor == fact.provider_id
                || self
                    .targets
                    .get(&fact.provider_id)
                    .is_some_and(|target| target == vendor);
        }
        // A vendorless pool belongs only to its declared vault account and
        // product route, not to another route offered by the same vendor.
        !self.media_route
            && row.account_id.as_deref() == Some(reading.account_id.as_str())
            && requires_account_pool(capacity)
            && self.vendorless_quota.get(&reading.provider) == Some(&fact.provider_id)
    }
}
