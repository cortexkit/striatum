//! A call-local view of the published ledger and cap policy. Counting never
//! reloads admissions from the disposable SQL projection used for eligibility.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use cortexkit_role_routing::errors::Refusal;
use serde_json::{json, Value};
use striatum_core::RouteStateStore;

use crate::account_state::{AccountState, InflightScope};
use crate::concurrency_caps::{CapScope, ResolvedCaps};

use super::{store_refusal, Deps};

pub(super) struct Snapshot {
    state: Arc<AccountState>,
    caps: ResolvedCaps,
    models: BTreeMap<(String, String), BTreeSet<String>>,
    enforcement_enabled: bool,
    now: i64,
}

impl Snapshot {
    pub(super) fn capture(
        deps: &Deps<'_>,
        config: &crate::config_overrides::LiveConfig,
        catalog_store: &striatum_store::SqliteRouteStateStore,
        now: i64,
    ) -> Result<Self, Refusal> {
        let state = crate::account_state::snapshot(deps.store).map_err(store_refusal)?;
        let mut credentials = deps
            .broca_credentials
            .lock()
            .map_err(store_refusal)?
            .snapshot();
        // A config reload may precede the poller's next publication. Apply it
        // only to this clone so the report uses the captured settings.
        credentials
            .concurrency_caps
            .publish_settings(config.concurrency_caps());
        let caps = credentials.concurrency_caps.resolved.clone();
        let mut models = state.inflight.active_models(now);
        for (provider, account) in caps.accounts() {
            models
                .entry((provider.to_owned(), account.to_owned()))
                .or_default();
        }
        let facts = catalog_store.get_facts().map_err(store_refusal)?;
        let providers = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        let quota = crate::ACCOUNT_QUOTA
            .get()
            .and_then(|quota| quota.lock().ok().map(|quota| quota.clone()))
            .unwrap_or_default();
        let account_ids = credentials.quota_account_ids(&quota);
        for row in credentials.listed_credentials() {
            let account = account_ids
                .get(&row.id)
                .and_then(Option::as_deref)
                .unwrap_or(&row.id);
            for provider in crate::broca_credentials::credential_providers(row, &providers) {
                models.entry((provider, account.to_owned())).or_default();
            }
        }
        for ((provider, account), models) in &mut models {
            models.extend(caps.model_exceptions(provider, account));
            models.extend(
                facts
                    .iter()
                    .filter(|fact| fact.provider_id == *provider)
                    .map(|fact| crate::account_selection::normalize(&fact.model_id)),
            );
        }
        Ok(Self {
            state,
            caps,
            models,
            enforcement_enabled: config.concurrency_caps().enforcement_enabled,
            now,
        })
    }

    pub(super) fn report(&self) -> Value {
        let accounts: Vec<_> = self
            .models
            .iter()
            .map(|((provider, account), models)| {
                let default = self.caps.default_cap(provider, account);
                let shared_limit = (default.scope == CapScope::Account)
                    .then_some(default.limit)
                    .flatten();
                let count = |scope| {
                    self.state
                        .inflight
                        .count_for_scope(&self.caps, provider, account, scope, self.now)
                };
                let mut counts = vec![json!({
                    "scope":"account", "model":null, "effective_cap":shared_limit,
                    "holds_in_flight":count(InflightScope::Account)
                })];
                for model in models {
                    let cap = self.caps.cap(provider, account, model);
                    if cap.scope == CapScope::AccountModel {
                        counts.push(json!({
                            "scope":"account_model", "model":model, "effective_cap":cap.limit,
                            "holds_in_flight":count(InflightScope::AccountModel(model))
                        }));
                    }
                }
                json!({"provider":provider, "account_id":account, "cap_counts":counts})
            })
            .collect();
        json!({"enforcement_enabled":self.enforcement_enabled, "accounts":accounts})
    }
}
