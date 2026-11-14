//! A call-local view of the published ledger and cap policy. Counting never
//! reloads admissions from the disposable SQL projection used for eligibility.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use cortexkit_role_routing::errors::Refusal;
use serde_json::{json, Value};
use striatum_core::RouteStateStore;

use crate::account_state::{AccountState, CapCount, InflightScope};
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
        // A waiter's per-model count is listed even when nothing holds it.
        for (_, waiter) in state.waiters.live(now) {
            for count in &waiter.counts {
                let models = models
                    .entry((count.provider.clone(), count.account.clone()))
                    .or_default();
                models.extend(count.model.clone());
            }
        }
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
        // Each live waiter is listed on every count it waits on. Waiters
        // exist only with enforcement on, and a count no one waits on carries
        // no `waiters` member.
        let mut waiting: BTreeMap<CapCount, Vec<(u64, Value)>> = BTreeMap::new();
        if self.enforcement_enabled {
            for (_, waiter) in self.state.waiters.live(self.now) {
                for count in &waiter.counts {
                    waiting.entry(count.clone()).or_default().push((
                        waiter.order,
                        json!({"class":waiter.class, "order":waiter.order}),
                    ));
                }
            }
        }
        let waiters = |provider: &str, account: &str, model: Option<&str>| {
            let count = CapCount {
                provider: provider.to_owned(),
                account: account.to_owned(),
                model: model.map(str::to_owned),
            };
            waiting.get(&count).map(|waiters| {
                let mut waiters = waiters.clone();
                waiters.sort_by_key(|(order, _)| *order);
                Value::Array(waiters.into_iter().map(|(_, waiter)| waiter).collect())
            })
        };
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
                let mut shared = json!({
                    "scope":"account", "model":null, "effective_cap":shared_limit,
                    "holds_in_flight":count(InflightScope::Account)
                });
                if let Some(waiters) = waiters(provider, account, None) {
                    shared["waiters"] = waiters;
                }
                let mut counts = vec![shared];
                for model in models {
                    let cap = self.caps.cap(provider, account, model);
                    if cap.scope == CapScope::AccountModel {
                        let mut modeled = json!({
                            "scope":"account_model", "model":model, "effective_cap":cap.limit,
                            "holds_in_flight":count(InflightScope::AccountModel(model))
                        });
                        if let Some(waiters) = waiters(provider, account, Some(model)) {
                            modeled["waiters"] = waiters;
                        }
                        counts.push(modeled);
                    }
                }
                json!({"provider":provider, "account_id":account, "cap_counts":counts})
            })
            .collect();
        json!({"enforcement_enabled":self.enforcement_enabled, "accounts":accounts})
    }
}
