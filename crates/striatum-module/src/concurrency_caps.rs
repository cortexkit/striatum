//! Immutable cap policy, rebuilt when plan prices, the vault roster or settings
//! are published, rather than resolving override precedence on every admission.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CapScope {
    #[default]
    Account,
    AccountModel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NoCap {
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum CapLimit {
    Number(#[schemars(range(min = 1))] u64),
    Unlimited(NoCap),
}

impl CapLimit {
    fn value(self) -> Option<u64> {
        match self {
            Self::Number(limit) => Some(limit),
            Self::Unlimited(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapOverride {
    pub(crate) limit: CapLimit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<CapScope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelCapOverride {
    pub(crate) limit: CapLimit,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CapSettings {
    /// Defaults off so operators can inspect counts before enabling enforcement.
    pub(crate) enforcement_enabled: bool,
    pub(crate) providers: BTreeMap<String, CapOverride>,
    pub(crate) plans: BTreeMap<String, CapOverride>,
    pub(crate) models: BTreeMap<String, ModelCapOverride>,
    pub(crate) accounts: BTreeMap<String, CapOverride>,
}

impl CapSettings {
    pub(crate) fn validate(&self) -> Result<(), String> {
        for (kind, values) in [
            ("providers", &self.providers),
            ("plans", &self.plans),
            ("accounts", &self.accounts),
        ] {
            for (key, value) in values {
                Self::validate_limit(kind, key, value.limit)?;
                Self::validate_key(kind, key)?;
            }
        }
        for (key, value) in &self.models {
            Self::validate_limit("models", key, value.limit)?;
            Self::validate_key("models", key)?;
        }
        Ok(())
    }

    fn validate_limit(kind: &str, key: &str, limit: CapLimit) -> Result<(), String> {
        if limit == CapLimit::Number(0) {
            return Err(format!("concurrency.{kind}.{key}.limit must be positive or \"none\"; use the exclude exclusion rules to exclude routes, not a zero concurrency cap"));
        }
        Ok(())
    }

    fn validate_key(kind: &str, key: &str) -> Result<(), String> {
        if key.is_empty()
            || (kind != "providers"
                && !key
                    .split_once('/')
                    .is_some_and(|(p, id)| !p.is_empty() && !id.is_empty()))
        {
            return Err(format!("concurrency.{kind}: invalid key {key:?}"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EffectiveCap {
    pub(crate) limit: Option<u64>,
    pub(crate) scope: CapScope,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct AccountCaps {
    default: EffectiveCap,
    models: HashMap<String, EffectiveCap>,
}

fn model_for_provider<'a>(key: &'a str, provider: &str) -> Option<&'a str> {
    let (p, model) = key.split_once('/')?;
    (p == provider).then_some(model)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ProviderCaps {
    default: AccountCaps,
    accounts: HashMap<String, AccountCaps>,
    presence: CapPresence,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CapPresence {
    any: bool,
    default: bool,
    models: HashMap<String, bool>,
}

/// Defaults cover models not yet in the catalog and accounts absent from the
/// vault roster. Model exceptions are exact stored-provider/model matches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ResolvedCaps {
    providers: HashMap<String, ProviderCaps>,
    any_caps: bool,
}

impl ResolvedCaps {
    pub(crate) fn any_caps(&self) -> bool {
        self.any_caps
    }

    pub(crate) fn provider_can_cap(&self, provider: &str) -> bool {
        self.providers
            .get(provider)
            .is_some_and(|caps| caps.presence.any)
    }

    /// Presence is only a cheap prefilter. Admission still reads the account's
    /// effective cap, including its more-specific unlimited overrides.
    pub(crate) fn model_can_cap(&self, provider: &str, normalized_model: &str) -> bool {
        self.providers.get(provider).is_some_and(|caps| {
            caps.presence
                .models
                .get(normalized_model)
                .copied()
                .unwrap_or(caps.presence.default)
        })
    }

    pub(crate) fn accounts(&self) -> impl Iterator<Item = (&str, &str)> {
        self.providers.iter().flat_map(|(provider, caps)| {
            caps.accounts
                .keys()
                .map(move |account| (provider.as_str(), account.as_str()))
        })
    }

    pub(crate) fn default_cap(&self, provider: &str, account: &str) -> EffectiveCap {
        self.providers
            .get(provider)
            .map(|caps| caps.accounts.get(account).unwrap_or(&caps.default).default)
            .unwrap_or_default()
    }

    pub(crate) fn model_exceptions(&self, provider: &str, account: &str) -> Vec<String> {
        self.providers
            .get(provider)
            .map(|caps| {
                caps.accounts
                    .get(account)
                    .unwrap_or(&caps.default)
                    .models
                    .keys()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn cap(&self, provider: &str, account: &str, model: &str) -> EffectiveCap {
        let model = crate::account_selection::normalize(model);
        let Some(provider) = self.providers.get(provider) else {
            return EffectiveCap::default();
        };
        let account = provider.accounts.get(account).unwrap_or(&provider.default);
        account
            .models
            .get(&model)
            .copied()
            .unwrap_or(account.default)
    }

    /// Use the model exactly as stored on the admission. Do not strip variants,
    /// pool hints or a provider prefix that storage retains.
    pub(crate) fn cap_count<'a>(
        &self,
        provider: &str,
        account: &str,
        model: &'a str,
    ) -> crate::account_state::InflightScope<'a> {
        match self.cap(provider, account, model).scope {
            CapScope::Account => crate::account_state::InflightScope::Account,
            CapScope::AccountModel => crate::account_state::InflightScope::AccountModel(model),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CapInputs {
    pub(crate) resolved: ResolvedCaps,
    catalog: crate::fusiform_catalog::PlanConcurrencyCatalog,
    settings: CapSettings,
    accounts: BTreeMap<(String, String), Option<String>>,
    warned: std::collections::BTreeSet<String>,
    selection_cache: Arc<SelectionCapCache>,
}

#[derive(Default)]
struct SelectionCapCache {
    entry: Mutex<Option<(CapSettings, Arc<ResolvedCaps>)>>,
}

impl std::fmt::Debug for SelectionCapCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Warming a memo does not publish new credential state. Keep snapshot
        // diagnostics independent of which read happened to populate it first.
        formatter.write_str("SelectionCapCache")
    }
}

impl CapInputs {
    /// Cloned `BrocaCredentials` snapshots share this cache until a publication
    /// changes their cap inputs. Config reloads may precede the credential poll;
    /// resolve that settings value once without copying the catalog or roster.
    pub(crate) fn resolved_for_settings(&self, settings: &CapSettings) -> Arc<ResolvedCaps> {
        let mut cache = self.selection_cache.entry.lock().unwrap();
        if let Some((previous, caps)) = &*cache {
            if previous == settings {
                return caps.clone();
            }
        }
        let caps = Arc::new(if settings == &self.settings {
            self.resolved.clone()
        } else {
            self.resolve(settings)
        });
        *cache = Some((settings.clone(), caps.clone()));
        caps
    }

    pub(crate) fn publish_catalog(&mut self, prices: &serde_json::Value, now: i64) {
        self.catalog = crate::fusiform_catalog::PlanConcurrencyCatalog::load(prices, now);
        for message in &self.catalog.warnings {
            if self.warned.insert(message.clone()) {
                tracing::warn!("{message}");
            }
        }
        self.rebuild();
    }

    pub(crate) fn publish_settings(&mut self, settings: &CapSettings) {
        if &self.settings != settings {
            self.settings = settings.clone();
            self.rebuild();
        }
    }

    pub(crate) fn publish_roster(&mut self, state: &crate::account_quota::AccountQuotaState) {
        let mut accounts = BTreeMap::new();
        if let Some(roster) = &state.roster {
            for reading in state.readings.readings.values() {
                let identity = crate::account_quota::identity_for_reading(roster, reading);
                if let Some((provider, tier)) =
                    crate::subscription_economics::plan_price_key(reading, identity)
                {
                    accounts.insert(
                        (provider.to_owned(), reading.account_id.clone()),
                        tier.map(str::to_owned),
                    );
                }
            }
        }
        if self.accounts != accounts {
            self.accounts = accounts;
            self.rebuild();
        }
    }

    fn account_caps(
        &self,
        settings: &CapSettings,
        provider: &str,
        tier: Option<&str>,
        account: Option<&str>,
    ) -> AccountCaps {
        let catalog = tier.and_then(|tier| self.catalog.cap(provider, tier));
        let scope = catalog.map_or(CapScope::Account, |cap| cap.scope);
        let account_override =
            account.and_then(|account| settings.accounts.get(&format!("{provider}/{account}")));
        let general = tier
            .and_then(|tier| settings.plans.get(&format!("{provider}/{tier}")))
            .or_else(|| settings.providers.get(provider));
        let effective = |setting: &CapOverride| EffectiveCap {
            limit: setting.limit.value(),
            scope: setting.scope.unwrap_or(scope),
        };
        let mut out = AccountCaps {
            default: account_override
                .or(general)
                .map(effective)
                .or(catalog)
                .unwrap_or_default(),
            models: HashMap::new(),
        };
        if account_override.is_none() {
            // A winning setting replaces the catalog limit as a whole. Its
            // inherited scope must not bring the catalog's model limits along.
            if general.is_none() {
                if let Some(models) = tier.and_then(|tier| self.catalog.model_caps(provider, tier))
                {
                    for (key, limit) in models {
                        if let Some(model) = model_for_provider(key, provider) {
                            out.models.insert(
                                model.to_owned(),
                                EffectiveCap {
                                    limit: Some(*limit),
                                    scope: CapScope::AccountModel,
                                },
                            );
                        }
                    }
                }
            }
            for (key, value) in &settings.models {
                if let Some(model) = model_for_provider(key, provider) {
                    out.models.insert(
                        model.to_owned(),
                        EffectiveCap {
                            limit: value.limit.value(),
                            scope: CapScope::AccountModel,
                        },
                    );
                }
            }
        }
        out
    }

    fn rebuild(&mut self) {
        self.resolved = self.resolve(&self.settings);
        self.selection_cache = Arc::default();
    }

    fn resolve(&self, settings: &CapSettings) -> ResolvedCaps {
        let mut resolved = ResolvedCaps::default();
        let mut providers: std::collections::BTreeSet<_> =
            settings.providers.keys().map(String::as_str).collect();
        providers.extend(self.accounts.keys().map(|(p, _)| p.as_str()));
        for key in settings.models.keys().chain(settings.accounts.keys()) {
            if let Some((provider, _)) = key.split_once('/') {
                providers.insert(provider);
            }
        }
        for provider in providers {
            resolved.providers.insert(
                provider.to_owned(),
                ProviderCaps {
                    default: self.account_caps(settings, provider, None, None),
                    accounts: HashMap::new(),
                    presence: CapPresence::default(),
                },
            );
        }
        for ((provider, account), tier) in &self.accounts {
            resolved
                .providers
                .get_mut(provider)
                .unwrap()
                .accounts
                .insert(
                    account.clone(),
                    self.account_caps(settings, provider, tier.as_deref(), Some(account)),
                );
        }
        for key in settings.accounts.keys() {
            if let Some((provider, account)) = key.split_once('/') {
                let row = resolved.providers.get_mut(provider).unwrap();
                row.accounts
                    .entry(account.to_owned())
                    .or_insert_with(|| self.account_caps(settings, provider, None, Some(account)));
            }
        }
        for provider in resolved.providers.values_mut() {
            let accounts: Vec<_> = std::iter::once(&provider.default)
                .chain(provider.accounts.values())
                .collect();
            let default = accounts.iter().any(|caps| caps.default.limit.is_some());
            let models: std::collections::BTreeSet<_> = accounts
                .iter()
                .flat_map(|caps| caps.models.keys())
                .collect();
            let models: HashMap<_, _> = models
                .into_iter()
                .map(|model| {
                    let capped = accounts.iter().any(|caps| {
                        caps.models
                            .get(model)
                            .unwrap_or(&caps.default)
                            .limit
                            .is_some()
                    });
                    (model.clone(), capped)
                })
                .collect();
            provider.presence = CapPresence {
                any: default || models.values().any(|capped| *capped),
                default,
                models,
            };
        }
        resolved.any_caps = resolved.providers.values().any(|caps| caps.presence.any);
        resolved
    }
}

#[cfg(test)]
#[path = "concurrency_caps_tests.rs"]
pub(crate) mod tests;
