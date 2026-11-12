//! Structural serving answers shared by requests on one published input view.
//! Billing planes, requested runner wire/auth pairs and provider limits, listing
//! age and probe ownership stay dynamic.

use crate::{broca_credentials::BrocaCredentials, config_overrides::LiveConfig};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, OnceLock},
};
use striatum_core::ModelFact;
use striatum_store::CatalogFingerprintKey;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FactKey {
    provider: String,
    model: String,
    methods: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_v1::request_cpu_bench as bench;

    #[test]
    fn catalog_interleaving_never_publishes_old_facts_under_new_generation() {
        let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
        let fixture = bench::representative_fixture(bench::FixtureCounts {
            subscription_rules: 1,
            exclusion_rules: 1,
            runner_route_rules: 0,
            catalog_facts: 3,
            listed_credentials: 2,
        });
        let config = fixture.operator_config();
        let credentials = fixture.credentials().lock().unwrap().snapshot();
        let snapshot = fixture.store().get_facts_with_generation().unwrap();
        let mut fact = snapshot.facts[0].clone();
        let old = FactKey::from(&fact);
        fact.auth_methods = Some(vec!["a-method-no-credential-has".into()]);
        let new = FactKey::from(&fact);
        let raced = ServingIndex::get_with_hook(&config, &credentials, &snapshot, || {
            fixture.store().with_writer_for_test(|connection| {
                connection.execute("UPDATE model_facts SET auth_methods_json=?1 WHERE provider_id=?2 AND model_id=?3",
                    rusqlite::params![serde_json::to_string(&fact.auth_methods).unwrap(), fact.provider_id, fact.model_id])
            }).unwrap();
        });
        assert_eq!(raced.generation, snapshot.generation);
        assert!(raced.facts.contains_key(&old));
        let later_snapshot = fixture.store().get_facts_with_generation().unwrap();
        let later = ServingIndex::get(&config, &credentials, &later_snapshot);
        assert!(!Arc::ptr_eq(&raced, &later));
        assert!(!later.facts.contains_key(&old));
        assert!(later.facts[&new].selection.is_empty());
        assert_eq!(later.generation, later_snapshot.generation);
        assert!(Arc::ptr_eq(
            &later,
            &ServingIndex::get(&config, &credentials, &later_snapshot)
        ));
    }
}

impl From<&ModelFact> for FactKey {
    fn from(fact: &ModelFact) -> Self {
        Self {
            provider: fact.provider_id.clone(),
            model: fact.model_id.clone(),
            methods: fact.auth_methods.clone(),
        }
    }
}

pub(crate) struct ServingRow {
    pub index: usize,
    pub covered: bool,
    pub metered: bool,
    pub wire_api: Option<String>,
    pub has_endpoint: bool,
}

pub(crate) struct FactServing {
    pub selection: Vec<ServingRow>,
    pub runner: Vec<ServingRow>,
}

pub(crate) struct ServingIndex {
    config: Arc<LiveConfig>,
    generation: CatalogFingerprintKey,
    credentials: u64,
    selection_catalog: BTreeSet<String>,
    runner_catalog: BTreeSet<String>,
    facts: BTreeMap<FactKey, Arc<FactServing>>,
}

fn cache() -> &'static Mutex<VecDeque<Arc<ServingIndex>>> {
    static CACHE: OnceLock<Mutex<VecDeque<Arc<ServingIndex>>>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

impl ServingIndex {
    pub(crate) fn get(
        config: &Arc<LiveConfig>,
        credentials: &BrocaCredentials,
        snapshot: &striatum_store::CatalogFactsSnapshot,
    ) -> Arc<Self> {
        Self::get_with_hook(config, credentials, snapshot, || {})
    }

    fn get_with_hook(
        config: &Arc<LiveConfig>,
        credentials: &BrocaCredentials,
        snapshot: &striatum_store::CatalogFactsSnapshot,
        after_facts: impl FnOnce(),
    ) -> Arc<Self> {
        let credential_generation = credentials.serving_generation();
        let mut cache = cache().lock().unwrap();
        if let Some(index) = cache.iter().find(|index| {
            Arc::ptr_eq(&index.config, config)
                && index.generation == snapshot.generation
                && index.credentials == credential_generation
        }) {
            return Arc::clone(index);
        }
        let mut facts = snapshot.facts.clone();
        after_facts();
        let targets = credentials.catalog_targets(&facts);
        striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
        let runner_catalog = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        let entries = facts
            .iter()
            .map(|fact| {
                (
                    FactKey::from(fact),
                    Arc::new(Self::build_fact(
                        config,
                        credentials,
                        fact,
                        &snapshot.providers,
                        &runner_catalog,
                        &targets,
                    )),
                )
            })
            .collect();
        // The token travels with its facts. A concurrent catalog publication can
        // make this request old, but cannot stamp its index with the newer token.
        let index = Arc::new(Self {
            config: Arc::clone(config),
            generation: snapshot.generation,
            credentials: credential_generation,
            facts: entries,
            selection_catalog: snapshot.providers.clone(),
            runner_catalog,
        });
        cache.push_front(Arc::clone(&index));
        // Keep config identities alive while cached, preventing address reuse.
        cache.truncate(8);
        index
    }

    fn build_fact(
        config: &LiveConfig,
        credentials: &BrocaCredentials,
        fact: &ModelFact,
        selection_catalog: &BTreeSet<String>,
        runner_catalog: &BTreeSet<String>,
        targets: &BTreeMap<String, String>,
    ) -> FactServing {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        let build = |catalog| {
            credentials
                .listed_credentials()
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    crate::broca_credentials::credential_serves_fact(
                        row,
                        fact,
                        catalog,
                        credentials,
                    )
                })
                .map(|(index, row)| {
                    let method = row.auth_method.as_deref().unwrap_or("");
                    ServingRow {
                        index,
                        covered: !targets.contains_key(&fact.provider_id)
                            && config
                                .compiled_routing_policy()
                                .subscription
                                .resolve(&key, row.credential_type == "oauth"),
                        metered: striatum_core::billing::is_api_metered_auth_method(method),
                        wire_api: credentials.wire_api(fact, method).map(str::to_owned),
                        has_endpoint: credentials.row_serves_wire_api(fact, method),
                    }
                })
                .collect()
        };
        FactServing {
            selection: build(selection_catalog),
            runner: build(runner_catalog),
        }
    }

    pub(crate) fn fact(
        &self,
        config: &Arc<LiveConfig>,
        credentials: &BrocaCredentials,
        fact: &ModelFact,
        catalog: &BTreeSet<String>,
        targets: &BTreeMap<String, String>,
    ) -> Arc<FactServing> {
        if Arc::ptr_eq(&self.config, config)
            && self.credentials == credentials.serving_generation()
            && &self.selection_catalog == catalog
        {
            if let Some(entry) = self.facts.get(&FactKey::from(fact)) {
                return Arc::clone(entry);
            }
        }
        // Engine routes and deliberately modified snapshots need not be catalog
        // rows. Resolve those against their own inputs without publishing them.
        Arc::new(Self::build_fact(
            config,
            credentials,
            fact,
            catalog,
            catalog,
            targets,
        ))
    }

    pub(crate) fn runner_fact(
        config: &LiveConfig,
        credentials: &BrocaCredentials,
        fact: &ModelFact,
        catalog: &BTreeSet<String>,
    ) -> Arc<FactServing> {
        let generation = credentials.serving_generation();
        if let Some(entry) = cache().lock().unwrap().iter().find_map(|index| {
            (std::ptr::eq(index.config.as_ref(), config)
                && index.credentials == generation
                && &index.runner_catalog == catalog)
                .then(|| index.facts.get(&FactKey::from(fact)).cloned())
                .flatten()
        }) {
            return entry;
        }
        Arc::new(Self::build_fact(
            config,
            credentials,
            fact,
            catalog,
            catalog,
            &BTreeMap::new(),
        ))
    }
}
