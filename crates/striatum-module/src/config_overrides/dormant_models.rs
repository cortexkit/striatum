use super::*;
use striatum_core::RouteStateStore;

pub(crate) struct ModelConfigPresence {
    pub dormant_keys: Vec<String>,
    pub manual_upsert_keys: HashSet<String>,
}

#[derive(Default)]
struct DormantModelWarnings {
    version: Option<i64>,
    keys: HashSet<String>,
}

impl DormantModelWarnings {
    fn take_new(&mut self, version: i64, keys: Vec<String>) -> Vec<String> {
        if self.version != Some(version) {
            self.version = Some(version);
            self.keys.clear();
        }
        keys.into_iter()
            .filter(|key| self.keys.insert(key.clone()))
            .collect()
    }
}

impl LiveConfig {
    pub(crate) fn model_config_presence(
        &self,
        store: &striatum_store::SqliteRouteStateStore,
        credentials: Option<&crate::broca_credentials::SharedBrocaCredentials>,
    ) -> Result<ModelConfigPresence, String> {
        let mut served = store
            .get_catalog_facts()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|row| row.fusiform_present == Some(true))
            .map(|row| row.fact)
            .collect::<Vec<_>>();
        let override_keys = store
            .list_override_route_keys()
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect::<HashSet<_>>();
        let mut manual = store
            .get_facts()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|fact| {
                override_keys.contains(&make_route_key(
                    &fact.provider_id,
                    &fact.model_id,
                    fact.variant.as_deref(),
                ))
            })
            .collect::<Vec<_>>();
        if let Some(credentials) = credentials {
            let credentials = credentials
                .lock()
                .map_err(|_| "credential cache poisoned".to_owned())?;
            let served_targets = credentials.catalog_targets(&served);
            let manual_targets = credentials.catalog_targets(&manual);
            striatum_core::score_alias::append_alias_rows(&mut served, &served_targets);
            striatum_core::score_alias::append_alias_rows(&mut manual, &manual_targets);
        }
        let dormant_keys = self.dormant_model_keys(&served);
        let manual_upsert_keys = matching_model_keys(dormant_keys.iter(), &manual)
            .into_iter()
            .cloned()
            .collect();
        Ok(ModelConfigPresence {
            dormant_keys,
            manual_upsert_keys,
        })
    }

    pub(crate) fn dormant_model_keys(&self, facts: &[ModelFact]) -> Vec<String> {
        let configured = &self.config.model_routing.models;
        let served = matching_model_keys(configured.keys(), facts);
        let mut keys = configured
            .keys()
            .filter(|key| !served.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    pub(crate) fn report_dormant_models(
        &self,
        path: &Path,
        version: i64,
        presence: ModelConfigPresence,
    ) {
        static WARNED: OnceLock<Mutex<HashMap<PathBuf, DormantModelWarnings>>> = OnceLock::new();
        let keys = WARNED
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_default()
            .take_new(version, presence.dormant_keys);
        if !keys.is_empty() {
            let manual = keys
                .iter()
                .filter(|key| presence.manual_upsert_keys.contains(*key))
                .collect::<Vec<_>>();
            eprintln!("[striatum] {} has models entries the catalog doesn't serve (catalog version {version}), so they do nothing: {keys:?}; of these, routes kept by a manual upsert, where the settings still apply: {manual:?}", path.display());
        }
    }
}

fn matching_model_keys<'a>(
    keys: impl Iterator<Item = &'a String>,
    facts: &[ModelFact],
) -> HashSet<&'a String> {
    let catalog = CatalogModelIds::from_facts(facts);
    let served: HashSet<_> = facts.iter().map(fact_model_key).collect();
    keys.filter(|key| canonical_model_key(key, &catalog).is_some_and(|id| served.contains(&id)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_manual_upserts_are_reported_but_keep_their_config_overlays() {
        use striatum_store::{dev_descriptor, FusiformCatalogCursor, SqliteRouteStateStore};
        let scratch = cortexkit_test_support::ScratchDir::new("manual-upsert-config-presence");
        let descriptor = dev_descriptor(
            "manual-upsert-config-presence",
            scratch.join("store.sqlite").to_string_lossy().into_owned(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let fact = serde_json::from_value(serde_json::json!({
            "providerID": "p", "modelID": "manual",
            "scores": {"elo": 1000, "eq": 1000, "speed": 0}, "confidence": "curated"
        }))
        .unwrap();
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        store
            .apply_fusiform_catalog_snapshot_for_providers(
                &[],
                FusiformCatalogCursor {
                    catalog_version: 42,
                    resolved_at_ms: 123,
                },
                &HashSet::new(),
                &HashSet::new(),
            )
            .unwrap();
        let config = LiveConfig::new(
            parse_config(r#"{"models":{"p/manual":{"elo":1500},"p/missing":{"elo":1600}}}"#)
                .unwrap(),
            None,
        );
        let presence = config.model_config_presence(&store, None).unwrap();
        assert_eq!(presence.dormant_keys, ["p/manual", "p/missing"]);
        assert_eq!(
            presence.manual_upsert_keys,
            HashSet::from(["p/manual".into()])
        );
        let mut facts = store.get_facts().unwrap();
        config.apply(&mut facts);
        assert_eq!(
            facts
                .iter()
                .find(|fact| fact.model_id == "manual")
                .unwrap()
                .scores
                .elo,
            1500.0
        );
        assert!(!facts.iter().any(|fact| fact.model_id == "missing"));
    }

    #[test]
    fn dormant_keys_are_logged_once_per_catalog_version_even_across_config_edits() {
        let mut warnings = DormantModelWarnings::default();
        let keys = || vec!["p/stale".to_owned()];
        assert_eq!(warnings.take_new(1, keys()), ["p/stale"]);
        assert!(warnings.take_new(1, keys()).is_empty());
        assert!(warnings.take_new(1, Vec::new()).is_empty());
        assert!(warnings.take_new(1, keys()).is_empty());
        assert_eq!(
            warnings.take_new(1, vec!["p/new".into(), "p/stale".into()]),
            ["p/new"]
        );
        assert_eq!(warnings.take_new(2, keys()), ["p/stale"]);
    }
}
