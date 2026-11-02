use super::*;

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
    pub(crate) fn dormant_model_keys(&self, facts: &[ModelFact]) -> Vec<String> {
        let served = facts
            .iter()
            .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()))
            .collect::<HashSet<_>>();
        let mut keys = self
            .config
            .model_routing
            .models
            .keys()
            .filter(|key| !served.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    pub(crate) fn report_dormant_models(&self, path: &Path, version: i64, facts: &[ModelFact]) {
        static WARNED: OnceLock<Mutex<HashMap<PathBuf, DormantModelWarnings>>> = OnceLock::new();
        let keys = WARNED
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_default()
            .take_new(version, self.dormant_model_keys(facts));
        if !keys.is_empty() {
            eprintln!("[prefrontal-routing] [TODO(user): dormant models config keys in {} for catalog version {version}: {keys:?}]", path.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
