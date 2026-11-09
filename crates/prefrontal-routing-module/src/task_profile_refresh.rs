//! Background-only task aggregation and atomic publication for store-free selects.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use prefrontal_routing_core::model_router::RouteTaskProfiles;
use prefrontal_routing_core::task_profile::{
    compute_profile, iq_bucket, ClosedTask, TokenVector, IQ_BUCKET_WIDTH, PROFILE_REFRESH_MS,
};
use prefrontal_routing_core::{make_route_key, ModelFact, RouteStateStore};
use prefrontal_routing_store::{
    ClosedTaskUsage, PublishedEfforts, ReasoningCatalog, SqliteRouteStateStore,
};

#[derive(Default)]
pub(crate) struct ProfileSnapshot {
    cells: HashMap<(String, u8), RouteTaskProfiles>,
    pub(crate) providers_without_cache_write_reports: HashSet<String>,
}

#[derive(Default)]
pub(crate) struct ProfileCache(RwLock<Arc<ProfileSnapshot>>);

impl ProfileCache {
    pub(crate) fn snapshot(&self) -> Arc<ProfileSnapshot> {
        Arc::clone(
            &self
                .0
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn publish(&self, snapshot: ProfileSnapshot) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(snapshot);
    }
}

pub(crate) fn cache() -> &'static ProfileCache {
    static CACHE: OnceLock<ProfileCache> = OnceLock::new();
    CACHE.get_or_init(ProfileCache::default)
}

impl ProfileSnapshot {
    /// A partial map must not mix profile pricing and cold-start pricing in one select.
    pub(crate) fn lookup(
        &self,
        workload: &str,
        iq: f64,
        shortlist: &[ModelFact],
    ) -> RouteTaskProfiles {
        let Some(bucket) = iq_bucket(iq) else {
            return RouteTaskProfiles::new();
        };
        let Some(cell) = self.cells.get(&(workload.to_owned(), bucket)) else {
            return RouteTaskProfiles::new();
        };
        let mut selected = RouteTaskProfiles::new();
        for fact in shortlist {
            let key = make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
            let Some(profile) = cell.get(&key) else {
                return RouteTaskProfiles::new();
            };
            selected.insert(key, profile.clone());
        }
        selected
    }
}

fn build_snapshot(rows: &[ClosedTaskUsage], facts: &[ModelFact]) -> ProfileSnapshot {
    let by_model: HashMap<_, _> = facts
        .iter()
        .map(|fact| {
            (
                (
                    fact.provider_id.as_str(),
                    fact.model_id.as_str(),
                    fact.variant.as_deref(),
                ),
                fact,
            )
        })
        .collect();
    let tasks: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            if row.multi_model || row.multi_provider {
                return None;
            }
            let bucket = u8::try_from(row.iq_bucket?).ok()?;
            if bucket > 100 / IQ_BUCKET_WIDTH {
                return None;
            }
            let model = by_model.get(&(
                row.provider_id.as_deref()?,
                row.selected_model_id.as_deref()?,
                row.selected_variant.as_deref(),
            ))?;
            Some(ClosedTask {
                workload: &row.workload,
                model,
                iq_bucket: bucket,
                // Unreported dimensions contribute no tokens. Reporting presence is
                // preserved separately so pricing can name provider omissions.
                tokens: TokenVector {
                    fresh_input: row.totals.fresh_input.unwrap_or(0.0),
                    cached_read: row.totals.cached_read.unwrap_or(0.0),
                    cache_write: row.totals.cache_write.unwrap_or(0.0),
                    output_excluding_reasoning: row
                        .totals
                        .output_excluding_reasoning
                        .unwrap_or(0.0),
                    reasoning: row.totals.reasoning.unwrap_or(0.0),
                },
            })
        })
        .collect();
    let workloads: HashSet<_> = tasks.iter().map(|task| task.workload).collect();
    let mut snapshot = ProfileSnapshot::default();
    for workload in workloads {
        for bucket in 0..=100 / IQ_BUCKET_WIDTH {
            let cell: RouteTaskProfiles = facts
                .iter()
                .filter_map(|fact| {
                    compute_profile(&tasks, fact, workload, bucket).map(|profile| {
                        (
                            make_route_key(
                                &fact.provider_id,
                                &fact.model_id,
                                fact.variant.as_deref(),
                            ),
                            profile,
                        )
                    })
                })
                .collect();
            if !cell.is_empty() {
                snapshot.cells.insert((workload.to_owned(), bucket), cell);
            }
        }
    }
    snapshot
}

fn expand_variants(
    facts: &mut Vec<ModelFact>,
    rows: &[ClosedTaskUsage],
    catalog: &ReasoningCatalog,
    aliases: &std::collections::BTreeMap<String, String>,
) {
    // Efforts can be published without separate fact rows. Prefer the generic
    // row's rates, while retaining an exact variant's own rates when present.
    facts.sort_by(|a, b| {
        (&a.provider_id, &a.model_id, &a.variant).cmp(&(&b.provider_id, &b.model_id, &b.variant))
    });
    let mut templates = HashMap::new();
    let mut known = HashSet::new();
    for fact in facts.iter() {
        templates
            .entry((fact.provider_id.clone(), fact.model_id.clone()))
            .or_insert_with(|| fact.clone());
        known.insert((
            fact.provider_id.clone(),
            fact.model_id.clone(),
            fact.variant.clone(),
        ));
    }
    let mut variants: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            Some((
                row.provider_id.clone()?,
                row.selected_model_id.clone()?,
                row.selected_variant.clone(),
            ))
        })
        .collect();
    variants.extend(facts.iter().filter_map(|fact| {
        Some((
            fact.provider_id.clone(),
            fact.model_id.clone(),
            Some(fact.prompt_defaults.as_ref()?.variant.clone()?),
        ))
    }));
    for ((provider, model), _) in &templates {
        if let Some(PublishedEfforts::Published(efforts)) =
            crate::variant_policy::published(catalog, aliases, provider, model)
        {
            variants.extend(
                efforts
                    .iter()
                    .map(|effort| (provider.clone(), model.clone(), Some(effort.clone()))),
            );
        }
    }
    for (provider, model, variant) in variants {
        if !known.insert((provider.clone(), model.clone(), variant.clone())) {
            continue;
        }
        if let Some(template) = templates.get(&(provider, model)) {
            let mut fact = template.clone();
            fact.variant = variant;
            facts.push(fact);
        }
    }
}

fn refresh(
    store: &SqliteRouteStateStore,
    path: &Path,
    now: i64,
    credentials: Option<&crate::SharedBrocaCredentials>,
) -> Result<ProfileSnapshot, String> {
    let rows = store
        .closed_task_usage(now)
        .map_err(|error| error.to_string())?;
    let mut facts = store.get_facts().map_err(|error| error.to_string())?;
    let live = crate::config_overrides::live_config(path);
    live.apply(&mut facts);
    let aliases = credentials
        .and_then(|credentials| credentials.lock().ok())
        .map(|credentials| credentials.catalog_targets(&facts))
        .unwrap_or_default();
    prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &aliases);
    expand_variants(
        &mut facts,
        &rows,
        &store
            .reasoning_catalog()
            .map_err(|error| error.to_string())?,
        &aliases,
    );
    live.apply(&mut facts);
    let mut snapshot = build_snapshot(&rows, &facts);
    snapshot.providers_without_cache_write_reports = store
        .provider_token_reporting(now)
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|provider| !provider.reports_cache_write)
        .map(|provider| provider.provider_id)
        .collect();
    Ok(snapshot)
}

fn refresh_cache(
    cache: &ProfileCache,
    store: &SqliteRouteStateStore,
    path: &Path,
    now: i64,
    credentials: Option<&crate::SharedBrocaCredentials>,
) -> Result<(), String> {
    let next = refresh(store, path, now, credentials)?;
    cache.publish(next);
    Ok(())
}

pub(crate) async fn refresh_loop(
    store: Arc<SqliteRouteStateStore>,
    path: PathBuf,
    credentials: crate::SharedBrocaCredentials,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(PROFILE_REFRESH_MS as u64));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let store = Arc::clone(&store);
        let path = path.clone();
        let credentials = Arc::clone(&credentials);
        // SQLite reads and statistics never occupy the async request workers.
        let next = tokio::task::spawn_blocking(move || {
            refresh_cache(
                cache(),
                &store,
                &path,
                crate::now_ms() as i64,
                Some(&credentials),
            )
        })
        .await;
        match next {
            Ok(Ok(())) => {},
            Ok(Err(error)) => crate::warn_operator(format!("[prefrontal-routing] task profile refresh failed; keeping last snapshot: {error}")),
            Err(error) => crate::warn_operator(format!("[prefrontal-routing] task profile refresh worker failed; keeping last snapshot: {error}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prefrontal_routing_core::task_profile::{ProfileLevel, PROFILE_WINDOW_MS, TASK_CLOSE_MS};
    use prefrontal_routing_core::{DecisionInput, FactSource, UsageObservation};

    const NOW: i64 = 100 * 24 * 60 * 60 * 1_000;

    struct Rig {
        dir: PathBuf,
        store: Arc<SqliteRouteStateStore>,
        config: PathBuf,
    }

    impl Rig {
        fn new(label: &str) -> Self {
            let dir = cortexkit_test_support::scratch_root().join(format!(
                "task-profile-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("routing.sqlite");
            let store = Arc::new(
                SqliteRouteStateStore::open(&prefrontal_routing_store::dev_descriptor(
                    label,
                    path.to_string_lossy(),
                ))
                .unwrap(),
            );
            Self {
                config: dir.join("routing.jsonc"),
                dir,
                store,
            }
        }

        fn model(&self, provider: &str, model: &str) -> ModelFact {
            let mut fact = crate::tests::fact(provider, model);
            fact.family = Some("gpt".into());
            fact.cost_per_m_tok_out = Some(1.0);
            fact.input_rate = Some(1.0);
            fact.cache_read_rate = Some(1.0);
            fact.cache_write_rate = Some(1.0);
            fact.reasoning_rate = Some(1.0);
            self.store.upsert_fact(&fact, FactSource::Override).unwrap();
            fact
        }

        fn task(&self, key: &str, provider: &str, model: &str, iq: f64, tokens: f64) {
            self.store
                .record_decision_with_task_ref(
                    &DecisionInput {
                        decision_id: Some(key.into()),
                        consumer: Some("mason".into()),
                        target_agent: "mason".into(),
                        selected_model_id: Some(model.into()),
                        requirements_json: serde_json::json!({"iq":iq}).to_string(),
                        ..Default::default()
                    },
                    None,
                    None,
                    Some(key),
                )
                .unwrap();
            self.usage(key, key, provider, model, tokens, NOW - 2 * TASK_CLOSE_MS);
        }

        fn usage(
            &self,
            id: &str,
            decision: &str,
            provider: &str,
            model: &str,
            tokens: f64,
            at: i64,
        ) {
            self.store
                .record_usage_delivery_for_decision(
                    &UsageObservation {
                        id: id.into(),
                        route_key: format!("{provider}/{model}"),
                        capacity_key: format!("{provider}/{model}"),
                        provider_id: provider.into(),
                        input_tokens: Some(tokens),
                        cached_read_tokens: Some(0.0),
                        cache_write_tokens: None,
                        output_tokens: tokens,
                        reasoning_tokens: Some(0.0),
                        duration_ms: 1_000.0,
                        workload_class: None,
                        account_key: None,
                        observed_at: at as f64,
                    },
                    decision,
                )
                .unwrap();
        }

        fn rebuild(&self, cache: &ProfileCache, now: i64) {
            refresh_cache(cache, &self.store, &self.config, now, None).unwrap();
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn empty_store_and_nineteen_tagged_tasks_remain_cold() {
        let rig = Rig::new("cold");
        let sol = rig.model("openai", "gpt-6-sol");
        let cache = ProfileCache::default();
        rig.rebuild(&cache, NOW);
        assert!(cache
            .snapshot()
            .lookup("mason", 75.0, std::slice::from_ref(&sol))
            .is_empty());
        for i in 0..19 {
            rig.task(&format!("t-{i}"), "openai", &sol.model_id, 75.0, 1_000.0);
        }
        rig.rebuild(&cache, NOW);
        assert!(cache.snapshot().lookup("mason", 75.0, &[sol]).is_empty());
    }

    #[test]
    fn iq_demands_read_their_own_sol_buckets_and_lookup_never_reads_the_store() {
        let rig = Rig::new("buckets");
        let sol = rig.model("openai", "gpt-6-sol");
        for i in 0..20 {
            rig.task(&format!("high-{i}"), "openai", &sol.model_id, 75.0, 7_000.0);
            rig.task(&format!("low-{i}"), "openai", &sol.model_id, 45.0, 4_000.0);
        }
        rig.rebuild(cache(), NOW);
        let raw = rusqlite::Connection::open(rig.dir.join("routing.sqlite")).unwrap();
        raw.execute_batch("DROP TABLE model_route_usage; DROP TABLE model_route_decisions;")
            .unwrap();
        for (iq, expected) in [(75.0, 7_000.0), (45.0, 4_000.0)] {
            let lookup = crate::select::task_profiles_for("mason", iq, std::slice::from_ref(&sol));
            let profile = &lookup.profiles["openai/gpt-6-sol"];
            assert_eq!(profile.level, ProfileLevel::OwnCell);
            assert_eq!(profile.task_count, 20);
            assert_eq!(profile.tokens.output_excluding_reasoning, expected);
            assert_eq!(
                lookup.providers_without_cache_write_reports,
                HashSet::from(["openai".into()])
            );
        }
        assert!(crate::select::task_profiles_for("sidekick", 75.0, &[sol])
            .profiles
            .is_empty());
    }

    #[test]
    fn first_refresh_stays_cold_until_the_complete_map_is_published() {
        let rig = Rig::new("first-publish");
        let sol = rig.model("openai", "gpt-6-sol");
        for i in 0..20 {
            rig.task(&format!("t-{i}"), "openai", &sol.model_id, 75.0, 1_000.0);
        }
        let cache = Arc::new(ProfileCache::default());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let worker_cache = Arc::clone(&cache);
        let store = Arc::clone(&rig.store);
        let path = rig.config.clone();
        let worker = std::thread::spawn(move || {
            let next = refresh(&store, &path, NOW, None).unwrap();
            ready_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            worker_cache.publish(next);
        });
        ready_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert!(cache
            .snapshot()
            .lookup("mason", 75.0, std::slice::from_ref(&sol))
            .is_empty());
        finish_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(cache.snapshot().lookup("mason", 75.0, &[sol]).len(), 1);
    }

    #[test]
    fn refresh_rebuilds_totals_removes_reopened_and_cross_provider_tasks_and_expires_the_window() {
        let rig = Rig::new("rebuild");
        let sol = rig.model("openai", "gpt-6-sol");
        let cache = ProfileCache::default();
        for i in 0..20 {
            rig.task(&format!("t-{i}"), "openai", &sol.model_id, 75.0, 1_000.0);
        }
        rig.rebuild(&cache, NOW);
        let held = cache.snapshot();
        rig.usage("follow-up", "t-0", "openai", &sol.model_id, 1_000.0, NOW);
        rig.rebuild(&cache, NOW);
        assert!(cache
            .snapshot()
            .lookup("mason", 75.0, std::slice::from_ref(&sol))
            .is_empty());
        assert_eq!(
            held.lookup("mason", 75.0, std::slice::from_ref(&sol)).len(),
            1
        );
        for i in 1..20 {
            rig.usage(
                &format!("follow-up-{i}"),
                &format!("t-{i}"),
                "openai",
                &sol.model_id,
                1_000.0,
                NOW,
            );
        }
        rig.rebuild(&cache, NOW + TASK_CLOSE_MS);
        let complete = cache
            .snapshot()
            .lookup("mason", 75.0, std::slice::from_ref(&sol));
        assert_eq!(complete["openai/gpt-6-sol"].task_count, 20);
        // Both executions belong to one task, so every task now totals 2,000.
        assert_eq!(complete["openai/gpt-6-sol"].tokens.fresh_input, 2_000.0);
        rig.usage(
            "other-provider",
            "t-0",
            "other",
            &sol.model_id,
            1_000.0,
            NOW,
        );
        rig.rebuild(&cache, NOW + TASK_CLOSE_MS);
        assert!(cache
            .snapshot()
            .lookup("mason", 75.0, std::slice::from_ref(&sol))
            .is_empty());
        rig.rebuild(&cache, NOW + PROFILE_WINDOW_MS + 1);
        assert!(cache.snapshot().cells.is_empty());
    }

    #[test]
    fn refresh_errors_keep_the_last_good_snapshot() {
        let rig = Rig::new("failed");
        let sol = rig.model("openai", "gpt-6-sol");
        let cache = ProfileCache::default();
        for i in 0..20 {
            rig.task(&format!("t-{i}"), "openai", &sol.model_id, 75.0, 1_000.0);
        }
        rig.rebuild(&cache, NOW);
        let held = cache.snapshot();
        let raw = rusqlite::Connection::open(rig.dir.join("routing.sqlite")).unwrap();
        raw.execute_batch("DROP TABLE model_route_usage;").unwrap();
        assert!(refresh_cache(&cache, &rig.store, &rig.config, NOW, None).is_err());
        assert!(Arc::ptr_eq(&held, &cache.snapshot()));
    }

    #[test]
    fn published_and_observed_efforts_have_profiles_without_separate_fact_rows() {
        let rig = Rig::new("efforts");
        let sol = rig.model("openai", "gpt-6-sol");
        for i in 0..20 {
            rig.task(&format!("t-{i}"), "openai", &sol.model_id, 75.0, 1_000.0);
        }
        let raw = rusqlite::Connection::open(rig.dir.join("routing.sqlite")).unwrap();
        raw.execute(
            "UPDATE model_route_decisions SET selected_variant='high'",
            [],
        )
        .unwrap();
        raw.execute("UPDATE model_facts SET reasoning_efforts_json='[\"high\",\"low\"]' WHERE provider_id='openai' AND model_id='gpt-6-sol'", []).unwrap();
        let cache = ProfileCache::default();
        rig.rebuild(&cache, NOW);
        let mut high = sol.clone();
        high.variant = Some("high".into());
        let mut low = sol;
        low.variant = Some("low".into());
        let profiles = cache.snapshot().lookup("mason", 75.0, &[high.clone(), low]);
        assert_eq!(
            profiles["openai/gpt-6-sol/high"].level,
            ProfileLevel::OwnCell
        );
        assert_eq!(
            profiles["openai/gpt-6-sol/low"].level,
            ProfileLevel::SameFamilyTier
        );
        assert_eq!(profiles.len(), 2);
        let missing = crate::tests::fact("unknown", "not-in-catalog");
        assert!(cache
            .snapshot()
            .lookup("mason", 75.0, &[high, missing])
            .is_empty());
    }

    #[test]
    fn expansion_keeps_curated_defaults_and_follows_catalog_aliases() {
        let mut generic = crate::tests::fact("catalog", "sol");
        generic.prompt_defaults =
            serde_json::from_value(serde_json::json!({"variant":"curated"})).ok();
        let mut exact = generic.clone();
        exact.variant = Some("high".into());
        exact.cost_per_m_tok_out = Some(123.0);
        let mut facts = vec![generic, exact];
        let aliases = std::collections::BTreeMap::from([("platform".into(), "catalog".into())]);
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &aliases);
        let catalog = ReasoningCatalog {
            models: std::collections::BTreeMap::from([(
                "catalog/sol".into(),
                PublishedEfforts::Published(vec!["high".into(), "low".into()]),
            )]),
        };
        expand_variants(&mut facts, &[], &catalog, &aliases);
        for provider in ["catalog", "platform"] {
            for variant in ["curated", "high", "low"] {
                assert!(
                    facts.iter().any(|fact| fact.provider_id == provider
                        && fact.variant.as_deref() == Some(variant)),
                    "{provider}/{variant}"
                );
            }
        }
        assert_eq!(
            facts
                .iter()
                .find(
                    |fact| fact.provider_id == "catalog" && fact.variant.as_deref() == Some("high")
                )
                .unwrap()
                .cost_per_m_tok_out,
            Some(123.0)
        );
    }
}
