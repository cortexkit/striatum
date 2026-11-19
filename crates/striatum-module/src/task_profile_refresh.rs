//! Background-only task aggregation and atomic publication for store-free selects.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use striatum_core::model_router::RouteTaskProfiles;
use striatum_core::task_profile::{
    iq_bucket, ClosedTask, TaskProfileIndex, TokenVector, IQ_BUCKET_WIDTH, PROFILE_REFRESH_MS,
};
use striatum_core::{make_route_key, ModelFact, RouteStateStore};
use striatum_store::{ClosedTaskUsage, PublishedEfforts, ReasoningCatalog, SqliteRouteStateStore};

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

#[cfg(test)]
pub(crate) fn publish_snapshot_for_test(snapshot: ProfileSnapshot) {
    cache().publish(snapshot);
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
        let Some(cell) = self.cells.get(&(
            striatum_core::module_identity::canonical(workload).into_owned(),
            bucket,
        )) else {
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

pub(crate) fn build_snapshot(rows: &[ClosedTaskUsage], facts: &[ModelFact]) -> ProfileSnapshot {
    let tasks = closed_tasks(rows, facts);
    let index = TaskProfileIndex::new(&tasks);
    build_cells(&tasks, facts, |fact, workload, bucket| {
        index.compute_profile(fact, workload, bucket)
    })
}

fn closed_tasks<'a>(rows: &'a [ClosedTaskUsage], facts: &'a [ModelFact]) -> Vec<ClosedTask<'a>> {
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
    rows.iter()
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
        .collect()
}

fn build_cells(
    tasks: &[ClosedTask<'_>],
    facts: &[ModelFact],
    profile: impl Fn(&ModelFact, &str, u8) -> Option<striatum_core::task_profile::TaskProfile>,
) -> ProfileSnapshot {
    let workloads: HashSet<_> = tasks.iter().map(|task| task.workload).collect();
    let mut snapshot = ProfileSnapshot::default();
    for workload in workloads {
        for bucket in 0..=100 / IQ_BUCKET_WIDTH {
            let cell: RouteTaskProfiles = facts
                .iter()
                .filter_map(|fact| {
                    profile(fact, workload, bucket).map(|profile| {
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
                snapshot.cells.insert(
                    (
                        striatum_core::module_identity::canonical(workload).into_owned(),
                        bucket,
                    ),
                    cell,
                );
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
    striatum_core::score_alias::append_alias_rows(&mut facts, &aliases);
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
        let next = striatum_store::job_timing::run(
            "task_profile_refresh",
            crate::stage_timing::spawn_job_blocking(move || {
                refresh_cache(
                    cache(),
                    &store,
                    &path,
                    crate::now_ms() as i64,
                    Some(&credentials),
                )
            }),
        )
        .await;
        match next {
            Ok(Ok(())) => {}
            Ok(Err(error)) => crate::warn_operator(format!(
                "[striatum] task profile refresh failed; keeping last snapshot: {error}"
            )),
            Err(error) => crate::warn_operator(format!(
                "[striatum] task profile refresh worker failed; keeping last snapshot: {error}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use striatum_core::task_profile::{ProfileLevel, PROFILE_WINDOW_MS, TASK_CLOSE_MS};
    use striatum_core::{DecisionInput, FactSource, UsageObservation};

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
                SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
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
    #[ignore = "live-sized CPU comparison; run in release with --ignored --nocapture"]
    fn live_sized_profile_refresh_cpu() {
        live_sized_profile_refresh(false);
    }

    #[cfg(feature = "heap-measure")]
    #[test]
    #[ignore = "live-shaped profile-refresh heap profile; run alone"]
    fn live_shaped_profile_heap() {
        live_sized_profile_refresh(true);
    }

    fn live_sized_profile_refresh(heap: bool) {
        use crate::routing_v1::request_cpu_bench::{cpu_us, representative_fixture, FixtureCounts};
        let fixture = representative_fixture(FixtureCounts {
            subscription_rules: 8,
            exclusion_rules: 8,
            runner_route_rules: 8,
            catalog_facts: if heap { 852 } else { 838 },
            listed_credentials: 40,
        });
        let mut facts = fixture.store().get_facts().unwrap();
        for (i, fact) in facts.iter_mut().enumerate() {
            fact.input_rate = Some(1.0 + (i % 7) as f64);
            fact.cache_read_rate = Some(0.1);
            fact.cache_write_rate = Some(1.25);
            fact.cost_per_m_tok_out = Some(2.0 + (i % 5) as f64);
            fact.reasoning_rate = Some(3.0);
        }
        assert_eq!(facts.len(), if heap { 852 } else { 838 });
        let rig = Rig::new("live-sized-cpu");
        let mut connection = rusqlite::Connection::open(rig.dir.join("routing.sqlite")).unwrap();
        connection.execute("DELETE FROM model_facts", []).unwrap();
        for fact in &facts {
            rig.store.upsert_fact(fact, FactSource::Seed).unwrap();
        }
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        tx.execute_batch("CREATE TEMP TABLE cpu_models(n INTEGER PRIMARY KEY,provider TEXT,model TEXT,variant TEXT,route TEXT);").unwrap();
        for (n, fact) in facts.iter().step_by(12).take(67).enumerate() {
            tx.execute(
                "INSERT INTO cpu_models VALUES(?1,?2,?3,?4,?5)",
                rusqlite::params![
                    n,
                    fact.provider_id,
                    fact.model_id,
                    fact.variant,
                    make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                ],
            )
            .unwrap();
        }
        // Match the measured thirty-day row counts. Repeated executions of a
        // task keep one model, workload and IQ demand so they remain eligible.
        tx.execute(
            &format!("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<{})
             INSERT INTO model_route_decisions(id,task_id,target_agent,consumer,selected_workload,requirements_json,selected_route_key,selected_model_id,selected_variant,score_json,rejected_json,anchor_version,created_at)
             SELECT 'd-'||x,'task-'||(x%16817),'mason','workload-'||((x%16817)%3),'ephemeral',
                    json_object('iq',((x%16817)%11)*10),m.route,m.model,m.variant,'{{}}','[]',1,?1
              FROM n JOIN cpu_models m ON m.n=(x%16817)%67",if heap {175701} else {181264}), [NOW - 3 * 24 * 60 * 60 * 1000],
        ).unwrap();
        tx.execute(
            &format!("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<{})
             INSERT INTO model_route_usage(id,task_id,task_workload,workload_class,route_key,capacity_key,provider_id,input_tokens,cached_read_tokens,cache_write_tokens,output_tokens,reasoning_tokens,observed_at)
             SELECT 'u-'||x,'task-'||(x%10530),'workload-'||((x%10530)%3),
                    CASE x%3 WHEN 0 THEN 'ephemeral' WHEN 1 THEN 'coding' ELSE 'reasoning' END,
                    m.route,m.route,m.provider,1000+x%100,100,50,500+x%50,100,?1-x%17*1000
              FROM n JOIN cpu_models m ON m.n=(x%10530)%67",if heap {707464} else {726485}), [NOW - 2 * 24 * 60 * 60 * 1000],
        ).unwrap();
        tx.commit().unwrap();
        #[cfg(feature = "heap-measure")]
        if heap {
            use crate::memory_measurements::{operation, profiler, release};
            if std::env::var_os("STRIATUM_NATIVE_RSS").is_some() {
                let snapshot =
                    crate::memory_measurements::native_rss("task_profile_refresh", || {
                        refresh(&rig.store, &rig.config, NOW, None).unwrap()
                    });
                println!("COUNTS profile_cells={} usage_rows=707465 decisions=175702 facts=852 tasks=10530",snapshot.cells.len());
                return;
            }
            let _profiler = profiler("profile-refresh");
            let snapshot = operation("task_profile_refresh", || {
                refresh(&rig.store, &rig.config, NOW, None).unwrap()
            });
            println!(
                "COUNTS profile_cells={} usage_rows=707465 decisions=175702 facts=852 tasks=10530",
                snapshot.cells.len()
            );
            let cache = ProfileCache::default();
            cache.publish(snapshot);
            let reader = operation("ProfileCache::snapshot", || cache.snapshot());
            release("ProfileSnapshot_reader_extra", reader);
            release("ProfileCache_and_snapshot", cache);
            let rows = operation("closed_task_usage", || {
                rig.store.closed_task_usage(NOW).unwrap()
            });
            release("profile_input_10530_tasks", rows);
            return;
        }
        let read_cpu = cpu_us();
        let rows = rig.store.closed_task_usage(NOW).unwrap();
        eprintln!("profile input read: cpu_us={} tasks={} usage_rows=726486 decisions=181265 facts=838 workloads=3 active_routes=67", cpu_us()-read_cpu, rows.len());
        assert_eq!(rows.len(), 10_530);
        let before = cpu_us();
        let tasks = closed_tasks(&rows, &facts);
        let reference = build_cells(&tasks, &facts, |fact, workload, bucket| {
            striatum_core::task_profile::compute_profile(&tasks, fact, workload, bucket)
        });
        let old_cpu = cpu_us() - before;
        let before = cpu_us();
        let mut optimized = None;
        for _ in 0..10 {
            optimized = Some(std::hint::black_box(build_snapshot(&rows, &facts)));
        }
        let new_cpu = (cpu_us() - before) / 10;
        assert!(!reference.cells.is_empty());
        assert_eq!(reference.cells, optimized.unwrap().cells);
        let before = cpu_us();
        let refreshed = refresh(&rig.store, &rig.config, NOW, None).unwrap();
        let refresh_cpu = cpu_us() - before;
        assert_eq!(refreshed.cells, reference.cells);
        eprintln!("profile aggregation: before_cpu_us={old_cpu} after_cpu_us={new_cpu}; optimized_end_to_end_refresh_cpu_us={refresh_cpu}");
        assert!(
            new_cpu < 500_000,
            "aggregation exceeded 0.5s CPU: {new_cpu} us"
        );
    }

    #[test]
    fn prefrontal_rename_snapshot_prices_new_requests_from_split_history() {
        let rig = Rig::new("rename-snapshot");
        let fact = rig.model("p", "sol");
        for i in 0..40 {
            rig.task(&format!("t-{i}"), "p", "sol", 75.0, (i + 1) as f64);
        }
        let mut rows = rig.store.closed_task_usage(NOW).unwrap();
        for row in &mut rows {
            row.workload = "prefrontal-core.mason".into();
        }
        let all_old = build_snapshot(&rows, std::slice::from_ref(&fact));
        let expected = all_old.lookup("prefrontal-core.mason", 75.0, std::slice::from_ref(&fact));
        assert_eq!(expected.len(), 1);
        for row in &mut rows[20..] {
            row.workload = "prefrontal.mason".into();
        }
        let mixed = build_snapshot(&rows, std::slice::from_ref(&fact));
        assert_eq!(
            mixed.lookup("prefrontal.mason", 75.0, std::slice::from_ref(&fact)),
            expected
        );
        assert_eq!(all_old.lookup("prefrontal.mason", 75.0, &[fact]), expected);
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
    fn catalog_workload_and_observed_efforts_have_profiles_without_separate_fact_rows() {
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
        let catalog = crate::fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&serde_json::json!({
                "catalog_version": 1, "resolved_at_ms": NOW,
                "models": {"openai/gpt-6-sol": {
                "existence":"present",
                    "capability.reasoning_options":[{"type":"effort","values":["high","low"]}],
                    "quality.graded_effort":"high",
                    "capability.reasoning_default":{"type":"effort","value":"high"},
                    "workload.code.effort_default":"low"
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        let (updates, _) = catalog.parts();
        crate::routing_v1::effort_preview::seed_for_test(
            rig.dir.join("routing.sqlite"),
            "openai/gpt-6-sol",
            updates[0].catalog_efforts.clone().unwrap(),
        );
        let efforts = crate::routing_v1::effort_preview::Efforts {
            models: rig.store.get_selection_catalog().unwrap().efforts.clone(),
            workloads: Default::default(),
        };
        let choice = efforts
            .resolve(&sol, &Default::default(), Some("code"), false, None)
            .unwrap();
        assert_eq!(choice.variant.as_deref(), Some("low"));
        let cache = ProfileCache::default();
        rig.rebuild(&cache, NOW);
        let mut high = sol.clone();
        high.variant = Some("high".into());
        let mut low = sol;
        low.variant = choice.variant;
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
    fn expansion_uses_published_options_and_follows_catalog_aliases() {
        let generic = crate::tests::fact("catalog", "sol");
        let mut exact = generic.clone();
        exact.variant = Some("high".into());
        exact.cost_per_m_tok_out = Some(123.0);
        let mut facts = vec![generic, exact];
        let aliases = std::collections::BTreeMap::from([("platform".into(), "catalog".into())]);
        striatum_core::score_alias::append_alias_rows(&mut facts, &aliases);
        let catalog = ReasoningCatalog {
            models: std::collections::BTreeMap::from([(
                "catalog/sol".into(),
                PublishedEfforts::Published(vec!["high".into(), "low".into()]),
            )]),
        };
        expand_variants(&mut facts, &[], &catalog, &aliases);
        for provider in ["catalog", "platform"] {
            assert!(
                !facts.iter().any(|fact| fact.provider_id == provider
                    && fact.variant.as_deref() == Some("curated")),
                "{provider}/curated is not published"
            );
            for variant in ["high", "low"] {
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
