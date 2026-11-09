use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as SyncMutex;

pub(crate) const ID: &str = "pj-settings";

pub(crate) struct Fake {
    pub state: SyncMutex<Value>,
    pub calls: SyncMutex<Vec<Value>>,
    pub fail: std::sync::atomic::AtomicBool,
    pub resolutions: SyncMutex<HashMap<String, Value>>,
}

impl Fake {
    pub(crate) fn new(census: Value) -> Arc<Self> {
        Arc::new(Self {
            state: SyncMutex::new(census),
            calls: SyncMutex::new(Vec::new()),
            fail: Default::default(),
            resolutions: Default::default(),
        })
    }

    fn count(&self, method: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|body| body["method"] == method)
            .count()
    }
}

#[async_trait::async_trait]
impl Source for Fake {
    async fn request(&self, body: Value) -> Result<Value, String> {
        self.calls.lock().unwrap().push(body.clone());
        tokio::task::yield_now().await;
        if self.fail.load(Ordering::Relaxed) {
            return Err("offline".into());
        }
        let census = self.state.lock().unwrap();
        if body["method"] == "enumerate" {
            assert_eq!(body, json!({"method":"enumerate","params":{}}));
            return Ok(census.clone());
        }
        assert_eq!(body["method"], "resolve_project_id");
        let id = body["params"]["projectId"].as_str().unwrap();
        if let Some(reply) = self.resolutions.lock().unwrap().get(id) {
            return Ok(reply.clone());
        }
        Ok(
            json!({"result":{"projectId":id,"gone":false,"via":"current",
            "generation":census["result"]["generation"],"incarnation":census["result"]["incarnation"]}}),
        )
    }
}

pub(crate) fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = cortexkit_test_support::scratch_root().join(format!(
        "identity-settings-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

pub(crate) fn write(root: &Path, body: &str) {
    std::fs::create_dir_all(root.join(".cortexkit")).unwrap();
    std::fs::write(root.join(".cortexkit/prefrontal-routing.jsonc"), body).unwrap();
}

pub(crate) fn census(projects: &[PathBuf], workspace: Option<&Path>) -> Value {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../../test-vectors/project-resolve-v1/enumerate.json"
    ))
    .unwrap();
    let mut reply = vectors[0]["reply"].clone();
    reply["result"]["projects"] = json!([{"projectId":ID,"name":"Settings", "implicit":false,"refKind":"local",
        "workspaceId":"ws-dashboard", "roots":projects,
        "rootRecords":projects.iter().map(|root| json!({"root":root,"identity":"bound",
            "approval":{"state":"approved","journalSeq":4}, "remotes":[],
            "incarnation":{"kind":"token","value":"0123456789abcdef"},"registrationEpoch":"epoch"})).collect::<Vec<_>>() }]);
    reply["result"]["workspaces"][0]["root"] = json!(workspace);
    reply
}

fn tier(layers: &Layers, model: &str) -> Option<cortexkit_role_routing::service_tier::ServiceTier> {
    layers.resolve(
        &crate::config_overrides::LiveConfig::default(),
        "worker",
        "p",
        model,
        None,
    )
}

#[tokio::test]
async fn applies_project_over_workspace_with_both_contributing() {
    use cortexkit_role_routing::service_tier::ServiceTier::*;
    let project = root();
    let workspace = root();
    write(&project, r#"{"models":{"p/m":{"service_tier":"fast"}}}"#);
    write(
        &workspace,
        r#"{"providers":{"p":{"service_tier":"ultrafast"}}}"#,
    );
    let resolver = Resolver::new(Fake::new(census(&[project], Some(&workspace))));
    let (layers, status) = resolver.layers(ID, 1).await;
    assert_eq!(status, ProjectSettings::Applied);
    assert_eq!(tier(&layers, "m"), Some(Fast));
    assert_eq!(tier(&layers, "other"), Some(Ultrafast));
}

#[tokio::test]
async fn no_eligible_file_is_none() {
    let resolver = Resolver::new(Fake::new(census(&[root()], None)));
    assert_eq!(resolver.layers(ID, 1).await.1, ProjectSettings::None);
}

#[tokio::test]
async fn cold_outage_is_unresolved() {
    let source = Fake::new(census(&[], None));
    source.fail.store(true, Ordering::Relaxed);
    assert_eq!(
        Resolver::new(source).layers(ID, 1).await.1,
        ProjectSettings::Unresolved
    );
}

#[tokio::test]
async fn warm_outage_uses_cache_until_ten_minute_bound() {
    let project = root();
    write(&project, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
    let source = Fake::new(census(&[project], None));
    let resolver = Resolver::new(source.clone());
    assert_eq!(resolver.layers(ID, 1).await.1, ProjectSettings::Applied);
    source.fail.store(true, Ordering::Relaxed);
    assert_eq!(
        resolver.layers(ID, MAX_STALE_MS + 1).await.1,
        ProjectSettings::Applied
    );
    let (layers, status) = resolver.layers(ID, MAX_STALE_MS + 2).await;
    assert_eq!(status, ProjectSettings::Unresolved);
    assert_eq!(tier(&layers, "m"), None);
}

#[tokio::test]
async fn unknown_id_rereads_once_and_retries_after_generation_moves() {
    let source = Fake::new(census(&[], None));
    let resolver = Resolver::new(source.clone());
    assert_eq!(
        resolver.layers("missing", 1).await.1,
        ProjectSettings::Unresolved
    );
    assert_eq!(source.count("enumerate"), 2);
    assert_eq!(
        resolver.layers("missing", 2).await.1,
        ProjectSettings::Unresolved
    );
    assert_eq!(source.count("enumerate"), 2);
    source.state.lock().unwrap()["result"]["generation"] = json!(3);
    assert_eq!(
        resolver.layers("missing", REFRESH_MS + 1).await.1,
        ProjectSettings::Unresolved
    );
    assert_eq!(source.count("enumerate"), 4);
}

#[tokio::test]
async fn malformed_file_is_unresolved_without_workspace_tiers() {
    let project = root();
    let workspace = root();
    write(&project, "broken");
    write(&workspace, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
    let (layers, status) = Resolver::new(Fake::new(census(&[project], Some(&workspace))))
        .layers(ID, 1)
        .await;
    assert_eq!(status, ProjectSettings::Unresolved);
    assert_eq!(tier(&layers, "m"), None);
}

#[tokio::test]
async fn unreadable_file_is_unresolved() {
    let project = root();
    std::fs::create_dir_all(project.join(".cortexkit/prefrontal-routing.jsonc")).unwrap();
    let status = Resolver::new(Fake::new(census(&[project], None)))
        .layers(ID, 1)
        .await
        .1;
    assert_eq!(status, ProjectSettings::Unresolved);
}

#[tokio::test]
async fn conflicting_project_documents_are_unresolved() {
    let a = root();
    let b = root();
    write(&a, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
    write(&b, r#"{"providers":{"p":{"service_tier":"standard"}}}"#);
    assert_eq!(
        Resolver::new(Fake::new(census(&[a, b], None)))
            .layers(ID, 1)
            .await
            .1,
        ProjectSettings::Unresolved
    );
}

#[tokio::test]
async fn unbound_or_unapproved_roots_never_supply_settings() {
    let a = root();
    let b = root();
    write(&a, "broken");
    write(&b, "broken");
    let mut value = census(&[a, b], None);
    value["result"]["projects"][0]["rootRecords"][0]["identity"] = json!("unbound");
    value["result"]["projects"][0]["rootRecords"][1]["approval"]["state"] = json!("unapproved");
    assert_eq!(
        Resolver::new(Fake::new(value)).layers(ID, 1).await.1,
        ProjectSettings::None
    );
}

#[tokio::test]
async fn served_resolve_project_id_vectors_cover_current_alias_and_gone() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../../test-vectors/project-resolve-v1/resolve-project-id.json"
    ))
    .unwrap();
    for case in vectors.as_array().unwrap() {
        let project = root();
        write(&project, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
        let mut value = census(&[project], None);
        value["result"]["generation"] = json!(4);
        value["result"]["projects"][0]["projectId"] = case["reply"]["result"]["projectId"].clone();
        let source = Fake::new(value);
        let id = case["request"]["params"]["projectId"].as_str().unwrap();
        source
            .resolutions
            .lock()
            .unwrap()
            .insert(id.into(), case["reply"].clone());
        let resolver = Resolver::new(source.clone());
        let expected = if case["reply"]["result"]["gone"] == true {
            ProjectSettings::Unresolved
        } else {
            ProjectSettings::Applied
        };
        assert_eq!(resolver.layers(id, 1).await.1, expected);
        assert_eq!(source.calls.lock().unwrap()[0], case["request"]);
        assert_eq!(resolver.layers(id, 2).await.1, expected);
        assert_eq!(source.count("resolve_project_id"), 1);
    }
    let enumerate: Value = serde_json::from_str(include_str!(
        "../../../../test-vectors/project-resolve-v1/enumerate.json"
    ))
    .unwrap();
    assert_eq!(
        Census::decode(enumerate[0]["reply"].clone())
            .unwrap()
            .reply
            .projects
            .len(),
        2
    );
}

async fn invalidation(mut change: impl FnMut(&mut Value, &Path)) {
    let a = root();
    let b = root();
    write(&a, r#"{"providers":{"p":{"service_tier":"standard"}}}"#);
    write(&b, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
    let source = Fake::new(census(&[a], None));
    let resolver = Resolver::new(source.clone());
    assert_eq!(
        tier(&resolver.layers(ID, 1).await.0, "m"),
        Some(cortexkit_role_routing::service_tier::ServiceTier::Standard)
    );
    change(&mut source.state.lock().unwrap(), &b);
    assert_eq!(
        tier(&resolver.layers(ID, REFRESH_MS + 1).await.0, "m"),
        Some(cortexkit_role_routing::service_tier::ServiceTier::Fast)
    );
    assert_eq!(source.count("enumerate"), 2);
}

fn move_root(value: &mut Value, root: &Path) {
    value["result"]["projects"][0]["roots"] = json!([root]);
    value["result"]["projects"][0]["rootRecords"][0]["root"] = json!(root);
}

#[tokio::test]
async fn incarnation_invalidates_census_and_alias_answers() {
    invalidation(|value, root| {
        move_root(value, root);
        value["result"]["incarnation"] = json!("new-incarnation");
    })
    .await;
}

#[tokio::test]
async fn generation_invalidates_census_and_alias_answers() {
    invalidation(|value, root| {
        move_root(value, root);
        value["result"]["generation"] = json!(9);
    })
    .await;
}

#[tokio::test]
async fn roots_replace_the_census_at_the_same_generation() {
    invalidation(move_root).await;
}

#[tokio::test]
async fn workspace_folder_replaces_the_census_at_the_same_generation() {
    invalidation(|value, root| {
        value["result"]["projects"][0]["roots"] = json!([]);
        value["result"]["projects"][0]["rootRecords"] = json!([]);
        value["result"]["workspaces"][0]["root"] = json!(root);
    })
    .await;
}

#[tokio::test]
async fn project_metadata_replaces_the_census_at_the_same_generation() {
    invalidation(|value, root| {
        move_root(value, root);
        value["result"]["projects"][0]["workspaceId"] = Value::Null;
        value["result"]["projects"][0]["name"] = json!("renamed");
    })
    .await;
}

#[tokio::test]
async fn concurrent_requests_share_one_refresh_and_cache_reuse() {
    let source = Fake::new(census(&[], None));
    let resolver = Arc::new(Resolver::new(source.clone()));
    let (a, b, c) = tokio::join!(
        resolver.layers(ID, 1),
        resolver.layers(ID, 1),
        resolver.layers(ID, 1)
    );
    for status in [a.1, b.1, c.1] {
        assert_eq!(status, ProjectSettings::None);
    }
    assert_eq!(source.count("enumerate"), 1);
    assert_eq!(source.count("resolve_project_id"), 1);
    assert_eq!(
        resolver.layers(ID, REFRESH_MS).await.1,
        ProjectSettings::None
    );
    assert_eq!(source.count("enumerate"), 1);
}

struct HangingSource {
    inner: Arc<Fake>,
    hang: std::sync::atomic::AtomicBool,
    calls: AtomicU64,
}

#[async_trait::async_trait]
impl Source for HangingSource {
    async fn request(&self, body: Value) -> Result<Value, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.hang.load(Ordering::Relaxed) {
            std::future::pending().await
        } else {
            self.inner.request(body).await
        }
    }
}

struct WarningCounter(Arc<AtomicU64>);

impl tracing::Subscriber for WarningCounter {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn never_answering_source_and_concurrent_waiter_have_independent_two_second_deadlines() {
    assert_eq!(SETTINGS_DEADLINE, Duration::from_secs(2));
    let warnings = Arc::new(AtomicU64::new(0));
    let _subscriber = tracing::subscriber::set_default(WarningCounter(warnings.clone()));
    let source = Arc::new(HangingSource {
        inner: Fake::new(census(&[], None)),
        hang: true.into(),
        calls: AtomicU64::new(0),
    });
    let resolver = Resolver::new(source.clone());
    let read = || async {
        let started = Instant::now();
        let result = resolver.layers(ID, 1).await;
        (result, started.elapsed())
    };
    let limit = Duration::from_millis(2500);
    let (first, second) = tokio::time::timeout(limit, async { tokio::join!(read(), read()) })
        .await
        .expect("a stalled source must not outlive each caller's settings deadline");
    for ((layers, status), elapsed) in [first, second] {
        assert_eq!(status, ProjectSettings::Unresolved);
        assert_eq!(tier(&layers, "m"), None);
        assert!(elapsed <= limit, "caller waited {elapsed:?}");
    }
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        1,
        "waiters must share the timed-out flight"
    );
    assert_eq!(
        warnings.load(Ordering::Relaxed),
        1,
        "one warning for this project outage"
    );
}

#[tokio::test]
async fn timed_out_refresh_returns_empty_layers_even_with_a_warm_cache() {
    let project = root();
    write(&project, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
    let source = Arc::new(HangingSource {
        inner: Fake::new(census(&[project], None)),
        hang: false.into(),
        calls: AtomicU64::new(0),
    });
    let resolver = Resolver::new(source.clone());
    assert_eq!(resolver.layers(ID, 1).await.1, ProjectSettings::Applied);
    source.hang.store(true, Ordering::Relaxed);
    let limit = Duration::from_millis(2500);
    let ((layers, status), (_, waiter_status)) = tokio::time::timeout(limit, async {
        tokio::join!(
            resolver.layers(ID, REFRESH_MS + 1),
            resolver.layers(ID, REFRESH_MS + 1)
        )
    })
    .await
    .expect("a warm-cache refresh and its waiter have bounded latency");
    assert_eq!(status, ProjectSettings::Unresolved);
    assert_eq!(waiter_status, ProjectSettings::Unresolved);
    assert_eq!(tier(&layers, "m"), None);
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        3,
        "one initial resolve/enumerate pair and one shared refresh"
    );
}

#[tokio::test]
async fn registry_errors_log_once_per_project_outage_and_rearm_after_recovery() {
    let warnings = Arc::new(AtomicU64::new(0));
    let _subscriber = tracing::subscriber::set_default(WarningCounter(warnings.clone()));
    let source = Fake::new(census(&[], None));
    let resolver = Resolver::new(source.clone());
    source.fail.store(true, Ordering::Relaxed);
    for now in [1, 2] {
        assert_eq!(
            resolver.layers(ID, now).await.1,
            ProjectSettings::Unresolved
        );
    }
    assert_eq!(warnings.load(Ordering::Relaxed), 1);
    source.fail.store(false, Ordering::Relaxed);
    assert_eq!(resolver.layers(ID, 3).await.1, ProjectSettings::None);
    source.fail.store(true, Ordering::Relaxed);
    for now in [REFRESH_MS + 3, 2 * REFRESH_MS + 3] {
        assert_eq!(resolver.layers(ID, now).await.1, ProjectSettings::None);
    }
    assert_eq!(
        warnings.load(Ordering::Relaxed),
        2,
        "warm-cache fallback must not rearm warnings while the registry is still unavailable"
    );
}

#[tokio::test]
async fn a_cache_waiter_has_its_own_deadline_before_any_request_can_start() {
    let source = Fake::new(census(&[], None));
    let resolver = Resolver::new(source.clone());
    let _held = resolver.cache.lock().await;
    let started = Instant::now();
    let (layers, status) =
        tokio::time::timeout(Duration::from_millis(2500), resolver.layers(ID, 1))
            .await
            .expect("cache contention must not outlive the waiting caller's deadline");
    assert_eq!(status, ProjectSettings::Unresolved);
    assert_eq!(tier(&layers, "m"), None);
    assert!(started.elapsed() <= Duration::from_millis(2500));
    assert!(source.calls.lock().unwrap().is_empty());
}
