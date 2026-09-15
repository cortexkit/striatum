//! Artificial Analysis index ingest and `aa_inferred` seeding.
//!
//! The API key is used only as an HTTP header. It is never written to a log
//! line, never interpolated into an error string, and never placed on a wire
//! reply. Fetch failure is fail-open: the last cached table stays, and the
//! cycle logs a key-free one-liner.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use prefrontal_ckcred_client::{
    CredentialErrorClass, CredentialResolver, CredentialResolverError, ResolvedCredential,
};
use prefrontal_routing_core::model_facts::{Confidence, ModelFact, ModelScores, ScoreProvenance};
use prefrontal_routing_core::{
    aa_elo_curve, fact_to_route, infer_elo_from_aa, inherit_eq_from_curated_neighbors,
    interpolate_elo, make_route_key, FactSource, RouteStateStore, AA_INFERRED_DEFAULT_SPEED,
};
use prefrontal_routing_store::SqliteRouteStateStore;
use serde::Deserialize;
use serde_json::{json, Value};

#[cfg(test)]
use crate::config_overrides::DEFAULT_AA_API_KEY_REF;
use crate::config_overrides::{self, AaConfig};

const AA_MODELS_FREE_URL: &str = "https://artificialanalysis.ai/api/v2/language/models/free";
const AA_MODELS_URL: &str = "https://artificialanalysis.ai/api/v2/language/models";
pub const AA_INGEST_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedAaRefusal {
    NoAaMapping,
    AaIndexAbsent,
    ExistingCuratedOrOverride,
    StoreReadFailed,
}

impl SeedAaRefusal {
    pub fn as_str(&self) -> &'static str {
        match self {
            SeedAaRefusal::NoAaMapping => "no_aa_mapping",
            SeedAaRefusal::AaIndexAbsent => "aa_index_absent",
            SeedAaRefusal::ExistingCuratedOrOverride => "existing_curated_or_override",
            SeedAaRefusal::StoreReadFailed => "store_read_failed",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            SeedAaRefusal::NoAaMapping => "no explicit aa.model_map entry for this router key",
            SeedAaRefusal::AaIndexAbsent => "AA index cache has no score for the mapped slug",
            SeedAaRefusal::ExistingCuratedOrOverride => {
                "a curated or override score already exists for this route"
            }
            SeedAaRefusal::StoreReadFailed => "the routing catalog could not be read",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeedAaOk {
    pub provenance: ScoreProvenance,
    pub aa_index: f64,
    pub elo: f64,
    pub eq: f64,
    pub review_by_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeedAaParams {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(default)]
    pub variant: Option<String>,
}

/// Parse an AA models payload into (slug, intelligence index) pairs.
/// Accepts both the current `/language/models` envelope and a bare array.
pub fn parse_aa_models(value: &Value) -> Vec<(String, f64)> {
    let items = if let Some(data) = value.get("data").and_then(Value::as_array) {
        data.clone()
    } else if let Some(array) = value.as_array() {
        array.clone()
    } else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let slug = item
            .get("slug")
            .or_else(|| item.get("id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|slug| !slug.is_empty());
        let index = item
            .pointer("/evaluations/artificial_analysis_intelligence_index")
            .or_else(|| item.get("artificial_analysis_intelligence_index"))
            .or_else(|| item.get("intelligence_index"))
            .and_then(Value::as_f64);
        if let (Some(slug), Some(index)) = (slug, index) {
            if index.is_finite() {
                out.push((slug.to_string(), index));
            }
        }
    }
    out
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Seed an `aa_inferred` row for one router key. Never logs the AA key.
pub fn seed_aa(
    store: &SqliteRouteStateStore,
    params: &SeedAaParams,
    aa_config: &AaConfig,
    now_ms: i64,
) -> Result<SeedAaOk, SeedAaRefusal> {
    let route_key = make_route_key(
        &params.provider_id,
        &params.model_id,
        params.variant.as_deref(),
    );
    let Some(slug) = aa_config.model_map.get(&route_key).cloned() else {
        return Err(SeedAaRefusal::NoAaMapping);
    };
    let Some((aa_index, _)) = store
        .get_aa_index(&slug)
        .map_err(|_| SeedAaRefusal::StoreReadFailed)?
    else {
        return Err(SeedAaRefusal::AaIndexAbsent);
    };
    let existing = store
        .get_fact_by_route_key(&route_key)
        .map_err(|_| SeedAaRefusal::StoreReadFailed)?;
    if let Some(existing) = existing.as_ref() {
        if existing.score_provenance == ScoreProvenance::Curated
            || existing.score_provenance == ScoreProvenance::Override
        {
            return Err(SeedAaRefusal::ExistingCuratedOrOverride);
        }
    }
    let curve = aa_elo_curve();
    let elo = infer_elo_from_aa(aa_index, curve).ok_or(SeedAaRefusal::AaIndexAbsent)?;
    let elo_interp = interpolate_elo(aa_index, curve).unwrap_or(elo);
    let neighbors: Vec<(f64, f64, String)> = store
        .get_facts()
        .map_err(|_| SeedAaRefusal::StoreReadFailed)?
        .into_iter()
        .filter(|fact| fact.score_provenance == ScoreProvenance::Curated)
        .map(|fact| {
            let key = fact_to_route(&fact).route_key;
            (fact.scores.elo, fact.scores.eq, key)
        })
        .collect();
    let neighbor_refs: Vec<(f64, f64, &str)> = neighbors
        .iter()
        .map(|(elo, eq, key)| (*elo, *eq, key.as_str()))
        .collect();
    let (eq, eq_source) = inherit_eq_from_curated_neighbors(elo_interp, &neighbor_refs)
        .map(|(eq, source)| (eq, source.as_str()))
        .unwrap_or_else(|| (1_000.0, "unknown_default".to_string()));
    let review_by_ms = now_ms.saturating_add(aa_config.review_by_ms);
    let mut fact = existing.unwrap_or_else(|| ModelFact {
        speed_is_fleet_median: false,
        provider_id: params.provider_id.clone(),
        model_id: params.model_id.clone(),
        variant: params.variant.clone(),
        family: None,
        scores: ModelScores {
            elo,
            eq,
            speed: AA_INFERRED_DEFAULT_SPEED,
        },
        context_window: None,
        max_output_tokens: None,
        cost_tier: None,
        cost_per_m_tok_out: None,
        supports_vision: None,
        supports_tool_use: None,
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: ScoreProvenance::AaInferred,
        aa_index: Some(aa_index),
        review_by_ms: Some(review_by_ms),
        eq_source: Some(eq_source.clone()),
    });
    fact.scores.elo = elo;
    fact.scores.eq = eq;
    if fact.scores.speed <= 0.0 || !fact.scores.speed.is_finite() {
        fact.scores.speed = AA_INFERRED_DEFAULT_SPEED;
    }
    fact.score_provenance = ScoreProvenance::AaInferred;
    fact.aa_index = Some(aa_index);
    fact.review_by_ms = Some(review_by_ms);
    fact.eq_source = Some(eq_source);
    store
        .upsert_fact(&fact, FactSource::AaInferred)
        .map_err(|_| SeedAaRefusal::ExistingCuratedOrOverride)?;
    Ok(SeedAaOk {
        provenance: ScoreProvenance::AaInferred,
        aa_index,
        elo,
        eq,
        review_by_ms,
    })
}

pub fn seed_aa_result_json(ok: &SeedAaOk) -> Value {
    json!({
        "provenance": ok.provenance.as_str(),
        "aaIndex": ok.aa_index,
        "elo": ok.elo,
        "eq": ok.eq,
        "reviewByMs": ok.review_by_ms,
    })
}

pub fn list_inferred_json(store: &SqliteRouteStateStore, now_ms: i64) -> Result<Value, String> {
    let inferred = store
        .list_inferred_rows(now_ms)
        .map_err(|error| error.to_string())?;
    let unscored = store
        .list_unscored_rows()
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "inferred": inferred,
        "unscored": unscored,
    }))
}

/// Fetch the AA models index. The key is sent only as `x-api-key` and is never
/// copied into returned errors.
pub async fn fetch_aa_index(api_key: &str) -> Result<Vec<(String, f64)>, &'static str> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| "transport")?;
    for url in [AA_MODELS_FREE_URL, AA_MODELS_URL] {
        match fetch_one(&client, url, api_key).await {
            Ok(rows) if !rows.is_empty() => return Ok(rows),
            Ok(_) => continue,
            Err("http_status") => continue,
            Err(label) => return Err(label),
        }
    }
    Err("http_status")
}

async fn fetch_one(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
) -> Result<Vec<(String, f64)>, &'static str> {
    let response = client
        .get(url)
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(fetch_error_label)?;
    let status = response.status();
    if !status.is_success() {
        return Err("http_status");
    }
    let value: Value = response.json().await.map_err(|_| "decode")?;
    Ok(parse_aa_models(&value))
}

fn fetch_error_label(error: reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else {
        "transport"
    }
}

pub fn log_aa_ingest_failure(label: &str) {
    // Key-free on purpose: a cycle failure must not echo credentials.
    eprintln!("[prefrontal-routing] aa ingest failed ({label}); keeping last index table");
    tracing::warn!(label, "aa ingest failed; keeping last index table");
}

pub fn log_aa_ingest_skipped() {
    eprintln!("[prefrontal-routing] aa ingest skipped: no credential reference configured");
    tracing::warn!("aa ingest skipped: no credential reference configured");
}

fn log_aa_credential_failure(class: CredentialErrorClass) {
    let class = class.classifier_code();
    eprintln!("[prefrontal-routing] aa ingest credential unavailable ({class}); skipping cycle");
    tracing::warn!(class, "aa ingest credential unavailable; skipping cycle");
}

type AaFetchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<(String, f64)>, &'static str>> + Send + 'a>>;

trait AaIndexFetcher: Send + Sync {
    fn fetch<'a>(&'a self, api_key: &'a str) -> AaFetchFuture<'a>;
}

struct LiveAaIndexFetcher;

impl AaIndexFetcher for LiveAaIndexFetcher {
    fn fetch<'a>(&'a self, api_key: &'a str) -> AaFetchFuture<'a> {
        Box::pin(fetch_aa_index(api_key))
    }
}

trait AaCredentialFailureLogger: Send + Sync {
    fn log(&self, class: CredentialErrorClass);
}

struct LiveAaCredentialFailureLogger;

impl AaCredentialFailureLogger for LiveAaCredentialFailureLogger {
    fn log(&self, class: CredentialErrorClass) {
        log_aa_credential_failure(class);
    }
}

#[derive(Debug, Default)]
pub struct AaCredentialLogState {
    last_class: Option<CredentialErrorClass>,
}

impl AaCredentialLogState {
    fn should_log(&mut self, class: CredentialErrorClass) -> bool {
        if self.last_class == Some(class) {
            return false;
        }
        self.last_class = Some(class);
        true
    }

    fn note_success(&mut self) {
        self.last_class = None;
    }
}

async fn resolve_aa_credential(
    resolver: &dyn CredentialResolver,
    credential_id: &str,
) -> Result<ResolvedCredential, CredentialResolverError> {
    match resolver.get_scoped(credential_id).await {
        Err(error) if error.class == CredentialErrorClass::Transport => {
            // The client drops its failed cached connection on a transport error.
            // One more call therefore performs the bounded redial; the hourly loop,
            // rather than an internal retry ladder, owns the next attempt after that.
            resolver.get_scoped(credential_id).await
        }
        result => result,
    }
}

pub async fn aa_ingest_loop(
    store: std::sync::Arc<SqliteRouteStateStore>,
    config_path: PathBuf,
    resolver: std::sync::Arc<dyn CredentialResolver>,
) {
    let mut credential_log_state = AaCredentialLogState::default();
    loop {
        run_aa_ingest_cycle(
            store.as_ref(),
            &config_path,
            resolver.as_ref(),
            &mut credential_log_state,
        )
        .await;
        tokio::time::sleep(AA_INGEST_INTERVAL).await;
    }
}

pub async fn run_aa_ingest_cycle(
    store: &SqliteRouteStateStore,
    config_path: &Path,
    resolver: &dyn CredentialResolver,
    credential_log_state: &mut AaCredentialLogState,
) {
    run_aa_ingest_cycle_with(
        store,
        config_path,
        resolver,
        credential_log_state,
        &LiveAaIndexFetcher,
        &LiveAaCredentialFailureLogger,
    )
    .await;
}

async fn run_aa_ingest_cycle_with(
    store: &SqliteRouteStateStore,
    config_path: &Path,
    resolver: &dyn CredentialResolver,
    credential_log_state: &mut AaCredentialLogState,
    fetcher: &dyn AaIndexFetcher,
    credential_logger: &dyn AaCredentialFailureLogger,
) {
    let config = match config_overrides::read_aa_config(config_path) {
        Ok(config) => config,
        Err(_) => {
            log_aa_ingest_failure("config");
            return;
        }
    };
    let Some(api_key_ref) = config.api_key_ref.as_deref() else {
        log_aa_ingest_skipped();
        return;
    };
    let credential = match resolve_aa_credential(resolver, api_key_ref).await {
        Ok(credential) => {
            credential_log_state.note_success();
            credential
        }
        Err(error) => {
            if credential_log_state.should_log(error.class) {
                credential_logger.log(error.class);
            }
            return;
        }
    };
    let fetch_result = match std::str::from_utf8(credential.expose()) {
        Ok(api_key) => fetcher.fetch(api_key).await,
        Err(_) => Err("credential_payload"),
    };
    // Dropping the resolved record scrubs its payload before any store or log work.
    drop(credential);
    match fetch_result {
        Ok(rows) => {
            if store.replace_aa_index_table(&rows, now_ms()).is_err() {
                log_aa_ingest_failure("store");
            }
        }
        Err(label) => log_aa_ingest_failure(label),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prefrontal_routing_store::dev_descriptor;
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    static N: AtomicU64 = AtomicU64::new(0);

    fn open_store() -> (tempfile_dir::Dir, SqliteRouteStateStore) {
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = tempfile_dir::Dir::new(n);
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "prefrontal-routing",
            dir.path.join("store.db").to_string_lossy().into_owned(),
        ))
        .unwrap();
        (dir, store)
    }

    mod tempfile_dir {
        pub struct Dir {
            pub path: std::path::PathBuf,
        }
        impl Dir {
            pub fn new(n: u64) -> Self {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos();
                let path = std::env::temp_dir().join(format!(
                    "prefrontal-routing-aa-ingest-{}-{nanos}-{n}",
                    std::process::id()
                ));
                std::fs::create_dir_all(&path).unwrap();
                Self { path }
            }
        }
        impl Drop for Dir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
    }

    struct ScriptedCredentialResolver {
        replies: Mutex<VecDeque<Result<ResolvedCredential, CredentialResolverError>>>,
        scoped_requests: Mutex<Vec<String>>,
    }

    impl ScriptedCredentialResolver {
        fn new(replies: Vec<Result<ResolvedCredential, CredentialResolverError>>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                scoped_requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl CredentialResolver for ScriptedCredentialResolver {
        fn resolve<'a>(
            &'a self,
            _raw_handle: &'a str,
            _min_ttl_ms: Option<u64>,
            _force_refresh: bool,
        ) -> prefrontal_ckcred_client::CredentialResolverFuture<
            'a,
            Result<ResolvedCredential, CredentialResolverError>,
        > {
            Box::pin(async { panic!("AA ingest uses only principal-scoped credentials") })
        }

        fn get_scoped<'a>(
            &'a self,
            credential_id: &'a str,
        ) -> prefrontal_ckcred_client::CredentialResolverFuture<
            'a,
            Result<ResolvedCredential, CredentialResolverError>,
        > {
            Box::pin(async move {
                self.scoped_requests
                    .lock()
                    .unwrap()
                    .push(credential_id.to_string());
                self.replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("credential script exhausted")
            })
        }
    }

    #[derive(Default)]
    struct RecordingFetcher {
        keys: Mutex<Vec<String>>,
    }

    impl AaIndexFetcher for RecordingFetcher {
        fn fetch<'a>(&'a self, api_key: &'a str) -> AaFetchFuture<'a> {
            Box::pin(async move {
                self.keys.lock().unwrap().push(api_key.to_string());
                Ok(vec![("fetched-slug".to_string(), 42.0)])
            })
        }
    }

    #[derive(Default)]
    struct RecordingCredentialLogger {
        classes: Mutex<Vec<CredentialErrorClass>>,
    }

    impl AaCredentialFailureLogger for RecordingCredentialLogger {
        fn log(&self, class: CredentialErrorClass) {
            self.classes.lock().unwrap().push(class);
        }
    }

    fn write_enabled_config(dir: &tempfile_dir::Dir) -> PathBuf {
        let path = dir.path.join("alfonso-routing.jsonc");
        std::fs::write(&path, r#"{ "aa": {} }"#).unwrap();
        path
    }

    #[test]
    fn parse_aa_models_reads_slug_and_intelligence_index() {
        let value = json!({
            "data": [{
                "id": "gpt-5-6-sol",
                "slug": "gpt-5-6-sol",
                "evaluations": { "artificial_analysis_intelligence_index": 60.9 }
            }]
        });
        assert_eq!(parse_aa_models(&value), vec![("gpt-5-6-sol".into(), 60.9)]);
    }

    #[test]
    fn seed_aa_refuses_unmapped_and_absent_index() {
        let (_dir, store) = open_store();
        let params = SeedAaParams {
            provider_id: "openai".into(),
            model_id: "gpt-5.6-sol".into(),
            variant: None,
        };
        let mut config = AaConfig::default();
        assert_eq!(
            seed_aa(&store, &params, &config, 1).unwrap_err(),
            SeedAaRefusal::NoAaMapping
        );
        config
            .model_map
            .insert("openai/gpt-5.6-sol".into(), "gpt-5-6-sol".into());
        assert_eq!(
            seed_aa(&store, &params, &config, 1).unwrap_err(),
            SeedAaRefusal::AaIndexAbsent
        );
    }

    #[test]
    fn unreadable_aa_index_is_not_reported_as_absent() {
        let (dir, store) = open_store();
        store
            .replace_aa_index_table(&[("fault-slug".into(), 60.0)], 1)
            .unwrap();
        let mut config = AaConfig::default();
        config
            .model_map
            .insert("custom/fault-model".into(), "fault-slug".into());
        let params = SeedAaParams {
            provider_id: "custom".into(),
            model_id: "fault-model".into(),
            variant: None,
        };
        let connection = rusqlite::Connection::open(dir.path.join("store.db")).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE aa_index_cache RENAME COLUMN aa_index TO unavailable_aa_index;",
            )
            .unwrap();
        assert!(store.get_aa_index("fault-slug").is_err());
        assert_eq!(
            seed_aa(&store, &params, &config, 1).unwrap_err(),
            SeedAaRefusal::StoreReadFailed
        );
    }

    #[test]
    fn unreadable_existing_fact_does_not_bypass_curated_protection() {
        let (dir, store) = open_store();
        store
            .replace_aa_index_table(&[("fault-slug".into(), 60.0)], 1)
            .unwrap();
        let route_key = "openai/gpt-5.5";
        let connection = rusqlite::Connection::open(dir.path.join("store.db")).unwrap();
        connection
            .execute(
                "UPDATE model_facts SET context_window = 'bad', fusiform_present = 0 WHERE route_key = ?1",
                [route_key],
            )
            .unwrap();
        assert!(store.get_fact_by_route_key(route_key).is_err());
        assert!(store.get_facts().is_ok());
        let mut config = AaConfig::default();
        config
            .model_map
            .insert(route_key.into(), "fault-slug".into());
        let params = SeedAaParams {
            provider_id: "openai".into(),
            model_id: "gpt-5.5".into(),
            variant: None,
        };
        assert_eq!(
            seed_aa(&store, &params, &config, 1).unwrap_err(),
            SeedAaRefusal::StoreReadFailed
        );
    }

    #[test]
    fn unreadable_neighbor_facts_do_not_default_inferred_eq() {
        let (dir, store) = open_store();
        store
            .replace_aa_index_table(&[("fault-slug".into(), 60.0)], 1)
            .unwrap();
        let connection = rusqlite::Connection::open(dir.path.join("store.db")).unwrap();
        connection
            .execute(
                "UPDATE model_facts SET context_window = 'bad' WHERE route_key = (SELECT route_key FROM model_facts LIMIT 1)",
                [],
            )
            .unwrap();
        assert!(store.get_facts().is_err());
        let mut config = AaConfig::default();
        config
            .model_map
            .insert("custom/fault-model".into(), "fault-slug".into());
        let params = SeedAaParams {
            provider_id: "custom".into(),
            model_id: "fault-model".into(),
            variant: None,
        };
        assert_eq!(
            seed_aa(&store, &params, &config, 1).unwrap_err(),
            SeedAaRefusal::StoreReadFailed
        );
    }

    #[test]
    fn seed_aa_refuses_existing_override_and_leaves_it_untouched() {
        let (_dir, store) = open_store();
        let mut override_fact = store
            .get_fact_by_route_key("openai/gpt-5.6-sol")
            .unwrap()
            .unwrap();
        let original = override_fact.scores.elo;
        override_fact.scores.elo = 1_111.0;
        store
            .upsert_fact(&override_fact, FactSource::Override)
            .unwrap();
        store
            .replace_aa_index_table(&[("gpt-5-6-sol".into(), 60.9)], 1)
            .unwrap();
        let mut config = AaConfig::default();
        config
            .model_map
            .insert("openai/gpt-5.6-sol".into(), "gpt-5-6-sol".into());
        let params = SeedAaParams {
            provider_id: "openai".into(),
            model_id: "gpt-5.6-sol".into(),
            variant: None,
        };
        assert_eq!(
            seed_aa(&store, &params, &config, 1).unwrap_err(),
            SeedAaRefusal::ExistingCuratedOrOverride
        );
        let kept = store
            .get_fact_by_route_key("openai/gpt-5.6-sol")
            .unwrap()
            .unwrap();
        assert!((kept.scores.elo - 1_111.0).abs() < 1e-9);
        assert_eq!(kept.score_provenance, ScoreProvenance::Override);
        let _ = original;
    }

    #[test]
    fn seed_aa_writes_inferred_when_unmapped_route_is_new() {
        let (_dir, store) = open_store();
        store
            .replace_aa_index_table(&[("new-slug".into(), 60.0)], 1)
            .unwrap();
        let mut config = AaConfig::default();
        config
            .model_map
            .insert("custom/new-model".into(), "new-slug".into());
        let params = SeedAaParams {
            provider_id: "custom".into(),
            model_id: "new-model".into(),
            variant: None,
        };
        let ok = seed_aa(&store, &params, &config, 1_000).unwrap();
        assert_eq!(ok.provenance, ScoreProvenance::AaInferred);
        assert!((ok.aa_index - 60.0).abs() < 1e-9);
        let stored = store
            .get_fact_by_route_key("custom/new-model")
            .unwrap()
            .unwrap();
        assert_eq!(stored.score_provenance, ScoreProvenance::AaInferred);
        assert!(stored
            .eq_source
            .as_deref()
            .unwrap()
            .starts_with("neighbor:"));
        let listed = list_inferred_json(&store, 1_000).unwrap();
        assert_eq!(listed["inferred"][0]["routeKey"], "custom/new-model");
        assert_eq!(listed["inferred"][0]["reviewByPassed"], false);
    }

    #[test]
    fn seed_and_list_replies_never_contain_the_api_key() {
        let secret = "SECRET_AA_KEY_TEST_TOKEN";
        let (_dir, store) = open_store();
        store
            .replace_aa_index_table(&[("new-slug".into(), 20.0)], 1)
            .unwrap();
        let mut config = AaConfig {
            api_key_ref: Some("apikey:test-aa".into()),
            model_map: HashMap::new(),
            review_by_ms: 14 * 24 * 60 * 60 * 1000,
        };
        config
            .model_map
            .insert("custom/secret-probe".into(), "new-slug".into());
        let params = SeedAaParams {
            provider_id: "custom".into(),
            model_id: "secret-probe".into(),
            variant: None,
        };
        let ok = seed_aa(&store, &params, &config, 1).unwrap();
        let reply = seed_aa_result_json(&ok).to_string();
        let listed = list_inferred_json(&store, 1).unwrap().to_string();
        assert!(
            !reply.contains(secret),
            "seed reply leaked api key: {reply}"
        );
        assert!(
            !listed.contains(secret),
            "list_inferred reply leaked api key: {listed}"
        );
        assert!(!format!("{config:?}").contains(secret));
    }

    #[tokio::test]
    async fn credential_absent_skips_all_http_calls_and_logs_once_until_class_changes() {
        let (dir, store) = open_store();
        let config_path = write_enabled_config(&dir);
        let resolver = ScriptedCredentialResolver::new(vec![
            Err(CredentialResolverError::new(
                CredentialErrorClass::CredentialAbsent,
                "credential is absent",
            )),
            Err(CredentialResolverError::new(
                CredentialErrorClass::CredentialAbsent,
                "credential is still absent",
            )),
            Err(CredentialResolverError::new(
                CredentialErrorClass::Unauthorized,
                "credential grant is absent",
            )),
        ]);
        let fetcher = RecordingFetcher::default();
        let logger = RecordingCredentialLogger::default();
        let mut log_state = AaCredentialLogState::default();

        for _ in 0..3 {
            run_aa_ingest_cycle_with(
                &store,
                &config_path,
                &resolver,
                &mut log_state,
                &fetcher,
                &logger,
            )
            .await;
        }

        assert!(fetcher.keys.lock().unwrap().is_empty());
        assert_eq!(
            logger.classes.lock().unwrap().as_slice(),
            &[
                CredentialErrorClass::CredentialAbsent,
                CredentialErrorClass::Unauthorized
            ],
            "a repeated class stays quiet while a changed class is reported once"
        );
    }

    #[tokio::test]
    async fn each_ingest_cycle_resolves_again_and_fetches_with_the_rotated_record() {
        let (dir, store) = open_store();
        let config_path = write_enabled_config(&dir);
        let resolver = ScriptedCredentialResolver::new(vec![
            Ok(ResolvedCredential::new(
                b"first-key".to_vec(),
                "record-v1".into(),
                None,
            )),
            Ok(ResolvedCredential::new(
                b"rotated-key".to_vec(),
                "record-v2".into(),
                None,
            )),
        ]);
        let fetcher = RecordingFetcher::default();
        let logger = RecordingCredentialLogger::default();
        let mut log_state = AaCredentialLogState::default();

        for _ in 0..2 {
            run_aa_ingest_cycle_with(
                &store,
                &config_path,
                &resolver,
                &mut log_state,
                &fetcher,
                &logger,
            )
            .await;
        }

        assert_eq!(
            fetcher.keys.lock().unwrap().as_slice(),
            &["first-key", "rotated-key"],
            "the second record version must carry its own rotated payload"
        );
        assert_eq!(
            resolver.scoped_requests.lock().unwrap().as_slice(),
            &[DEFAULT_AA_API_KEY_REF, DEFAULT_AA_API_KEY_REF]
        );
        assert!(logger.classes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transport_failure_gets_one_redial_before_the_cycle_skips() {
        let (dir, store) = open_store();
        let config_path = write_enabled_config(&dir);
        let resolver = ScriptedCredentialResolver::new(vec![
            Err(CredentialResolverError::transport("first connection lost")),
            Err(CredentialResolverError::transport("redial failed")),
        ]);
        let fetcher = RecordingFetcher::default();
        let logger = RecordingCredentialLogger::default();
        let mut log_state = AaCredentialLogState::default();

        run_aa_ingest_cycle_with(
            &store,
            &config_path,
            &resolver,
            &mut log_state,
            &fetcher,
            &logger,
        )
        .await;

        assert_eq!(resolver.scoped_requests.lock().unwrap().len(), 2);
        assert!(fetcher.keys.lock().unwrap().is_empty());
        assert_eq!(
            logger.classes.lock().unwrap().as_slice(),
            &[CredentialErrorClass::Transport]
        );
    }

    #[tokio::test]
    #[ignore = "requires live subc, a supervised launch nonce, and the AA claustrum grant"]
    async fn live_aa_models_payload_contains_the_operator_map_slugs() {
        let home = std::env::var_os("HOME").expect("HOME is required for the live connection path");
        let resolver = prefrontal_ckcred_client::CkcredCredentialResolver::new(
            PathBuf::from(home).join(".local/share/cortexkit/run/subc-connection.json"),
            std::env::current_dir().unwrap(),
            "prefrontal-routing",
        );
        let credential = resolver
            .get_scoped(DEFAULT_AA_API_KEY_REF)
            .await
            .expect("the routing principal must read the AA key");
        let api_key = std::str::from_utf8(credential.expose()).expect("AA key must be UTF-8");
        let rows = fetch_aa_index(api_key).await.expect("live AA models fetch");
        drop(credential);
        let slugs: std::collections::HashSet<_> = rows.into_iter().map(|(slug, _)| slug).collect();

        for expected in ["gemini-3-8-flash", "glm-5-3", "glm-5-3-flash"] {
            assert!(slugs.contains(expected), "AA payload missing {expected}");
        }
    }

    #[test]
    fn module_source_never_interpolates_the_api_key_into_logs_or_replies() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        files.push(root.join("aa_ingest.rs"));
        files.push(root.join("main.rs"));
        files.push(root.join("config_overrides.rs"));
        for path in files {
            let text = std::fs::read_to_string(&path).unwrap();
            for (idx, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if !(trimmed.contains("eprintln!")
                    || trimmed.contains("tracing::")
                    || trimmed.contains("json!")
                    || trimmed.contains("format!"))
                {
                    continue;
                }
                assert!(
                    !trimmed.contains("api_key") && !trimmed.contains("api-key"),
                    "{}:{} interpolates api_key into a log or reply path: {trimmed}",
                    path.display(),
                    idx + 1
                );
            }
            // The header setter is the only permitted live use of the key value.
            if path.ends_with("aa_ingest.rs") {
                assert!(
                    text.contains(".header(\"x-api-key\", api_key)"),
                    "aa ingest must send the key only as x-api-key"
                );
            }
        }
    }
}
