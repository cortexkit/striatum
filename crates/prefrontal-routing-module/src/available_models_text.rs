use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::Path,
};

use prefrontal_routing_core::{
    make_capacity_key, make_route_key, resolve_routing_policy, ModelFact, ModelRoutingPolicyInput,
};
use prefrontal_routing_store::SqliteRouteStateStore;
use serde::Deserialize;
use serde_json::json;
use subc_protocol::{Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::{
    config_overrides, provider_auth_types, send, send_route_error, ModuleError, SharedStore,
};

pub(crate) const OP: &str = "routing.available_models_text";

#[derive(Debug, Deserialize)]
struct AvailableModelsTextBody {
    params: AvailableModelsTextParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AvailableModelsTextParams {
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    query: String,
    #[serde(default)]
    include: Include,

    #[serde(default)]
    available_models: Vec<String>,
    #[serde(default)]
    subscription_entries: Vec<String>,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
}

fn default_limit() -> usize {
    25
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Include {
    #[default]
    Scored,
    All,
}

#[derive(Debug)]
struct Row {
    excluded: bool,
    /// The upstream catalog marks the route deprecated; routing never offers it.
    deprecated: bool,

    model_key: String,
    elo: Option<f64>,
    eq: Option<f64>,
    cost: String,
    context: String,
    vision: &'static str,
}

pub(super) async fn handle(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    operator_config_path: &Path,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params = match serde_json::from_slice::<AvailableModelsTextBody>(&frame.body) {
        Ok(body) => body.params,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("routing.available_models_text params not decodable: {error}"),
            )
            .await;
        }
    };
    let operator_policy = config_overrides::read_operator_routing_policy(operator_config_path);
    let text = match render_from_store(&params, store.as_ref(), &operator_policy) {
        Ok(text) => text,
        Err(error) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &error)
                .await;
        }
    };
    let body =
        serde_json::to_vec(&json!({ "result": { "text": text } })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

fn render_from_store(
    params: &AvailableModelsTextParams,
    store: &SqliteRouteStateStore,
    operator_policy: &ModelRoutingPolicyInput,
) -> Result<String, String> {
    let catalog = store
        .get_catalog_facts()
        .map_err(|error| format!("available_models_text: {error}"))?;
    let caller_policy = ModelRoutingPolicyInput {
        exclude: Vec::new(),
        subscription: params.subscription_entries.clone(),
        ..ModelRoutingPolicyInput::default()
    };
    let capacity_keys = params.available_models.iter().cloned().collect::<Vec<_>>();
    let resolved_policy = resolve_routing_policy(
        capacity_keys,
        operator_policy,
        &caller_policy,
        &provider_auth_types(params.provider_auth_types.as_ref()),
    );
    let allowed_keys = resolved_policy
        .available_models
        .into_iter()
        .collect::<HashSet<_>>();
    let subscription_keys = resolved_policy
        .subscription_keys
        .into_iter()
        .map(|key| resolve_model_key(&key).1)
        .collect::<HashSet<_>>();
    let facts = catalog.iter().map(|entry| &entry.fact).collect::<Vec<_>>();
    Ok(render_text(
        &params.available_models,
        &facts,
        &subscription_keys,
        &allowed_keys,
        params,
    ))
}

fn render_text(
    available_models: &[String],
    facts: &[&ModelFact],
    subscription_keys: &HashSet<String>,
    allowed_keys: &HashSet<String>,
    params: &AvailableModelsTextParams,
) -> String {
    let mut by_route_key = HashMap::new();
    let mut by_capacity_key: HashMap<String, &ModelFact> = HashMap::new();
    for fact in facts {
        by_route_key.insert(
            make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()),
            *fact,
        );
        let capacity_key = make_capacity_key(&fact.provider_id, &fact.model_id);
        let replace = by_capacity_key
            .get(&capacity_key)
            .is_none_or(|previous| fact.scores.elo > previous.scores.elo);
        if replace {
            by_capacity_key.insert(capacity_key, *fact);
        }
    }

    let mut rows = available_models
        .iter()
        .map(|model_key| {
            let (route_key, capacity_key) = resolve_model_key(model_key);
            let fact = route_key.as_ref().and_then(|route_key| {
                by_route_key
                    .get(route_key)
                    .or_else(|| by_route_key.get(model_key))
                    .copied()
                    .or_else(|| by_capacity_key.get(&capacity_key).copied())
            });
            Row {
                excluded: !allowed_keys.contains(model_key),
                deprecated: fact.is_some_and(|value| value.is_deprecated_upstream()),
                model_key: model_key.clone(),
                elo: fact
                    .map(|value| value.scores.elo)
                    .filter(|elo| elo.is_finite() && *elo > 0.0),
                eq: fact.map(|value| value.scores.eq),
                cost: if subscription_keys.contains(&capacity_key) {
                    "sub".to_string()
                } else {
                    fact.and_then(|value| value.cost_per_m_tok_out)
                        .map(|cost| format!("${}/Mtok", js_number(cost)))
                        .unwrap_or_else(|| "n/a".to_string())
                },
                context: format_context(fact.and_then(|value| value.context_window)),
                vision: match fact.and_then(|value| value.supports_vision) {
                    Some(true) => "yes",
                    Some(false) => "no",
                    None => "n/a",
                },
            }
        })
        .collect::<Vec<_>>();
    let query = params.query.to_lowercase();
    rows.retain(|row| {
        row.model_key.to_lowercase().contains(&query)
            && (matches!(params.include, Include::All)
                || (!row.excluded && !row.deprecated && row.elo.is_some()))
    });
    rows.sort_by(compare_rows);

    let recommended = rows
        .iter()
        .find(|row| !row.excluded && !row.deprecated && row.elo.is_some())
        .map(|row| row.model_key.as_str());
    let count = rows.len();
    let page = rows
        .iter()
        .skip(params.offset)
        .take(params.limit.clamp(1, 100))
        .collect::<Vec<_>>();
    let model_width = page
        .iter()
        .map(|row| row.model_key.chars().count())
        .fold(12, usize::max);
    let header = format!(
        "{}  ELO   EQ    COST         CTX    VISION",
        pad_end("MODEL", model_width)
    );
    let lines = page.iter().map(|row| {
        let marker = if Some(row.model_key.as_str()) == recommended {
            "  ← recommended (highest Elo)"
        } else {
            ""
        };
        let mut reasons = Vec::new();
        if row.excluded {
            reasons.push("excluded by operator");
        }
        if row.deprecated {
            reasons.push("deprecated upstream");
        }
        if row.elo.is_none() {
            reasons.push("no score");
        }
        let reason = if reasons.is_empty() {
            String::new()
        } else {
            format!("  [{}]", reasons.join("; "))
        };
        format!(
            "{}  {}  {}  {}  {}  {}{}{}",
            pad_end(&row.model_key, model_width),
            pad_end(&format_score(row.elo), 5),
            pad_end(&format_score(row.eq), 5),
            pad_end(&row.cost, 12),
            pad_end(&row.context, 5),
            row.vision,
            marker,
            reason,
        )
    });
    let note = format!(
        "{count} model{} available · sub = subscription (no per-token cost)",
        if count == 1 { "" } else { "s" }
    );
    let recommendation = recommended
        .map(|model| format!("Recommended head (highest Elo): {model}"))
        .unwrap_or_else(|| "Recommended head: none (no Elo data); let the user pick.".to_string());
    std::iter::once(note)
        .chain(std::iter::once(recommendation))
        .chain(std::iter::once(String::new()))
        .chain(std::iter::once(header))
        .chain(lines)
        .chain(std::iter::once(format!(
            "shown {} of {count} · next: offset={}",
            page.len(),
            params.offset.saturating_add(page.len())
        )))
        .collect::<Vec<_>>()
        .join("\n")
}

fn resolve_model_key(model_key: &str) -> (Option<String>, String) {
    let Some((provider, rest)) = model_key.split_once('/') else {
        return (None, model_key.to_string());
    };
    let (model_id, variant) = match rest.split_once('/') {
        Some((model_id, variant)) => (model_id, (!variant.is_empty()).then_some(variant)),
        None => (rest, None),
    };
    (
        Some(make_route_key(provider, model_id, variant)),
        make_capacity_key(provider, model_id),
    )
}

fn compare_rows(left: &Row, right: &Row) -> Ordering {
    match (left.elo, right.elo) {
        (None, None) => left.model_key.cmp(&right.model_key),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        // Row elo is finite and positive (non-finite scores become `None` when
        // rows are built), so the total order matches the numeric one.
        (Some(left_elo), Some(right_elo)) => right_elo
            .total_cmp(&left_elo)
            .then_with(|| left.model_key.cmp(&right.model_key)),
    }
}

fn format_score(value: Option<f64>) -> String {
    match value {
        None => "n/a".to_string(),
        Some(value) if value.is_nan() => "n/a".to_string(),
        Some(value) if value.fract() == 0.0 => js_number(value),
        Some(value) => js_to_fixed(value, 1),
    }
}

fn format_context(tokens: Option<f64>) -> String {
    let Some(tokens) = tokens else {
        return "n/a".to_string();
    };
    if tokens <= 0.0 {
        return "n/a".to_string();
    }
    if tokens >= 1_000_000.0 {
        let millions = tokens / 1_000_000.0;
        return if millions.fract() == 0.0 {
            format!("{}M", js_number(millions))
        } else {
            format!("{}M", js_to_fixed(millions, 1))
        };
    }
    if tokens >= 1_000.0 {
        let thousands = tokens / 1_000.0;
        return if thousands.fract() == 0.0 {
            format!("{}k", js_number(thousands))
        } else {
            format!("{}k", js_to_fixed(thousands, 0))
        };
    }
    js_number(tokens)
}

fn js_number(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else {
        value.to_string()
    }
}

fn js_to_fixed(value: f64, precision: usize) -> String {
    if !value.is_finite() {
        return js_number(value);
    }
    let factor = 10_f64.powi(precision as i32);
    let rounded = (value * factor).round() / factor;
    format!("{rounded:.precision$}")
}

fn pad_end(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        value.to_string()
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use prefrontal_routing_core::{
        Confidence, FactSource, ModelScores, RouteStateStore, ScoreProvenance,
    };
    use prefrontal_routing_store::dev_descriptor;

    use super::*;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct StoreRig {
        directory: std::path::PathBuf,
        store: SqliteRouteStateStore,
    }

    impl StoreRig {
        fn new(label: &str) -> Self {
            let sequence = TEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let directory = prefrontal_test_support::scratch_root().join(format!(
                "available-models-text-{label}-{}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir_all(&directory).expect("create available-models test directory");
            let descriptor = dev_descriptor(
                "prefrontal-routing",
                directory.join("store.db").to_string_lossy().into_owned(),
            );
            let store = SqliteRouteStateStore::open(&descriptor).expect("open routing test store");
            Self { directory, store }
        }
    }

    impl Drop for StoreRig {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    fn fact(
        provider: &str,
        model: &str,
        elo: f64,
        eq: f64,
        cost: f64,
        context: f64,
        vision: bool,
    ) -> ModelFact {
        ModelFact {
            billing_plane: None,
            auth_methods: None,
            speed_is_fleet_median: false,
            provider_id: provider.to_string(),
            model_id: model.to_string(),
            variant: None,
            family: None,
            scores: ModelScores {
                elo,
                eq,
                speed: 1.0,
            },
            context_window: Some(context),
            max_output_tokens: None,
            cost_tier: None,
            cost_per_m_tok_out: Some(cost),
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: Some(vision),
            supports_tool_use: None,
            prompt_defaults: None,
            confidence: Confidence::Curated,
            notes: None,
            speed_multiplier: None,
            subscription_multiplier: None,
            score_provenance: ScoreProvenance::Curated,
            aa_index: None,
            review_by_ms: None,
            eq_source: None,
            class_vector: None,
            class_vector_is_overlay: false,
            upstream_status: None,
        }
    }

    fn params(models: &[&str], subscriptions: &[&str]) -> AvailableModelsTextParams {
        AvailableModelsTextParams {
            available_models: models.iter().map(|value| (*value).to_string()).collect(),
            subscription_entries: subscriptions
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            provider_auth_types: Some(HashMap::new()),
            offset: 0,
            limit: default_limit(),
            query: String::new(),
            include: Include::Scored,
        }
    }

    #[test]
    fn available_models_default_page_is_scored_and_recommended() {
        let rig = StoreRig::new("typescript-corpus");
        rig.store
            .upsert_fact(
                &fact("a", "cheap", 10.0, 5.0, 1.0, 128_000.0, false),
                FactSource::Seed,
            )
            .unwrap();
        rig.store
            .upsert_fact(
                &fact("b", "premium", 50.0, 80.0, 9.0, 200_000.0, true),
                FactSource::Seed,
            )
            .unwrap();

        let rendered = render_from_store(
            &params(&["z/unknown", "a/cheap", "b/premium"], &["b/premium"]),
            &rig.store,
            &ModelRoutingPolicyInput::default(),
        )
        .unwrap();
        assert_eq!(
            rendered,
            "2 models available · sub = subscription (no per-token cost)\nRecommended head (highest Elo): b/premium\n\nMODEL         ELO   EQ    COST         CTX    VISION\nb/premium     50     80     sub           200k   yes  ← recommended (highest Elo)\na/cheap       10     5      $1/Mtok       128k   no\nshown 2 of 2 · next: offset=2"
        );

        let unknown = render_from_store(
            &params(&["x/unknown"], &[]),
            &rig.store,
            &ModelRoutingPolicyInput::default(),
        )
        .unwrap();
        assert_eq!(
            unknown,
            "0 models available · sub = subscription (no per-token cost)\nRecommended head: none (no Elo data); let the user pick.\n\nMODEL         ELO   EQ    COST         CTX    VISION\nshown 0 of 0 · next: offset=0"
        );
    }

    #[test]
    fn available_models_filters_pages_queries_and_marks_all_reasons() {
        let rig = StoreRig::new("pages");
        let mut models = Vec::new();
        for n in 1..=30 {
            let model = format!("model{n:02}");
            models.push(format!("p/{model}"));
            rig.store
                .upsert_fact(
                    &fact("p", &model, n as f64, 50.0, 1.0, 1000.0, false),
                    FactSource::Seed,
                )
                .unwrap();
        }
        models.extend(["p/zero".into(), "p/unknown".into()]);
        rig.store
            .upsert_fact(
                &fact("p", "zero", 0.0, 0.0, 1.0, 1000.0, false),
                FactSource::Seed,
            )
            .unwrap();
        let policy = ModelRoutingPolicyInput {
            exclude: vec!["p/model30".into()],
            subscription: vec![],
            ..ModelRoutingPolicyInput::default()
        };
        let mut request = params(&[], &[]);
        request.available_models = models;
        let render = |request: &AvailableModelsTextParams| {
            render_from_store(request, &rig.store, &policy).unwrap()
        };
        let first = render(&request);
        assert!(first.ends_with("shown 25 of 29 · next: offset=25"));
        assert!(!first.contains("p/model30"));
        assert!(!first.contains("p/zero"));
        assert!(!first.contains("p/unknown"));
        assert!(first.find("p/model29").unwrap() < first.find("p/model28").unwrap());
        request.offset = 25;
        let second = render(&request);
        assert!(second.ends_with("shown 4 of 29 · next: offset=29"));
        assert!(second.contains("p/model01"));
        assert!(!second.contains("p/model05"));
        request.offset = 0;
        request.query = "P/MODEL02".into();
        assert!(render(&request).ends_with("shown 1 of 1 · next: offset=1"));
        request.query.clear();
        request.include = Include::All;
        request.limit = 1000;
        let all = render(&request);
        assert!(all.ends_with("shown 32 of 32 · next: offset=32"));
        assert!(all
            .lines()
            .any(|line| line.starts_with("p/model30") && line.contains("excluded by operator")));
        for model in ["p/zero", "p/unknown"] {
            assert!(all
                .lines()
                .any(|line| line.starts_with(model) && line.contains("no score")));
        }
        assert!(all.contains("Recommended head (highest Elo): p/model29"));
        request.offset = 99;
        assert!(render(&request).ends_with("shown 0 of 32 · next: offset=99"));
        request.offset = 0;
        request.available_models = (0..101).map(|n| format!("unknown/model{n}")).collect();
        assert!(render(&request).ends_with("shown 100 of 101 · next: offset=100"));
    }

    #[test]
    fn live_store_state_gates_available_models_render() {
        let rig = StoreRig::new("state-gate");
        let request = params(&["state/model"], &[]);
        let before =
            render_from_store(&request, &rig.store, &ModelRoutingPolicyInput::default()).unwrap();

        rig.store
            .upsert_fact(
                &fact("state", "model", 61.25, 80.04, 2.5, 1_500_000.0, true),
                FactSource::Override,
            )
            .unwrap();
        let after =
            render_from_store(&request, &rig.store, &ModelRoutingPolicyInput::default()).unwrap();

        assert_ne!(before, after);
        assert!(before.contains("Recommended head: none"));
        assert!(after.contains("Recommended head (highest Elo): state/model"));
        assert!(after.contains("61.3   80.0   $2.5/Mtok     1.5M   yes"));
    }
}
