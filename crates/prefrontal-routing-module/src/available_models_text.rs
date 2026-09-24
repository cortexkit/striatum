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
    available_models: Vec<String>,
    #[serde(default)]
    subscription_entries: Vec<String>,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
}

#[derive(Debug)]
struct Row {
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
    opencode_auth_path: &Path,
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
    let text = match render_from_store(
        &params,
        store.as_ref(),
        &operator_policy,
        opencode_auth_path,
    ) {
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
    opencode_auth_path: &Path,
) -> Result<String, String> {
    let catalog = store
        .get_catalog_facts()
        .map_err(|error| format!("available_models_text: {error}"))?;
    let caller_policy = ModelRoutingPolicyInput {
        exclude: Vec::new(),
        subscription: params.subscription_entries.clone(),
        prefer_fast_when_relaxed: Vec::new(),
    };
    let capacity_keys = params
        .available_models
        .iter()
        .map(|model_key| resolve_model_key(model_key).1)
        .collect::<Vec<_>>();
    let resolved_policy = resolve_routing_policy(
        capacity_keys,
        operator_policy,
        &caller_policy,
        &provider_auth_types(params.provider_auth_types.as_ref(), opencode_auth_path),
        false,
    );
    let subscription_keys = resolved_policy
        .subscription_keys
        .into_iter()
        .collect::<HashSet<_>>();
    let facts = catalog.iter().map(|entry| &entry.fact).collect::<Vec<_>>();
    Ok(render_text(
        &params.available_models,
        &facts,
        &subscription_keys,
    ))
}

fn render_text(
    available_models: &[String],
    facts: &[&ModelFact],
    subscription_keys: &HashSet<String>,
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
                model_key: model_key.clone(),
                elo: fact.map(|value| value.scores.elo),
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
    rows.sort_by(compare_rows);

    let recommended = rows
        .iter()
        .find(|row| row.elo.is_some())
        .map(|row| row.model_key.as_str());
    let model_width = rows
        .iter()
        .map(|row| row.model_key.chars().count())
        .fold(12, usize::max);
    let header = format!(
        "{}  ELO   EQ    COST         CTX    VISION",
        pad_end("MODEL", model_width)
    );
    let lines = rows.iter().map(|row| {
        let marker = if Some(row.model_key.as_str()) == recommended {
            "  ← recommended (highest Elo)"
        } else {
            ""
        };
        format!(
            "{}  {}  {}  {}  {}  {}{}",
            pad_end(&row.model_key, model_width),
            pad_end(&format_score(row.elo), 5),
            pad_end(&format_score(row.eq), 5),
            pad_end(&row.cost, 12),
            pad_end(&row.context, 5),
            row.vision,
            marker,
        )
    });
    let count = rows.len();
    let note = format!(
        "{count} model{} available · sub = subscription (no per-token cost)",
        if count == 1 { "" } else { "s" }
    );
    let recommendation = recommended
        .map(|model| format!("RECOMMENDED HEAD (highest Elo: recommend exactly this): {model}"))
        .unwrap_or_else(|| "RECOMMENDED HEAD: none (no Elo data): let the user pick.".to_string());
    std::iter::once(note)
        .chain(std::iter::once(recommendation))
        .chain(std::iter::once(String::new()))
        .chain(std::iter::once(header))
        .chain(lines)
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
        (Some(left_elo), Some(right_elo)) => right_elo
            .partial_cmp(&left_elo)
            .unwrap_or(Ordering::Equal)
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

    impl StoreRig {
        /// Never read: every request here carries its own provider auth
        /// types. It sits under the scratch root so the no-live-auth guard in
        /// `provider_auth_types` holds even if a request stops carrying them.
        fn opencode_auth(&self) -> std::path::PathBuf {
            self.directory.join("opencode-auth.json")
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
        }
    }

    #[test]
    fn typescript_available_models_corpus_is_byte_identical() {
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
            &rig.opencode_auth(),
        )
        .unwrap();
        assert_eq!(
            rendered,
            "3 models available · sub = subscription (no per-token cost)\nRECOMMENDED HEAD (highest Elo: recommend exactly this): b/premium\n\nMODEL         ELO   EQ    COST         CTX    VISION\nb/premium     50     80     sub           200k   yes  ← recommended (highest Elo)\na/cheap       10     5      $1/Mtok       128k   no\nz/unknown     n/a    n/a    n/a           n/a    n/a"
        );

        let unknown = render_from_store(
            &params(&["x/unknown"], &[]),
            &rig.store,
            &ModelRoutingPolicyInput::default(),
            &rig.opencode_auth(),
        )
        .unwrap();
        assert_eq!(
            unknown,
            "1 model available · sub = subscription (no per-token cost)\nRECOMMENDED HEAD: none (no Elo data): let the user pick.\n\nMODEL         ELO   EQ    COST         CTX    VISION\nx/unknown     n/a    n/a    n/a           n/a    n/a"
        );
    }

    #[test]
    fn live_store_state_gates_available_models_render() {
        let rig = StoreRig::new("state-gate");
        let request = params(&["state/model"], &[]);
        let before = render_from_store(
            &request,
            &rig.store,
            &ModelRoutingPolicyInput::default(),
            &rig.opencode_auth(),
        )
        .unwrap();

        rig.store
            .upsert_fact(
                &fact("state", "model", 61.25, 80.04, 2.5, 1_500_000.0, true),
                FactSource::Override,
            )
            .unwrap();
        let after = render_from_store(
            &request,
            &rig.store,
            &ModelRoutingPolicyInput::default(),
            &rig.opencode_auth(),
        )
        .unwrap();

        assert_ne!(before, after);
        assert!(before.contains("RECOMMENDED HEAD: none"));
        assert!(
            after.contains("RECOMMENDED HEAD (highest Elo: recommend exactly this): state/model")
        );
        assert!(after.contains("61.3   80.0   $2.5/Mtok     1.5M   yes"));
    }
}
