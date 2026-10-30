use std::collections::HashMap;
use std::sync::OnceLock;

use prefrontal_routing_core::model_resolution::build_usage_observation;
use prefrontal_routing_core::{
    find_most_specific_fallback_entry, fuzzy_match_model, resolve_compatible_model_settings,
    resolve_model_capabilities, resolve_model_id_alias, resolve_model_pipeline,
    resolve_user_model_pin, DesiredModelSettings, FallbackEntry, ModelCapabilitiesInput,
    ModelCapabilitiesSnapshot, ModelCapabilitiesSnapshotEntry, ModelNaming, ModelResolutionRequest,
    NativeUsageFacts, RouteStateStore, SnapshotLimit, SnapshotModalities, UserModelPinRequest,
    VariantFact,
};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::{send, send_route_error, ModuleError, SharedStore};

pub(crate) const RESOLVE_MODEL_OP: &str = "route.resolve_model";
pub(crate) const MODEL_CAPABILITIES_OP: &str = "route.model_capabilities";
pub(crate) const AGENT_MODEL_REQUIREMENTS_OP: &str = "route.agent_model_requirements";

const AGENT_MODEL_REQUIREMENTS_JSON: &str = include_str!("../model/agent_model_requirements.json");

#[derive(Debug, Deserialize)]
struct ParamsBody<P> {
    params: P,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolveModelParams {
    action: String,
    #[serde(default)]
    request: Option<ModelResolutionRequest>,
    #[serde(default)]
    user_pin: Option<UserModelPinRequest>,
    #[serde(default, rename = "providerID")]
    provider_id: Option<String>,
    #[serde(default, rename = "modelID")]
    model_id: Option<String>,
    #[serde(default)]
    fallback_chain: Vec<FallbackEntry>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    available_models: Vec<String>,
    #[serde(default)]
    providers: Vec<String>,
    #[serde(default)]
    suggestion: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CapabilitiesParams {
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "modelID")]
    model_id: String,
    #[serde(default)]
    runtime_model: Option<Value>,
    #[serde(default)]
    desired: Option<DesiredModelSettings>,
}

fn agent_model_requirements() -> &'static Value {
    static TABLE: OnceLock<Value> = OnceLock::new();
    TABLE.get_or_init(|| {
        serde_json::from_str(AGENT_MODEL_REQUIREMENTS_JSON)
            .expect("agent_model_requirements.json must be valid JSON")
    })
}

fn resolve(params: ResolveModelParams, naming: &ModelNaming) -> Result<Value, String> {
    match params.action.as_str() {
        "pipeline" => Ok(json!({
            "resolution": resolve_model_pipeline(
                params.request.as_ref().ok_or("pipeline requires request")?,
                naming,
            )
        })),
        "user-pin" => Ok(json!({
            "model": resolve_user_model_pin(
                params.user_pin.as_ref().ok_or("user-pin requires userPin")?
            )
        })),
        "fallback-entry" => Ok(json!({
            "entry": find_most_specific_fallback_entry(
                params.provider_id.as_deref().ok_or("fallback-entry requires providerID")?,
                params.model_id.as_deref().ok_or("fallback-entry requires modelID")?,
                &params.fallback_chain,
            )
        })),
        "fuzzy" => Ok(json!({
            "model": fuzzy_match_model(
                params.target.as_deref().ok_or("fuzzy requires target")?,
                &params.available_models,
                &params.providers,
            )
        })),
        "suggestion" => {
            let provider_id = params
                .provider_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or("suggestion requires providerID")?;
            let suggestion = params
                .suggestion
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or("suggestion requires suggestion")?;
            Ok(json!({ "model": { "providerID": provider_id, "modelID": suggestion } }))
        }
        action => Err(format!("unknown resolve_model action '{action}'")),
    }
}

async fn send_result(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    result: Value,
) -> Result<(), ModuleError> {
    let body = serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        frame.header.ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

pub(super) async fn handle_resolve_model(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    naming: &ModelNaming,
) -> Result<(), ModuleError> {
    let params = match serde_json::from_slice::<ParamsBody<ResolveModelParams>>(&frame.body) {
        Ok(body) => body.params,
        Err(error) => {
            return send_route_error(
                writer,
                frame.header.ver,
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                "invalid_request",
                &format!("route.resolve_model params not decodable: {error}"),
            )
            .await;
        }
    };
    match resolve(params, naming) {
        Ok(result) => send_result(frame, writer, result).await,
        Err(error) => {
            send_route_error(
                writer,
                frame.header.ver,
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                "invalid_request",
                &error,
            )
            .await
        }
    }
}

fn snapshot_from_store(
    store: &SharedStore,
    provider_id: &str,
    model_id: &str,
) -> Result<ModelCapabilitiesSnapshot, String> {
    let alias = resolve_model_id_alias(model_id);
    let candidates = [model_id, alias.canonical_model_id.as_str()];
    let mut selected = None;
    for candidate in candidates {
        let route_key = prefrontal_routing_core::make_route_key(provider_id, candidate, None);
        match store.get_fact_by_route_key(&route_key) {
            Ok(Some(fact)) => {
                selected = Some((route_key, fact));
                break;
            }
            Ok(None) => {}
            Err(error) => return Err(format!("model_capabilities: {error}")),
        }
    }
    let Some((_route_key, fact)) = selected else {
        return Ok(ModelCapabilitiesSnapshot::default());
    };
    let mut input_modalities = vec!["text".to_string()];
    if fact.supports_vision == Some(true) {
        input_modalities.push("image".to_string());
    }
    let entry = ModelCapabilitiesSnapshotEntry {
        id: alias.canonical_model_id.clone(),
        family: fact.grand_family_id().map(str::to_string),
        tool_call: fact.supports_tool_use,
        modalities: Some(SnapshotModalities {
            input: Some(input_modalities),
            output: Some(vec!["text".to_string()]),
        }),
        limit: Some(SnapshotLimit {
            context: fact.context_window,
            output: fact.max_output_tokens.map(|value| value as f64),
            ..SnapshotLimit::default()
        }),
        ..ModelCapabilitiesSnapshotEntry::default()
    };
    Ok(ModelCapabilitiesSnapshot {
        models: HashMap::from([(alias.canonical_model_id, entry)]),
    })
}

pub(super) async fn handle_model_capabilities(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    naming: &ModelNaming,
) -> Result<(), ModuleError> {
    let params = match serde_json::from_slice::<ParamsBody<CapabilitiesParams>>(&frame.body) {
        Ok(body)
            if !body.params.provider_id.trim().is_empty()
                && !body.params.model_id.trim().is_empty() =>
        {
            body.params
        }
        Ok(_) => {
            return send_route_error(
                writer,
                frame.header.ver,
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                "invalid_request",
                "route.model_capabilities requires providerID and modelID",
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                frame.header.ver,
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                "invalid_request",
                &format!("route.model_capabilities params not decodable: {error}"),
            )
            .await;
        }
    };
    match capabilities_reply(store, params, naming) {
        Ok(reply) => send_result(frame, writer, reply).await,
        Err(error) => {
            send_route_error(
                writer,
                frame.header.ver,
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                "store_error",
                &error,
            )
            .await
        }
    }
}

fn capabilities_reply(
    store: &SharedStore,
    params: CapabilitiesParams,
    naming: &ModelNaming,
) -> Result<Value, String> {
    let snapshot = snapshot_from_store(store, &params.provider_id, &params.model_id)?;
    let capabilities = resolve_model_capabilities(
        &ModelCapabilitiesInput {
            provider_id: params.provider_id,
            model_id: params.model_id.clone(),
            runtime_model: params.runtime_model,
            runtime_snapshot: Some(snapshot),
            bundled_snapshot: None,
        },
        naming,
    );
    let compatible_settings = params.desired.as_ref().map(|desired| {
        resolve_compatible_model_settings(&params.model_id, desired, Some(&capabilities), naming)
    });
    Ok(json!({
        "capabilities": capabilities,
        "compatibleSettings": compatible_settings,
        "catalogVersion": catalog_version(store),
    }))
}

/// The catalog snapshot this answer was computed against. The answer is a pure
/// function of the request and the catalog, so a caller that keeps replies can
/// tell from this number alone whether what it kept is still the module's
/// answer: the same version means the same answer, and a higher one retires
/// everything computed against the older catalog. Absent when no snapshot has
/// been recorded yet or the cursor cannot be read, which says the answer has no
/// stated validity and must not be kept; version 0 is a real recorded snapshot
/// and is not the same thing.
fn catalog_version(store: &SharedStore) -> Option<i64> {
    store
        .fusiform_catalog_cursor()
        .ok()
        .flatten()
        .map(|cursor| cursor.catalog_version)
}

pub(super) async fn handle_agent_model_requirements(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
) -> Result<(), ModuleError> {
    send_result(
        frame,
        writer,
        json!({ "requirements": agent_model_requirements() }),
    )
    .await
}

pub(super) fn usage_observation_from_store(
    params: NativeUsageFacts,
    store: &SharedStore,
) -> Result<Option<prefrontal_routing_core::UsageObservation>, String> {
    let catalog = store
        .get_catalog_facts()
        .map_err(|error| format!("record_usage catalog: {error}"))?;
    let variants = catalog
        .into_iter()
        .map(|entry| VariantFact {
            provider_id: entry.fact.provider_id,
            model_id: entry.fact.model_id,
            variant: entry
                .fact
                .prompt_defaults
                .and_then(|defaults| defaults.variant)
                .or(entry.fact.variant),
        })
        .collect::<Vec<_>>();
    Ok(build_usage_observation(&params, &variants))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The model naming a converted config carries: the converter's shipped
    /// vendor prefixes, provider rewrites and capability families.
    fn default_model_naming() -> prefrontal_routing_core::ModelNaming {
        serde_json::from_value(crate::config_convert::defaults()["model_naming"].clone())
            .expect("converter model naming")
    }

    #[test]
    fn model_resolution_module_table_matches_the_deleted_typescript_rows() {
        let requirements = agent_model_requirements();
        assert_eq!(requirements.as_object().map(|value| value.len()), Some(4));
        assert_eq!(
            requirements["alfonso"]["fallbackChain"][0],
            json!({
                "providers": ["anthropic", "github-copilot", "opencode", "vercel"],
                "model": "claude-opus-4-7",
                "variant": "max"
            })
        );
        assert_eq!(
            requirements["mason"]["fallbackChain"][4]["model"],
            "big-pickle"
        );
    }

    #[test]
    fn model_resolution_wire_actions_keep_the_typescript_shapes() {
        let pipeline = resolve(
            ResolveModelParams {
                action: "pipeline".into(),
                request: Some(ModelResolutionRequest {
                    intent: prefrontal_routing_core::ModelResolutionIntent {
                        user_model: Some("openai/gpt-5.3-codex".into()),
                        ..Default::default()
                    },
                    constraints: Default::default(),
                    policy: Default::default(),
                }),
                user_pin: None,
                provider_id: None,
                model_id: None,
                fallback_chain: Vec::new(),
                target: None,
                available_models: Vec::new(),
                providers: Vec::new(),
                suggestion: None,
            },
            &default_model_naming(),
        )
        .unwrap();
        assert_eq!(pipeline["resolution"]["provenance"], "override");
        assert_eq!(pipeline["resolution"]["model"], "openai/gpt-5.3-codex");

        let suggestion = resolve(
            ResolveModelParams {
                action: "suggestion".into(),
                request: None,
                user_pin: None,
                provider_id: Some("anthropic".into()),
                model_id: Some("claude-sonet-4".into()),
                fallback_chain: Vec::new(),
                target: None,
                available_models: Vec::new(),
                providers: Vec::new(),
                suggestion: Some("claude-sonnet-4".into()),
            },
            &default_model_naming(),
        )
        .unwrap();
        assert_eq!(suggestion["model"]["modelID"], "claude-sonnet-4");
    }

    #[test]
    fn known_route_capabilities_family_uses_catalog_lineage() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "routing-capabilities-lineage-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store: SharedStore = std::sync::Arc::new(
            prefrontal_routing_store::SqliteRouteStateStore::open(
                &prefrontal_routing_store::dev_descriptor(
                    "capabilities-lineage",
                    dir.join("store.db").to_string_lossy().into_owned(),
                ),
            )
            .unwrap(),
        );
        let request = || CapabilitiesParams {
            provider_id: "lineage-fixture".into(),
            model_id: "claude-impostor-a".into(),
            runtime_model: None,
            desired: None,
        };
        let snapshot = crate::fusiform_catalog::decode_catalog_get(include_bytes!(
            "../fixtures/catalog-lineage.json"
        ))
        .unwrap();
        crate::fusiform_catalog::apply_mapped_catalog(
            &store,
            &snapshot,
            None,
            Some("lineage-fixture"),
        )
        .unwrap();
        assert_eq!(
            capabilities_reply(&store, request(), &default_model_naming()).unwrap()["capabilities"]
                ["family"],
            "qwen"
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unknown_route_keeps_an_empty_capabilities_snapshot() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "routing-capabilities-unknown-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store: SharedStore = std::sync::Arc::new(
            prefrontal_routing_store::SqliteRouteStateStore::open(
                &prefrontal_routing_store::dev_descriptor(
                    "capabilities-unknown",
                    dir.join("store.db").to_string_lossy().into_owned(),
                ),
            )
            .unwrap(),
        );
        let snapshot = snapshot_from_store(&store, "lineage-fixture", "claude-impostor-a").unwrap();
        assert_eq!(snapshot, ModelCapabilitiesSnapshot::default());
        let reply = capabilities_reply(
            &store,
            CapabilitiesParams {
                provider_id: "lineage-fixture".into(),
                model_id: "claude-impostor-a".into(),
                runtime_model: None,
                desired: None,
            },
            &default_model_naming(),
        )
        .unwrap();
        assert_eq!(
            reply["capabilities"]["diagnostics"]["snapshot"]["source"],
            "none"
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The capabilities answer names the catalog it was computed against so a
    /// caller can invalidate a cached answer after a catalog refresh.
    #[test]
    fn capabilities_reply_states_the_catalog_it_was_computed_against() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-capabilities-version-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after the Unix epoch")
                .as_nanos(),
        ));
        std::fs::create_dir_all(&dir).expect("create capabilities test directory");
        let store: SharedStore = std::sync::Arc::new(
            prefrontal_routing_store::SqliteRouteStateStore::open(
                &prefrontal_routing_store::dev_descriptor(
                    "prefrontal-routing-capabilities-version",
                    dir.join("store.db").to_string_lossy().into_owned(),
                ),
            )
            .expect("open capabilities test store"),
        );
        let request = || CapabilitiesParams {
            provider_id: "anthropic".to_string(),
            model_id: "claude-sonnet-4-5".to_string(),
            runtime_model: None,
            desired: None,
        };

        // No snapshot has been recorded yet, so the module states no validity
        // rather than a number the caller could mistake for one.
        let unversioned = capabilities_reply(&store, request(), &default_model_naming())
            .expect("capabilities project");
        assert_eq!(unversioned["catalogVersion"], Value::Null);
        assert!(unversioned["capabilities"]["canonicalModelId"].is_string());

        for version in [0, 9] {
            store
                .apply_fusiform_catalog(
                    &[],
                    prefrontal_routing_store::FusiformCatalogCursor {
                        catalog_version: version,
                        resolved_at_ms: 1_000 + version,
                    },
                )
                .expect("record a catalog snapshot");
            assert_eq!(
                capabilities_reply(&store, request(), &default_model_naming())
                    .expect("capabilities project")["catalogVersion"],
                json!(version),
                "version {version} is a recorded snapshot, not an absent one"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
