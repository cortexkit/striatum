//! Translate the Serde shape into the daemon's settings schema vocabulary.

use super::{RoutingConfigFile, DEFAULT_CATALOG_POLL_MS};
use serde_json::{json, Map, Value};
use striatum_core::accounts::AllocatorPolicy;
use striatum_core::{
    DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD, DEFAULT_DEMAND_EXPONENT,
    MIN_BEST_EFFORT_ADEQUACY,
};

pub(super) fn render() -> String {
    let mut schema = schemars::schema_for!(RoutingConfigFile).to_value();
    let mut defs = schema.as_object_mut().unwrap().remove("$defs").unwrap();
    let defs = defs.as_object_mut().unwrap();

    // Account overrides inherit root settings rather than restating their defaults.
    defs.insert(
        "DefaultAccountsPolicy".into(),
        defs["ProviderAccountsPolicy"].clone(),
    );
    schema["properties"]["accounts"]["$ref"] = json!("#/$defs/DefaultAccountsPolicy");

    // Inline scalar definitions so the field's description and absence policy
    // apply where it appears. Keep shared record shapes in one definition.
    let scalar_defs: Map<_, _> = defs
        .iter()
        .filter(|(_, node)| node["type"] != "object")
        .map(|(name, node)| (name.clone(), node.clone()))
        .collect();
    defs.retain(|name, _| !scalar_defs.contains_key(name));
    reshape(&mut schema, &scalar_defs, "", "");
    for (name, node) in defs.iter_mut() {
        reshape(node, &scalar_defs, name, "");
    }
    schema["$defs"] = json!(defs);
    serde_json::to_string_pretty(&schema).unwrap() + "\n"
}

fn reshape(node: &mut Value, scalars: &Map<String, Value>, owner: &str, field: &str) {
    let node = node.as_object_mut().unwrap();
    if let Some(kind) = node.get_mut("type") {
        if let Some(types) = kind.as_array_mut() {
            types.retain(|kind| kind != "null");
            if types.len() == 1 {
                *kind = types.pop().unwrap();
            }
        }
    }
    if let Some(variants) = node.remove("anyOf") {
        let variants = variants.as_array().unwrap();
        assert_eq!(
            variants.len(),
            2,
            "unexpected alternatives at {owner}.{field}"
        );
        assert_eq!(
            variants[1]["type"], "null",
            "unexpected union at {owner}.{field}"
        );
        node.extend(variants[0].as_object().unwrap().clone());
    }
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        if let Some(scalar) = scalars.get(reference.strip_prefix("#/$defs/").unwrap()) {
            node.remove("$ref");
            for (key, value) in scalar.as_object().unwrap() {
                node.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
    }
    if node.get("default").is_some_and(Value::is_null) {
        node.remove("default");
    }
    if let Some(Value::Object(default)) = node.get_mut("default") {
        // Serde serializes unset fields of default records as null. They
        // belong in neither the declared default nor the effective settings.
        remove_unset(default);
    }
    if let Some(default) = effective_default(owner, field) {
        node.insert("default".into(), default);
    }
    if matches!(
        field,
        "max_hold" | "hold_when_reset_within" | "unknown_reset_retry"
    ) {
        // The duration parser accepts a leading '+' and the three unit suffixes.
        node.insert("pattern".into(), json!(r"^\+?[0-9]+[smh]$"));
        node.insert("x-ck-kind".into(), json!("duration"));
    }

    if node.get("type").is_some_and(|kind| kind == "object") {
        node.insert("unevaluatedProperties".into(), json!(false));
    }
    if node
        .get("additionalProperties")
        .is_some_and(Value::is_object)
    {
        let (pattern, description) = map_entry(owner, field);
        let mut child = node.remove("additionalProperties").unwrap();
        reshape(&mut child, scalars, owner, "*");
        annotate(&mut child, Some(description), false);
        node.insert("patternProperties".into(), json!({ pattern: child }));
    }
    if let Some(item) = node.get_mut("items") {
        reshape(item, scalars, owner, "*");
        annotate(item, Some(item_description(owner, field)), false);
    }
    let required = node.get("required").cloned().unwrap_or_default();
    if let Some(Value::Object(fields)) = node.get_mut("properties") {
        for (key, child) in fields {
            reshape(child, scalars, owner, key);
            annotate(
                child,
                None,
                required
                    .as_array()
                    .is_some_and(|keys| keys.contains(&json!(key))),
            );
        }
    }
}

fn remove_unset(object: &mut Map<String, Value>) {
    object.retain(|_, value| !value.is_null());
    for value in object.values_mut() {
        if let Value::Object(child) = value {
            remove_unset(child);
        }
    }
}

fn annotate(node: &mut Value, description: Option<&str>, required: bool) {
    if let Some(description) = description {
        node["description"] = json!(description);
    }
    node["x-ck-apply"] = json!("live");
    node["x-ck-layers"] = json!(["user"]);
    if node.get("default").is_none() {
        node["x-ck-required"] = json!(required);
    }
}

fn effective_default(owner: &str, field: &str) -> Option<Value> {
    Some(match (owner, field) {
        ("", "consecutive_terminal_failure_threshold") => {
            json!(DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD)
        }
        ("", "demand_exponent") => json!(DEFAULT_DEMAND_EXPONENT),
        ("", "min_best_effort_adequacy") => json!(MIN_BEST_EFFORT_ADEQUACY),
        ("", "subscription_multipliers") => json!({}),
        ("CatalogSection", "poll_ms") => json!(DEFAULT_CATALOG_POLL_MS),
        ("DefaultAccountsPolicy", field) => {
            let policy = AllocatorPolicy::default();
            match field {
                "strategy" => json!(policy.strategy),
                "order" => json!(policy.account_order),
                "ordered" => json!(if policy.fallback_first {
                    "fallback-first"
                } else {
                    "main-first"
                }),
                "floors" => json!(policy.pool_floors),
                "max_hold" => json!(format!("{}s", policy.max_hold_s)),
                "unknown_reset_retry" => {
                    json!(format!("{}s", policy.unknown_reset_retry_ms / 1000))
                }
                "paid_credits" => json!(policy.paid_credits),
                _ => return None,
            }
        }
        ("ProviderPolicy", "prompt_cache") => json!(true),
        ("ProviderPolicy", "keep_free_percent") => {
            json!(striatum_core::RoutePricing::default().keep_free_percent(""))
        }
        ("RetentionSection", field) => {
            let windows = crate::retention::RetentionWindows::default();
            match field {
                "account_selection_days" => json!(windows.account_selection_days),
                "account_observation_days" => json!(windows.account_observation_days),
                "model_route_decisions_days" => json!(windows.model_route_decisions_days),
                "model_route_decisions_full_detail_days" => {
                    json!(windows.model_route_decisions_full_detail_days)
                }
                "model_route_decisions_full_detail_sample_one_in" => {
                    json!(windows.model_route_decisions_full_detail_sample_one_in)
                }
                "model_route_usage_days" => json!(windows.model_route_usage_days),
                _ => return None,
            }
        }
        _ => return None,
    })
}

fn map_entry(owner: &str, field: &str) -> (&'static str, &'static str) {
    const NAME: &str = r"^\S+$";
    const PROVIDER: &str = r"^[^/\s]+$";
    const PROVIDER_AND_ID: &str = r"^[^/]+/.+$";
    match (owner, field) {
        ("", "consumer_priorities") => (NAME, "Scheduling priority for this consumer."),
        ("", "consumers") => (NAME, "Routing preferences for this consumer."),
        ("", "providers") => (PROVIDER, "Settings for this provider."),
        ("", "models") => (
            r"^[^/]+/[^/]+(?:/[^/]*)?$",
            "Overrides for this model or model variant.",
        ),
        ("", "subscription_multipliers") => (
            PROVIDER,
            "Positive number to divide this provider's estimated subscription cost by.",
        ),
        ("ConsumerPolicy", "projects") => (r"^.+$", "Model preference in this project."),
        ("ConsumerPolicy", "service_tiers") => (NAME, "Service speed for this provider or model."),
        ("CapSettings", "providers") => (PROVIDER, "Request limit for this provider."),
        ("CapSettings", "plans") => (PROVIDER_AND_ID, "Request limit for this plan."),
        ("CapSettings", "models") => (PROVIDER_AND_ID, "Request limit for this model."),
        ("CapSettings", "accounts") => (PROVIDER_AND_ID, "Request limit for this account."),
        _ => panic!("choose an ID pattern for {owner}.{field}"),
    }
}

fn item_description(owner: &str, field: &str) -> &'static str {
    match (owner, field) {
        ("", "pin_origin_callers") => "Full identity of one trusted caller.",
        ("", "head_agents") => "Name of one agent allowed to use reserved quota.",
        ("", "quota_model_pools") => {
            "One additional quota pool and the model prefix it applies to."
        }
        ("" | "ConsumerPolicy", "runners") => "One module that can run the selected model.",
        ("ProviderAccountsPolicy" | "DefaultAccountsPolicy", "order") => {
            "One account ID in preference order."
        }
        ("ProviderAccountsPolicy" | "DefaultAccountsPolicy", "floors") => {
            "Minimum remaining quota for one pool or window."
        }
        ("ProviderPolicy", "models") => "One model this provider may serve.",
        ("ProviderPolicy" | "ModelFactOverride", "only_for" | "not_for" | "pin_only_for") => {
            "One consumer name, including its dotted subgroups."
        }
        ("ConsumerPolicy", "ladder") => {
            "One model preference, as provider/model or provider/model:variant."
        }
        ("RunnerPolicy", "routes") => {
            "One route or group of routes, as provider, provider/model, or provider/model/variant."
        }
        ("RunnerPolicy" | "ConsumerPolicy", "tool_capabilities") => "One tool capability tag.",
        _ => panic!("describe one item of {owner}.{field}"),
    }
}
