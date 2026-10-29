//! One-shot conversion of routing configuration. Runtime loading never calls
//! this module: an operator must explicitly convert a legacy document.

use serde_json::{json, Map, Value};

pub const COMMAND: &str = "ck-prefrontal-routing config-convert";

pub(crate) fn defaults() -> Value {
    serde_json::from_str(include_str!("../data/config_convert_defaults.json"))
        .expect("converter defaults")
}

fn entry<'a>(root: &'a mut Map<String, Value>, section: &str, key: &str) -> &'a mut Value {
    root.entry(section)
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("config section object")
        .entry(key)
        .or_insert_with(|| json!({}))
}

/// Convert a parsed document, retaining operational tuning not replaced by the
/// entity policy. Earlier entity-policy documents lose the retired account flag.
pub fn convert_value(value: Value) -> Result<Value, String> {
    let mut root = value
        .as_object()
        .cloned()
        .ok_or("routing config must be an object")?;
    remove_account_flag(&mut root)?;
    if !root.contains_key("model_routing")
        && !root.contains_key("account_selection")
        && ["models", "providers", "runners", "consumers", "accounts"]
            .iter()
            .any(|key| root.contains_key(*key))
    {
        return Ok(Value::Object(root));
    }
    let data = defaults();
    let mut old = root
        .remove("model_routing")
        .unwrap_or_else(|| json!({}))
        .as_object()
        .cloned()
        .ok_or("model_routing must be an object")?;
    for (name, section) in root
        .iter()
        .filter(|(name, _)| ["models", "providers", "consumers"].contains(&name.as_str()))
    {
        if !section
            .as_object()
            .is_some_and(|entries| entries.values().all(Value::is_object))
        {
            return Err(format!("{name} must contain object entries"));
        }
    }
    if old.get("models").is_some_and(|section| {
        !section
            .as_object()
            .is_some_and(|entries| entries.values().all(Value::is_object))
    }) {
        return Err("model_routing.models must contain object entries".into());
    }
    for section in ["models", "providers", "consumers"] {
        root.entry(section).or_insert_with(|| json!({}));
    }
    if let Some(models) = old.remove("models") {
        root.insert("models".into(), models);
    }
    let ids = old
        .remove("consumers")
        .unwrap_or_else(|| data["consumers"].clone());
    for id in ids
        .as_array()
        .ok_or("model_routing.consumers must be an array")?
    {
        let id = id.as_str().ok_or("consumer id must be a string")?;
        entry(&mut root, "consumers", id);
    }
    for id in data["pay_per_token"].as_array().unwrap() {
        if let Some(consumer) = root["consumers"].get_mut(id.as_str().unwrap()) {
            consumer["pay_per_token"] = json!(true);
        }
    }
    root.insert(
        "runners".into(),
        old.remove("runners")
            .unwrap_or_else(|| data["runners"].clone()),
    );
    if let Some(runners) = root.get_mut("runners").and_then(Value::as_array_mut) {
        for runner in runners {
            if let Some(default) = data["runners"]
                .as_array()
                .unwrap()
                .iter()
                .find(|default| default["module"] == runner["module"])
            {
                if let Some(runner) = runner.as_object_mut() {
                    runner
                        .entry("via_vault")
                        .or_insert_with(|| default["via_vault"].clone());
                }
            }
        }
    }
    let specs = old.remove("subscription").unwrap_or_else(|| json!([]));
    for spec in data["subscription"]
        .as_array()
        .unwrap()
        .iter()
        .chain(specs.as_array().ok_or("subscription must be an array")?)
    {
        let spec = spec
            .as_str()
            .ok_or("subscription spec must be a string")?
            .trim();
        let (key, enabled) = spec
            .strip_prefix('-')
            .map_or((spec, true), |key| (key.trim(), false));
        let section = if key.contains('/') {
            "models"
        } else {
            "providers"
        };
        entry(&mut root, section, key)["subscription"] = json!(enabled);
    }
    let providers = old
        .remove("providers_without_prompt_cache")
        .unwrap_or_else(|| data["providers_without_prompt_cache"].clone());
    for provider in providers
        .as_array()
        .ok_or("providers_without_prompt_cache must be an array")?
    {
        entry(
            &mut root,
            "providers",
            provider.as_str().ok_or("provider must be a string")?.trim(),
        )["prompt_cache"] = json!(false);
    }
    let mut scoped_groups = std::collections::BTreeMap::new();
    let mut scoped_keys = std::collections::BTreeSet::new();
    if let Some(groups) = old.remove("exclude") {
        for (group, specs) in groups
            .as_object()
            .ok_or("exclude_shape: exclude must be an object")?
        {
            let mut entries = Vec::new();
            for spec in specs
                .as_array()
                .ok_or("exclude group expected a sequence")?
            {
                let spec = spec.as_str().ok_or("exclude spec must be a string")?.trim();
                if spec.is_empty() {
                    continue;
                }
                entries.push(spec.to_owned());
                let (key, excluded) = spec
                    .strip_prefix('-')
                    .map_or((spec, true), |key| (key.trim(), false));
                let entity = entry(
                    &mut root,
                    if key.contains('/') {
                        "models"
                    } else {
                        "providers"
                    },
                    key,
                );
                if group == "all" {
                    entity["pin_only"] = json!(excluded);
                } else {
                    scoped_keys.insert(key.to_owned());
                }
            }
            if group != "all" {
                scoped_groups.insert(
                    group.clone(),
                    prefrontal_routing_core::provider_model_spec::parse_spec_list(&entries),
                );
            }
        }
    }
    // A model entry replaces its provider's list, so materialize every scoped
    // group's effective restriction before writing the model's not_for list.
    for key in scoped_keys {
        let not_for: Vec<_> = scoped_groups
            .iter()
            .filter(|(_, specs)| {
                prefrontal_routing_core::provider_model_spec::resolve_spec(specs, &key, false)
            })
            .map(|(group, _)| group.clone())
            .collect();
        entry(
            &mut root,
            if key.contains('/') {
                "models"
            } else {
                "providers"
            },
            &key,
        )["not_for"] = json!(not_for);
    }
    if let Some(groups) = old.remove("variants") {
        let groups = groups.as_object().ok_or("variants must be an object")?;
        if let Some(all) = groups.get("all") {
            root.insert("variants".into(), all.clone());
        }
        let ids: Vec<_> = root["consumers"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for id in ids {
            let mut variants = json!({});
            if let Some(specific) = groups.get(&id) {
                variants
                    .as_object_mut()
                    .ok_or("variants must be an object")?
                    .extend(
                        specific
                            .as_object()
                            .ok_or("variants must be an object")?
                            .clone(),
                    );
            }
            entry(&mut root, "consumers", &id)["variants"] = variants;
        }
    }
    if let Some(pins) = old.remove("pins") {
        for (slot, pin) in pins.as_object().ok_or("pins must be an object")? {
            entry(&mut root, "consumers", slot)["pin"] = pin.clone();
        }
    }
    if let Some(projects) = old.remove("project_pins") {
        for (project, consumers) in projects
            .as_object()
            .ok_or("project_pins must be an object")?
        {
            for (consumer, pin) in consumers
                .as_object()
                .ok_or("project pins must be an object")?
            {
                let target = entry(&mut root, "consumers", consumer);
                if target.get("projects").is_none() {
                    target["projects"] = json!({});
                }
                target["projects"][project] = json!({"pin": pin});
            }
        }
    }
    for field in ["subscription_multipliers", "keep_free_percent"] {
        if let Some(values) = old.remove(field) {
            for (provider, value) in values
                .as_object()
                .ok_or("provider tuning must be an object")?
            {
                entry(&mut root, "providers", provider)[if field == "subscription_multipliers" {
                    "subscription_multiplier"
                } else {
                    field
                }] = value.clone();
            }
        }
    }
    old.remove("score_alias");
    root.remove("pins");
    root.remove("score_alias");
    root.extend(old);
    root.remove("model_families");
    root.entry("model_naming")
        .or_insert_with(|| data["model_naming"].clone());
    root.entry("head_agents")
        .or_insert_with(|| data["head_agents"].clone());
    root.entry("quota_model_pools")
        .or_insert_with(|| data["quota_model_pools"].clone());
    if let Some(accounts) = root.remove("account_selection") {
        root.insert("accounts".into(), accounts);
        remove_account_flag(&mut root)?;
    }
    let accounts = root
        .entry("accounts")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or("accounts must be an object")?;
    accounts
        .entry("model_families")
        .or_insert_with(|| data["account_model_families"].clone());
    Ok(Value::Object(root))
}

fn remove_account_flag(root: &mut Map<String, Value>) -> Result<(), String> {
    if let Some(accounts) = root.get_mut("accounts") {
        let accounts = accounts
            .as_object_mut()
            .ok_or("accounts must be an object")?;
        accounts.remove("enabled");
        if accounts.is_empty() {
            root.remove("accounts");
        }
    }
    Ok(())
}

pub fn convert(document: &str) -> Result<String, String> {
    let json = subc_jsonc::jsonc_to_json(document)?;
    let value = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    serde_json::to_string_pretty(&convert_value(value)?).map_err(|error| error.to_string())
}
