//! Only the parts of a daemon view that may alter project routing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Map, Value};
use subc_protocol::config::{ConfigView, Layer};

use crate::tier_settings::Consumer;

#[derive(Default)]
pub(crate) struct Overrides {
    pub(crate) consumers: Arc<BTreeMap<String, Consumer>>,
    pub(crate) names: BTreeSet<String>,
    pub(crate) applied: bool,
    pub(crate) ignored_policy: bool,
}

impl Overrides {
    pub(crate) fn has_scoped_layer(view: &ConfigView) -> bool {
        view.sources
            .values()
            .any(|layer| matches!(layer, Layer::Workspace | Layer::Project))
            || view.layers.iter().any(|layer| {
                layer.bytes.is_some()
                    && matches!(
                        layer.layer.known_layer(),
                        Some(Layer::Workspace | Layer::Project)
                    )
            })
    }

    pub(crate) fn read(global: &Value, view: &ConfigView) -> Result<Self, Vec<String>> {
        let fail = || vec!["config.get returned invalid project routing settings".into()];
        let effective = view.effective.as_object().ok_or_else(fail)?;
        let global = global.as_object().ok_or_else(fail)?;
        let consumers = consumer_map(effective).ok_or_else(fail)?;
        let global_consumers = consumer_map(global).ok_or_else(fail)?;
        let mut result = Self::default();
        result.applied = Self::has_scoped_layer(view);
        // Compare user-only settings without copying the document or treating a
        // removed runner item as a change to the user's enrollment policy.
        result.ignored_policy = effective
            .iter()
            .any(|(key, value)| key != "consumers" && global.get(key) != Some(value))
            || global
                .keys()
                .any(|key| key != "consumers" && !effective.contains_key(key));
        let mut projected = BTreeMap::new();
        for (name, entry) in consumers.into_iter().flatten() {
            let entry = entry.as_object().ok_or_else(fail)?;
            let user = global_consumers
                .and_then(|entries| entries.get(name))
                .and_then(Value::as_object);
            if user.is_none() {
                result.names.insert(name.clone());
            }
            result.ignored_policy |= entry.iter().any(|(key, value)| {
                !scoped_field(key) && user.and_then(|entry| entry.get(key)) != Some(value)
            }) || user.is_some_and(|user| {
                user.keys()
                    .any(|key| !scoped_field(key) && !entry.contains_key(key))
            });
            let mut fields = Map::new();
            for key in ["runners", "service_tier", "service_tiers"] {
                let Some(value) = entry.get(key) else {
                    continue;
                };
                let pointer = format!(
                    "/consumers/{}/{key}",
                    name.replace('~', "~0").replace('/', "~1")
                );
                if user.and_then(|user| user.get(key)) == Some(value)
                    && !matches!(
                        view.sources.get(&pointer),
                        Some(Layer::Workspace | Layer::Project)
                    )
                {
                    continue;
                }
                let value = if key == "runners" {
                    let mut list = Vec::new();
                    for item in value.as_array().ok_or_else(fail)? {
                        let item = item.as_object().ok_or_else(fail)?;
                        let mut choice = Map::new();
                        for (key, value) in item {
                            if matches!(key.as_str(), "module" | "harness" | "routes") {
                                choice.insert(key.clone(), value.clone());
                            } else {
                                result.ignored_policy = true;
                            }
                        }
                        list.push(Value::Object(choice));
                    }
                    Value::Array(list)
                } else {
                    value.clone()
                };
                fields.insert(key.into(), value);
            }
            if !fields.is_empty() {
                let policy = serde_json::from_value(Value::Object(fields)).map_err(|_| fail())?;
                projected.insert(name.clone(), policy);
            }
        }
        result.ignored_policy |= global_consumers.into_iter().flatten().any(|(name, entry)| {
            !consumers.is_some_and(|entries| entries.contains_key(name))
                && entry
                    .as_object()
                    .is_some_and(|entry| entry.keys().any(|key| !scoped_field(key)))
        });
        result.applied |= !result.names.is_empty() || !projected.is_empty();
        result.consumers = Arc::new(projected);
        Ok(result)
    }
}

fn consumer_map(object: &Map<String, Value>) -> Option<Option<&Map<String, Value>>> {
    match object.get("consumers") {
        Some(value) => value.as_object().map(Some),
        None => Some(None),
    }
}

fn scoped_field(key: &str) -> bool {
    matches!(key, "runners" | "service_tier" | "service_tiers")
}
