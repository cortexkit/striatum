//! Operator route-to-harness mapping (`model_routing.substrates`).
//!
//! Workload, consumer, and `all` groups are independent fallback levels. A
//! partial group never hides matching rules in the next group. No catalog or
//! score alias participates: keys always name the route's own provider.

use std::collections::BTreeMap;

use prefrontal_routing_core::provider_model_spec::{parse_spec, spec_names_key};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize};

use crate::select_policy::Consumer;

/// Harness names are a closed config contract, not inferred from providers.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Harness {
    Broca,
    Opencode,
    Pi,
}

impl Harness {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Broca => crate::legacy_wire::BROCA,
            Self::Opencode => crate::legacy_wire::OPENCODE,
            Self::Pi => crate::legacy_wire::PI,
        }
    }
}

const GROUP_NAMES: [&str; 19] = [
    "all",
    "mason",
    "sidekick",
    "athena",
    "lab",
    "core",
    "codemode",
    "flow",
    "sidekick.explore",
    "sidekick.librarian",
    "sidekick.gather",
    "sidekick.comments",
    "sidekick.planner",
    "sidekick.lead",
    "sidekick.reader",
    "sidekick.map",
    "sidekick.reviewer",
    "hire",
    "persona_seat",
];

type Entries = BTreeMap<String, Harness>;

/// Missing groups are empty; an explicitly configured group must be an object.
#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct SubstrateGroups(BTreeMap<String, Entries>);

impl<'de> Deserialize<'de> for SubstrateGroups {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_value(value).map_err(serde::de::Error::custom)
    }
}

impl SubstrateGroups {
    // Decode with config paths so a bad scalar reports its key, not just its type.
    fn from_value(value: serde_json::Value) -> Result<Self, String> {
        let groups = value
            .as_object()
            .ok_or("model_routing.substrates must be an object")?;
        let mut result = BTreeMap::new();
        for (group, value) in groups {
            let path = format!("model_routing.substrates.{group}");
            if !GROUP_NAMES.contains(&group.as_str()) {
                return Err(format!("{path}: unknown group"));
            }
            let entries = value
                .as_object()
                .ok_or_else(|| format!("{path} must be an object"))?;
            let mut parsed = BTreeMap::new();
            for (key, value) in entries {
                if key != "default"
                    && (key.trim() != key || parse_spec(key).is_none_or(|spec| spec.negated))
                {
                    return Err(format!(
                        "{path}: key {key:?} must be default, provider, provider/model or provider/model/variant (no negation)"
                    ));
                }
                let harness = serde_json::from_value::<Harness>(value.clone()).map_err(|_| {
                    let [first, second, last] = crate::legacy_wire::HARNESSES;
                    format!("{path}: key {key:?} must name a harness: {first}, {second} or {last}")
                })?;
                parsed.insert(key.clone(), harness);
            }
            result.insert(group.clone(), parsed);
        }
        Ok(Self(result))
    }

    fn matching(&self, group: &str, route_key: &str) -> Option<Harness> {
        let entries = self.0.get(group)?;
        entries
            .iter()
            .filter(|(key, _)| key.as_str() != "default")
            .filter_map(|(key, harness)| {
                let spec = parse_spec(key)?;
                spec_names_key(&spec, route_key).then_some((key.len(), *harness))
            })
            .max_by_key(|(length, _)| *length)
            .map(|(_, harness)| harness)
            .or_else(|| entries.get("default").copied())
    }
}

impl JsonSchema for SubstrateGroups {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SubstrateGroups".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let mut group = generator.subschema_for::<Entries>();
        // The first slash separates provider from model; later slashes belong
        // to the model/prefix. Match parse_spec, but reject negation and padding.
        group.insert(
            "propertyNames".into(),
            serde_json::json!({"allOf": [
                {"pattern": "^[^/]+(/[\\s\\S]+)?$"},
                {"not": {"pattern": "^[-\\s]|\\s$"}}
            ]}),
        );
        let properties: serde_json::Map<String, serde_json::Value> = GROUP_NAMES
            .iter()
            .map(|name| ((*name).into(), group.as_value().clone()))
            .collect();
        schemars::json_schema!({
            "type": "object",
            "additionalProperties": false,
            "properties": properties
        })
    }
}

/// Resolve only the mapping. Availability and workload support are checked by
/// selection and launch respectively, never by guessing another harness here.
pub(crate) fn resolve(
    groups: &SubstrateGroups,
    route_key: &str,
    consumer: Option<Consumer>,
    workload: Option<&str>,
) -> Harness {
    let consumer = consumer.map(|consumer| match consumer {
        Consumer::Mason => "mason",
        Consumer::Sidekick => "sidekick",
        Consumer::Athena => "athena",
        Consumer::Lab => "lab",
        Consumer::Core => "core",
        Consumer::Codemode => "codemode",
        Consumer::Flow => "flow",
    });
    workload
        .into_iter()
        .chain(consumer)
        .chain(["all"])
        .find_map(|group| groups.matching(group, route_key))
        .unwrap_or(Harness::Broca)
}

#[cfg(test)]
mod tests;
