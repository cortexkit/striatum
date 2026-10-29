//! Per-route availability for launch and Broca executor selections.

use std::collections::{HashMap, HashSet};

use prefrontal_routing_core::{model_facts::fact_to_route, ModelFact};
use serde::Deserialize;

use crate::{
    select_policy::Consumer,
    substrate_policy::{self, Harness, SubstrateGroups},
};

#[derive(Debug, Clone, Copy, Deserialize)]
pub(crate) enum Workload {
    #[serde(rename = "mason")]
    Mason,
    #[serde(rename = "sidekick.explore")]
    Explore,
    #[serde(rename = "sidekick.librarian")]
    Librarian,
    #[serde(rename = "sidekick.gather")]
    Gather,
    #[serde(rename = "sidekick.comments")]
    Comments,
    /// The retired sidekick planner session. Still accepted so a running
    /// core that predates the lead keeps routing.
    #[serde(rename = "sidekick.planner")]
    Planner,
    /// The lead session of a sidekick call: answers research itself, hands
    /// questions to readers, or reviews comments through core's changed_text.
    #[serde(rename = "sidekick.lead")]
    Lead,
    /// A reader the sidekick lead opened to answer one question.
    #[serde(rename = "sidekick.reader")]
    Reader,
    /// A sidekick map lane: returns the code excerpts a change needs.
    #[serde(rename = "sidekick.map")]
    Map,
    /// A review session core opened for a sidekick comment-review handoff.
    #[serde(rename = "sidekick.reviewer")]
    Reviewer,
    #[serde(rename = "athena")]
    Athena,
    #[serde(rename = "hire")]
    Hire,
    #[serde(rename = "persona_seat")]
    PersonaSeat,
}

impl Workload {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Mason => "mason",
            Self::Explore => "sidekick.explore",
            Self::Librarian => "sidekick.librarian",
            Self::Gather => "sidekick.gather",
            Self::Comments => "sidekick.comments",
            Self::Planner => "sidekick.planner",
            Self::Lead => "sidekick.lead",
            Self::Reader => "sidekick.reader",
            Self::Map => "sidekick.map",
            Self::Reviewer => "sidekick.reviewer",
            Self::Athena => "athena",
            Self::Hire => "hire",
            Self::PersonaSeat => "persona_seat",
        }
    }

    pub(crate) fn for_agent(agent: &str) -> Option<Self> {
        match agent {
            "mason" => Some(Self::Mason),
            "explore" => Some(Self::Explore),
            "librarian" => Some(Self::Librarian),
            _ => None,
        }
    }
}

/// The legacy `executor` request value. Its one accepted spelling is the
/// legacy wire's Broca name.
#[derive(Debug)]
pub(crate) enum Executor {
    Broca,
}

impl<'de> Deserialize<'de> for Executor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value == crate::legacy_wire::BROCA {
            Ok(Self::Broca)
        } else {
            Err(serde::de::Error::unknown_variant(
                &value,
                &[crate::legacy_wire::BROCA],
            ))
        }
    }
}

pub(crate) struct SelectionHarness<'a> {
    pub groups: &'a SubstrateGroups,
    pub consumer: Option<Consumer>,
    pub workload: Option<Workload>,
    pub executor: bool,
    pub required: Option<Harness>,
    pub caller: Option<&'a str>,
    pub host_models: &'a HashSet<String>,
    pub broca_models: &'a HashSet<String>,
    pub executor_models: &'a HashSet<String>,
}

impl SelectionHarness<'_> {
    pub(crate) fn mapped(&self, route_key: &str) -> Harness {
        substrate_policy::resolve(
            self.groups,
            route_key,
            self.consumer,
            self.workload.map(Workload::as_str),
        )
    }

    pub(crate) fn rejection(&self, route_key: &str, capacity_key: &str) -> Option<String> {
        let harness = self.mapped(route_key);
        if let Some(required) = self.required.filter(|required| *required != harness) {
            return Some(format!(
                "harness_mismatch: task runs on {}",
                required.as_str()
            ));
        }
        if self.executor && harness != Harness::Broca {
            return Some(format!(
                "harness {} is not the executor {}",
                harness.as_str(),
                crate::legacy_wire::BROCA
            ));
        }
        let models = if self.executor {
            self.executor_models
        } else if harness == Harness::Broca {
            self.broca_models
        } else if self.caller == Some(harness.as_str()) {
            self.host_models
        } else {
            return Some(format!(
                "harness {} does not serve this route for this request",
                harness.as_str()
            ));
        };
        if models.contains(route_key) || models.contains(capacity_key) {
            None
        } else {
            Some(format!(
                "harness {} does not serve this route for this request",
                harness.as_str()
            ))
        }
    }

    pub(crate) fn availability(&self, facts: &[ModelFact]) -> MappedAvailability {
        let mut result = MappedAvailability::default();
        for fact in facts {
            let route = fact_to_route(fact);
            if let Some(detail) = self.rejection(&route.route_key, &route.capacity_key) {
                result.rejected.insert(route.route_key, detail);
            } else {
                // Keep route keys, not capacity keys: variants of one model can
                // map to different harnesses and must be admitted independently.
                result.models.insert(route.route_key);
            }
        }
        result
    }
}

#[derive(Default)]
pub(crate) struct MappedAvailability {
    pub models: HashSet<String>,
    pub rejected: HashMap<String, String>,
}
