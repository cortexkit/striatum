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

#[derive(Debug, Deserialize)]
pub(crate) enum Executor {
    #[serde(rename = "broca")]
    Broca,
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
                "harness {} is not the executor broca",
                harness.as_str()
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
