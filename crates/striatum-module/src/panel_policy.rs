//! Translate the panel wire request and the live operator policy into routing inputs.

use crate::config_policy::{ExtraRanking, PanelPolicies, Ranking};
use striatum_core::panel::{
    ClassPanelPolicy, PanelClass, PanelDiversity, PanelExtraOrder, PanelOrder, PanelSpec,
};
use striatum_core::{fact_to_route, ExclusionPolicy, ExclusionSpecs, ModelFact};
use serde::{Deserialize, Serialize};

/// Decode retired size, IQ-floor and diversity hints without letting them
/// override the width, quality bar or family rule in the operator's class policy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PanelRequestSpec {
    pub class: PanelClass,
    #[serde(default)]
    size: usize,
    #[serde(default)]
    iq_floor: f64,
    #[serde(default)]
    diversity: Option<PanelDiversity>,
    #[serde(default)]
    temperature_spread: Option<Vec<f64>>,
}

impl PanelRequestSpec {
    pub(crate) fn selection_spec(&self) -> PanelSpec {
        PanelSpec {
            class: self.class,
            size: 1,
            iq_floor: 0.0,
            diversity: PanelDiversity::Family,
            temperature_spread: self.temperature_spread.clone(),
        }
    }
}

/// The operator's own policy for a class, when settings name one; otherwise the
/// class runs on `ClassPanelPolicy::default_for_class`.
pub(crate) fn configured_class_policy(
    policies: &PanelPolicies,
    class: PanelClass,
) -> Option<&crate::config_policy::PanelPolicy> {
    match class {
        PanelClass::Audit => &policies.audit,
        PanelClass::DesignReview => &policies.design_review,
        PanelClass::Diagnose => &policies.diagnose,
        PanelClass::Evaluate => &policies.evaluate,
        PanelClass::Explain => &policies.explain,
        PanelClass::Spec => &policies.spec,
        PanelClass::Lab => &policies.lab,
    }
    .as_ref()
}

pub(crate) fn class_policy(policies: &PanelPolicies, class: PanelClass) -> ClassPanelPolicy {
    let Some(policy) = configured_class_policy(policies, class) else {
        return ClassPanelPolicy::default_for_class(class);
    };
    ClassPanelPolicy {
        top_k: usize::from(policy.top_k),
        by: match policy.by {
            Ranking::Iq => PanelOrder::Iq,
            Ranking::Eq => PanelOrder::Eq,
            Ranking::Cost => PanelOrder::Cost,
        },
        extra: usize::from(policy.extra),
        extra_by: policy.extra_by.map(|order| match order {
            ExtraRanking::Random => PanelExtraOrder::Random,
            ExtraRanking::Iq => PanelExtraOrder::Iq,
            ExtraRanking::Eq => PanelExtraOrder::Eq,
            ExtraRanking::Cost => PanelExtraOrder::Cost,
        }),
        floor: policy.floor,
    }
}

/// Panel admission accepts one operator list. Resolve `exclude.all` and the
/// class's group separately, then pass their matching route keys as that list.
/// This prevents `-` exceptions in a group from canceling an `all` exclusion and
/// preserves operator-exclusion rejection reasons instead of hiding candidates.
pub(crate) fn operator_exclusions(
    facts: &[ModelFact],
    all: &[String],
    group: &[String],
) -> Vec<String> {
    let specs = ExclusionSpecs::parse(ExclusionPolicy {
        operator: all,
        ..ExclusionPolicy::default()
    })
    .with_operator_group(group);
    facts
        .iter()
        .map(|fact| fact_to_route(fact).route_key)
        .filter(|key| specs.excludes(key))
        .collect()
}
