//! The catalog effort chain is a call-local input to `route.what_if`, not a
//! setting that could change a live selection.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{json, Value};
use striatum_core::{make_route_key, ModelFact};
use striatum_store::CatalogEfforts;

use crate::config_overrides::LiveConfig;
use crate::fusiform_catalog::efforts::{rank, WORKLOADS};

#[cfg(test)]
#[path = "effort_preview_tests.rs"]
mod tests;

pub(crate) struct Preview {
    pub models: BTreeMap<String, CatalogEfforts>,
    pub workloads: BTreeMap<String, String>,
}

impl Preview {
    pub fn workload<'a>(&'a self, consumer: &str, settings: &'a LiveConfig) -> Option<&'a str> {
        self.workloads
            .iter()
            .filter(|(name, _)| crate::config_policy::consumer_matches(name, consumer))
            .max_by_key(|(name, _)| crate::config_policy::consumer_specificity(name))
            .map(|(_, workload)| workload.as_str())
            .or_else(|| settings.consumer_workload(consumer))
            .or_else(|| declared_workload(consumer))
            .filter(|workload| WORKLOADS.contains(workload))
    }

    pub fn resolve(
        &self,
        fact: &ModelFact,
        targets: &BTreeMap<String, String>,
        workload: Option<&str>,
        pinned: bool,
        explicit: Option<&str>,
    ) -> Result<Choice, Rejection> {
        resolve(
            self.model(fact, targets),
            workload,
            pinned,
            explicit,
            fact.scores.elo,
            fact.scores.eq,
        )
    }

    fn model(
        &self,
        fact: &ModelFact,
        targets: &BTreeMap<String, String>,
    ) -> Option<&CatalogEfforts> {
        let route = make_route_key(&fact.provider_id, &fact.model_id, None);
        let target = striatum_core::score_alias::catalog_key(targets, &route);
        self.models.get(&route).or_else(|| self.models.get(&target))
    }

    pub fn head_default(
        &self,
        fact: &ModelFact,
        targets: &BTreeMap<String, String>,
        allowed: impl Fn(&str) -> bool,
    ) -> Choice {
        // The head picker lists the person's explicit choices. Workload policy
        // governs automatic picks, so it cannot filter or raise a head choice.
        let mut choice = self.resolve(fact, targets, None, true, None).unwrap();
        if choice
            .variant
            .as_deref()
            .is_some_and(|effort| !allowed(effort))
        {
            let graded = self
                .model(fact, targets)
                .and_then(|model| model.graded.as_deref())
                .unwrap_or("unrecorded");
            choice.variant = None;
            choice.effort_source = Source::None;
            choice.grade = Grade {
                elo: fact.scores.elo,
                eq: fact.scores.eq,
                elo_effort: graded.into(),
                eq_effort: graded.into(),
            };
        }
        choice
    }
}

// Module declarations will supply the default once the routing contract has
// a workload member. Keep that lookup separate from operator overrides.
fn declared_workload(_consumer: &str) -> Option<&'static str> {
    None
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Source {
    Pin,
    WorkloadDefault,
    Graded,
    VendorDefault,
    None,
    MinimumRaised,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Choice {
    pub variant: Option<String>,
    effort_source: Source,
    pub grade: Grade,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum_unverified: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Grade {
    pub elo: f64,
    pub eq: f64,
    elo_effort: String,
    eq_effort: String,
}

impl Choice {
    pub fn apply_scores(&self, fact: &mut ModelFact) {
        fact.scores.elo = self.grade.elo;
        fact.scores.eq = self.grade.eq;
    }

    pub fn append_to(&self, output: &mut Value) {
        output.as_object_mut().unwrap().extend(
            serde_json::to_value(self)
                .expect("catalog choice is serializable")
                .as_object_mut()
                .map(std::mem::take)
                .unwrap(),
        );
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub(crate) enum Rejection {
    WorkloadExcluded {
        workload: String,
    },
    WorkloadMinimumUnreachable {
        workload: String,
        minimum: String,
        offered: Vec<String>,
    },
}

impl Rejection {
    pub fn code(&self) -> &'static str {
        match self {
            Self::WorkloadExcluded { .. } => "workload_excluded",
            Self::WorkloadMinimumUnreachable { .. } => "workload_minimum_unreachable",
        }
    }
}

#[derive(Default)]
pub(super) struct Diagnostics {
    pub rejected: BTreeMap<String, Rejection>,
    pub minimum_unverified: BTreeMap<String, String>,
}

impl Diagnostics {
    pub fn unverified_models(&self, blocked: &BTreeSet<&str>) -> usize {
        self.minimum_unverified
            .iter()
            .filter(|(route, _)| !blocked.contains(route.as_str()))
            .map(|(_, model)| model)
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn exclusions(&self) -> impl Iterator<Item = Value> + '_ {
        self.rejected.iter().map(|(route, reason)| {
            let mut value = json!(reason);
            value["route"] = json!(route);
            value
        })
    }
}

fn resolve(
    model: Option<&CatalogEfforts>,
    workload: Option<&str>,
    pinned: bool,
    explicit: Option<&str>,
    plain_elo: f64,
    plain_eq: f64,
) -> Result<Choice, Rejection> {
    let rules = model.and_then(|model| workload.and_then(|workload| model.workloads.get(workload)));
    if !pinned && rules.is_some_and(|rules| rules.excluded) {
        return Err(Rejection::WorkloadExcluded {
            workload: workload.unwrap().to_owned(),
        });
    }
    let graded = model.and_then(|model| model.graded.as_deref());
    let (mut variant, mut effort_source) = if let Some(explicit) = explicit {
        (Some(explicit), Source::Pin)
    } else if let Some(default) = rules.and_then(|rules| rules.default.as_deref()) {
        (Some(default), Source::WorkloadDefault)
    } else if let Some(graded) = graded {
        (Some(graded), Source::Graded)
    } else if let Some(default) = model.and_then(|model| model.vendor_default.as_deref()) {
        (Some(default), Source::VendorDefault)
    } else {
        (None, Source::None)
    };
    let mut minimum_unverified = None;
    if let Some(minimum) = rules
        .filter(|_| !pinned)
        .and_then(|rules| rules.minimum.as_deref())
    {
        if let Some(offered) = model.and_then(|model| model.offered.as_ref()) {
            let threshold = rank(minimum).expect("catalog minimum was validated");
            if variant.and_then(rank).is_none_or(|rank| rank < threshold) {
                variant = offered
                    .iter()
                    .filter_map(|effort| rank(effort).map(|rank| (rank, effort.as_str())))
                    .filter(|(rank, _)| *rank >= threshold)
                    .min_by_key(|(rank, _)| *rank)
                    .map(|(_, effort)| effort);
                if variant.is_none() {
                    return Err(Rejection::WorkloadMinimumUnreachable {
                        workload: workload.unwrap().to_owned(),
                        minimum: minimum.to_owned(),
                        offered: offered.clone(),
                    });
                }
                effort_source = Source::MinimumRaised;
            }
        } else {
            variant = None;
            effort_source = Source::None;
            minimum_unverified = Some(minimum.to_owned());
        }
    }
    // Without a measurement at the chosen effort, use the plain grade and
    // identify the effort it actually measures. A future cheaper-effort drop
    // must not use this fallback: it needs a measured grade at the lower effort.
    let grade = |plain, scores: &BTreeMap<String, f64>| match variant {
        Some(effort) if scores.contains_key(effort) => (scores[effort], effort),
        _ => (plain, graded.unwrap_or("unrecorded")),
    };
    let (elo, elo_effort) = model.map_or((plain_elo, "unrecorded"), |m| grade(plain_elo, &m.elo));
    let (eq, eq_effort) = model.map_or((plain_eq, "unrecorded"), |m| grade(plain_eq, &m.eq));
    Ok(Choice {
        variant: variant.map(str::to_owned),
        effort_source,
        grade: Grade {
            elo,
            eq,
            elo_effort: elo_effort.to_owned(),
            eq_effort: eq_effort.to_owned(),
        },
        minimum_unverified,
    })
}
