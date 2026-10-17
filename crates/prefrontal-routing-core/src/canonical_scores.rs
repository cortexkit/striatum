//! One quality score per model, shared by every route that serves it.
//!
//! The same weights are served under many route keys: the maker's own
//! provider, resellers such as OpenRouter, contributor tiers and catalog
//! aliases. Fusiform states which routes serve the same model through
//! `model.canonical_id` (passed through verbatim from models.dev). Routing
//! stores scores per route key, so without this step every reseller route of a
//! scored model reads Elo 0 until someone scores it by hand.
//!
//! Which fields are shared: Elo and EQ only. They measure the weights, so they
//! are the same wherever the model is served. Speed is measured per route (a
//! reseller's throughput is its own), and context, output ceilings, rates,
//! login methods and capability flags are what each provider chooses to serve,
//! so they stay on the route's own row.
//!
//! Resolution order for a route's Elo and EQ:
//! 1. the route's own operator score (a seed or override row with a positive Elo);
//! 2. else an operator score from another route with the same canonical id and
//!    the same variant;
//! 3. else whatever the route already has (an AA-inferred estimate, or nothing,
//!    in which case it stays unscored).
//!
//! A route whose canonical id is null never borrows: null means the upstream
//! states no identity, not that the route is unique, and names are never used
//! to guess one.

use std::collections::BTreeMap;

use crate::model_facts::{Confidence, ModelFact, ScoreProvenance};

/// One operator-scored route inside a group whose scores disagree.
#[derive(Debug, Clone, PartialEq)]
pub struct ConflictingScore {
    pub route_key: String,
    pub elo: f64,
    pub eq: f64,
}

/// Routes that serve the same model (same canonical id and variant) but carry
/// different operator scores. Each route keeps its own score; routes without
/// one borrow from the preferred donor named here.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalScoreConflict {
    pub canonical_id: String,
    pub variant: Option<String>,
    /// The route whose score unscored routes in the group borrow.
    pub donor_route_key: String,
    pub scores: Vec<ConflictingScore>,
}

impl CanonicalScoreConflict {
    /// A stable identity for "this exact disagreement", so a caller can report
    /// it once and report it again only when the scores change.
    pub fn signature(&self) -> String {
        let scores = self
            .scores
            .iter()
            .map(|score| format!("{}={}/{}", score.route_key, score.elo, score.eq))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{}|{}|{}",
            self.canonical_id,
            self.variant.as_deref().unwrap_or(""),
            scores
        )
    }
}

/// What resolution did, parallel to the facts it was given.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CanonicalScoreResolution {
    /// For each fact, the route key it borrowed its Elo and EQ from, if any.
    pub borrowed_from: Vec<Option<String>>,
    pub conflicts: Vec<CanonicalScoreConflict>,
}

/// Whether a fact carries a score an operator placed (a migration seed or a
/// `route.model_upsert`/config override). A row stamped curated with no Elo
/// has nothing to share and may borrow instead.
pub fn has_operator_score(fact: &ModelFact) -> bool {
    matches!(
        fact.score_provenance,
        ScoreProvenance::Curated | ScoreProvenance::Override
    ) && fact.scores.elo.is_finite()
        && fact.scores.elo > 0.0
}

/// A canonical id that is not blank. The value itself is compared verbatim.
fn canonical(value: Option<&String>) -> Option<&str> {
    value.map(String::as_str).filter(|id| !id.trim().is_empty())
}

/// Fill Elo and EQ on routes without an operator score from an operator-scored
/// route that serves the same model. `canonical_ids` is parallel to `facts`.
///
/// When several routes of one model carry operator scores, the donor is chosen
/// in this order: the route whose `provider/model` equals the canonical id (the
/// maker's own listing), then an override over a seed (the newer explicit
/// placement), then the smallest route key, so the choice never depends on
/// read order.
pub fn resolve_canonical_scores(
    facts: &mut [ModelFact],
    canonical_ids: &[Option<String>],
) -> CanonicalScoreResolution {
    assert_eq!(
        facts.len(),
        canonical_ids.len(),
        "canonical ids must be parallel to facts"
    );
    // (canonical id, variant) -> indexes of operator-scored routes.
    let mut donors: BTreeMap<(String, Option<String>), Vec<usize>> = BTreeMap::new();
    for (index, fact) in facts.iter().enumerate() {
        let Some(id) = canonical(canonical_ids[index].as_ref()) else {
            continue;
        };
        if has_operator_score(fact) {
            donors
                .entry((id.to_string(), fact.variant.clone()))
                .or_default()
                .push(index);
        }
    }

    let route_key = |fact: &ModelFact| {
        crate::model_facts::make_route_key(
            &fact.provider_id,
            &fact.model_id,
            fact.variant.as_deref(),
        )
    };
    let mut preferred: BTreeMap<(String, Option<String>), usize> = BTreeMap::new();
    let mut conflicts = Vec::new();
    for ((id, variant), indexes) in &donors {
        let best = *indexes
            .iter()
            .min_by_key(|index| {
                let fact = &facts[**index];
                let home = format!("{}/{}", fact.provider_id, fact.model_id) == *id;
                let seed = fact.score_provenance != ScoreProvenance::Override;
                (!home, seed, route_key(fact))
            })
            .expect("a donor group is never empty");
        preferred.insert((id.clone(), variant.clone()), best);
        let first = &facts[indexes[0]].scores;
        let disagree = indexes.iter().any(|index| {
            let scores = &facts[*index].scores;
            scores.elo != first.elo || scores.eq != first.eq
        });
        if disagree {
            let mut scores = indexes
                .iter()
                .map(|index| ConflictingScore {
                    route_key: route_key(&facts[*index]),
                    elo: facts[*index].scores.elo,
                    eq: facts[*index].scores.eq,
                })
                .collect::<Vec<_>>();
            scores.sort_by(|left, right| left.route_key.cmp(&right.route_key));
            conflicts.push(CanonicalScoreConflict {
                canonical_id: id.clone(),
                variant: variant.clone(),
                donor_route_key: route_key(&facts[best]),
                scores,
            });
        }
    }

    let mut borrowed_from = vec![None; facts.len()];
    let mut grants = Vec::new();
    for (index, fact) in facts.iter().enumerate() {
        if has_operator_score(fact) {
            continue;
        }
        let Some(id) = canonical(canonical_ids[index].as_ref()) else {
            continue;
        };
        if let Some(donor) = preferred.get(&(id.to_string(), fact.variant.clone())) {
            grants.push((index, *donor));
        }
    }
    for (index, donor) in grants {
        let donor_fact = facts[donor].clone();
        let fact = &mut facts[index];
        fact.scores.elo = donor_fact.scores.elo;
        fact.scores.eq = donor_fact.scores.eq;
        // The borrowed score is the donor's placement, so the row is as
        // routable and as trusted as the donor. Its AA-inference metadata
        // described an estimate that is no longer in force.
        fact.score_provenance = donor_fact.score_provenance;
        if donor_fact.confidence == Confidence::Curated {
            fact.confidence = Confidence::Curated;
        }
        fact.aa_index = None;
        fact.review_by_ms = None;
        fact.eq_source = None;
        borrowed_from[index] = Some(route_key(&donor_fact));
    }

    CanonicalScoreResolution {
        borrowed_from,
        conflicts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_facts::make_route_key;
    use crate::model_facts_seed::model_facts_seed;

    fn row(route: &str, elo: f64, eq: f64, provenance: ScoreProvenance) -> ModelFact {
        let (provider, model) = route.split_once('/').unwrap();
        let mut fact = model_facts_seed()[0].clone();
        fact.provider_id = provider.to_string();
        fact.model_id = model.to_string();
        fact.variant = None;
        fact.scores.elo = elo;
        fact.scores.eq = eq;
        fact.score_provenance = provenance;
        fact.confidence = if provenance == ScoreProvenance::Unscored {
            Confidence::Unknown
        } else {
            Confidence::Curated
        };
        fact
    }

    fn unscored(route: &str) -> ModelFact {
        row(route, 0.0, 0.0, ScoreProvenance::Unscored)
    }

    fn ids(values: &[Option<&str>]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| value.map(str::to_string))
            .collect()
    }

    fn key(fact: &ModelFact) -> String {
        make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
    }

    #[test]
    fn unscored_routes_of_one_model_borrow_its_operator_score() {
        let mut facts = vec![
            row(
                "meta/muse-spark-1.3",
                1800.0,
                1500.0,
                ScoreProvenance::Override,
            ),
            unscored("meta/muse-spark-1.3-contributor"),
            unscored("openrouter/meta/muse-spark-1.3"),
        ];
        let resolution = resolve_canonical_scores(
            &mut facts,
            &ids(&[
                Some("meta/muse-spark-1.3"),
                Some("meta/muse-spark-1.3"),
                Some("meta/muse-spark-1.3"),
            ]),
        );
        for fact in &facts {
            assert_eq!(
                (fact.scores.elo, fact.scores.eq),
                (1800.0, 1500.0),
                "{}",
                key(fact)
            );
            assert_eq!(fact.score_provenance, ScoreProvenance::Override);
            assert_eq!(fact.confidence, Confidence::Curated);
        }
        assert_eq!(
            resolution.borrowed_from,
            vec![
                None,
                Some("meta/muse-spark-1.3".to_string()),
                Some("meta/muse-spark-1.3".to_string())
            ]
        );
        assert!(resolution.conflicts.is_empty());
    }

    #[test]
    fn a_route_with_no_canonical_id_never_borrows() {
        let mut facts = vec![
            row(
                "meta/muse-spark-1.3",
                1800.0,
                1500.0,
                ScoreProvenance::Override,
            ),
            unscored("opencode/muse-spark-1.3-contributor-free"),
            unscored("openrouter/meta/muse-spark-1.3-contributor"),
        ];
        let resolution = resolve_canonical_scores(
            &mut facts,
            &ids(&[Some("meta/muse-spark-1.3"), None, Some("")]),
        );
        for fact in &facts[1..] {
            assert_eq!(fact.scores.elo, 0.0, "{}", key(fact));
            assert_eq!(fact.score_provenance, ScoreProvenance::Unscored);
        }
        assert_eq!(resolution.borrowed_from, vec![None, None, None]);
    }

    #[test]
    fn a_routes_own_score_wins_and_the_disagreement_is_reported() {
        let mut facts = vec![
            row(
                "openrouter/meta/muse-spark-1.3",
                1700.0,
                1400.0,
                ScoreProvenance::Override,
            ),
            row(
                "meta/muse-spark-1.3",
                1800.0,
                1500.0,
                ScoreProvenance::Curated,
            ),
            unscored("meta/muse-spark-1.3-contributor"),
        ];
        let resolution = resolve_canonical_scores(
            &mut facts,
            &ids(&[
                Some("meta/muse-spark-1.3"),
                Some("meta/muse-spark-1.3"),
                Some("meta/muse-spark-1.3"),
            ]),
        );
        assert_eq!(facts[0].scores.elo, 1700.0, "own score is kept");
        assert_eq!(facts[1].scores.elo, 1800.0, "own score is kept");
        // The maker's own listing is the preferred donor over a reseller's override.
        assert_eq!((facts[2].scores.elo, facts[2].scores.eq), (1800.0, 1500.0));
        assert_eq!(resolution.conflicts.len(), 1);
        let conflict = &resolution.conflicts[0];
        assert_eq!(conflict.canonical_id, "meta/muse-spark-1.3");
        assert_eq!(conflict.donor_route_key, "meta/muse-spark-1.3");
        assert_eq!(
            conflict
                .scores
                .iter()
                .map(|score| score.route_key.as_str())
                .collect::<Vec<_>>(),
            vec!["meta/muse-spark-1.3", "openrouter/meta/muse-spark-1.3"]
        );
    }

    #[test]
    fn variants_borrow_only_from_the_same_variant() {
        let mut high = row(
            "meta/muse-spark-1.3",
            1850.0,
            1500.0,
            ScoreProvenance::Override,
        );
        high.variant = Some("high".into());
        let mut reseller_high = unscored("openrouter/meta/muse-spark-1.3");
        reseller_high.variant = Some("high".into());
        let mut facts = vec![
            row(
                "meta/muse-spark-1.3",
                1800.0,
                1500.0,
                ScoreProvenance::Override,
            ),
            high,
            reseller_high,
            unscored("openrouter/meta/muse-spark-1.3"),
        ];
        resolve_canonical_scores(&mut facts, &ids(&[Some("meta/muse-spark-1.3"); 4]));
        assert_eq!(facts[2].scores.elo, 1850.0);
        assert_eq!(facts[3].scores.elo, 1800.0);
    }

    #[test]
    fn an_inferred_estimate_yields_to_a_peer_placement_but_survives_without_one() {
        let mut inferred = row(
            "openrouter/meta/muse-spark-1.3",
            1600.0,
            1300.0,
            ScoreProvenance::AaInferred,
        );
        inferred.aa_index = Some(61.0);
        let lone = row(
            "openrouter/acme/lone-1",
            1500.0,
            1200.0,
            ScoreProvenance::AaInferred,
        );
        let mut facts = vec![
            row(
                "meta/muse-spark-1.3",
                1800.0,
                1500.0,
                ScoreProvenance::Curated,
            ),
            inferred,
            lone,
        ];
        resolve_canonical_scores(
            &mut facts,
            &ids(&[
                Some("meta/muse-spark-1.3"),
                Some("meta/muse-spark-1.3"),
                Some("acme/lone-1"),
            ]),
        );
        assert_eq!(facts[1].scores.elo, 1800.0);
        assert_eq!(facts[1].score_provenance, ScoreProvenance::Curated);
        assert_eq!(facts[1].aa_index, None);
        assert_eq!(facts[2].scores.elo, 1500.0);
        assert_eq!(facts[2].score_provenance, ScoreProvenance::AaInferred);
    }
}
