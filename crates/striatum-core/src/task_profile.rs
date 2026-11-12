//! Pure task-profile statistics and knowledge transfer for routing.
//!
//! The caller supplies closed, single-model tasks in the trailing window. Task
//! identity, lifecycle, persistence, and refresh scheduling belong to the caller.

use crate::model_facts::{BasketDimension, ModelFact};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const IQ_BUCKET_WIDTH: u8 = 10;
pub const MIN_TASKS: usize = 20;
pub const TRIM_PERCENT: usize = 5;
pub const PROFILE_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
pub const TASK_CLOSE_MS: i64 = 24 * 60 * 60 * 1_000;
pub const PROFILE_REFRESH_MS: i64 = 15 * 60 * 1_000;

/// Levels tried in declaration order with at least `MIN_TASKS` tasks before
/// trimming. ScaledFamily also requires supported fleet-bucket and fleet-workload
/// populations to derive its per-dimension scaling ratios.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileLevel {
    OwnCell,
    SameFamilyTier,
    SameFamily,
    ScaledFamily,
    FleetBucket,
    FleetWorkload,
}

impl ProfileLevel {
    pub const fn keeps_tokenizer(self) -> bool {
        !matches!(self, Self::FleetBucket | Self::FleetWorkload)
    }
}

/// The model line encoded in the id, not the selected reasoning variant.
/// Unknown lines do not establish a same-tier relationship.
pub fn model_tier(model_id: &str) -> Option<&'static str> {
    const TIERS: &[&str] = &[
        "sol", "luna", "astra", "opus", "sonnet", "haiku", "spark", "wonder", "flash", "pro",
    ];
    model_id
        .split(|c: char| !c.is_ascii_alphanumeric())
        .find_map(|part| {
            TIERS
                .iter()
                .copied()
                .find(|tier| part.eq_ignore_ascii_case(tier))
        })
}

/// Task totals, with reasoning excluded from the ordinary output counter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenVector {
    pub fresh_input: f64,
    pub cached_read: f64,
    pub cache_write: f64,
    pub output_excluding_reasoning: f64,
    pub reasoning: f64,
}

impl TokenVector {
    pub fn dimension(self, dimension: BasketDimension) -> f64 {
        match dimension {
            BasketDimension::FreshInput => self.fresh_input,
            BasketDimension::CachedRead => self.cached_read,
            BasketDimension::CacheWrite => self.cache_write,
            BasketDimension::OutputExcludingReasoning => self.output_excluding_reasoning,
            BasketDimension::Reasoning => self.reasoning,
        }
    }

    fn zip(self, other: Self, f: impl Fn(f64, f64) -> f64) -> Self {
        Self {
            fresh_input: f(self.fresh_input, other.fresh_input),
            cached_read: f(self.cached_read, other.cached_read),
            cache_write: f(self.cache_write, other.cache_write),
            output_excluding_reasoning: f(
                self.output_excluding_reasoning,
                other.output_excluding_reasoning,
            ),
            reasoning: f(self.reasoning, other.reasoning),
        }
    }

    fn is_valid(self) -> bool {
        BasketDimension::ALL.into_iter().all(|dimension| {
            let value = self.dimension(dimension);
            value.is_finite() && value >= 0.0
        })
    }

    /// Unknown rates are harmless only for dimensions with no observed tokens.
    pub fn list_cost(self, model: &ModelFact) -> Option<f64> {
        if !self.is_valid() {
            return None;
        }
        let mut cost = 0.0;
        for dimension in BasketDimension::ALL {
            let tokens = self.dimension(dimension);
            if tokens == 0.0 {
                continue;
            }
            let rate = model.basket_rate(dimension)?;
            if !rate.is_finite() || rate < 0.0 {
                return None;
            }
            cost += (tokens / 1_000_000.0) * rate;
        }
        cost.is_finite().then_some(cost)
    }
}

/// One closed, single-model task in the caller's trailing window, not one run.
/// The attached fact supplies that task's own list rates and family.
#[derive(Debug, Clone)]
pub struct ClosedTask<'a> {
    pub workload: &'a str,
    pub model: &'a ModelFact,
    pub iq_bucket: u8,
    pub tokens: TokenVector,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileStats {
    pub tokens: TokenVector,
    /// Population size before trimming, used for support and explanations.
    pub task_count: usize,
}

/// A served profile and the evidence needed to explain its transfer and blend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskProfile {
    pub tokens: TokenVector,
    pub level: ProfileLevel,
    /// The serving population's size before trimming, not the retained size.
    pub task_count: usize,
    pub own_task_count: usize,
    /// Zero without own data, one for an own-cell profile, otherwise n / 20.
    pub own_weight: f64,
    pub keeps_tokenizer: bool,
}

/// Invalid demands do not silently turn into a supported bucket.
pub fn iq_bucket(iq: f64) -> Option<u8> {
    (iq.is_finite() && (0.0..=100.0).contains(&iq))
        .then(|| (iq / f64::from(IQ_BUCKET_WIDTH)).floor() as u8)
}

struct RatedTask<'a> {
    task: &'a ClosedTask<'a>,
    list_cost: f64,
}

fn rated<'a>(tasks: &'a [ClosedTask<'a>]) -> Vec<RatedTask<'a>> {
    tasks
        .iter()
        .filter_map(|task| {
            Some(RatedTask {
                task,
                list_cost: task.tokens.list_cost(task.model)?,
            })
        })
        .collect()
}

fn summarize_rated(mut tasks: Vec<&RatedTask<'_>>) -> Option<ProfileStats> {
    let task_count = tasks.len();
    if task_count == 0 {
        return None;
    }
    tasks.sort_by(|a, b| {
        a.list_cost.total_cmp(&b.list_cost).then_with(|| {
            // Equal-cost tasks may have different vectors. A vector tie-break
            // makes the trimmed result independent of the input row order.
            BasketDimension::ALL
                .into_iter()
                .map(|dimension| {
                    a.task
                        .tokens
                        .dimension(dimension)
                        .total_cmp(&b.task.tokens.dimension(dimension))
                })
                .find(|order| !order.is_eq())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    let trim = task_count / (100 / TRIM_PERCENT);
    tasks.truncate(task_count - trim);
    let mut tokens = TokenVector::default();
    for (i, task) in tasks.into_iter().enumerate() {
        // An online mean avoids overflowing a sum of finite task totals.
        tokens = tokens.zip(task.task.tokens, |mean, value| {
            mean + (value - mean) / (i + 1) as f64
        });
    }
    Some(ProfileStats { tokens, task_count })
}

#[derive(Hash, PartialEq, Eq)]
enum Population<'a> {
    Own(&'a str, &'a str, Option<&'a str>, u8),
    FamilyTier(&'a str, Option<&'a str>, &'static str, u8),
    FamilyBucket(&'a str, Option<&'a str>, u8),
    Family(&'a str, Option<&'a str>),
    FleetBucket(&'a str, u8),
    Fleet(&'a str),
}

/// Summarize each shared population once, rather than rating and sorting the
/// entire history again for every candidate and IQ bucket in a refresh.
pub struct TaskProfileIndex<'a> {
    stats: HashMap<Population<'a>, ProfileStats>,
}

impl<'a> TaskProfileIndex<'a> {
    pub fn new(tasks: &'a [ClosedTask<'a>]) -> Self {
        let rated = rated(tasks);
        let mut populations: HashMap<Population<'a>, Vec<&RatedTask<'_>>> = HashMap::new();
        for rated in &rated {
            let task = rated.task;
            let workload = task.workload;
            let bucket = task.iq_bucket;
            let mut add = |key| populations.entry(key).or_default().push(rated);
            add(Population::Own(
                workload,
                &task.model.model_id,
                task.model.variant.as_deref(),
                bucket,
            ));
            add(Population::FleetBucket(workload, bucket));
            add(Population::Fleet(workload));
            {
                let family = task.model.grand_family_id();
                add(Population::FamilyBucket(workload, family, bucket));
                add(Population::Family(workload, family));
                if let Some(tier) = model_tier(&task.model.model_id) {
                    add(Population::FamilyTier(workload, family, tier, bucket));
                }
            }
        }
        Self {
            stats: populations
                .into_iter()
                .filter_map(|(key, tasks)| summarize_rated(tasks).map(|stats| (key, stats)))
                .collect(),
        }
    }

    /// Resolve the same fallback chain as `compute_profile`, using summaries
    /// priced at the rates of the models that ran the tasks, not the candidate's rates.
    pub fn compute_profile(
        &self,
        candidate: &ModelFact,
        workload: &str,
        bucket: u8,
    ) -> Option<TaskProfile> {
        let get = |key| self.stats.get(&key).copied();
        let supported =
            |stats: Option<ProfileStats>| stats.filter(|stats| stats.task_count >= MIN_TASKS);
        let own = get(Population::Own(
            workload,
            &candidate.model_id,
            candidate.variant.as_deref(),
            bucket,
        ));
        if let Some(stats) = supported(own) {
            return Some(served(stats, ProfileLevel::OwnCell, own));
        }
        {
            let family = candidate.grand_family_id();
            if let Some(tier) = model_tier(&candidate.model_id) {
                if let Some(stats) =
                    supported(get(Population::FamilyTier(workload, family, tier, bucket)))
                {
                    return Some(served(stats, ProfileLevel::SameFamilyTier, own));
                }
            }
            if let Some(stats) = supported(get(Population::FamilyBucket(workload, family, bucket)))
            {
                return Some(served(stats, ProfileLevel::SameFamily, own));
            }
            if let (Some(family), Some(bucket), Some(all)) = (
                supported(get(Population::Family(workload, family))),
                supported(get(Population::FleetBucket(workload, bucket))),
                supported(get(Population::Fleet(workload))),
            ) {
                let ratio =
                    bucket.tokens.zip(
                        all.tokens,
                        |bucket, all| {
                            if all == 0.0 {
                                1.0
                            } else {
                                bucket / all
                            }
                        },
                    );
                let tokens = family.tokens.zip(ratio, |family, ratio| family * ratio);
                if tokens.is_valid() {
                    return Some(served(
                        ProfileStats { tokens, ..family },
                        ProfileLevel::ScaledFamily,
                        own,
                    ));
                }
            }
        }
        if let Some(stats) = supported(get(Population::FleetBucket(workload, bucket))) {
            return Some(served(stats, ProfileLevel::FleetBucket, own));
        }
        supported(get(Population::Fleet(workload)))
            .map(|stats| served(stats, ProfileLevel::FleetWorkload, own))
    }
}

/// Pool individual tasks, rank at their own rates, and trim the most costly
/// `floor(0.05 * n)` tasks.
/// Invalid totals or unknown rates for observed dimensions cannot be ranked and
/// are excluded before counting support. An empty population has no estimate.
pub fn summarize(tasks: &[ClosedTask<'_>]) -> Option<ProfileStats> {
    summarize_rated(rated(tasks).iter().collect())
}

fn same_family(a: &ModelFact, b: &ModelFact) -> bool {
    a.grand_family_id() == b.grand_family_id()
}

fn served(stats: ProfileStats, level: ProfileLevel, own: Option<ProfileStats>) -> TaskProfile {
    let own_task_count = own.map_or(0, |own| own.task_count);
    let own_weight = if level == ProfileLevel::OwnCell {
        1.0
    } else {
        own_task_count as f64 / MIN_TASKS as f64
    };
    let tokens = own
        .filter(|_| level != ProfileLevel::OwnCell)
        .map_or(stats.tokens, |own| {
            own.tokens.zip(stats.tokens, |own, fallback| {
                own * own_weight + fallback * (1.0 - own_weight)
            })
        });
    TaskProfile {
        tokens,
        level,
        task_count: stats.task_count,
        own_task_count,
        own_weight,
        keeps_tokenizer: level.keeps_tokenizer(),
    }
}

/// Resolve the ordered hierarchy from closed tasks, without a store or clock.
/// Family pools include the candidate's own tasks as individual observations.
/// Call during refresh and cache the returned profiles for selection.
pub fn compute_profile(
    tasks: &[ClosedTask<'_>],
    candidate: &ModelFact,
    workload: &str,
    bucket: u8,
) -> Option<TaskProfile> {
    let tasks = rated(tasks);
    let population = |predicate: &dyn Fn(&ClosedTask<'_>) -> bool| {
        summarize_rated(
            tasks
                .iter()
                .filter(|task| task.task.workload == workload && predicate(task.task))
                .collect(),
        )
    };
    let supported =
        |stats: Option<ProfileStats>| stats.filter(|stats| stats.task_count >= MIN_TASKS);
    let own = population(&|task| {
        task.iq_bucket == bucket
            && task.model.model_id == candidate.model_id
            && task.model.variant == candidate.variant
    });
    if let Some(stats) = supported(own) {
        return Some(served(stats, ProfileLevel::OwnCell, own));
    }
    let tier = model_tier(&candidate.model_id);
    let family_bucket =
        |task: &ClosedTask<'_>| task.iq_bucket == bucket && same_family(task.model, candidate);
    if tier.is_some() {
        if let Some(stats) = supported(population(&|task| {
            family_bucket(task) && model_tier(&task.model.model_id) == tier
        })) {
            return Some(served(stats, ProfileLevel::SameFamilyTier, own));
        }
    }
    if let Some(stats) = supported(population(&family_bucket)) {
        return Some(served(stats, ProfileLevel::SameFamily, own));
    }
    let fleet_bucket = population(&|task| task.iq_bucket == bucket);
    let fleet_workload = population(&|_| true);
    if let (Some(family), Some(bucket), Some(all)) = (
        supported(population(&|task| same_family(task.model, candidate))),
        supported(fleet_bucket),
        supported(fleet_workload),
    ) {
        let ratio = bucket.tokens.zip(
            all.tokens,
            |bucket, all| if all == 0.0 { 1.0 } else { bucket / all },
        );
        let tokens = family.tokens.zip(ratio, |family, ratio| family * ratio);
        if tokens.is_valid() {
            return Some(served(
                ProfileStats { tokens, ..family },
                ProfileLevel::ScaledFamily,
                own,
            ));
        }
    }
    if let Some(stats) = supported(fleet_bucket) {
        return Some(served(stats, ProfileLevel::FleetBucket, own));
    }
    supported(fleet_workload).map(|stats| served(stats, ProfileLevel::FleetWorkload, own))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batched_refresh_matches_full_recompute_over_randomized_history_edits() {
        let mut seed = 0x5eed_u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed
        };
        let mut facts: Vec<_> = (0..24)
            .map(|i| {
                let mut fact = model(
                    &format!("model-{}-{}", i / 2, ["sol", "luna", "unknown"][i % 3]),
                    [Some("gpt"), Some("claude"), None, Some("other")][i % 4],
                    (i % 5) as f64,
                );
                fact.provider_id = format!("provider-{}", i % 2);
                fact.variant = (i % 3 != 0).then(|| format!("effort-{}", i % 2));
                fact
            })
            .collect();
        let workloads = ["mason", "sidekick", "alfonso", "unseen"];
        let mut history = Vec::new();
        for _ in 0..600 {
            history.push((
                next() as usize % facts.len(),
                next() as usize % 3,
                (next() % 11) as u8,
                TokenVector {
                    fresh_input: (next() % 100) as f64,
                    cached_read: (next() % 50) as f64,
                    cache_write: (next() % 20) as f64,
                    output_excluding_reasoning: (next() % 100) as f64,
                    reasoning: (next() % 10) as f64,
                },
            ));
        }
        for model in 0..facts.len() {
            for n in 0..25 {
                history.push((model, model % 3, 7, tokens(n as f64)));
            }
        }
        let holdouts = [
            model("unseen-sol", Some("gpt"), 2.0),
            model("unseen", Some("gpt"), 1.0),
            model("alien", Some("alien"), 3.0),
        ];
        let mut levels_seen = [false; 6];
        for refresh in 0..12 {
            // Removals represent expiry or reopening; replacements represent
            // newly closed tasks, changed totals, and changed classification.
            history.drain(..17);
            for _ in 0..19 {
                history.push((
                    next() as usize % facts.len(),
                    next() as usize % 3,
                    (next() % 11) as u8,
                    tokens((next() % 80) as f64),
                ));
            }
            history[0].3.fresh_input = f64::NAN;
            history[1].3.reasoning = -1.0;
            facts[refresh].input_rate = Some((next() % 10) as f64);
            facts[refresh].family = Some(format!("changed-{}", refresh % 2));
            let tasks: Vec<_> = history
                .iter()
                .map(|(model, workload, bucket, tokens)| ClosedTask {
                    model: &facts[*model],
                    workload: workloads[*workload],
                    iq_bucket: *bucket,
                    tokens: *tokens,
                })
                .collect();
            let index = TaskProfileIndex::new(&tasks);
            for workload in workloads {
                for bucket in 0..=10 {
                    for candidate in facts.iter().chain(&holdouts) {
                        let full = compute_profile(&tasks, candidate, workload, bucket);
                        let batched = index.compute_profile(candidate, workload, bucket);
                        assert_eq!(
                            batched, full,
                            "refresh={refresh} {workload}/{bucket} {}",
                            candidate.model_id
                        );
                        if let Some(profile) = &batched {
                            levels_seen[profile.level as usize] = true;
                        }
                        assert_eq!(
                            batched.map(|p| p.tokens.list_cost(candidate)),
                            full.map(|p| p.tokens.list_cost(candidate))
                        );
                    }
                }
            }
        }
        assert!(
            levels_seen.into_iter().all(|seen| seen),
            "every fallback level must be exercised"
        );
    }

    fn model(id: &str, family: Option<&str>, rate: f64) -> ModelFact {
        serde_json::from_value(serde_json::json!({
            "providerID": "provider", "modelID": id, "family": family,
            "scores": {"elo": 0.0, "eq": 0.0, "speed": 0.0},
            "confidence": "curated", "inputRate": rate, "cacheReadRate": rate,
            "costPerMTokOut": rate
        }))
        .unwrap()
    }

    fn tokens(value: f64) -> TokenVector {
        TokenVector {
            fresh_input: value,
            cached_read: value * 2.0,
            cache_write: value * 3.0,
            output_excluding_reasoning: value * 4.0,
            reasoning: value * 5.0,
        }
    }

    fn samples<'a>(
        model: &'a ModelFact,
        count: usize,
        bucket: u8,
        tokens: TokenVector,
    ) -> Vec<ClosedTask<'a>> {
        (0..count)
            .map(|_| ClosedTask {
                workload: "mason",
                model,
                iq_bucket: bucket,
                tokens,
            })
            .collect()
    }

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }

    fn vector_close(actual: TokenVector, expected: TokenVector) {
        for dimension in BasketDimension::ALL {
            close(actual.dimension(dimension), expected.dimension(dimension));
        }
    }

    #[test]
    fn ten_own_tasks_blend_half_own_and_half_first_fallback() {
        let own = model("gpt-6-sol", Some("gpt"), 1.0);
        let donor = model("gpt-5-sol", Some("gpt"), 1.0);
        let mut tasks = samples(&own, 10, 7, tokens(10.0));
        tasks.extend(samples(&donor, 20, 7, tokens(30.0)));
        let profile = compute_profile(&tasks, &own, "mason", 7).unwrap();
        // The family-tier pool contains all 30 tasks, and trims one donor.
        let fallback = (100.0 + 19.0 * 30.0) / 29.0;
        vector_close(profile.tokens, tokens(5.0 + 0.5 * fallback));
        assert_eq!(profile.level, ProfileLevel::SameFamilyTier);
        assert_eq!(profile.task_count, 30);
        assert_eq!(profile.own_task_count, 10);
        assert_eq!(profile.own_weight, 0.5);
        assert!(profile.keeps_tokenizer);
    }

    #[test]
    fn thirty_donor_tasks_are_pooled_and_trim_one_at_their_own_rates() {
        let cheap = model("gpt-5-sol", Some("gpt"), 1.0);
        let expensive = model("gpt-6-sol", Some("gpt"), 100.0);
        let candidate = model("gpt-7-sol", Some("gpt"), 0.01);
        let cheap_tokens = TokenVector {
            fresh_input: 100.0,
            cached_read: 10.0,
            cache_write: 20.0,
            output_excluding_reasoning: 30.0,
            reasoning: 40.0,
        };
        let mut tasks = samples(&cheap, 25, 7, cheap_tokens);
        tasks.extend(samples(&expensive, 4, 7, tokens(1.0)));
        tasks.extend(samples(
            &expensive,
            1,
            7,
            TokenVector {
                fresh_input: 2.0,
                ..tokens(1.0)
            },
        ));
        let stats = summarize(&tasks).unwrap();
        assert_eq!(stats.task_count, 30);
        vector_close(
            stats.tokens,
            TokenVector {
                fresh_input: 2504.0 / 29.0,
                cached_read: 258.0 / 29.0,
                cache_write: 512.0 / 29.0,
                output_excluding_reasoning: 766.0 / 29.0,
                reasoning: 1020.0 / 29.0,
            },
        );
        let profile = compute_profile(&tasks, &candidate, "mason", 7).unwrap();
        assert_eq!(profile.level, ProfileLevel::SameFamilyTier);
        assert_eq!(profile.task_count, 30);
        vector_close(profile.tokens, stats.tokens);
    }

    #[test]
    fn fallback_order_and_explanation_record_the_serving_population() {
        let own = model("gpt-6-sol", Some("gpt"), 1.0);
        let tier = model("gpt-5-sol", Some("gpt"), 1.0);
        let family = model("gpt-6-luna", Some("gpt"), 1.0);
        let all_family = model("gpt-6-astra", Some("gpt"), 1.0);
        let fleet_bucket = model("claude-opus-5.5", Some("claude"), 1.0);
        let fleet_all = model("gemini-4-pro", Some("gemini"), 1.0);
        let groups = [
            samples(&own, 20, 7, tokens(10.0)),
            samples(&tier, 20, 7, tokens(20.0)),
            samples(&family, 20, 7, tokens(30.0)),
            samples(&all_family, 20, 4, tokens(40.0)),
            samples(&fleet_bucket, 20, 7, tokens(50.0)),
            samples(&fleet_all, 20, 4, tokens(60.0)),
        ];
        let levels = [
            ProfileLevel::OwnCell,
            ProfileLevel::SameFamilyTier,
            ProfileLevel::SameFamily,
            ProfileLevel::ScaledFamily,
            ProfileLevel::FleetBucket,
            ProfileLevel::FleetWorkload,
        ];
        for (start, level) in levels.into_iter().enumerate() {
            let tasks: Vec<_> = groups[start..].iter().flatten().cloned().collect();
            let profile = compute_profile(&tasks, &own, "mason", 7).unwrap();
            assert_eq!(profile.level, level);
            assert_eq!(profile.task_count, 20);
            assert_eq!(profile.keeps_tokenizer, start < 4);
            assert_eq!(profile.own_task_count, if start == 0 { 20 } else { 0 });
            assert_eq!(profile.own_weight, if start == 0 { 1.0 } else { 0.0 });
            let explanation = serde_json::to_value(&profile).unwrap();
            assert_eq!(explanation["taskCount"], 20);
            assert_eq!(explanation["keepsTokenizer"], start < 4);
        }
    }

    #[test]
    fn scaled_family_uses_per_dimension_trimmed_fleet_ratio() {
        let candidate = model("gpt-6-sol", Some("gpt"), 1.0);
        let donor = model("gpt-6-luna", Some("gpt"), 1.0);
        let fleet = model("claude-opus-5.5", Some("claude"), 1.0);
        let family_tokens = TokenVector {
            fresh_input: 10.0,
            cached_read: 100.0,
            cache_write: 2.0,
            output_excluding_reasoning: 0.0,
            reasoning: 0.0,
        };
        let bucket_tokens = TokenVector {
            fresh_input: 30.0,
            cached_read: 10.0,
            cache_write: 4.0,
            output_excluding_reasoning: 0.0,
            reasoning: 0.0,
        };
        let mut tasks = samples(&donor, 20, 4, family_tokens);
        tasks.extend(samples(&fleet, 20, 7, bucket_tokens));
        let profile = compute_profile(&tasks, &candidate, "mason", 7).unwrap();
        assert_eq!(profile.level, ProfileLevel::ScaledFamily);
        assert_eq!(profile.task_count, 20);
        // Two of the more costly family tasks leave the all-bucket fleet mean.
        vector_close(
            profile.tokens,
            TokenVector {
                fresh_input: 10.0 * 30.0 / ((18.0 * 10.0 + 20.0 * 30.0) / 38.0),
                cached_read: 100.0 * 10.0 / ((18.0 * 100.0 + 20.0 * 10.0) / 38.0),
                cache_write: 2.0 * 4.0 / ((18.0 * 2.0 + 20.0 * 4.0) / 38.0),
                output_excluding_reasoning: 0.0,
                reasoning: 0.0,
            },
        );
        assert!(profile.tokens.is_valid());
    }

    #[test]
    fn zero_fleet_denominator_uses_one_instead_of_nan_or_zero_ratio() {
        let candidate = model("gpt-6-sol", Some("gpt"), 1.0);
        let donor = model("gpt-6-luna", Some("gpt"), 1.0);
        let fleet = model("claude-opus-5.5", Some("claude"), 1.0);
        let mut tasks = samples(
            &donor,
            20,
            4,
            TokenVector {
                fresh_input: 1.0,
                ..TokenVector::default()
            },
        );
        // The only cached-read observation survives the family trim but is in
        // the top two fleet costs, leaving a zero fleet cached-read mean.
        tasks[19].tokens.cached_read = 100.0;
        tasks[18].tokens.fresh_input = 200.0;
        tasks.extend(samples(
            &fleet,
            20,
            7,
            TokenVector {
                fresh_input: 1.0,
                ..TokenVector::default()
            },
        ));
        let profile = compute_profile(&tasks, &candidate, "mason", 7).unwrap();
        assert_eq!(profile.level, ProfileLevel::ScaledFamily);
        close(profile.tokens.cached_read, 100.0 / 19.0);
        assert!(profile.tokens.is_valid());
    }

    #[test]
    fn scaled_family_is_skipped_for_unseen_or_thin_fleet_bucket() {
        let own = model("gpt-6-sol", Some("gpt"), 1.0);
        let donor = model("gpt-6-luna", Some("gpt"), 1.0);
        let fleet = model("claude-opus-5.5", Some("claude"), 1.0);
        let mut tasks = samples(&donor, 20, 4, tokens(10.0));
        let profile = compute_profile(&tasks, &own, "mason", 7).unwrap();
        assert_eq!(profile.level, ProfileLevel::FleetWorkload);
        assert!(!profile.keeps_tokenizer);
        tasks.extend(samples(&fleet, 19, 7, tokens(20.0)));
        assert_eq!(
            compute_profile(&tasks, &own, "mason", 7).unwrap().level,
            ProfileLevel::FleetWorkload
        );
    }

    #[test]
    fn support_is_counted_before_trimming_and_missing_data_is_none() {
        let own = model("gpt-6-sol", Some("gpt"), 1.0);
        assert!(compute_profile(&[], &own, "mason", 7).is_none());
        assert!(summarize(&[]).is_none());
        let mut tasks = samples(&own, 19, 7, tokens(10.0));
        assert!(compute_profile(&tasks, &own, "mason", 7).is_none());
        tasks.extend(samples(&own, 1, 7, tokens(1_000.0)));
        let profile = compute_profile(&tasks, &own, "mason", 7).unwrap();
        assert_eq!(profile.level, ProfileLevel::OwnCell);
        assert_eq!(profile.task_count, 20);
        vector_close(profile.tokens, tokens(10.0));
        assert!(compute_profile(&tasks, &own, "sidekick", 7).is_none());
    }

    #[test]
    fn identity_uses_variant_but_family_tier_uses_model_line() {
        let mut own = model("gpt-6-sol", Some("gpt"), 1.0);
        own.variant = Some("high".into());
        let mut donor = own.clone();
        donor.variant = Some("low".into());
        let tasks = samples(&donor, 20, 7, tokens(10.0));
        let profile = compute_profile(&tasks, &own, "mason", 7).unwrap();
        assert_eq!(profile.level, ProfileLevel::SameFamilyTier);
        assert_eq!(profile.own_task_count, 0);
        own.family = None;
        assert_eq!(
            compute_profile(&tasks, &own, "mason", 7).unwrap().level,
            ProfileLevel::FleetBucket
        );
        assert_eq!(iq_bucket(75.0), Some(7));
        assert_eq!(iq_bucket(45.0), Some(4));
        assert_eq!(iq_bucket(100.0), Some(10));
        assert_eq!(iq_bucket(f64::NAN), None);
        assert_eq!(iq_bucket(-1.0), None);
    }

    #[test]
    fn list_cost_uses_dimension_rates_and_existing_missing_rate_fallbacks() {
        let mut fact = model("gpt-6-sol", Some("gpt"), 1.0);
        fact.input_rate = Some(2.0);
        fact.cost_per_m_tok_out = Some(5.0);
        fact.cache_read_rate = Some(0.5);
        close(
            tokens(1_000_000.0).list_cost(&fact).unwrap(),
            2.0 + 1.0 + 6.0 + 20.0 + 25.0,
        );
        fact.reasoning_rate = Some(7.0);
        fact.cache_write_rate = Some(3.0);
        close(
            tokens(1_000_000.0).list_cost(&fact).unwrap(),
            2.0 + 1.0 + 9.0 + 20.0 + 35.0,
        );
        fact.cache_read_rate = None;
        assert!(tokens(1.0).list_cost(&fact).is_none());
        assert!(TokenVector {
            fresh_input: 10.0,
            ..TokenVector::default()
        }
        .list_cost(&fact)
        .is_some());
    }

    #[test]
    fn invalid_or_unpriceable_samples_never_manufacture_support() {
        let mut fact = model("gpt-6-sol", Some("gpt"), 1.0);
        let mut tasks = samples(&fact, 19, 7, tokens(10.0));
        for value in [f64::NAN, f64::INFINITY, -1.0] {
            tasks.extend(samples(
                &fact,
                1,
                7,
                TokenVector {
                    fresh_input: value,
                    ..tokens(10.0)
                },
            ));
        }
        assert_eq!(summarize(&tasks).unwrap().task_count, 19);
        assert!(compute_profile(&tasks, &fact, "mason", 7).is_none());
        fact.cache_read_rate = None;
        let tasks = samples(&fact, 20, 7, tokens(10.0));
        assert!(compute_profile(&tasks, &fact, "mason", 7).is_none());
    }

    #[test]
    fn equal_cost_ties_have_row_order_independent_trimming() {
        let fact = model("gpt-6-sol", Some("gpt"), 0.0);
        let mut tasks: Vec<_> = (1..=40)
            .flat_map(|n| samples(&fact, 1, 7, tokens(n as f64)))
            .collect();
        let forward = summarize(&tasks).unwrap();
        tasks.reverse();
        assert_eq!(summarize(&tasks).unwrap(), forward);
    }

    #[test]
    fn tiers_come_from_model_lines_not_variants_or_substrings() {
        for tier in [
            "sol", "luna", "astra", "opus", "sonnet", "haiku", "spark", "wonder", "flash", "pro",
        ] {
            assert_eq!(model_tier(&format!("vendor/model-{tier}-6.1")), Some(tier));
        }
        assert_eq!(model_tier("Claude-OPUS-5.5"), Some("opus"));
        assert_eq!(model_tier("provider/model-professional"), None);
        assert_eq!(model_tier("gpt-6.1"), None);
    }
}
