//! Over-qualification penalty replay.
//!
//! Re-runs every recorded single-route decision of the study window through the
//! routing core's own selection (`select_model_route_with_config`) with the
//! inputs `extract.py` recovered from the decision row, first with no penalty
//! (the baseline that must reproduce the recorded winner) and then once per grid
//! point of the proposed term.
//!
//! Two placements of the term are replayed:
//! - `pre`: the term is added to the route's own selection cost, which is what
//!   "add it to selection cost" means in today's code. It is run through the
//!   real library by raising the route's quota pressure by `penalty / 0.5`
//!   (selection cost = cost + 0.5 x pressure, nothing else reads pressure).
//! - `post`: the term is added AFTER the subscription-pool collapse, so a route
//!   keeps its own term even when a cheaper pool-mate sets the pool's price.
//!   The library has no hook for this, so the ordering is re-implemented here;
//!   the same re-implementation is checked against the library on `pre` for
//!   every decision and grid point (`reimpl_mismatch` in the output).
//! - `guard`: `pre`, except the term may only move a decision to a route whose
//!   adequacy is at least the baseline winner's; otherwise the baseline winner
//!   stands (re-implemented ordering only).
//!
//! Build in Striatum: copy to crates/striatum-store/examples/overq_replay.rs and
//! `cargo run --release -p striatum-store --example overq_replay --
//! <store copy> <decisions.jsonl> <world.json> <out.jsonl>`. Opening the store
//! applies the routing store's migrations, so always point it at a copy.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};

use striatum_core::model_facts::{
    make_route_key, ModelFact, ModelRequirements, ModelRoute, RoutePricing,
};
use striatum_core::usage_facts::{CLASS_PRODUCER, COUNTERS_PRODUCER};
use striatum_core::model_router::price_selection_shortlist;
use striatum_core::provider_model_spec::ExclusionPolicy;
use striatum_core::route_state_store::RouteStateStore;
use striatum_core::{
    admit_routes, select_model_route_with_config, ModelRouteStateSnapshot, RouteRuntimeState,
    SelectModelRouteInput, COST_REFERENCE_PER_MTOK, DEFAULT_ROUTING_CONFIG,
    OBJECTIVE_WEIGHT_QUOTA, RELATIVE_SHORTLIST_DELTA,
};
use striatum_store::{dev_descriptor, SqliteRouteStateStore};
use serde_json::{json, Value};

const EFFECTIVELY_FREE: f64 = 0.05;

#[derive(Clone, Copy)]
struct Grid {
    x: f64,
    h: f64,
    base: f64,
}

fn penalty(model_iq: f64, demand: f64, g: Grid) -> f64 {
    let surplus = model_iq - demand;
    if surplus <= g.x {
        0.0
    } else {
        g.base * (2f64.powf((surplus - g.x) / g.h) - 1.0)
    }
}

/// The inverse of `demand_bar`: the demand (0..100) whose bar equals this score.
fn model_iq(elo: f64, top: f64, exponent: f64) -> f64 {
    if top <= 0.0 {
        return 0.0;
    }
    100.0 * (elo / top).clamp(0.0, 1.0).powf(1.0 / exponent)
}

struct Config {
    exclude: Vec<String>,
    subscription_providers: HashSet<String>,
    subscription_routes: HashSet<String>,
    metered_on_pi: HashSet<String>,
    epoch_winners: HashMap<String, Vec<String>>,
    epoch_excludes: HashMap<String, Vec<String>>,
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

fn is_covered(cfg: &Config, harness: Option<&str>, route: &ModelRoute) -> bool {
    if harness == Some("pi") && cfg.metered_on_pi.contains(&route.provider_id) {
        return false;
    }
    cfg.subscription_routes.contains(&route.route_key)
        || cfg.subscription_providers.contains(&route.provider_id)
}

fn normalized_metered_cost(fact: &ModelFact) -> f64 {
    let mult = match fact.subscription_multiplier {
        Some(m) if m.is_finite() && m > 0.0 => m,
        _ => 1.0,
    };
    if let Some(p) = fact.cost_per_m_tok_out {
        return (p / mult / COST_REFERENCE_PER_MTOK).clamp(0.0, 1.0);
    }
    if let Some(t) = fact.cost_tier {
        return (t / 6.0).min(1.0);
    }
    1.0
}

fn canon(x: f64) -> f64 {
    x + 0.0
}

struct Cand {
    key: String,
    provider: String,
    covered: bool,
    cost: f64,
    pen: f64,
    adequacy: f64,
    raw_adequacy: f64,
    abundance: Option<f64>,
    speed: f64,
    metered: f64,
}

/// Re-implementation of `route_order_keys` + `compare_route_order_keys`
/// (model_router.rs) with the penalty placed before or after the pool collapse.
fn reimpl_winner(cands: &[Cand], best_effort: bool, post: bool) -> Option<String> {
    let own = |c: &Cand| if post { c.cost } else { c.cost + c.pen };
    let mut pools: HashMap<&str, (f64, usize)> = HashMap::new();
    for c in cands.iter().filter(|c| c.covered) {
        let e = pools.entry(c.provider.as_str()).or_insert((f64::INFINITY, 0));
        e.0 = e.0.min(own(c));
        e.1 += 1;
    }
    struct K<'a> {
        key: &'a str,
        cost_key: f64,
        in_bucket: bool,
        adequacy: f64,
        raw: f64,
        abundance: i64,
        speed: f64,
        metered: f64,
    }
    let keys: Vec<K> = cands
        .iter()
        .map(|c| {
            let (pool_cost, shared) = if c.covered {
                let (m, n) = pools[c.provider.as_str()];
                (m, n >= 2)
            } else {
                (own(c), false)
            };
            let priced = if post { pool_cost + c.pen } else { pool_cost };
            let free = priced <= EFFECTIVELY_FREE;
            let ab = c.abundance.filter(|v| v.is_finite()).unwrap_or(1.0);
            K {
                key: &c.key,
                cost_key: if free { 0.0 } else { priced },
                in_bucket: free || shared,
                adequacy: c.adequacy,
                raw: c.raw_adequacy,
                abundance: (ab * 1000.0).round() as i64,
                speed: c.speed,
                metered: c.metered,
            }
        })
        .collect();
    let cmp = |a: &K, b: &K| {
        use std::cmp::Ordering;
        let ba = |k: &K| if k.in_bucket { k.adequacy } else { 0.0 };
        let bb = |k: &K| if k.in_bucket { k.abundance } else { 0 };
        let first = if best_effort {
            canon(b.adequacy).total_cmp(&canon(a.adequacy))
        } else {
            Ordering::Equal
        };
        first
            .then(canon(a.cost_key).total_cmp(&canon(b.cost_key)))
            .then(b.in_bucket.cmp(&a.in_bucket))
            .then(canon(ba(b)).total_cmp(&canon(ba(a))))
            .then(bb(b).cmp(&bb(a)))
            .then(canon(b.speed).total_cmp(&canon(a.speed)))
            .then(canon(a.metered).total_cmp(&canon(b.metered)))
            .then(canon(b.adequacy).total_cmp(&canon(a.adequacy)))
            .then(canon(b.raw).total_cmp(&canon(a.raw)))
            .then(a.key.cmp(b.key))
    };
    keys.iter().min_by(|a, b| cmp(a, b)).map(|k| k.key.to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (store_path, decisions_path, config_path, out_path) =
        (&args[1], &args[2], &args[3], &args[4]);
    let store = SqliteRouteStateStore::open(&dev_descriptor("striatum", store_path))
        .expect("open store copy");
    let config: Value =
        serde_json::from_str(&std::fs::read_to_string(config_path).unwrap()).unwrap();
    let to_map = |v: &Value| -> HashMap<String, f64> {
        v.as_object()
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_f64().map(|f| (k.clone(), f)))
                    .collect()
            })
            .unwrap_or_default()
    };
    let pricing = RoutePricing {
        subscription_multipliers: to_map(&config["subscription_multipliers"]),
        speed_multipliers: to_map(&config["speed_multipliers"]),
        ..Default::default()
    };
    let cfg = Config {
        exclude: strs(&config["exclude"]),
        subscription_providers: strs(&config["subscription_providers"]).into_iter().collect(),
        subscription_routes: strs(&config["subscription_routes"]).into_iter().collect(),
        metered_on_pi: strs(&config["metered_on_pi"]).into_iter().collect(),
        epoch_winners: config["epoch_winners"]
            .as_object()
            .map(|o| o.iter().map(|(k, v)| (k.clone(), strs(v))).collect())
            .unwrap_or_default(),
        epoch_excludes: config["epoch_excludes"]
            .as_object()
            .map(|o| o.iter().map(|(k, v)| (k.clone(), strs(v))).collect())
            .unwrap_or_default(),
    };
    // OVERQ_BASELINE_ONLY skips the grid; calibrate.py uses it to fit the
    // per-revision fence quickly.
    let baseline_only = std::env::var_os("OVERQ_BASELINE_ONLY").is_some();
    let mut grid = Vec::new();
    for x in [20.0, 30.0, 40.0] {
        for h in [5.0, 10.0, 20.0] {
            for base in [0.02, 0.05, 0.1] {
                grid.push(Grid { x, h, base });
            }
        }
    }
    let routing_config = DEFAULT_ROUTING_CONFIG;
    let exponent = routing_config.demand_exponent;

    let mut facts_cache: Option<(i64, Vec<ModelFact>)> = None;
    let mut out = std::io::BufWriter::new(std::fs::File::create(out_path).unwrap());
    let reader = std::io::BufReader::new(std::fs::File::open(decisions_path).unwrap());
    for line in reader.lines() {
        let d: Value = serde_json::from_str(&line.unwrap()).unwrap();
        if d["kind"] != "single" {
            continue;
        }
        let ts = d["ts"].as_f64().unwrap();
        let bucket = (ts / 60_000.0).floor() as i64;
        if facts_cache.as_ref().map(|(b, _)| *b) != Some(bucket) {
            facts_cache = Some((bucket, store.get_facts_with_observed_speed(ts, &pricing).unwrap()));
        }
        let facts = facts_cache.as_ref().unwrap().1.clone();
        let harness = d["harness"].as_str();
        let requirements = ModelRequirements {
            iq: d["iq"].as_f64().unwrap_or(0.0),
            eq: d["eq"].as_f64().unwrap_or(0.0),
        };

        // Availability: the mason's recorded host list when joinable, else every
        // catalog route minus what the decision recorded as not available.
        let available: HashSet<String> = match d.get("available").and_then(Value::as_array) {
            Some(list) if !list.is_empty() => strs(&d["available"]).into_iter().collect(),
            _ => {
                let na: HashSet<String> = strs(&d["not_available"]).into_iter().collect();
                facts
                    .iter()
                    .map(|f| make_route_key(&f.provider_id, &f.model_id, f.variant.as_deref()))
                    .filter(|k| !na.contains(k))
                    .collect()
            }
        };
        // Operator fence: today's list, re-admitting routes that won a decision
        // under the same recorded policy revision, adding the routes
        // calibrate.py inferred were fenced in that revision, then re-excluding
        // what this decision itself recorded as operator-excluded.
        let mut operator = cfg.exclude.clone();
        if let Some(policy) = d["policy"].as_str() {
            for w in cfg.epoch_winners.get(policy).into_iter().flatten() {
                operator.push(format!("-{w}"));
            }
            operator.extend(cfg.epoch_excludes.get(policy).into_iter().flatten().cloned());
        }
        // OVERQ_TODAY_FENCE replays every decision under today's fence alone
        // (world.json carries no per-revision entries in that mode): the
        // forward-looking view of what the term would do from now on.
        if std::env::var_os("OVERQ_TODAY_FENCE").is_none() {
            operator.extend(strs(&d["listed_operator_excluded"]));
        }
        let mut caller = strs(&d["walls"]);
        caller.extend(strs(&d["caller_excluded"]));

        // Runtime: recorded pressure per capacity, abundance from the nearest
        // recorded winner of the same provider window.
        let mut snapshot = ModelRouteStateSnapshot {
            now: ts,
            ..Default::default()
        };
        if let Some(p) = d["pressure"].as_object() {
            for (cap, v) in p {
                if let Some(v) = v.as_f64() {
                    snapshot.by_capacity.entry(cap.clone()).or_default().quota_pressure = Some(v);
                }
            }
        }
        let abundance: HashMap<String, f64> = d["abundance"]
            .as_object()
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v["value"].as_f64().map(|f| (k.clone(), f)))
                    .collect()
            })
            .unwrap_or_default();
        for f in &facts {
            if let Some(a) = abundance.get(&f.provider_id) {
                let cap = striatum_core::model_facts::fact_to_route(f).capacity_key;
                let s = snapshot.by_capacity.entry(cap).or_default();
                s.quota_abundance = Some(*a);
                s.quota_abundance_window = Some("replay".into());
            }
        }

        let make_input = |snap: ModelRouteStateSnapshot| -> SelectModelRouteInput<'_> {
            let h = harness.map(String::from);
            let cfg_ref = &cfg;
            SelectModelRouteInput {
                target_agent: d["agent"].as_str().unwrap_or("").to_string(),
                // The study replays worker decisions; a head (the operator's
                // `head_agents`) may spend a subscription's kept-free reserve,
                // which the decision row does not record.
                target_is_head: false,
                requirements,
                available_models: available.clone(),
                facts: facts.clone(),
                runtime_snapshot: Some(snap),
                exclude_route_keys: caller.clone(),
                operator_exclude_keys: operator.clone(),
                required_capabilities: None,
                is_subscription_route: Some(Box::new(move |r: &ModelRoute| {
                    is_covered(cfg_ref, h.as_deref(), r)
                })),
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
                providers_without_cache_write_reports: HashSet::new(),
            }
        };

        let baseline = select_model_route_with_config(make_input(snapshot.clone()), &routing_config);
        let mut rec = json!({
            "id": d["id"],
            "recorded": d["winner"],
        });
        let Ok(baseline) = baseline else {
            rec["baseline"] = Value::Null;
            rec["baseline_error"] = json!(baseline.err().unwrap().reason.as_str());
            writeln!(out, "{rec}").unwrap();
            continue;
        };
        rec["baseline"] = json!(baseline.route_key);
        rec["baseline_cost"] = json!(baseline.score.selection_cost);
        if baseline_only {
            writeln!(out, "{rec}").unwrap();
            continue;
        }

        // Candidate facts for the term and the re-implemented ordering.
        let admission = admit_routes(
            &facts,
            Some(&snapshot),
            ExclusionPolicy {
                operator: &operator,
                caller: &caller,
            },
            None,
            &available,
            &requirements,
            &routing_config,
        );
        let top = admission.top_routable.elo;
        rec["top_elo"] = json!(top);
        rec["top_eq"] = json!(admission.top_routable.eq);
        let priced = price_selection_shortlist(
            make_input(snapshot.clone()),
            &routing_config,
            COUNTERS_PRODUCER,
            CLASS_PRODUCER,
        )
            .ok();
        let max_adequacy = admission
            .admitted
            .iter()
            .map(|a| a.adequacy.adequacy)
            .fold(f64::NEG_INFINITY, f64::max);
        let cleared = admission
            .admitted
            .iter()
            .filter(|a| a.adequacy.clears_adequacy_floor())
            .count();
        let best_effort = cleared == 0;
        let shortlist: Vec<&striatum_core::AdmittedRoute> = admission
            .admitted
            .iter()
            .filter(|a| {
                if best_effort {
                    a.adequacy.adequacy >= max_adequacy - RELATIVE_SHORTLIST_DELTA
                } else {
                    a.adequacy.clears_adequacy_floor()
                }
            })
            .collect();
        let cost_by_key: HashMap<String, f64> = priced
            .as_ref()
            .map(|p| {
                p.routes
                    .iter()
                    .map(|r| (r.route_key.clone(), r.selection_cost))
                    .collect()
            })
            .unwrap_or_default();
        let iq_of: HashMap<String, f64> = shortlist
            .iter()
            .map(|a| {
                (
                    a.route.route_key.clone(),
                    model_iq(a.route.fact.scores.elo, top, exponent),
                )
            })
            .collect();
        rec["shortlist"] = json!(shortlist
            .iter()
            .map(|a| json!({
                "k": a.route.route_key,
                "iq": iq_of[&a.route.route_key],
                "eqd": model_iq(a.route.fact.scores.eq, admission.top_routable.eq, exponent),
                "a": a.adequacy.adequacy,
                "c": cost_by_key.get(&a.route.route_key),
                "cov": is_covered(&cfg, harness, &a.route),
                "sp": ((a.route.fact.scores.speed - 50.0) / 150.0).clamp(0.0, 1.0),
            }))
            .collect::<Vec<_>>());
        let mut cands: Vec<Cand> = shortlist
            .iter()
            .map(|a| Cand {
                key: a.route.route_key.clone(),
                provider: a.route.provider_id.clone(),
                covered: is_covered(&cfg, harness, &a.route),
                cost: cost_by_key.get(&a.route.route_key).copied().unwrap_or(f64::NAN),
                pen: 0.0,
                adequacy: a.adequacy.adequacy,
                raw_adequacy: a.adequacy.raw_adequacy,
                abundance: a.runtime.quota_abundance,
                speed: ((a.route.fact.scores.speed - 50.0) / 150.0).clamp(0.0, 1.0),
                metered: normalized_metered_cost(&a.route.fact),
            })
            .collect();
        let reimpl_base = reimpl_winner(&cands, best_effort, false);
        rec["reimpl_baseline_ok"] = json!(reimpl_base.as_deref() == Some(baseline.route_key.as_str()));

        let mut variants = serde_json::Map::new();
        let mut mismatches = 0;
        for g in &grid {
            for c in cands.iter_mut() {
                c.pen = penalty(iq_of[&c.key], requirements.iq, *g);
            }
            // pre: through the real library via pressure injection.
            let mut snap = snapshot.clone();
            for c in &cands {
                if c.pen > 0.0 {
                    let route = shortlist.iter().find(|a| a.route.route_key == c.key).unwrap();
                    let base_state: RouteRuntimeState = {
                        let mut s = snap
                            .by_capacity
                            .get(&route.route.capacity_key)
                            .cloned()
                            .unwrap_or_default();
                        if let Some(r) = snap.by_route.get(&c.key) {
                            if r.quota_pressure.is_some() {
                                s.quota_pressure = r.quota_pressure;
                            }
                        }
                        s
                    };
                    let entry = snap.by_route.entry(c.key.clone()).or_default();
                    entry.quota_pressure = Some(
                        base_state.quota_pressure.unwrap_or(0.0) + c.pen / OBJECTIVE_WEIGHT_QUOTA,
                    );
                }
            }
            let pre = select_model_route_with_config(make_input(snap), &routing_config)
                .map(|s| s.route_key)
                .ok();
            let pre_reimpl = reimpl_winner(&cands, best_effort, false);
            if pre != pre_reimpl {
                mismatches += 1;
            }
            let post = reimpl_winner(&cands, best_effort, true);
            // guard: `pre`, but the term may only move the decision to a route
            // that fits the demand at least as well (adequacy >= the baseline
            // winner's); otherwise the baseline winner stands. A route's term
            // can then never trade a demanded axis (in practice eq) for cost.
            let adequacy_of = |k: &str| {
                cands.iter().find(|c| c.key == k).map(|c| c.adequacy).unwrap_or(0.0)
            };
            let guard = match &pre_reimpl {
                Some(w) if adequacy_of(w) >= adequacy_of(&baseline.route_key) - 1e-12 => {
                    pre_reimpl.clone()
                }
                _ => Some(baseline.route_key.clone()),
            };
            variants.insert(
                format!("{}|{}|{}", g.x, g.h, g.base),
                json!({"pre": pre, "post": post, "guard": guard}),
            );
        }
        rec["variants"] = Value::Object(variants);
        rec["reimpl_mismatch"] = json!(mismatches);
        writeln!(out, "{rec}").unwrap();
    }
}
