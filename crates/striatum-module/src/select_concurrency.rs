//! Read-only select capacity. Selection deliberately never touches waiters.

use striatum_core::concurrency_availability::{AccountConcurrency, ConcurrencyAvailability};
use striatum_core::model_router::{NoRouteError, RouteFailureReason};

pub(crate) fn pin_capacity(pin: &str, facts: &[striatum_core::ModelFact]) -> String {
    let variant = crate::work_model_pin::named_variant(pin, facts);
    crate::work_model_pin::route_keys(pin, variant.as_deref(), facts).1
}

pub(crate) fn considered(
    fact: &striatum_core::ModelFact,
    available: &std::collections::HashSet<String>,
    pin: Option<&str>,
) -> bool {
    let capacity = striatum_core::model_facts::make_capacity_key(&fact.provider_id, &fact.model_id);
    if let Some(pin) = pin {
        return capacity == pin;
    }
    available.contains(&capacity)
        || available.contains(&striatum_core::model_facts::make_route_key(
            &fact.provider_id,
            &fact.model_id,
            fact.variant.as_deref(),
        ))
}

pub(crate) fn capture(
    store: &striatum_store::SqliteRouteStateStore,
    classes: Option<&crate::account_selection::ClassContext>,
    facts: &[striatum_core::ModelFact],
    priced: bool,
    now: i64,
    consider: impl Fn(&striatum_core::ModelFact) -> bool,
) -> striatum_core::RouteStateResult<ConcurrencyAvailability> {
    let mut availability = ConcurrencyAvailability::default();
    let Some(classes) = classes else {
        return Ok(availability);
    };
    let caps = classes
        .credentials
        .concurrency_caps
        .resolved_for_settings(classes.config.concurrency_caps());
    if !caps.any_caps() {
        return Ok(availability);
    }
    let mut state = None;
    let mut checked = std::collections::HashSet::new();
    for fact in facts {
        let provider = &fact.provider_id;
        if !caps.provider_can_cap(provider) || !consider(fact) {
            continue;
        }
        let model = crate::account_selection::normalize(&fact.model_id);
        if !caps.model_can_cap(provider, &model) {
            continue;
        }
        let capacity = striatum_core::model_facts::make_capacity_key(provider, &fact.model_id);
        let route = striatum_core::model_facts::make_route_key(
            provider,
            &fact.model_id,
            fact.variant.as_deref(),
        );
        if !checked.insert(capacity.clone()) {
            if let Some(full) = availability.full_routes.get(&capacity).cloned() {
                availability.full_routes.insert(route, full);
            }
            continue;
        }
        let accounts = classes.select_eligible_accounts(provider, &model, priced);
        if accounts.is_empty() {
            continue;
        }
        if state.is_none() {
            state = Some(crate::account_state::snapshot(store)?);
        }
        let state = state.as_ref().unwrap();
        let mut full = Vec::new();
        for account in &accounts {
            let Some(cap) = caps.cap(provider, account, &model).limit else {
                break;
            };
            let count = state
                .inflight
                .count_for_model(&caps, provider, account, &model, now);
            if (count as u64) < cap {
                break;
            }
            let reset = state
                .inflight
                .next_free_at(&caps, provider, account, &model, cap, now)
                .unwrap_or(now.saturating_add(30_000))
                .min(now.saturating_add(30_000));
            full.push(AccountConcurrency {
                account_id: account.clone(),
                count,
                cap,
                reset_at_ms: reset,
            });
        }
        if full.len() == accounts.len() {
            availability.full_routes.insert(capacity, full.clone());
            availability.full_routes.insert(route, full);
        }
    }
    Ok(availability)
}

pub(crate) fn head(config: &crate::config_overrides::LiveConfig, consumer: &str) -> bool {
    crate::routing_v1::consumers::effective(config)
        .defaults_for(consumer)
        .priority
        .unwrap_or_default()
        == cortexkit_role_routing::declarations::ConsumerPriority::Interactive
}

pub(crate) fn record(
    surface: &str,
    consumer: &str,
    config: &crate::config_overrides::LiveConfig,
    error: &NoRouteError,
) {
    if error.reason != RouteFailureReason::ConcurrencyFull || head(config, consumer) {
        return;
    }
    let class = crate::routing_v1::consumers::effective(config)
        .defaults_for(consumer)
        .priority
        .unwrap_or_default();
    let routes: Vec<_> = error
        .rejected
        .iter()
        .filter(|r| r.reason == "concurrency_full")
        .map(|route| {
            let counts: serde_json::Value = route
                .detail
                .as_deref()
                .and_then(|detail| serde_json::from_str(detail).ok())
                .unwrap_or_default();
            serde_json::json!({"route":route.route_key,"accounts":counts})
        })
        .collect();
    let detail =
        serde_json::json!({"surface":surface,"consumer":consumer,"class":class,"routes":routes});
    let event = if config.concurrency_caps().enforcement_enabled {
        "refused"
    } else {
        "would refuse"
    };
    crate::info_operator(format!("routing select concurrency {event} {detail}"));
}
