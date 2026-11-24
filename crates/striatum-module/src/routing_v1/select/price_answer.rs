//! Settlement prices use catalog rates and task tokens, never the quota-scarcity
//! adjustment used to rank routes.

use std::collections::BTreeSet;

use cortexkit_role_routing::{
    errors::{NoRouteReason, Refusal},
    price::{Price, RatesPerMtok},
    select::SelectRequest,
    service_tier::ServiceTier,
};
use serde_json::Number;
use striatum_core::model_router::{RejectedRoute, SelectedModelRoute};
use striatum_core::price_rates::{
    resolve_price_rates, PriceBasis, PriceBilling, PriceRateRequest, PriceRates, ReasoningRate,
};
use striatum_core::task_profile::TokenVector;
use striatum_core::ModelFact;

use crate::{account_selection::ClassContext, config_overrides::LiveConfig};

pub(super) const UNKNOWN: &str = "price_unknown";

pub(super) fn unknown_refusal() -> Refusal {
    Refusal::no_route(NoRouteReason::parse(UNKNOWN), None)
}

pub(super) fn record_rejections(rejected: &mut Vec<RejectedRoute>, unpriceable: &BTreeSet<String>) {
    if unpriceable.is_empty() {
        return;
    }
    // The generic availability filter marks excluded candidates as unavailable.
    // Preserve the specific reason even when an ordered pass omits those rows.
    rejected.retain(|row| !unpriceable.contains(&row.route_key));
    rejected.extend(unpriceable.iter().map(|key| RejectedRoute {
        route_key: key.clone(),
        reason: UNKNOWN.into(),
        detail: None,
        credential_ids: Vec::new(),
    }));
}

pub(super) fn for_selection(
    selected: &SelectedModelRoute,
    facts: &[ModelFact],
    classes: Option<&ClassContext>,
    config: &LiveConfig,
    priced_per_token: bool,
    request: &SelectRequest,
    iq: f64,
    tier: Option<ServiceTier>,
    lookup: &super::ProfileLookup,
) -> Option<Price> {
    if !super::super::describe::capabilities()
        .contains(&cortexkit_role_routing::capabilities::SELECT_PRICE)
    {
        return None;
    }
    // Tier ranking can scale subscription rates by quota draw. Start with the
    // pre-tier catalog plane instead, then resolve the class that actually served.
    let mut model = facts
        .iter()
        .filter(|fact| {
            fact.provider_id == selected.model.provider_id
                && fact.model_id == selected.model.model_id
        })
        .min_by_key(|fact| u8::from(fact.variant != selected.model.variant))?
        .clone();
    model.variant = selected.prompt_params.variant.clone();
    let profile = lookup(&request.consumer, iq, std::slice::from_ref(&model))
        .profiles
        .remove(&striatum_core::fact_to_route(&model).route_key);
    let mut metered = model.clone();
    let mut has_metered_plane = false;
    let mut billing = PriceBilling {
        priced_per_token_only: priced_per_token,
        subscription_pricing: selected.score.objective.subscription_pricing.as_ref(),
        ..Default::default()
    };
    if let Some(classes) = classes {
        let chosen = classes.class(&model, priced_per_token);
        billing.covering_subscription = chosen.subscription;
        billing.metered_fallback = classes.config.metered_fallback(&model.provider_id);
        if let Some(method) = chosen
            .rows
            .first()
            .and_then(|row| row.auth_method.as_deref())
        {
            classes
                .credentials
                .price_for_method(&mut model, method, config.pricing());
        }
        let paid = classes.class(&model, true);
        billing.covering_metered = !paid.rows.is_empty();
        if let Some(method) = paid.rows.first().and_then(|row| row.auth_method.as_deref()) {
            classes
                .credentials
                .price_for_method(&mut metered, method, config.pricing());
            has_metered_plane = true;
        }
        if let Some(tier) = tier.filter(|tier| !tier.is_standard()) {
            // Resolve each route's tier on its own billing plane, falling back
            // to catalog modes only when the page carries none. These prices
            // are separate from a plan's extra quota consumption.
            let key = format!("{}/{}", model.provider_id, model.model_id);
            for fact in [&mut model, &mut metered] {
                if let Some(plane) = &mut fact.billing_plane {
                    let modes = classes.credentials.billing_catalog.mode_rates(
                        &key,
                        Some(&plane.auth_method),
                        tier.as_str(),
                    );
                    let suffix = format!(".mode.{}", tier.as_str());
                    plane.rates.retain(|key, _| !key.ends_with(&suffix));
                    for (key, rate) in &modes {
                        let mut rate = rate.clone();
                        if let striatum_core::billing::BillingRate::BilledAs { key } = &mut rate {
                            if modes.contains_key(key) {
                                key.push_str(&suffix);
                            }
                        }
                        plane.rates.insert(format!("{key}{suffix}"), rate);
                    }
                }
            }
        }
    } else {
        billing.covering_subscription =
            !priced_per_token && selected.score.objective.subscription_pricing.is_some();
    }
    billing.metered_plane = has_metered_plane
        .then_some(metered.billing_plane.as_ref())
        .flatten();
    let rates = resolve_price_rates(
        &model,
        billing,
        PriceRateRequest {
            service_tier: tier.map(|tier| tier.as_str()),
            input_tokens: request.requirements.input_tokens,
            context: request
                .requirements
                .context_window
                .map(|tokens| tokens as f64),
            profile_fresh_input: profile.as_ref().map(|profile| profile.tokens.fresh_input),
        },
    )?;
    build(&rates, profile.map(|profile| profile.tokens))
}

// JSON number equality distinguishes integers from floats. Keep whole catalog
// rates as integers; divided subscription rates remain floating-point numbers.
fn number(value: f64) -> Option<Number> {
    let float = Number::from_f64(value)?;
    if value >= 0.0 && value < u64::MAX as f64 && value.fract() == 0.0 {
        Some(Number::from(value as u64))
    } else {
        Some(float)
    }
}

fn build(rates: &PriceRates, tokens: Option<TokenVector>) -> Option<Price> {
    let rate_number = |value| match rates.basis {
        PriceBasis::Metered => number(value),
        PriceBasis::SubscriptionEquivalent => Number::from_f64(value),
    };
    let mut wire = RatesPerMtok::new(rate_number(rates.input)?, rate_number(rates.output)?);
    if let Some(rate) = rates.cache_read {
        wire = wire.with_cache_read(rate_number(rate)?);
    }
    if let Some(rate) = rates.cache_write {
        wire = wire.with_cache_write(rate_number(rate)?);
    }
    if !rates.cache_write_ttl.is_empty() {
        wire = wire.with_cache_write_ttl(
            rates
                .cache_write_ttl
                .iter()
                .map(|(ttl, rate)| Some((ttl.clone(), rate_number(*rate)?)))
                .collect::<Option<_>>()?,
        );
    }
    let mut price = match rates.basis {
        PriceBasis::Metered => Price::metered(wire),
        PriceBasis::SubscriptionEquivalent => {
            Price::subscription_equivalent(wire, number(rates.multiplier?)?)
        }
    };
    if let Some(estimate) =
        tokens.and_then(|tokens| estimate(tokens, &price.rates_per_mtok, rates.reasoning))
    {
        price = price.with_estimated_task_usd(estimate);
    }
    price.check().ok()?;
    Some(price)
}

fn estimate(tokens: TokenVector, rates: &RatesPerMtok, reasoning: ReasoningRate) -> Option<Number> {
    let mut cost = 0.0;
    let (output, reasoning_tokens, reasoning_rate) = match reasoning {
        ReasoningRate::IncludedInOutput => (
            tokens.output_excluding_reasoning + tokens.reasoning,
            0.0,
            None,
        ),
        ReasoningRate::Separate(rate) => (
            tokens.output_excluding_reasoning,
            tokens.reasoning,
            Some(rate),
        ),
        ReasoningRate::Unpriced => (tokens.output_excluding_reasoning, tokens.reasoning, None),
    };
    for (tokens, rate) in [
        (tokens.fresh_input, rates.input.as_f64()),
        (
            tokens.cached_read,
            rates.cache_read.as_ref().and_then(Number::as_f64),
        ),
        (
            tokens.cache_write,
            rates.cache_write.as_ref().and_then(Number::as_f64),
        ),
        (output, rates.output.as_f64()),
        (reasoning_tokens, reasoning_rate),
    ] {
        if tokens == 0.0 {
            continue;
        }
        cost += tokens * rate? / 1_000_000.0;
    }
    number(cost)
}
