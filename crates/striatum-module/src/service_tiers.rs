//! Request-local tier eligibility and pricing. Public facts never infer a plan.

use cortexkit_claustrum_client::ListedCredential;
use serde_json::Value;
use striatum_core::{BillingRate, ModelFact};

use crate::broca_credentials::BrocaCredentials;

/// Tier eligibility needs all three independently refreshed account views.
/// An empty published answer is authoritative; an absent snapshot is not.
pub(crate) fn unobserved_account_input(credentials: &BrocaCredentials) -> Option<&'static str> {
    if credentials.listing_version().is_none() {
        Some("routing just started and hasn't read the credential list yet")
    } else if !credentials.billing_catalog.is_loaded() {
        Some("routing just started and hasn't read the models' service tiers yet")
    } else if credentials.plan_prices["prices"].as_array().is_none() {
        Some("routing just started and hasn't read plan prices yet")
    } else {
        None
    }
}

fn decimal(value: &Value) -> Option<f64> {
    let exponent = value["exponent"].as_u64()?;
    if exponent > 6 {
        return None;
    }
    let rate = value["units"].as_f64()? / 10_f64.powi(exponent as i32);
    (rate.is_finite() && rate > 0.0).then_some(rate)
}

pub(crate) fn plan_rate(
    prices: &Value,
    row: &ListedCredential,
    provider: &str,
    tier: &str,
    credits: bool,
) -> Option<f64> {
    if tier == "standard" {
        return Some(1.0);
    }
    let plan = row.plan_tier.as_deref()?;
    let (plan_provider, plan) = plan.split_once('/').unwrap_or((provider, plan));
    if plan_provider != provider {
        return None;
    }
    let mut rows = prices["prices"]
        .as_array()?
        .iter()
        .filter(|price| price["provider_id"] == provider && price["tier"] == plan);
    let price = rows.next()?;
    if rows.next().is_some() || price["service_tiers"]["state"] != "established" {
        return None;
    }
    let rate = price["service_tiers"]["tiers"]
        .as_array()?
        .iter()
        .find(|entry| entry["tier"] == tier)?;
    decimal(
        &rate[if credits {
            "credit_rate"
        } else {
            "included_usage_rate"
        }],
    )
}

pub(crate) fn runner_supports(
    credentials: &BrocaCredentials,
    runner: &crate::routing_v1::runners::EffectiveRunner,
    fact: &ModelFact,
    row: &ListedCredential,
    tier: &str,
) -> bool {
    if tier == "standard" {
        return true;
    }
    let Some(wire) = row
        .auth_method
        .as_deref()
        .and_then(|method| credentials.wire_api(fact, method))
    else {
        return false;
    };
    if !runner.follows_account_choice
        || !runner.pairs.as_ref().is_none_or(|pairs| {
            pairs.iter().any(|pair| {
                pair.wire_api == wire
                    && Some(pair.auth_method.as_str()) == row.auth_method.as_deref()
            })
        })
    {
        return false;
    }
    credentials
        .runner_service_tiers
        .get(&runner.policy.module)
        .and_then(|map| map.get(wire))
        .is_some_and(|support| support.levels.iter().any(|level| level.as_str() == tier))
}

pub(crate) fn model_offers(credentials: &BrocaCredentials, fact: &ModelFact, tier: &str) -> bool {
    tier == "standard"
        || credentials
            .billing_catalog
            .offers_tier(&format!("{}/{}", fact.provider_id, fact.model_id), tier)
}

/// Credits pay only after included quota is exhausted. A balance alone is not
/// evidence that the account has stopped drawing its included allowance.
pub(crate) fn drawing_credits(entry: &Value) -> bool {
    let exhausted = ["primary", "secondary", "tertiary"].iter().any(|key| {
        let window = &entry["usage"][key];
        window["rawUsedPercent"]
            .as_f64()
            .or_else(|| window["usedPercent"].as_f64())
            .is_some_and(|used| used >= 100.0)
    });
    exhausted
        && entry["spend"].as_array().is_some_and(|pools| {
            pools.iter().any(|pool| {
                pool["id"] == "credits"
                    && pool["spendable"].as_bool() != Some(false)
                    && pool["remaining"]["minor"]
                        .as_i64()
                        .is_some_and(|balance| balance > 0)
            })
        })
}

pub(crate) fn price_fact(
    credentials: &BrocaCredentials,
    fact: &mut ModelFact,
    tier: &str,
    subscription: bool,
    multiplier: f64,
) {
    if tier == "standard" {
        return;
    }
    if let Some(plane) = &mut fact.billing_plane {
        if subscription {
            for rate in plane.rates.values_mut() {
                if let BillingRate::Priced { rate } = rate {
                    *rate *= multiplier;
                }
            }
        } else {
            // A mode does not inherit missing dimensions from standard.
            plane.rates = credentials
                .billing_catalog
                .mode_rates(&format!("{}/{}", fact.provider_id, fact.model_id), tier);
        }
    }
}

pub(crate) fn usage_cost(
    fact: &ModelFact,
    usage: &cortexkit_role_routing::report::Usage,
) -> Option<f64> {
    use striatum_core::BasketDimension;
    let output = usage.output_tokens?;
    let reasoning = usage.reasoning_tokens?;
    let dimensions = [
        (BasketDimension::FreshInput, usage.input_tokens?),
        (
            BasketDimension::OutputExcludingReasoning,
            output.saturating_sub(reasoning),
        ),
        (BasketDimension::Reasoning, reasoning),
        (BasketDimension::CachedRead, usage.cached_input_tokens?),
        (BasketDimension::CacheWrite, usage.cache_write_tokens?),
    ];
    let mut total = 0.0;
    for (dimension, tokens) in dimensions {
        if tokens != 0 {
            total += tokens as f64 * fact.basket_rate(dimension)? / 1_000_000.0;
        }
    }
    total.is_finite().then_some(total)
}
