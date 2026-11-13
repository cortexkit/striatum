//! Resolve settlement rates from the selection's already-loaded billing facts.
//! Quota scarcity affects ranking through plan pressure, not the caller's token price.

use std::collections::BTreeMap;

use crate::{subscription::SubscriptionPricing, BillingPlane, ModelFact};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceBasis {
    Metered,
    SubscriptionEquivalent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceRates {
    pub basis: PriceBasis,
    pub multiplier: Option<f64>,
    pub input: f64,
    pub output: f64,
    pub cache_read: Option<f64>,
    pub cache_write: Option<f64>,
    pub cache_write_ttl: BTreeMap<String, f64>,
}

/// Credential coverage must be for the answered route, not merely its provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct PriceBilling<'a> {
    pub covering_subscription: bool,
    pub covering_metered: bool,
    pub priced_per_token_only: bool,
    pub metered_fallback: bool,
    /// Pay-per-use plane for a mixed-credential fallback. The subscription
    /// plane may quote different list rates and cannot stand in for this one.
    pub metered_plane: Option<&'a BillingPlane>,
    /// The result of the same `SubscriptionPool::price` call used for ranking.
    pub subscription_pricing: Option<&'a SubscriptionPricing>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PriceRateRequest<'a> {
    pub service_tier: Option<&'a str>,
    pub input_tokens: Option<u64>,
    pub context: Option<f64>,
    pub profile_fresh_input: Option<f64>,
}

impl PriceRateRequest<'_> {
    fn input_size(self) -> f64 {
        self.input_tokens
            .filter(|tokens| *tokens > 0)
            .map(|tokens| tokens as f64)
            .or(self.context)
            .or(self.profile_fresh_input)
            .unwrap_or(0.0)
    }
}

/// `model` holds catalog rates before service-tier ranking overlays. Mode keys
/// select a non-standard tier without borrowing standard rates or the extra
/// quota draw a subscription plan charges for that tier. No catalog reads,
/// quota reads or profile lookups occur here.
/// Missing required rates stay unpriced; cache-write never borrows input rates.
pub fn resolve_price_rates(
    model: &ModelFact,
    billing: PriceBilling<'_>,
    request: PriceRateRequest<'_>,
) -> Option<PriceRates> {
    let mixed_fallback =
        billing.covering_subscription && billing.metered_fallback && billing.covering_metered;
    let subscription =
        billing.covering_subscription && !billing.priced_per_token_only && !mixed_fallback;
    let multiplier = if subscription {
        let pricing = billing.subscription_pricing?;
        if pricing.value_source == "unmeasured"
            || pricing
                .plan_values
                .iter()
                .any(|plan| plan.weight > 0.0 && plan.source == "default")
        {
            return None;
        }
        let value = pricing.value_multiplier;
        Some(value).filter(|value| value.is_finite() && *value > 0.0)?;
        Some(value)
    } else {
        None
    };
    let divisor = multiplier.unwrap_or(1.0);
    let tier = request.service_tier.filter(|tier| *tier != "standard");
    let plane = if mixed_fallback {
        Some(billing.metered_plane?)
    } else if subscription {
        model.billing_plane.as_ref()
    } else {
        billing.metered_plane.or(model.billing_plane.as_ref())
    };
    let input_size = request.input_size();
    let rate = |key: &str| {
        let value = match plane {
            Some(plane) => plane_rate(plane, key, tier, input_size),
            None if tier.is_none() => match key {
                "rate.input" => model.input_rate,
                "rate.output" => model.cost_per_m_tok_out,
                "rate.cache_read" => model.cache_read_rate,
                "rate.cache_write" => model.cache_write_rate,
                _ => None,
            },
            None => None,
        }? / divisor;
        (value.is_finite() && value >= 0.0).then_some(value)
    };
    let mut cache_write_ttl = BTreeMap::new();
    if let Some(plane) = plane {
        for key in plane.rates.keys() {
            if let Some(lifetime) = key.strip_prefix("rate.cache_write.ttl.") {
                let lifetime = lifetime.split('.').next().unwrap_or_default();
                if !lifetime.is_empty() {
                    if let Some(value) = rate(&format!("rate.cache_write.ttl.{lifetime}")) {
                        cache_write_ttl.insert(lifetime.to_string(), value);
                    }
                }
            }
        }
    }
    Some(PriceRates {
        basis: if subscription {
            PriceBasis::SubscriptionEquivalent
        } else {
            PriceBasis::Metered
        },
        multiplier,
        input: rate("rate.input")?,
        output: rate("rate.output")?,
        cache_read: rate("rate.cache_read"),
        cache_write: rate("rate.cache_write"),
        cache_write_ttl,
    })
}

fn plane_rate(plane: &BillingPlane, key: &str, tier: Option<&str>, input: f64) -> Option<f64> {
    let suffix = tier.map(|tier| format!(".mode.{tier}"));
    let prefix = format!("{key}.above_context.");
    let selected = plane
        .rates
        .keys()
        .filter_map(|candidate| {
            let base = match &suffix {
                Some(suffix) => candidate.strip_suffix(suffix)?,
                None => candidate.as_str(),
            };
            let threshold = base.strip_prefix(&prefix)?.parse::<u64>().ok()?;
            (input > threshold as f64).then_some((threshold, candidate.as_str()))
        })
        .max_by_key(|(threshold, _)| *threshold)
        .map(|(_, key)| key);
    let base = format!("{key}{}", suffix.as_deref().unwrap_or_default());
    plane.rate(selected.unwrap_or(&base))
}

#[cfg(test)]
mod tests;
