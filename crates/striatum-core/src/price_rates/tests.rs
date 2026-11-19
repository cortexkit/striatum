use super::*;
use crate::{
    model_facts_seed,
    subscription::{
        MeasuredValue, PlanMeasurements, QuotaAccount, SubscriptionPlan, SubscriptionPool,
    },
    BillingRate,
};

fn model() -> ModelFact {
    let mut model = model_facts_seed()[0].clone();
    model.provider_id = "openai".into();
    model.model_id = "gpt-6.1".into();
    model.billing_plane = Some(BillingPlane {
        provider_id: model.provider_id.clone(),
        auth_method: "chatgpt".into(),
        kind: "quota_proxy".into(),
        selection_reason: "roster".into(),
        rates: BTreeMap::from([
            priced("rate.input", 2.5),
            priced("rate.output", 15.0),
            priced("rate.cache_read", 0.25),
            priced("rate.cache_write", 3.0),
            priced("rate.cache_write.ttl.30m", 3.125),
            priced("rate.cache_write.ttl.12h", 5.0),
        ]),
    });
    model
}

fn priced(key: &str, rate: f64) -> (String, BillingRate) {
    (key.into(), BillingRate::Priced { rate })
}

fn resolve(
    model: &ModelFact,
    billing: PriceBilling<'_>,
    request: PriceRateRequest<'_>,
) -> PriceRates {
    resolve_price_rates(model, billing, request).expect("route has resolved rates")
}

fn plan(multiplier: f64, used: f64) -> SubscriptionPlan {
    SubscriptionPlan {
        tier: None,
        plan: None,
        measured: Some(PlanMeasurements::from_latest(MeasuredValue {
            multiplier,
            interval_end_ms: 999,
            row_date: "fresh".into(),
        })),
        fee_usd: 200.0,
        accounts: vec![QuotaAccount {
            weekly_used: used,
            weekly_remaining_fraction: 1.0,
            hours_to_reset: 168.0,
            short_walled: false,
            short_windows: vec![],
            banked_resets: 0,
            banked_expires_at_ms: None,
            banked_credit_expiries_ms: vec![],
            prepaid_credits: false,
        }],
    }
}

fn subscription(pricing: &SubscriptionPricing) -> PriceBilling<'_> {
    PriceBilling {
        covering_subscription: true,
        subscription_pricing: Some(pricing),
        ..Default::default()
    }
}

#[test]
fn metered_uses_list_rates_and_no_multiplier() {
    let pricing = SubscriptionPool::default()
        .price(1.0, Some(22.0), 0.0, false)
        .unwrap();
    let price = resolve(
        &model(),
        PriceBilling {
            subscription_pricing: Some(&pricing),
            ..Default::default()
        },
        PriceRateRequest::default(),
    );
    assert_eq!(price.basis, PriceBasis::Metered);
    assert_eq!(price.multiplier, None);
    assert_eq!((price.input, price.output), (2.5, 15.0));
    assert_eq!(price.cache_read, Some(0.25));
    assert_eq!(price.cache_write, Some(3.0));
    assert_eq!(
        price.cache_write_ttl,
        BTreeMap::from([("30m".into(), 3.125), ("12h".into(), 5.0)])
    );
}

#[test]
fn openai_subscription_divides_every_rate_by_twenty_two_without_pressure_or_rounding() {
    let pool = SubscriptionPool {
        provider: "openai".into(),
        plans: vec![plan(22.0, 50.0)],
        now_ms: 1000,
        ..Default::default()
    };
    let pricing = pool.price(1.0, Some(3.0), 0.0, false).unwrap();
    assert_eq!(pricing.plan_pressure, 4.0);
    let price = resolve(
        &model(),
        subscription(&pricing),
        PriceRateRequest::default(),
    );
    assert_eq!(price.basis, PriceBasis::SubscriptionEquivalent);
    assert_eq!(price.multiplier, Some(22.0));
    assert_eq!(price.input, 2.5 / 22.0);
    assert_eq!(price.output, 15.0 / 22.0);
    assert_eq!(price.cache_read, Some(0.25 / 22.0));
    assert_eq!(price.cache_write, Some(3.0 / 22.0));
    assert_eq!(price.cache_write_ttl["30m"], 3.125 / 22.0);
    assert_eq!(price.cache_write_ttl["12h"], 5.0 / 22.0);
}

#[test]
fn subscription_uses_the_pools_spendable_points_harmonic_mean() {
    let pool = SubscriptionPool {
        plans: vec![plan(4.0, 10.0), plan(2.0, 70.0)],
        now_ms: 1000,
        ..Default::default()
    };
    let pricing = pool.price(1.0, None, 10.0, false).unwrap();
    let price = resolve(
        &model(),
        subscription(&pricing),
        PriceRateRequest::default(),
    );
    assert_eq!(price.multiplier, Some(pricing.value_multiplier));
    assert!((price.multiplier.unwrap() - 10.0 / 3.0).abs() < 1e-12);
    assert!((price.input - 0.75).abs() < 1e-12);
    assert!((price.output - 4.5).abs() < 1e-12);
}

#[test]
fn covering_credentials_and_per_token_decision_determine_basis() {
    let pricing = SubscriptionPool::default()
        .price(1.0, Some(22.0), 0.0, false)
        .unwrap();
    let mut metered_model = model();
    metered_model
        .billing_plane
        .as_mut()
        .unwrap()
        .rates
        .insert("rate.input".into(), BillingRate::Priced { rate: 9.0 });
    for (metered, fallback, per_token, expected) in [
        (false, false, false, PriceBasis::SubscriptionEquivalent),
        (true, false, false, PriceBasis::SubscriptionEquivalent),
        (true, true, false, PriceBasis::Metered),
        (false, true, false, PriceBasis::SubscriptionEquivalent),
        (true, false, true, PriceBasis::Metered),
        (false, false, true, PriceBasis::Metered),
    ] {
        let price = resolve(
            &model(),
            PriceBilling {
                covering_metered: metered,
                metered_fallback: fallback,
                priced_per_token_only: per_token,
                metered_plane: metered_model.billing_plane.as_ref(),
                ..subscription(&pricing)
            },
            PriceRateRequest::default(),
        );
        assert_eq!(price.basis, expected);
        assert_eq!(
            price.multiplier,
            (expected == PriceBasis::SubscriptionEquivalent).then_some(22.0)
        );
        assert_eq!(
            price.input,
            if expected == PriceBasis::Metered {
                9.0
            } else {
                2.5 / 22.0
            }
        );
    }
}

#[test]
fn billing_plane_zero_aliases_and_lifetime_strings_are_preserved() {
    let mut model = model();
    let rates = &mut model.billing_plane.as_mut().unwrap().rates;
    rates.insert("rate.cache_read".into(), BillingRate::NotBilled);
    rates.insert(
        "rate.cache_write".into(),
        BillingRate::BilledAs {
            key: "rate.input".into(),
        },
    );
    rates.insert(
        "rate.cache_write.ttl.5m".into(),
        BillingRate::BilledAs {
            key: "rate.cache_write".into(),
        },
    );
    rates.insert("rate.cache_write.ttl.1h".into(), BillingRate::NotBilled);
    let price = resolve(&model, PriceBilling::default(), PriceRateRequest::default());
    assert_eq!(price.cache_read, Some(0.0));
    assert_eq!(price.cache_write, Some(2.5));
    assert_eq!(price.cache_write_ttl["5m"], 2.5);
    assert_eq!(price.cache_write_ttl["1h"], 0.0);
    assert_eq!(price.cache_write_ttl.len(), 4);
}

#[test]
fn above_context_uses_positive_estimate_then_context_then_fresh_input_and_strict_highest_threshold()
{
    let mut model = model();
    let rates = &mut model.billing_plane.as_mut().unwrap().rates;
    rates.extend([
        priced("rate.input.above_context.200000", 5.0),
        priced("rate.input.above_context.400000", 7.5),
        priced("rate.output.above_context.200000", 30.0),
        priced("rate.cache_write.ttl.30m.above_context.200000", 6.25),
        (
            "rate.cache_write.above_context.200000".into(),
            BillingRate::BilledAs {
                key: "rate.input.above_context.200000".into(),
            },
        ),
    ]);
    for (estimate, context, profile, input, output) in [
        (Some(300_000), None, None, 5.0, 30.0),
        (Some(100_000), Some(300_000.0), Some(500_000.0), 2.5, 15.0),
        (None, Some(300_000.0), Some(100_000.0), 5.0, 30.0),
        (Some(0), Some(300_000.0), Some(100_000.0), 5.0, 30.0),
        (None, None, Some(300_000.0), 5.0, 30.0),
        (Some(0), None, Some(300_000.0), 5.0, 30.0),
        (Some(200_000), None, Some(300_000.0), 2.5, 15.0),
        (Some(400_000), None, None, 5.0, 30.0),
        (Some(400_001), None, None, 7.5, 30.0),
        (None, None, None, 2.5, 15.0),
    ] {
        let price = resolve(
            &model,
            PriceBilling::default(),
            PriceRateRequest {
                input_tokens: estimate,
                context,
                profile_fresh_input: profile,
                ..Default::default()
            },
        );
        assert_eq!(
            (price.input, price.output),
            (input, output),
            "estimate={estimate:?} context={context:?} profile={profile:?}"
        );
        assert_eq!(
            price.cache_write,
            Some(if input == 2.5 { 3.0 } else { 5.0 })
        );
        assert_eq!(
            price.cache_write_ttl["30m"],
            if input == 2.5 { 3.125 } else { 6.25 }
        );
    }
}

#[test]
fn nonstandard_tier_requires_its_own_catalog_rates_including_thresholds_and_lifetimes() {
    let mut model = model();
    model.billing_plane.as_mut().unwrap().rates.extend([
        priced("rate.input.mode.fast", 5.0),
        priced("rate.output.mode.fast", 30.0),
        priced("rate.input.above_context.200000.mode.fast", 10.0),
        priced("rate.cache_write.ttl.30m.mode.fast", 6.25),
    ]);
    let request = PriceRateRequest {
        service_tier: Some("fast"),
        ..Default::default()
    };
    let price = resolve(&model, PriceBilling::default(), request);
    assert_eq!((price.input, price.output), (5.0, 30.0));
    assert_eq!(price.cache_read, None);
    assert_eq!(price.cache_write, None);
    assert_eq!(
        price.cache_write_ttl,
        BTreeMap::from([("30m".into(), 6.25)])
    );
    assert_eq!(
        resolve(
            &model,
            PriceBilling::default(),
            PriceRateRequest {
                input_tokens: Some(300_000),
                ..request
            }
        )
        .input,
        10.0
    );
    assert_eq!(
        resolve_price_rates(
            &model,
            PriceBilling::default(),
            PriceRateRequest {
                service_tier: Some("ultrafast"),
                ..Default::default()
            }
        ),
        None
    );
    model
        .billing_plane
        .as_mut()
        .unwrap()
        .rates
        .remove("rate.output.mode.fast");
    assert_eq!(
        resolve_price_rates(&model, PriceBilling::default(), request),
        None
    );
    assert_eq!(
        resolve(
            &model,
            PriceBilling::default(),
            PriceRateRequest {
                service_tier: Some("standard"),
                ..Default::default()
            }
        )
        .input,
        2.5
    );
}

#[test]
fn required_rates_and_measured_or_operator_subscription_value_are_mandatory() {
    let model = model();
    let pool = SubscriptionPool::default();
    let unmeasured = pool.price(1.0, None, 0.0, false).unwrap();
    assert_eq!(
        resolve_price_rates(
            &model,
            subscription(&unmeasured),
            PriceRateRequest::default()
        ),
        None
    );
    assert_eq!(
        resolve_price_rates(
            &model,
            PriceBilling {
                covering_subscription: true,
                ..Default::default()
            },
            PriceRateRequest::default()
        ),
        None
    );
    let operator = pool.price(1.0, Some(22.0), 0.0, false).unwrap();
    assert_eq!(
        resolve(&model, subscription(&operator), PriceRateRequest::default()).multiplier,
        Some(22.0)
    );
    for missing in ["rate.input", "rate.output"] {
        let mut model = model.clone();
        model.provider_id = "antigravity".into();
        // Legacy fields must not fill holes in a credential-specific plane.
        model.input_rate = Some(99.0);
        model.cost_per_m_tok_out = Some(99.0);
        model.billing_plane.as_mut().unwrap().rates.remove(missing);
        assert_eq!(
            resolve_price_rates(&model, PriceBilling::default(), PriceRateRequest::default()),
            None
        );
    }
    let mixed_pool = SubscriptionPool {
        plans: vec![
            plan(4.0, 10.0),
            SubscriptionPlan {
                measured: None,
                ..plan(2.0, 70.0)
            },
        ],
        now_ms: 1000,
        ..Default::default()
    };
    let mixed = mixed_pool.price(1.0, None, 10.0, false).unwrap();
    assert_eq!(
        resolve_price_rates(&model, subscription(&mixed), PriceRateRequest::default()),
        None
    );
}

#[test]
fn ordinary_fact_rates_remain_available_without_cache_write_fallback() {
    let mut model = model();
    model.billing_plane = None;
    model.input_rate = Some(2.5);
    model.cost_per_m_tok_out = Some(15.0);
    model.cache_read_rate = None;
    model.cache_write_rate = None;
    let price = resolve(&model, PriceBilling::default(), PriceRateRequest::default());
    assert_eq!((price.input, price.output), (2.5, 15.0));
    assert_eq!((price.cache_read, price.cache_write), (None, None));
    model.input_rate = None;
    assert_eq!(
        resolve_price_rates(&model, PriceBilling::default(), PriceRateRequest::default()),
        None
    );
    model.input_rate = Some(2.5);
    assert_eq!(
        resolve_price_rates(
            &model,
            PriceBilling::default(),
            PriceRateRequest {
                service_tier: Some("fast"),
                ..Default::default()
            }
        ),
        None
    );
}
