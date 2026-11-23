//! Class-cache tests verify that indexed eligibility stays consistent with the resolver.

use super::*;
use crate::routing_v1::request_cpu_bench::{self as bench, FixtureCounts};

fn live_counts() -> FixtureCounts {
    FixtureCounts {
        subscription_rules: 10,
        exclusion_rules: 103,
        runner_route_rules: 0,
        catalog_facts: 836,
        listed_credentials: 39,
    }
}

// The pre-compilation class path: parsed lists, a cloned fact, and no memo.
fn reference<'a>(
    context: &'a ClassContext,
    fact: &striatum_core::ModelFact,
    priced: bool,
) -> RouteClass<'a> {
    if context.media_route {
        return RouteClass {
            rows: context
                .credentials
                .listed_credentials()
                .iter()
                .filter(|row| media_credential_serves(row, &fact.provider_id))
                .collect(),
            subscription: false,
        };
    }
    let specs = striatum_core::provider_model_spec::parse_spec_list(
        &context.config.operator_policy_ref().subscription,
    );
    let mut restricted = fact.clone();
    if let Some(credential) = &context.continuation_credential {
        let method = context.method(credential);
        restricted.auth_methods = Some(
            if method != "unknown"
                && fact
                    .auth_methods
                    .as_ref()
                    .is_none_or(|allowed| allowed.iter().any(|allowed| allowed == method))
            {
                vec![method.into()]
            } else {
                Vec::new()
            },
        );
    }
    resolve_class(
        context.credentials.listed_credentials(),
        &restricted,
        &context.catalog,
        &context.credentials,
        &specs,
        context.targets.contains_key(&fact.provider_id),
        priced,
        context.config.metered_fallback(&fact.provider_id),
        |row| context.usable(row, &restricted),
    )
}

fn answer(class: RouteClass<'_>) -> (bool, Vec<String>) {
    (
        class.subscription,
        class.rows.iter().map(|row| row.id.clone()).collect(),
    )
}

fn assert_reference(context: &ClassContext, fact: &striatum_core::ModelFact) {
    for priced in [false, true] {
        assert_eq!(
            answer(context.class(fact, priced)),
            answer(reference(context, fact, priced)),
            "{}/{} priced={priced}",
            fact.provider_id,
            fact.model_id
        );
    }
}

#[test]
fn compiled_class_equals_old_class_for_every_fact_and_payment_on_both_s0_fixtures() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    for production in [true, false] {
        let fixture = if production {
            bench::production_fixture()
        } else {
            bench::representative_fixture(live_counts())
        };
        let config = fixture.operator_config();
        let context = ClassContext::load(
            fixture.store(),
            &fixture.credentials(),
            config.clone(),
            crate::ACCOUNT_QUOTA.get(),
            bench::FIXTURE_NOW as i64,
        )
        .unwrap();
        assert!(std::sync::Arc::ptr_eq(&config, &context.config));
        for fact in &context.facts {
            assert_reference(&context, fact);
            assert_reference(&context, fact);
            let memo = context.class_memo.lock().unwrap();
            for entry in &memo[&fact.provider_id][&fact.model_id][..2] {
                assert_eq!(
                    entry.as_ref().unwrap().misses,
                    1,
                    "repeated class calls must share the memo"
                );
            }
        }
    }
}

#[test]
fn class_memo_revalidates_mutable_restrictions_and_clone_inputs() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let fixture = bench::representative_fixture(live_counts());
    let mut context = ClassContext::load(
        fixture.store(),
        &fixture.credentials(),
        fixture.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        bench::FIXTURE_NOW as i64,
    )
    .unwrap();
    let mut fact = context
        .facts
        .iter()
        .find(|fact| fact.provider_id == "cpu-provider-0")
        .unwrap()
        .clone();
    fact.auth_methods = None;
    let scratch = cortexkit_test_support::ScratchDir::new("compiled-class-memo");
    let path = scratch.path().join("routing.json");
    std::fs::write(
        &path,
        r#"{"providers":{"cpu-provider-0":{"accounts":{"metered_fallback":true}}}}"#,
    )
    .unwrap();
    crate::config_overrides::initialize_config(&path).unwrap();
    context.config = crate::config_overrides::live_config(&path);
    let mut rows = context.credentials.listed_credentials().to_vec();
    let mut api = rows
        .iter()
        .find(|row| row.provider_ids.contains(&fact.provider_id))
        .unwrap()
        .clone();
    api.id = "apikey:memo".into();
    api.credential_type = "api_key".into();
    api.auth_method = Some("apikey".into());
    api.account_id = None;
    rows.push(api.clone());
    context.credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "memo".into(),
            credentials: rows,
            ..Default::default()
        },
        bench::FIXTURE_NOW,
    );
    context.credentials.runner_declarations.clear();
    context.replace_refusals(Vec::new());
    context.replace_rechecks(Vec::new());
    context.replace_fences(BTreeMap::new());
    context.quota = Default::default();
    assert_reference(&context, &fact);
    let baseline = answer(context.class(&fact, false));
    assert!(baseline.0);

    let mut tier = context.clone();
    tier.tier_accounts = Some(BTreeMap::from([(
        format!("{}/{}", fact.provider_id, fact.model_id),
        vec![api.id.clone()],
    )]));
    assert_reference(&tier, &fact);
    assert!(
        !tier.class(&fact, false).subscription,
        "a tier excluding OAuth must use the metered fallback"
    );
    assert_reference(&context, &fact);
    assert_eq!(
        answer(context.class(&fact, false)),
        baseline,
        "a clone's restrictions must not poison the original memo"
    );

    let mut continuation = context.clone();
    continuation.revalidate_credential(&api.id);
    assert_reference(&continuation, &fact);
    assert!(!continuation.class(&fact, false).subscription);
    continuation.revalidate_credential("missing-credential");
    assert_reference(&continuation, &fact);
    assert!(continuation.class(&fact, false).rows.is_empty());

    let mut media = context.clone();
    media.for_engine(&fact.provider_id, &fact.model_id).unwrap();
    assert_reference(&media, &fact);
    assert!(!media.class(&fact, false).subscription);

    let mut probing = context.clone();
    let oauth: Vec<_> = probing
        .credentials
        .listed_credentials()
        .iter()
        .filter(|row| {
            row.provider_ids.contains(&fact.provider_id) && row.credential_type == "oauth"
        })
        .map(|row| row.id.clone())
        .collect();
    probing.replace_rechecks(
        oauth
            .iter()
            .map(|id| striatum_store::FenceRecheck {
                evidence: id.clone(),
                kind: "credential".into(),
                origin_key: id.clone(),
                provider: fact.provider_id.clone(),
                account_id: None,
                credential_id: Some(id.clone()),
                console_credit_micros_at_refusal: None,
                model: None,
                size_class: None,
                principal: None,
                admission_id: None,
                evidence_at: probing.now_ms - 1,
                quota: false,
                classified: true,
                next_recheck_at: probing.now_ms - 1,
                reset_check_at: None,
                last_claimed_at: None,
                claim_owner: None,
                cleared_models: Vec::new(),
            })
            .collect(),
    );
    assert_reference(&probing, &fact);
    let (hold, _) = probe_reservations::Selection::new(
        &fixture.credentials(),
        oauth
            .iter()
            .map(|id| (fact.provider_id.clone(), id.clone()))
            .collect(),
        probing.now_ms,
    );
    assert_reference(&probing, &fact);
    assert!(
        !probing.class(&fact, false).subscription,
        "another request's probe hold must invalidate usability"
    );
    probing.probe_owner = Some(hold.owner.clone());
    assert_reference(&probing, &fact);
    assert!(probing.class(&fact, false).subscription);
    probing.selection_probes = Some(std::sync::Arc::new(hold));
    probing.probe_owner = None;
    probing.now_ms += 120_001;
    assert_reference(&probing, &fact);
    assert!(
        probing.class(&fact, false).subscription,
        "expired holds cannot survive in the memo"
    );

    let mut denied = fact.clone();
    denied.auth_methods = Some(Vec::new());
    assert_reference(&context, &denied);
    assert!(context.class(&denied, false).rows.is_empty());
    assert_reference(&context, &fact);
}
