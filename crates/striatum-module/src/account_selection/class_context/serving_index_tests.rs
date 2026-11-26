//! Serving-index tests cover the structural view shared across account requests.

use super::*;
use crate::routing_v1::{request_cpu_bench as bench, runners::Registry};
use striatum_core::route_state_store::RouteStateStore;

// These linear readers intentionally do not use any request-local indexes.
// They preserve the exemption that lets a blocked credential run its due
// recovery probe, as well as the other pre-index predicates.
fn old_probe(c: &ClassContext, provider: &str, model: &str, credential: &str) -> bool {
    let account = crate::credential_fences::credential_account_id(&c.account_ids, credential)
        .unwrap_or_else(|| credential.into());
    c.rechecks.iter().any(|o| {
        c.origin_applies(o, provider, &account, credential, model)
            && c.origin_due(o, &account, model)
    })
}

fn old_eligible(
    c: &ClassContext,
    row: &cortexkit_claustrum_client::ListedCredential,
    f: &striatum_core::ModelFact,
) -> bool {
    !c.rechecks.iter().any(|o| {
        !matches!(
            o.kind.as_str(),
            "account"
                | "account_route"
                | "non_quota_route"
                | "transient_route"
                | "observation"
                | "route"
                | "provider"
                | "failure"
        ) && c.origin_applies(o, &f.provider_id, c.account_id(row), &row.id, &f.model_id)
            && !c.origin_due(o, c.account_id(row), &f.model_id)
    }) && (!old_probe(c, &f.provider_id, &f.model_id, &row.id)
        || probe_reservations::blocked_until(
            &c.probe_scope,
            &f.provider_id,
            &row.id,
            c.probe_owner.as_deref(),
            c.now_ms,
        )
        .is_none())
        && c.tier_accounts.as_ref().is_none_or(|a| {
            a.get(&format!("{}/{}", f.provider_id, f.model_id))
                .is_some_and(|ids| ids.contains(&row.id))
        })
        && !c.eligibility.iter().any(|e| {
            e.provider == f.provider_id
                && e.credential_id == row.id
                && (e.model.is_empty() || e.model == f.model_id)
        })
        && !c.refusals.iter().any(|e| {
            e.provider_id == f.provider_id
                && e.credential_id == row.id
                && !old_probe(c, &f.provider_id, &f.model_id, &row.id)
        })
}

fn old_view(
    c: &ClassContext,
    row: &cortexkit_claustrum_client::ListedCredential,
    f: &striatum_core::ModelFact,
) -> AccountView {
    let account = c.account_id(row);
    let capacity = format!("{}/{}", f.provider_id, f.model_id);
    let pools = c.config.quota_model_pools();
    let reading = c
        .quota
        .readings
        .readings
        .values()
        .filter(|r| {
            r.account_id == account
                && r.tombstoned_at_ms.is_none()
                && r.routing_provider().is_some_and(|p| {
                    p == f.provider_id || c.targets.get(&f.provider_id).is_some_and(|t| t == p)
                })
        })
        .max_by_key(|r| {
            r.entry
                .get("fetchedAt")
                .and_then(Value::as_str)
                .and_then(account_quota::parse_rfc3339_ms)
        });
    let usage = reading
        .and_then(|r| serde_json::from_value::<AccountEntry>(r.entry.clone()).ok())
        .and_then(|e| e.usage);
    let pooled = usage
        .as_ref()
        .and_then(|u| u.extra_rate_windows.as_ref())
        .into_iter()
        .flatten()
        .any(striatum_core::quota_extract::is_account_pool_window);
    let model_reach = if pooled {
        ModelReach::Only(
            usage
                .as_ref()
                .filter(|u| {
                    !striatum_core::quota_extract::account_rate_windows(u, Some(&capacity), &pools)
                        .is_empty()
                })
                .map(|_| f.model_id.clone())
                .into_iter()
                .collect(),
        )
    } else {
        ModelReach::All
    };
    AccountView {
        account_id: account.into(),
        routing_provider: f.provider_id.clone(),
        refresh_adapter: row.refresh_adapter.clone().unwrap_or_default(),
        credentials: vec![CredentialRecord {
            credential_id: row.id.clone(),
            default_record: is_default_record(&row.id),
            needs_reauth: false,
        }],
        model_reach,
        quota: reading
            .filter(|_| {
                !c.rechecks.iter().any(|o| {
                    o.quota
                        && c.origin_applies(o, &f.provider_id, account, &row.id, &f.model_id)
                        && c.origin_due(o, account, &f.model_id)
                })
            })
            .filter(|_| crate::credential_fences::has_quota_signal_for_account(&c.quota, account))
            .and_then(|r| quota_reading_for_model(&r.entry, account, Some(&capacity), &pools)),
    }
}

fn old_allocation(
    c: &ClassContext,
    provider: &str,
    model: &str,
) -> striatum_core::accounts::AllocationContext {
    let mut result = striatum_core::accounts::AllocationContext::default();
    for row in c.credentials.listed_credentials() {
        if c.rechecks.iter().any(|o| {
            c.origin_applies(o, provider, c.account_id(row), &row.id, model)
                && c.origin_due(o, c.account_id(row), model)
        }) {
            result.credential_probe_notes.insert(
                row.id.clone(),
                crate::credential_fences::probe_note(provider, &row.id),
            );
        }
    }
    if let Some(fact) = c
        .facts
        .iter()
        .find(|f| f.provider_id == provider && f.model_id == model)
    {
        for row in c.credentials.listed_credentials().iter().filter(|r| {
            crate::broca_credentials::credential_serves_fact(r, fact, &c.catalog, &c.credentials)
        }) {
            let account = c.account_id(row);
            if let Some(reading) = c
                .quota
                .readings
                .readings
                .values()
                .filter(|r| r.account_id == account && r.tombstoned_at_ms.is_none())
                .max_by_key(|r| {
                    r.entry
                        .get("fetchedAt")
                        .and_then(Value::as_str)
                        .and_then(account_quota::parse_rfc3339_ms)
                })
            {
                add_spend_inputs(&mut result, account, &reading.entry);
            }
        }
    }
    result
}

fn old_class<'a>(
    c: &'a ClassContext,
    fact: &striatum_core::ModelFact,
    priced: bool,
) -> RouteClass<'a> {
    let mut f = fact.clone();
    if let Some(id) = &c.continuation_credential {
        let method = c.method(id);
        f.auth_methods = Some(
            if method != "unknown"
                && fact
                    .auth_methods
                    .as_ref()
                    .is_none_or(|a| a.iter().any(|m| m == method))
            {
                vec![method.into()]
            } else {
                Vec::new()
            },
        );
    }
    let specs = striatum_core::provider_model_spec::parse_spec_list(
        &c.config.operator_policy_ref().subscription,
    );
    resolve_class(
        c.credentials.listed_credentials(),
        &f,
        &c.catalog,
        &c.credentials,
        &specs,
        c.targets.contains_key(&f.provider_id),
        priced,
        c.config.metered_fallback(&f.provider_id),
        |row| {
            if !old_eligible(c, row, &f) {
                return false;
            }
            let key = format!("{}/{}", f.provider_id, f.model_id);
            let covered = |r: &cortexkit_claustrum_client::ListedCredential| {
                !c.targets.contains_key(&f.provider_id)
                    && striatum_core::provider_model_spec::resolve_spec(
                        &specs,
                        &key,
                        r.credential_type == "oauth",
                    )
            };
            let siblings = c
                .credentials
                .listed_credentials()
                .iter()
                .filter(|r| {
                    covered(r) == covered(row)
                        && old_eligible(c, r, &f)
                        && crate::broca_credentials::credential_serves_fact(
                            r,
                            &f,
                            &c.catalog,
                            &c.credentials,
                        )
                })
                .map(|r| old_view(c, r, &f))
                .collect();
            matches!(striatum_core::accounts::decide_with_context(&synthetic_request(&f.provider_id, &f.model_id, "class-preflight".into()), &AllocatorState {
                roster: Some(c.roster(siblings)), affinity: None,
                fences: c.probe_fences(&f.provider_id, &f.model_id, c.fences.get(&f.provider_id).cloned().unwrap_or_default()),
                principal_exclusions: Vec::new(), admissions: Vec::new(), operator_override: None, policy: c.config.allocator_policy_ref(&f.provider_id).clone(),
            }, c.now_ms, &old_allocation(c, &f.provider_id, &f.model_id)).outcome, Outcome::Admit(a) if a.account_id == c.account_id(row))
        },
    )
}

fn answer(class: RouteClass<'_>) -> (bool, Vec<String>) {
    (
        class.subscription,
        class.rows.into_iter().map(|r| r.id.clone()).collect(),
    )
}

fn context(f: &bench::RequestFixture) -> ClassContext {
    ClassContext::load(
        f.store(),
        &f.credentials(),
        f.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        bench::FIXTURE_NOW as i64,
    )
    .unwrap()
}

fn counts() -> bench::FixtureCounts {
    bench::FixtureCounts {
        subscription_rules: 10,
        exclusion_rules: 103,
        runner_route_rules: 0,
        catalog_facts: 836,
        listed_credentials: 39,
    }
}

fn assert_differential(base: &ClassContext) {
    for due in [false, true] {
        for continuation in [false, true] {
            let mut c = base.clone();
            if continuation {
                c.revalidate_credential(&c.credentials.listed_credentials()[0].id.clone());
            }
            let origins = c
                .credentials
                .listed_credentials()
                .iter()
                .flat_map(|row| {
                    row.provider_ids
                        .iter()
                        .map(|provider| striatum_store::FenceRecheck {
                            evidence: row.id.clone(),
                            kind: "credential".into(),
                            origin_key: row.id.clone(),
                            provider: provider.clone(),
                            account_id: None,
                            credential_id: Some(row.id.clone()),
                            console_credit_micros_at_refusal: None,
                            model: None,
                            size_class: None,
                            principal: None,
                            admission_id: None,
                            evidence_at: c.now_ms - 2,
                            quota: false,
                            classified: true,
                            next_recheck_at: c.now_ms + if due { -1 } else { 60_000 },
                            reset_check_at: None,
                            last_claimed_at: None,
                            claim_owner: None,
                            early_billing_probe: false,
                            cleared_models: Vec::new(),
                        })
                })
                .collect();
            c.replace_rechecks(origins);
            for priced in [false, true] {
                let mut facts = c.facts.clone();
                c.credentials.price_facts(&mut facts, c.config.pricing());
                for fact in &mut facts {
                    let old = old_class(&c, fact, priced);
                    let new = c.class(fact, priced);
                    assert_eq!(answer(new), answer(old), "before class pricing due={due} continuation={continuation} priced={priced}");
                    if let Some(method) = old_class(&c, fact, priced)
                        .rows
                        .first()
                        .and_then(|r| r.auth_method.as_deref())
                    {
                        c.credentials
                            .price_for_method(fact, method, c.config.pricing());
                    }
                    assert!(
                        fact.billing_plane.is_some(),
                        "both payment classes must use selection pricing"
                    );
                    assert_eq!(
                        answer(c.class(fact, priced)),
                        answer(old_class(&c, fact, priced)),
                        "after class pricing {}/{}",
                        fact.provider_id,
                        fact.model_id
                    );
                    for row in c.credentials.listed_credentials().iter().filter(|row| {
                        crate::broca_credentials::credential_serves_fact(
                            row,
                            fact,
                            &c.catalog,
                            &c.credentials,
                        )
                    }) {
                        assert_eq!(
                            c.eligible(row, fact),
                            old_eligible(&c, row, fact),
                            "eligible {}",
                            row.id
                        );
                        assert_eq!(
                            serde_json::to_value(c.view(row, fact)).unwrap(),
                            serde_json::to_value(old_view(&c, row, fact)).unwrap(),
                            "view {}",
                            row.id
                        );
                    }
                }
                for now in [c.now_ms as u64, c.now_ms as u64 + 1_000_000] {
                    for harness in [None, Some("default")] {
                        let new = Registry::build_for_harness(
                            c.config.as_ref(),
                            &c.credentials,
                            &facts,
                            now,
                            harness,
                        );
                        let old = Registry::build_for_harness(
                            c.config.v1_runners(),
                            &c.credentials,
                            &facts,
                            now,
                            harness,
                        );
                        assert_eq!(
                            serde_json::to_value(&new.entries).unwrap(),
                            serde_json::to_value(&old.entries).unwrap()
                        );
                        for runner in &new.entries {
                            for fact in &facts {
                                let route = striatum_core::make_route_key(
                                    &fact.provider_id,
                                    &fact.model_id,
                                    fact.variant.as_deref(),
                                );
                                let capacity = format!("{}/{}", fact.provider_id, fact.model_id);
                                assert_eq!(
                                    new.runs(&runner.policy.module, &route, &capacity),
                                    old.runs(&runner.policy.module, &route, &capacity)
                                );
                                assert_eq!(
                                    new.can_run(&runner.policy.module, &route, &capacity),
                                    old.can_run(&runner.policy.module, &route, &capacity)
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn serving_index_differential_all_facts_payments_probes_and_continuations() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let production = bench::production_fixture();
    assert_differential(&context(&production));
    drop(production);
    let fixture = bench::representative_fixture(counts());
    let mut previous = context(&fixture);
    assert!(previous.facts.iter().any(|f| f.provider_id == "cpu-pool-0"));
    assert!(!previous.catalog.contains("cpu-pool-0"));
    assert_differential(&previous);
    let unchanged = context(&fixture);
    assert!(std::sync::Arc::ptr_eq(
        &previous.serving_index,
        &unchanged.serving_index
    ));
    let patched = std::sync::Arc::new((*previous.config).clone());
    let isolated = crate::serving_index::ServingIndex::get(
        &patched,
        &previous.credentials,
        &fixture.store().get_facts_with_generation().unwrap(),
    );
    assert!(!std::sync::Arc::ptr_eq(&previous.serving_index, &isolated));

    for change in 0..6 {
        match change {
            0 => fixture.edit_config(|config| {
                config["models"]["cpu-provider-0/model-0"]["subscription"] = json!(false)
            }),
            1 => {
                let mut fact = fixture.store().get_facts().unwrap()[0].clone();
                fact.auth_methods = Some(vec!["apikey".into()]);
                fixture.store().with_writer_for_test(|connection| {
                        connection.execute("UPDATE model_facts SET auth_methods_json=?1 WHERE provider_id=?2 AND model_id=?3",
                            rusqlite::params![serde_json::to_string(&fact.auth_methods).unwrap(), fact.provider_id, fact.model_id])
                    }).unwrap();
            }
            2 => {
                let shared = fixture.credentials();
                let mut credentials = shared.lock().unwrap();
                let mut rows = credentials.listed_credentials().to_vec();
                rows[0].state = "disabled".into();
                credentials.apply_listing(
                    &cortexkit_claustrum_client::ScopedCredentialListing {
                        view: "changed-listing".into(),
                        credentials: rows,
                        ..Default::default()
                    },
                    bench::FIXTURE_NOW,
                );
            }
            3 => {
                let shared = fixture.credentials();
                let mut credentials = shared.lock().unwrap();
                for declaration in credentials.runner_declarations.values_mut() {
                    declaration.follows_account_choice = !declaration.follows_account_choice;
                }
            }
            4 => fixture
                .credentials()
                .lock()
                .unwrap()
                .publish_billing_catalog(Default::default()),
            5 => {
                let shared = fixture.credentials();
                let mut credentials = shared.lock().unwrap();
                // An alias borrows model rows from a catalog provider only
                // while it has no catalog rows of its own. Supplying its
                // rows therefore removes that target without a vault change.
                credentials.refresh_serving_methods(
                    &Default::default(),
                    &previous.facts,
                    bench::FIXTURE_NOW as i64,
                    &[],
                );
            }
            _ => unreachable!(),
        }
        let next = context(&fixture);
        assert!(
            !std::sync::Arc::ptr_eq(&previous.serving_index, &next.serving_index),
            "changed input {change} reused a stale index"
        );
        assert_differential(&next);
        previous = next;
    }
    let shared = fixture.credentials();
    {
        let mut credentials = shared.lock().unwrap();
        let listing = cortexkit_claustrum_client::ScopedCredentialListing {
            view: credentials.listing_version().unwrap().0.into(),
            credentials: credentials.listed_credentials().to_vec(),
            ..Default::default()
        };
        credentials.apply_listing(&listing, bench::FIXTURE_NOW + 1);
        let declarations = credentials.runner_declarations.clone();
        credentials.runner_declarations = declarations;
        let billing = credentials.billing_catalog.clone();
        credentials.publish_billing_catalog(billing);
        credentials.refresh_serving_methods(
            &Default::default(),
            &previous.facts,
            bench::FIXTURE_NOW as i64 + 1,
            &[],
        );
    }
    assert!(
        std::sync::Arc::ptr_eq(&previous.serving_index, &context(&fixture).serving_index),
        "equal refreshes must share the index"
    );
}

#[test]
fn dynamic_indexes_are_lazy_once_and_probe_reservation_replaces_rechecks() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let fixture = bench::representative_fixture(counts());
    let mut c = context(&fixture);
    take_index_build_counts();
    let clone = c.clone();
    assert_eq!(take_index_build_counts(), [0; 3]);
    assert!(clone.indexes.blocks.get().is_none());
    let row = c.credentials.listed_credentials()[0].clone();
    let fact = c
        .facts
        .iter()
        .find(|f| row.provider_ids.contains(&f.provider_id))
        .unwrap()
        .clone();
    let origin = striatum_store::FenceRecheck {
        evidence: "test-only-old-snapshot".into(),
        kind: "credential".into(),
        origin_key: row.id.clone(),
        provider: fact.provider_id.clone(),
        account_id: None,
        credential_id: Some(row.id.clone()),
        console_credit_micros_at_refusal: None,
        model: None,
        size_class: None,
        principal: None,
        admission_id: None,
        evidence_at: c.now_ms - 2,
        quota: false,
        classified: true,
        next_recheck_at: c.now_ms - 1,
        reset_check_at: None,
        last_claimed_at: None,
        claim_owner: None,
        early_billing_probe: false,
        cleared_models: Vec::new(),
    };
    c.replace_rechecks(vec![origin]);
    for _ in 0..2 {
        assert!(c.probe_eligible(&fact.provider_id, &fact.model_id, &row.id));
        c.eligible(&row, &fact);
        c.view(&row, &fact);
    }
    assert_eq!(take_index_build_counts(), [1, 1, 1]);
    c.reserve_selection_probes(fixture.store(), &fixture.credentials())
        .unwrap();
    assert!(
        c.indexes.rechecks.get().is_none(),
        "replacement and invalidation are one operation"
    );
    assert!(!c
        .rechecks
        .iter()
        .any(|o| o.evidence == "test-only-old-snapshot"));
    assert_eq!(
        c.probe_eligible(&fact.provider_id, &fact.model_id, &row.id),
        old_probe(&c, &fact.provider_id, &fact.model_id, &row.id)
    );
    assert_eq!(take_index_build_counts()[2], 1);
}
