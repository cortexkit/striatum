//! Independent reference predicates for quota grouping and the request's
//! policy/listing composition. They do not call the core block evaluator.

#[path = "block_parity_tests/native_fixture.rs"]
mod native_fixture;

use super::*;
use crate::routing_v1::request_cpu_bench as bench;
use striatum_store::FenceRecheck;

fn old_targets(
    c: &ClassContext,
    origin: &FenceRecheck,
    provider: &str,
    account: &str,
    credential: &str,
    model: &str,
) -> bool {
    let named = origin
        .credential_id
        .as_deref()
        .filter(|id| {
            // Quota is shared by the account's credentials. Credit and backoff
            // require the queried credential to match the block's credential.
            origin.quota
                && crate::credential_fences::credential_account_id(&c.account_ids, id).as_deref()
                    == Some(account)
        })
        .unwrap_or(credential);
    origin.applies(provider, account, named, model)
}

fn old_applies(
    c: &ClassContext,
    origin: &FenceRecheck,
    provider: &str,
    account: &str,
    credential: &str,
    model: &str,
) -> bool {
    origin.principal.is_none()
        && origin.size_class.is_none()
        && old_targets(c, origin, provider, account, credential, model)
        && (origin.kind != "account"
            || c.fences.get(provider).is_some_and(|fences| {
                fences.iter().any(|fence| {
                    fence.account_id == account
                        && fence.observed_at_ms == origin.evidence_at
                        && fence.applies_to(
                            model,
                            c.config
                                .allocator_policy_ref(provider)
                                .family_for_model(model),
                            request_pool(None, provider, model).as_deref(),
                        )
                })
            }))
}

fn old_due(c: &ClassContext, origin: &FenceRecheck, account: &str, model: &str) -> bool {
    if c.origin_recovered(origin, account) {
        return true;
    }
    let at = if origin.quota {
        FenceRecheck::quota_group_at(
            c.rechecks.iter().filter(|other| {
                other.quota
                    && old_targets(c, other, &origin.provider, account, "", model)
                    && other.principal.is_none()
                    && other
                        .size_class
                        .is_none_or(|class| Some(class) == origin.size_class)
                    && (other.kind != "account"
                        || old_applies(c, other, &origin.provider, account, "", model))
            }),
            c.probe_owner.as_deref(),
        )
    } else {
        origin.due_at()
    };
    let owned = c.probe_owner.is_some() && origin.claim_owner == c.probe_owner;
    (at <= c.now_ms || owned) && (!origin.quota || !c.origin_has_binding_reading(origin, account))
}

fn old_probe(c: &ClassContext, provider: &str, model: &str, credential: &str) -> bool {
    let account = crate::credential_fences::credential_account_id(&c.account_ids, credential)
        .unwrap_or_else(|| credential.into());
    c.rechecks.iter().any(|origin| {
        old_applies(c, origin, provider, &account, credential, model)
            && old_due(c, origin, &account, model)
    })
}

fn old_eligible(
    c: &ClassContext,
    row: &cortexkit_claustrum_client::ListedCredential,
    fact: &striatum_core::ModelFact,
) -> bool {
    !c.rechecks.iter().any(|origin| {
        !matches!(
            origin.kind.as_str(),
            "account"
                | "account_route"
                | "non_quota_route"
                | "transient_route"
                | "observation"
                | "route"
                | "provider"
                | "failure"
        ) && old_applies(
            c,
            origin,
            &fact.provider_id,
            c.account_id(row),
            &row.id,
            &fact.model_id,
        ) && !old_due(c, origin, c.account_id(row), &fact.model_id)
    }) && (!old_probe(c, &fact.provider_id, &fact.model_id, &row.id)
        || c.probe_reserved_until(&fact.provider_id, &row.id).is_none())
        && c.tier_accounts.as_ref().is_none_or(|accounts| {
            accounts
                .get(&format!("{}/{}", fact.provider_id, fact.model_id))
                .is_some_and(|ids| ids.contains(&row.id))
        })
        && !c.eligibility.iter().any(|e| {
            e.provider == fact.provider_id
                && e.credential_id == row.id
                && (e.model.is_empty() || e.model == fact.model_id)
        })
        && (!c
            .refusals
            .iter()
            .any(|r| r.provider_id == fact.provider_id && r.credential_id == row.id)
            || old_probe(c, &fact.provider_id, &fact.model_id, &row.id))
}

fn context(fixture: &bench::RequestFixture) -> ClassContext {
    ClassContext::load(
        fixture.store(),
        &fixture.credentials(),
        fixture.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        bench::FIXTURE_NOW as i64,
    )
    .unwrap()
}

struct ParityIdentity<'a> {
    account: &'a str,
    row: &'a cortexkit_claustrum_client::ListedCredential,
    fact: &'a striatum_core::ModelFact,
    source: bool,
}

fn compare_identity(
    new: &ClassContext,
    old: &ClassContext,
    reference_origins: &[usize],
    identity: &ParityIdentity<'_>,
) {
    let ParityIdentity {
        account,
        row,
        fact,
        source,
    } = identity;
    let provider = &fact.provider_id;
    let credential = &row.id;
    let model = &fact.model_id;
    let at = new.now_ms;
    let owner = &new.probe_owner;
    let continuation = new.continuation_credential.is_some();
    let media = new.media_route;
    for (origin, &reference_index) in new.rechecks.iter().zip(reference_origins) {
        let reference_origin = &old.rechecks[reference_index];
        assert_eq!(
            new.origin_applies(origin, provider, account, credential, model),
            old_applies(old, reference_origin, provider, account, credential, model),
            "origin_applies source={source} identity {provider}/{account}/{credential}/{model} now={at} owner={owner:?} continuation={continuation} media={media} origin={origin:?}"
        );
    }
    assert_eq!(
        new.probe_eligible(provider, model, credential),
        old_probe(old, provider, model, credential),
        "probe source={source} identity {provider}/{account}/{credential}/{model} now={at} owner={owner:?} continuation={continuation} media={media}"
    );
    assert_eq!(
        new.eligible(row, fact),
        old_eligible(old, row, fact),
        "eligible source={source} identity {provider}/{account}/{credential}/{model} now={at} owner={owner:?} continuation={continuation} media={media}"
    );
}

fn compare(fixture: &bench::RequestFixture, mut current: ClassContext) {
    let now = bench::FIXTURE_NOW as i64;
    let report = fixture.store().block_parity(now).unwrap();
    assert!(report.differences.is_empty(), "{report:#?}");
    assert!(report.identities > 0 && report.function_comparisons > 0);
    let loaded = fixture.store().load_blocks(now).unwrap();
    let mut reference = current.clone();
    reference.replace_rechecks(
        fixture
            .store()
            .effective_blocks_oracle_for_test(now)
            .unwrap(),
    );
    current.replace_rechecks(loaded.blocks.iter().map(FenceRecheck::from).collect());
    let times = fixture.store().block_parity_times(now).unwrap();
    let source_identities = fixture.store().block_parity_identities().unwrap();
    // The reference lookup and synthetic rows depend only on the fixture, not
    // on the time or policy inputs. Keep them out of the comparison loop.
    let reference_origins: Vec<_> = current
        .rechecks
        .iter()
        .map(|origin| {
            reference
                .rechecks
                .iter()
                .position(|r| r.kind == origin.kind && r.origin_key == origin.origin_key)
                .unwrap()
        })
        .collect();
    let source_rows: Vec<_> = source_identities
        .iter()
        .map(|[provider, _, credential, model]| {
            let mut row = current.credentials.listed_credentials()[0].clone();
            row.id = credential.clone();
            let mut fact = current.facts[0].clone();
            fact.provider_id = provider.clone();
            fact.model_id = model.clone();
            (row, fact)
        })
        .collect();
    let mut identities: Vec<_> = source_identities
        .iter()
        .zip(&source_rows)
        .map(|([_, account, _, _], (row, fact))| ParityIdentity {
            account,
            row,
            fact,
            source: true,
        })
        .collect();
    for row in current.credentials.listed_credentials() {
        for fact in &current.facts {
            identities.push(ParityIdentity {
                account: current.account_id(row),
                row,
                fact,
                source: false,
            });
        }
    }
    let expected = times.len() as u64 * 3 * 2 * 2 * identities.len() as u64;
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8);
    let chunk_size = identities.len().div_ceil(workers).max(1);
    let comparisons: u64 = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for chunk in identities.chunks(chunk_size) {
            // ClassContext has worker-local lazy indexes. Share immutable
            // identities, but retain a separate context pair for each worker.
            let current = current.clone();
            let reference = reference.clone();
            let times = &times;
            let reference_origins = &reference_origins;
            threads.push(scope.spawn(move || {
                let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
                let mut comparisons = 0_u64;
                for &at in times {
                    for owner in [None, Some("parity-owner"), Some("parity-other-owner")] {
                        for continuation in [false, true] {
                            for media in [false, true] {
                                let mut new = current.clone();
                                let mut old = reference.clone();
                                for c in [&mut new, &mut old] {
                                    c.now_ms = at;
                                    c.probe_owner = owner.map(String::from);
                                    c.continuation_credential = continuation
                                        .then(|| c.credentials.listed_credentials()[0].id.clone());
                                    c.media_route = media;
                                }
                                for identity in chunk {
                                    compare_identity(&new, &old, reference_origins, identity);
                                    comparisons += 1;
                                }
                            }
                        }
                    }
                }
                comparisons
            }));
        }
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    });
    assert_eq!(comparisons, expected);
    assert!(comparisons > 0);
    println!(
        "full ClassContext parity: {comparisons} identity/context combinations, {} predicate comparisons, {} workers",
        comparisons * (reference_origins.len() as u64 + 2),
        identities.len().div_ceil(chunk_size)
    );
}

fn smoke() -> bench::FixtureCounts {
    bench::FixtureCounts {
        subscription_rules: 7,
        exclusion_rules: 5,
        runner_route_rules: 3,
        catalog_facts: 12,
        listed_credentials: 6,
    }
}

#[test]
fn blocks_parity_fixture() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let fixture = bench::representative_fixture(smoke());
    let mut c = context(&fixture);
    native_fixture::seed(&fixture, bench::FIXTURE_NOW as i64);
    c.fences.insert(
        "cpu-provider-0".into(),
        fixture
            .store()
            .account_read(|tx| tx.active_fences("cpu-provider-0"))
            .unwrap()
            .into_iter()
            .map(|row| serde_json::from_str(&row.fence_json).unwrap())
            .collect(),
    );
    compare(&fixture, c);
}

#[test]
fn blocks_parity_production_fixture() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let fixture = bench::production_fixture();
    let c = context(&fixture);
    compare(&fixture, c);
}

#[test]
fn blocks_parity_representative_smoke_fixture() {
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let fixture = bench::representative_fixture(smoke());
    let c = context(&fixture);
    compare(&fixture, c);
}
