use super::super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

fn request_groups<K: Eq>(
    production_inputs: impl Fn(bool, bool) -> K,
) -> Vec<(K, Vec<(bool, bool)>)> {
    let mut groups: Vec<(K, Vec<_>)> = Vec::new();
    for continuation in [false, true] {
        for media in [false, true] {
            let key = production_inputs(continuation, media);
            if let Some((_, flags)) = groups.iter_mut().find(|(inputs, _)| inputs == &key) {
                flags.push((continuation, media));
            } else {
                groups.push((key, vec![(continuation, media)]));
            }
        }
    }
    groups
}

/// Evaluate every identity/time/flag combination. Each worker reads
/// the same immutable SQL inputs and owns its counts and mismatch messages.
pub(in crate::fence_rechecks) fn compare(
    conn: &Connection,
    now: i64,
    rows: &BTreeMap<(String, String), FenceRecheck>,
    blocks: &[Block],
    report: &mut BlockParity,
) -> rusqlite::Result<()> {
    use striatum_core::blocks::{self, Target};
    let identities = parity_identities(conn)?;
    let times = parity_times(conn, now)?;
    report.identities = identities.len();
    report.combinations_per_identity = times.len() * 16 * 3 * 2 * 2;
    let pairs: Vec<_> = blocks
        .iter()
        .filter_map(|block| {
            rows.get(&(block.kind.clone(), block.origin_key.clone()))
                .map(|row| (block, row))
        })
        .collect();
    let next = AtomicUsize::new(0);
    // Group by the arguments forwarded to the pure predicates, not by the
    // request labels. The other arguments are fixed by the enclosing loops.
    // Neither request flag is forwarded today. If a predicate takes new
    // request inputs, include them in this grouping key so calls with
    // different inputs cannot share a result.
    let request_groups = request_groups(|_continuation, _media| ());
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8);
    let results = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..workers {
            let identities = &identities;
            let times = &times;
            let pairs = &pairs;
            let request_groups = &request_groups;
            let next = &next;
            threads.push(scope.spawn(move || {
                let mut comparisons=0;
                let mut differences=Vec::new();
                loop {
                    let start=next.fetch_add(64,Ordering::Relaxed);
                    if start>=identities.len(){break;}
                    for [provider,account,credential,model] in &identities[start..(start+64).min(identities.len())] {
                        let(provider,account,credential,model)=(provider.as_str(),account.as_str(),credential.as_str(),model.as_str());
                        let target=Target {provider,account,credential,model};
                        for at in times {
                            for mask in 0..16 {
                                for owner in 0..3 {
                                    let alias=mask&1!=0;
                                    let pool=mask&2!=0;
                                    let recovered=mask&4!=0;
                                    let binding=mask&8!=0;
                                    for (block,row) in pairs {
                                        let origin=block.origin();
                                        let named=row.credential_id.as_deref().filter(|_| {
                                            (row.quota || matches!(row.kind.as_str(),"account_route"|"non_quota_route"|"transient_route")) && alias
                                        }).unwrap_or(credential);
                                        let old_targets=row.applies(provider,account,named,model);
                                        let old_applies=row.principal.is_none() && row.size_class.is_none() && old_targets && (row.kind!="account" || pool);
                                        let owned=owner==1 && row.claim_owner.is_some();
                                        let old_due=recovered || ((row.due_at()<=*at || owned) && (!row.quota || !binding));
                                        let old_eligible=matches!(row.kind.as_str(),"account"|"account_route"|"non_quota_route"|"transient_route"|"observation"|"route"|"provider"|"failure") || !old_applies || old_due;
                                        let old_probe=old_applies && old_due;
                                        for ((), flags) in request_groups {
                                            let new_targets=blocks::targets(&origin,target,alias.then_some(account));
                                            let new_applies=blocks::origin_applies(&origin,||new_targets,||pool);
                                            let new_due=blocks::due(block.lifetime,recovered,block.reset_check_at.map_or(block.next_recheck_at,|reset|reset.min(block.next_recheck_at)),*at,owned,||binding);
                                            let new_eligible=blocks::eligible(&origin,||new_applies,||new_due);
                                            let new_probe=blocks::probe_eligible(new_applies,||new_due);
                                            for &(continuation, media) in flags {
                                                comparisons+=3;
                                                if old_targets!=new_targets || old_applies!=new_applies || old_eligible!=new_eligible || old_probe!=new_probe {
                                                    differences.push(format!("functions origin=({}, {}) identity={target:?} now={at} mask={mask} owner={owner} continuation={continuation} media={media}: targets {old_targets}/{new_targets}, applies {old_applies}/{new_applies}, eligible {old_eligible}/{new_eligible}, probe {old_probe}/{new_probe}",block.kind,block.origin_key));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                (comparisons,differences)
            }));
        }
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    for (comparisons, differences) in results {
        report.function_comparisons += comparisons;
        report.differences.extend(differences);
    }
    assert_eq!(
        report.function_comparisons,
        report.identities as u64 * report.combinations_per_identity as u64 * pairs.len() as u64 * 3,
        "each identity, request combination and matched origin must be compared"
    );
    Ok(())
}
