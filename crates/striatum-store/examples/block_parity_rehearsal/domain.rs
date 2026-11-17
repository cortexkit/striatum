use std::collections::BTreeMap;
use striatum_core::blocks::{self, Block, Target};
use striatum_store::{BlockParity, FenceRecheck};

/// Use the original per-origin SQL rules as the reference. The flags cover
/// alias, pool, recovery and binding; continuation/media remain inert here.
pub(super) fn compare(
    identities: &[[String; 4]],
    times: &[i64],
    rows: &BTreeMap<(String, String), FenceRecheck>,
    native: &[Block],
    report: &mut BlockParity,
) -> u64 {
    let mut mismatches = 0;
    report.identities = identities.len();
    report.combinations_per_identity = times.len() * 16 * 3 * 2 * 2;
    report.function_comparisons = 0;
    let pairs: Vec<_> = native
        .iter()
        .filter_map(|block| {
            rows.get(&(block.kind.clone(), block.origin_key.clone()))
                .map(|row| (block, row))
        })
        .collect();
    for [provider, account, credential, model] in identities {
        let (provider, account, credential, model) = (
            provider.as_str(),
            account.as_str(),
            credential.as_str(),
            model.as_str(),
        );
        let target = Target {
            provider,
            account,
            credential,
            model,
        };
        for at in times {
            for mask in 0..16 {
                for owner in 0..3 {
                    for continuation in [false, true] {
                        for media in [false, true] {
                            let alias = mask & 1 != 0;
                            let pool = mask & 2 != 0;
                            let recovered = mask & 4 != 0;
                            let binding = mask & 8 != 0;
                            for (block, row) in &pairs {
                                let named = row
                                    .credential_id
                                    .as_deref()
                                    .filter(|_| {
                                        (row.quota
                                            || matches!(
                                                row.kind.as_str(),
                                                "account_route"
                                                    | "non_quota_route"
                                                    | "transient_route"
                                            ))
                                            && alias
                                    })
                                    .unwrap_or(credential);
                                let old_targets = row.applies(provider, account, named, model);
                                let new_targets = blocks::targets(
                                    &block.origin(),
                                    target,
                                    alias.then_some(account),
                                );
                                let old_applies = row.principal.is_none()
                                    && row.size_class.is_none()
                                    && old_targets
                                    && (row.kind != "account" || pool);
                                let new_applies = blocks::origin_applies(
                                    &block.origin(),
                                    || new_targets,
                                    || pool,
                                );
                                let owned = owner == 1 && row.claim_owner.is_some();
                                let old_due = recovered
                                    || ((row.due_at() <= *at || owned) && (!row.quota || !binding));
                                let new_due = blocks::due(
                                    block.lifetime,
                                    recovered,
                                    block.reset_check_at.map_or(block.next_recheck_at, |reset| {
                                        reset.min(block.next_recheck_at)
                                    }),
                                    *at,
                                    owned,
                                    || binding,
                                );
                                let old_eligible = matches!(
                                    row.kind.as_str(),
                                    "account"
                                        | "account_route"
                                        | "non_quota_route"
                                        | "transient_route"
                                        | "observation"
                                        | "route"
                                        | "provider"
                                        | "failure"
                                ) || !old_applies
                                    || old_due;
                                let new_eligible =
                                    blocks::eligible(&block.origin(), || new_applies, || new_due);
                                let old_probe = old_applies && old_due;
                                let new_probe = blocks::probe_eligible(new_applies, || new_due);
                                report.function_comparisons += 3;
                                if (old_targets, old_applies, old_eligible, old_probe)
                                    != (new_targets, new_applies, new_eligible, new_probe)
                                {
                                    mismatches += 1;
                                    if report.differences.len() < 100 {
                                        report.differences.push(format!("functions kind={} key={} identity={target:?} now={at} mask={mask} owner={owner} continuation={continuation} media={media}: targets {old_targets}/{new_targets}, applies {old_applies}/{new_applies}, eligible {old_eligible}/{new_eligible}, probe {old_probe}/{new_probe}",block.kind,block.origin_key));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    mismatches
}
