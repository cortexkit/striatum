//! Keep a runner's last successful description across brief management outages.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::broca_credentials::BrocaCredentials;
use crate::config_policy::RunnerPolicy;

pub(super) const LAST_GOOD_MS: i64 = 5 * 60_000;

pub(super) fn fresh_observation(credentials: &BrocaCredentials, module: &str, now: i64) -> bool {
    credentials
        .runner_describe_observed
        .get(module)
        .is_some_and(|at| now.saturating_sub(*at) < LAST_GOOD_MS)
}

#[derive(Default)]
pub(crate) struct DescribeCache {
    modules: BTreeSet<String>,
    failed: BTreeSet<String>,
    auto_enrolled: BTreeSet<String>,
}

impl DescribeCache {
    pub(crate) fn catalog(
        &mut self,
        modules: &Value,
        policies: &[RunnerPolicy],
        credentials: &mut BrocaCredentials,
        now: i64,
    ) -> BTreeSet<String> {
        let previous = std::mem::replace(
            &mut credentials.runner_declarations,
            super::declarations_from_catalog(modules),
        );
        let seen: BTreeSet<_> = previous
            .keys()
            .chain(credentials.runner_declarations.keys())
            .cloned()
            .collect();
        credentials.runner_declarations_seen.extend(seen);
        let present: BTreeSet<_> = modules
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry["module_id"].as_str())
            .collect();
        credentials.runner_retained_declarations.clear();
        // A restart gap retains the full declaration under the same observation
        // clock as tiers and accounts. A present replacement or removed policy
        // is not a gap, so neither may borrow an old declaration.
        for (module, declaration) in previous {
            if let Some(current) = credentials.runner_declarations.get(&module) {
                if current.harnesses != declaration.harnesses {
                    credentials.runner_serves.remove(&module);
                }
            }
            if !present.contains(module.as_str())
                && policies.iter().any(|policy| policy.module == module)
                && fresh_observation(credentials, &module, now)
            {
                credentials
                    .runner_declarations
                    .insert(module.clone(), declaration);
                credentials.runner_retained_declarations.insert(module);
            }
        }
        super::enrollment::replace_catalog(modules, credentials);
        let auto_enrolled: BTreeSet<_> = credentials
            .first_party_runners
            .keys()
            .filter(|module| !policies.iter().any(|policy| policy.module == **module))
            .cloned()
            .collect();
        for module in auto_enrolled.difference(&self.auto_enrolled) {
            crate::warn_operator(format!("runner auto-enrolled: module={module:?}"));
        }
        for module in self.auto_enrolled.difference(&auto_enrolled) {
            crate::warn_operator(format!("runner auto-enrollment left: module={module:?}"));
        }
        self.auto_enrolled = auto_enrolled;
        let policies = super::enrollment::policies_at(policies, credentials, now);
        let policies = policies.as_slice();
        self.modules =
            super::tier_discovery_modules(modules, &credentials.runner_declarations, policies);
        self.expire(credentials, now);
        // The catalog lists live connections, so a configured runner can vanish
        // during a restart without losing tier support. Keep its last answer
        // until the original deadline, but do not trust an unverified replacement
        // or keep an absent runner after its routing policy has been removed.
        let observed: BTreeSet<_> = credentials
            .runner_describe_observed
            .keys()
            .chain(credentials.runner_service_tiers.keys())
            .chain(credentials.runner_retained_declarations.iter())
            .cloned()
            .collect();
        for module in observed {
            if !self.modules.contains(&module) {
                let cause = if present.contains(module.as_str()) {
                    "cleared_catalog_unverified"
                } else if policies.iter().any(|policy| policy.module == module) {
                    "retained_catalog_gap"
                } else {
                    "cleared_catalog_missing_policy"
                };
                view_event(credentials, &module, cause, now);
                if cause.starts_with("cleared_") {
                    credentials.runner_service_tiers.remove(&module);
                    credentials.runner_routed_accounts.remove(&module);
                }
            }
        }
        credentials.runner_describe_observed.retain(|module, _| {
            self.modules.contains(module)
                || (!present.contains(module.as_str())
                    && policies.iter().any(|policy| policy.module == *module))
        });
        credentials.runner_serves.retain(|module, _| {
            self.modules.contains(module)
                || (!present.contains(module.as_str())
                    && policies.iter().any(|policy| policy.module == *module))
        });
        self.failed.retain(|module| {
            self.modules.contains(module)
                || credentials.runner_describe_observed.contains_key(module)
        });
        self.expire(credentials, now);
        self.modules.clone()
    }

    pub(crate) fn reply(
        &mut self,
        module: &str,
        describe: Option<&Value>,
        credentials: &mut BrocaCredentials,
        now: i64,
    ) -> bool {
        // A late describe can refresh tiers, but cannot revive an already
        // expired routing declaration without a new trusted catalog row.
        self.expire(credentials, now);
        let Some(describe) = describe.filter(|describe| {
            let describe = describe.get("result").unwrap_or(describe);
            describe["capabilities"].as_array().is_some_and(|groups| {
                (!groups.iter().any(|group| group == "service_tier")
                    || super::service_tiers_from_describe(describe).is_some())
                    && (!groups.iter().any(|group| group == "routed_accounts")
                        || super::routed_accounts_from_describe(describe).is_some())
            })
        }) else {
            self.missed(module, "describe", credentials, now);
            self.expire(credentials, now);
            return false;
        };
        let describe = describe.get("result").unwrap_or(describe);
        let accepts_serves = if describe["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|group| group == cortexkit_role_llm_runner::capabilities::SERVES)
        {
            describe.get("serves").is_some_and(|raw| {
                let credentials = &mut **credentials;
                let declared = credentials
                    .runner_declarations
                    .get(module)
                    .and_then(|declaration| declaration.harnesses.as_deref());
                match credentials
                    .runner_serves
                    .entry(module.to_owned())
                    .or_default()
                    .apply(raw, declared, now)
                {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(runner = module, %error, "runner serves answer refused");
                        false
                    }
                }
            })
        } else {
            credentials.runner_serves.remove(module);
            true
        };
        let previous = credentials.runner_service_tiers.get(module).cloned();
        let had_observation = credentials.runner_describe_observed.contains_key(module);
        credentials
            .runner_describe_observed
            .insert(module.to_owned(), now);
        if let Some(tiers) = super::service_tiers_from_describe(describe) {
            credentials
                .runner_service_tiers
                .insert(module.to_owned(), tiers);
        } else {
            credentials.runner_service_tiers.remove(module);
        }
        if let Some(methods) = super::routed_accounts_from_describe(describe) {
            credentials
                .runner_routed_accounts
                .insert(module.to_owned(), methods);
        } else {
            credentials.runner_routed_accounts.remove(module);
        }
        let current = credentials.runner_service_tiers.get(module);
        let recovered = self.failed.remove(module);
        if previous.as_ref() != current || recovered || !had_observation {
            let cause = match (previous.as_ref(), current) {
                (None, Some(_)) => "appeared_describe",
                (Some(_), None) => "cleared_describe_withdrawal",
                (None, None) if !had_observation => "observed_describe_without_tiers",
                _ if recovered => "observed_after_miss",
                _ => "updated_describe",
            };
            view_event(credentials, module, cause, now);
        }
        accepts_serves
    }

    pub(crate) fn missed_all(&mut self, credentials: &mut BrocaCredentials, now: i64) {
        for module in self.modules.clone() {
            self.missed(&module, "catalog", credentials, now);
        }
        self.expire(credentials, now);
    }

    fn missed(&mut self, module: &str, stage: &str, credentials: &mut BrocaCredentials, now: i64) {
        self.failed.insert(module.to_owned());
        let observed = credentials.runner_describe_observed.contains_key(module)
            || credentials.runner_service_tiers.contains_key(module);
        if observed && super::tier_view_unknown(credentials, module, now) {
            return;
        }
        let state = if observed { "retained" } else { "unobserved" };
        view_event(credentials, module, &format!("{state}_{stage}_miss"), now);
    }

    fn expire(&self, credentials: &mut BrocaCredentials, now: i64) {
        // A missed poll cannot extend trust in the last answer. Only a fresh
        // successful role.describe moves the five-minute deadline forward.
        let observed: BTreeSet<_> = credentials
            .runner_describe_observed
            .keys()
            .chain(credentials.runner_service_tiers.keys())
            .chain(credentials.runner_retained_declarations.iter())
            .cloned()
            .collect();
        let fresh: BTreeSet<_> = observed
            .iter()
            .filter(|module| fresh_observation(credentials, module, now))
            .cloned()
            .collect();
        for module in observed.difference(&fresh) {
            let cause = if credentials.runner_describe_observed.contains_key(module) {
                "expired"
            } else {
                "cleared_catalog"
            };
            view_event(credentials, module, cause, now);
        }
        let credentials = &mut **credentials;
        credentials.runner_declarations.retain(|module, _| {
            !credentials.runner_retained_declarations.contains(module) || fresh.contains(module)
        });
        credentials
            .runner_retained_declarations
            .retain(|module| fresh.contains(module));
        credentials
            .runner_service_tiers
            .retain(|module, _| fresh.contains(module));
        credentials
            .runner_routed_accounts
            .retain(|module, _| fresh.contains(module));
        credentials
            .runner_describe_observed
            .retain(|module, _| fresh.contains(module));
    }
}

fn view_event(credentials: &mut BrocaCredentials, module: &str, cause: &str, now: i64) {
    if let Some(suppressed) = credentials.runner_describe_logs.admit(module, cause, now) {
        let last_observed_at_ms = credentials.runner_describe_observed.get(module);
        crate::warn_operator(format!("runner service-tier view changed: module={module:?} cause={cause} last_observed_at_ms={last_observed_at_ms:?} suppressed={suppressed}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn describe() -> Value {
        json!({"capabilities":["service_tier","routed_accounts"],
            "service_tiers":{"openai.responses":{"levels":["fast"],"served":"reported"}},
            "routed_accounts":["oauth"]})
    }

    fn setup() -> (DescribeCache, BrocaCredentials, Vec<RunnerPolicy>, Value) {
        let mut cache = DescribeCache::default();
        let mut credentials = BrocaCredentials::default();
        let policies: Vec<RunnerPolicy> =
            serde_json::from_value(json!([{"module":"runner-a","via_vault":true}])).unwrap();
        let modules = json!([{"module_id":"runner-a","nonce_verified":true}]);
        cache.catalog(&modules, &policies, &mut credentials, 0);
        cache.reply("runner-a", Some(&describe()), &mut credentials, 0);
        (cache, credentials, policies, modules)
    }

    fn declared_modules() -> Value {
        json!([{"module_id":"runner-a","nonce_verified":true,
        "role_declarations":{"routing/v1":{"runner":{
            "harnesses":["runner-a"],
            "harness_limits":{"runner-a":{"capabilities":["one_off"]}}
        }}}}])
    }

    #[test]
    fn declaration_retention_shares_deadline_and_marks_survive_repeated_gaps() {
        let (mut cache, mut credentials, policies, _) = setup();
        let modules = declared_modules();
        for cycle in 0..64 {
            let start = cycle * LAST_GOOD_MS * 2;
            cache.catalog(&modules, &policies, &mut credentials, start);
            cache.reply("runner-a", Some(&describe()), &mut credentials, start);
            assert_eq!(
                super::super::enrollment::policies_at(&policies, &credentials, start).len(),
                1
            );
            let declaration = credentials.runner_declarations["runner-a"].clone();
            for age in [1, LAST_GOOD_MS - 1] {
                cache.catalog(&json!([]), &policies, &mut credentials, start + age);
                assert_eq!(credentials.runner_declarations["runner-a"], declaration);
                assert!(credentials.runner_service_tiers.contains_key("runner-a"));
                assert_eq!(credentials.runner_routed_accounts["runner-a"], ["oauth"]);
                assert_eq!(credentials.runner_describe_observed["runner-a"], start);
            }
            cache.missed_all(&mut credentials, start + LAST_GOOD_MS);
            assert!(credentials.runner_declarations.is_empty());
            assert!(credentials.runner_service_tiers.is_empty());
            assert!(credentials.runner_routed_accounts.is_empty());
            assert!(credentials.runner_describe_observed.is_empty());
            assert!(super::super::enrollment::policies_at(
                &policies,
                &credentials,
                start + LAST_GOOD_MS
            )
            .is_empty());
            assert_eq!(
                credentials.runner_declarations_seen,
                BTreeSet::from(["runner-a".into()])
            );
        }
        cache.catalog(&json!([]), &[], &mut credentials, 128 * LAST_GOOD_MS);
        assert!(credentials.runner_declarations_seen.contains("runner-a"));
    }

    #[test]
    fn declaration_retention_never_masks_replacement_or_removed_policy() {
        for (replacement, keep_policy) in [
            (
                json!([{"module_id":"runner-a","nonce_verified":false}]),
                true,
            ),
            (
                json!([{"module_id":"runner-a","nonce_verified":true}]),
                true,
            ),
            (
                json!([{"module_id":"runner-a","nonce_verified":true,"role_declarations":{"routing/v1":{"runner":{"pairs":"invalid"}}}}]),
                true,
            ),
            (json!([]), false),
        ] {
            let (mut cache, mut credentials, policies, _) = setup();
            cache.catalog(&declared_modules(), &policies, &mut credentials, 0);
            cache.catalog(
                &replacement,
                if keep_policy { &policies } else { &[] },
                &mut credentials,
                1,
            );
            assert!(credentials.runner_declarations.is_empty(), "{replacement}");
            assert!(credentials.runner_retained_declarations.is_empty());
            assert!(credentials.runner_declarations_seen.contains("runner-a"));
            assert!(super::super::enrollment::policies_at(&policies, &credentials, 1).is_empty());
        }
    }

    #[test]
    fn expired_declaration_is_not_restored_by_describe() {
        let (mut cache, mut credentials, policies, _) = setup();
        cache.catalog(&declared_modules(), &policies, &mut credentials, 0);
        cache.catalog(&json!([]), &policies, &mut credentials, 1);
        DescribeCache::default().reply(
            "runner-a",
            Some(&describe()),
            &mut credentials,
            LAST_GOOD_MS,
        );
        assert!(credentials.runner_declarations.is_empty());
        assert!(credentials.runner_service_tiers.contains_key("runner-a"));
        assert!(
            super::super::enrollment::policies_at(&policies, &credentials, LAST_GOOD_MS).is_empty()
        );
    }

    #[test]
    fn healthy_watch_backstop_keeps_tiers_and_routed_accounts_during_redescribe() {
        let (mut cache, mut credentials, policies, modules) = setup();
        let next_refresh = super::super::WATCH_BACKSTOP.as_millis() as i64;
        cache.catalog(&modules, &policies, &mut credentials, next_refresh);
        assert!(!super::super::admission::tier_view_unknown(
            &credentials,
            "runner-a",
            next_refresh
        ));
        assert_eq!(credentials.runner_routed_accounts["runner-a"], ["oauth"]);
    }

    #[test]
    fn missed_polls_retain_both_views_until_five_minutes_after_last_answer() {
        let (mut cache, mut credentials, _, _) = setup();
        cache.reply("runner-a", None, &mut credentials, 30_000);
        cache.missed_all(&mut credentials, 299_999);
        assert!(credentials.runner_service_tiers.contains_key("runner-a"));
        assert_eq!(credentials.runner_routed_accounts["runner-a"], ["oauth"]);
        cache.reply("runner-a", None, &mut credentials, 300_000);
        assert!(credentials.runner_service_tiers.is_empty());
        assert!(credentials.runner_routed_accounts.is_empty());
    }

    #[test]
    fn catalog_gap_retains_both_views_without_extending_the_deadline() {
        let (mut cache, mut credentials, policies, _) = setup();
        assert!(cache
            .catalog(&json!([]), &policies, &mut credentials, 1)
            .is_empty());
        cache.catalog(&json!([]), &policies, &mut credentials, LAST_GOOD_MS - 1);
        assert!(credentials.runner_service_tiers.contains_key("runner-a"));
        assert_eq!(credentials.runner_routed_accounts["runner-a"], ["oauth"]);
        assert_eq!(credentials.runner_describe_observed["runner-a"], 0);
        cache.catalog(&json!([]), &policies, &mut credentials, LAST_GOOD_MS);
        assert!(credentials.runner_service_tiers.is_empty());
        assert!(credentials.runner_routed_accounts.is_empty());
        assert!(credentials.runner_describe_observed.is_empty());
    }

    #[test]
    fn removed_policy_or_unverified_replacement_drops_both_views_immediately() {
        for unverified in [false, true] {
            let (mut cache, mut credentials, policies, _) = setup();
            if unverified {
                cache.catalog(
                    &json!([{"module_id":"runner-a","nonce_verified":false}]),
                    &policies,
                    &mut credentials,
                    1,
                );
            } else {
                cache.catalog(&json!([]), &[], &mut credentials, 1);
            }
            assert!(credentials.runner_service_tiers.is_empty());
            assert!(credentials.runner_routed_accounts.is_empty());
            assert!(credentials.runner_describe_observed.is_empty());
        }
    }

    #[test]
    fn successful_answers_replace_support_and_failures_and_recovery_are_logged() {
        let (mut cache, mut credentials, _, _) = setup();
        crate::take_operator_warnings();
        cache.reply("runner-a", None, &mut credentials, 1);
        cache.reply(
            "runner-a",
            Some(&json!({"code":"unavailable"})),
            &mut credentials,
            2,
        );
        assert!(credentials.runner_service_tiers.contains_key("runner-a"));
        cache.reply(
            "runner-a",
            Some(&json!({"capabilities":[]})),
            &mut credentials,
            3,
        );
        assert!(credentials.runner_service_tiers.is_empty());
        assert!(credentials.runner_routed_accounts.is_empty());
        let lines = crate::take_operator_warnings();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(lines[0].contains("cause=retained_describe_miss"));
        assert!(lines[1].contains("cause=cleared_describe_withdrawal"));
        assert!(lines.iter().all(|line| line.contains("runner-a")));
    }

    #[test]
    fn tier_view_events_are_rate_limited_and_name_each_retention_and_removal_cause() {
        crate::take_operator_warnings();
        let (mut cache, mut credentials, policies, modules) = setup();
        cache.reply("runner-a", None, &mut credentials, 1);
        cache.reply("runner-a", None, &mut credentials, 2);
        cache.catalog(&json!([]), &policies, &mut credentials, 3);
        cache.catalog(&json!([]), &policies, &mut credentials, 4);
        cache.missed_all(&mut credentials, LAST_GOOD_MS);
        cache.catalog(&modules, &policies, &mut credentials, LAST_GOOD_MS + 1);
        cache.reply(
            "runner-a",
            Some(&describe()),
            &mut credentials,
            LAST_GOOD_MS + 1,
        );
        cache.catalog(
            &json!([{"module_id":"runner-a","nonce_verified":false}]),
            &policies,
            &mut credentials,
            LAST_GOOD_MS + 2,
        );
        let lines = crate::take_operator_warnings();
        assert_eq!(lines.len(), 6, "{lines:?}");
        for (line, cause) in lines.iter().zip([
            "appeared_describe",
            "retained_describe_miss",
            "retained_catalog_gap",
            "expired",
            "appeared_describe",
            "cleared_catalog_unverified",
        ]) {
            assert!(
                line.contains("runner service-tier view changed: "),
                "{line}"
            );
            assert!(line.contains(&format!("cause={cause}")), "{line}");
            assert!(line.contains("runner-a"), "{line}");
        }
    }
}
