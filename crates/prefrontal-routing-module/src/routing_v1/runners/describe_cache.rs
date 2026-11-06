//! Keep a runner's last successful description across brief management outages.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::account_refusal_log::LogLimiter;
use crate::broca_credentials::BrocaCredentials;
use crate::config_policy::RunnerPolicy;

const LAST_GOOD_MS: i64 = 5 * 60_000;

#[derive(Default)]
pub(crate) struct DescribeCache {
    modules: BTreeSet<String>,
    last_good: BTreeMap<String, i64>,
    failed: BTreeSet<String>,
    logs: LogLimiter,
}

impl DescribeCache {
    pub(crate) fn catalog(
        &mut self,
        modules: &Value,
        policies: &[RunnerPolicy],
        credentials: &mut BrocaCredentials,
        now: i64,
    ) -> BTreeSet<String> {
        credentials.runner_declarations = super::declarations_from_catalog(modules);
        self.modules =
            super::tier_discovery_modules(modules, &credentials.runner_declarations, policies);
        self.last_good
            .retain(|module, _| self.modules.contains(module));
        self.failed.retain(|module| self.modules.contains(module));
        // A successful catalog read is authoritative about removal, even while
        // a runner's last successful description would otherwise still be fresh.
        credentials
            .runner_service_tiers
            .retain(|module, _| self.modules.contains(module));
        credentials
            .runner_routed_accounts
            .retain(|module, _| self.modules.contains(module));
        self.expire(credentials, now);
        self.modules.clone()
    }

    pub(crate) fn reply(
        &mut self,
        module: &str,
        describe: Option<&Value>,
        credentials: &mut BrocaCredentials,
        now: i64,
    ) {
        let Some(describe) = describe.filter(|describe| {
            let describe = describe.get("result").unwrap_or(describe);
            describe["capabilities"].as_array().is_some_and(|groups| {
                (!groups.iter().any(|group| group == "service_tier")
                    || super::service_tiers_from_describe(describe).is_some())
                    && (!groups.iter().any(|group| group == "routed_accounts")
                        || super::routed_accounts_from_describe(describe).is_some())
            })
        }) else {
            self.missed(module, "describe", now);
            self.expire(credentials, now);
            return;
        };
        self.last_good.insert(module.to_owned(), now);
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
        if self.failed.remove(module) {
            if let Some(suppressed) = self.logs.admit(module, "runner_poll_recovered", now) {
                crate::warn_operator(format!("runner role.describe poll answered again; its tier and account view is current module={module:?} suppressed={suppressed}"));
            }
        }
    }

    pub(crate) fn missed_all(&mut self, credentials: &mut BrocaCredentials, now: i64) {
        for module in self.modules.clone() {
            self.missed(&module, "catalog", now);
        }
        self.expire(credentials, now);
    }

    fn missed(&mut self, module: &str, stage: &str, now: i64) {
        self.failed.insert(module.to_owned());
        if let Some(suppressed) = self.logs.admit(module, "runner_poll_failed", now) {
            crate::warn_operator(format!("runner role.describe poll failed; routing keeps the runner's last tier and account view for up to 5 minutes module={module:?} stage={stage} suppressed={suppressed}"));
        }
    }

    fn expire(&self, credentials: &mut BrocaCredentials, now: i64) {
        // A missed poll cannot extend trust in the last answer. Only a fresh
        // successful role.describe moves the five-minute deadline forward.
        let fresh = |module: &String| {
            self.last_good
                .get(module)
                .is_some_and(|at| now.saturating_sub(*at) < LAST_GOOD_MS)
        };
        credentials
            .runner_service_tiers
            .retain(|module, _| fresh(module));
        credentials
            .runner_routed_accounts
            .retain(|module, _| fresh(module));
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
    fn catalog_removal_drops_both_views_immediately() {
        let (mut cache, mut credentials, policies, _) = setup();
        cache.catalog(&json!([]), &policies, &mut credentials, 1);
        assert!(credentials.runner_service_tiers.is_empty());
        assert!(credentials.runner_routed_accounts.is_empty());
        assert!(cache.last_good.is_empty());
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
        assert!(lines[0].contains("runner role.describe poll failed"));
        assert!(lines[1].contains("runner role.describe poll answered again"));
        assert!(lines.iter().all(|line| line.contains("runner-a")));
    }
}
