//! The private account view that retry diagnostics read when every account is
//! temporarily ineligible.
//!
//! A quota recheck can remove every account from eligibility for a short
//! time. To still name the account reset a caller could wait for, diagnostics
//! read a copy of the class context with those quota refusals and quota
//! rechecks removed. Building that copy clones the whole context, so a
//! refusal that inspects many catalog routes builds it once and reads every
//! route from the same copy.

use super::*;

// Tests count view constructions rather than inferring avoided work from timing.
// Route handlers run on a blocking pool thread, so the count is kept per
// credential vault (each test harness has its own) instead of per thread.
#[cfg(test)]
static RECOVERY_VIEW_BUILDS: std::sync::Mutex<BTreeMap<usize, u64>> =
    std::sync::Mutex::new(BTreeMap::new());

/// How many recovery views contexts reading this credential vault have built.
#[cfg(test)]
pub(crate) fn recovery_view_builds(credentials: &crate::SharedBrocaCredentials) -> u64 {
    let key = std::sync::Arc::as_ptr(credentials) as usize;
    *RECOVERY_VIEW_BUILDS.lock().unwrap().get(&key).unwrap_or(&0)
}

/// A copy of a class context with temporary quota exclusions lifted.
///
/// Permanent credential and policy failures remain in force; the view is used
/// only for retry diagnostics, never for admission.
pub(crate) struct RecoveryView {
    recovered: ClassContext,
    keep_free: f64,
    /// The allocator policy per provider with the keep-free margin applied,
    /// so routes of the same provider share one copy.
    policies: std::cell::RefCell<std::collections::HashMap<String, AllocatorPolicy>>,
}

impl ClassContext {
    /// Build the recovery view once; read each route with
    /// [`RecoveryView::account_recovery`].
    ///
    /// The settings keep-free margin for workers (`keep_free_percent`) can
    /// exceed the allocator's own reserve. The view applies it only to its
    /// own probes, so a weekly reset that frees that margin is diagnosed
    /// without changing account admission policy.
    pub(crate) fn recovery_view(&self, keep_free: f64) -> RecoveryView {
        #[cfg(test)]
        {
            *RECOVERY_VIEW_BUILDS
                .lock()
                .unwrap()
                .entry(self.probe_scope.as_ptr() as usize)
                .or_default() += 1;
        }
        let mut recovered = self.clone();
        // A credential refusal whose quota recheck is pending is the
        // temporary exclusion this view lifts; other refusals stay.
        recovered.replace_refusals(
            self.refusals
                .iter()
                .filter(|refusal| {
                    !self.rechecks.iter().any(|origin| {
                        origin.quota
                            && origin.kind == "credential"
                            && origin.provider == refusal.provider_id
                            && origin.credential_id.as_deref()
                                == Some(refusal.credential_id.as_str())
                    })
                })
                .cloned()
                .collect(),
        );
        recovered.replace_rechecks(
            self.rechecks
                .iter()
                .filter(|origin| !origin.quota)
                .cloned()
                .collect(),
        );
        RecoveryView {
            recovered,
            keep_free,
            policies: Default::default(),
        }
    }
}

impl RecoveryView {
    /// The dated account block for one route in this view, if its accounts
    /// are exhausted or reserve-limited.
    pub(crate) fn account_recovery(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Option<AccountBlock> {
        let mut policies = self.policies.borrow_mut();
        let policy = policies.entry(provider.to_owned()).or_insert_with(|| {
            let mut policy = self.recovered.config.allocator_policy_ref(provider).clone();
            policy.reserve_seven_day = policy.reserve_seven_day.max(self.keep_free);
            policy
        });
        self.recovered
            .blocked_account_with_policy(provider, model, priced, policy)
    }
}
