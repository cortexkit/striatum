//! Sweep-private account evidence. The published snapshot supplies the initial
//! state; repairs change only this copy and survive the preview's later
//! evaluations at other demand levels (IQ, the request's reasoning-demand
//! score) and the eligibility checks for later panel seats.

use super::*;
use crate::account_state::{AccountState, Published};
use std::sync::{Arc, Mutex};
use striatum_store::DryRunRepair;

pub(crate) struct Sweep {
    pub(crate) initial: Arc<AccountState>,
    state: Mutex<AccountState>,
    #[cfg(test)]
    pub(super) oracle: Option<Arc<SqliteRouteStateStore>>,
}

impl Sweep {
    pub(crate) fn capture(store: &SqliteRouteStateStore) -> RouteStateResult<Self> {
        let initial = crate::account_state::snapshot(store)?;
        Ok(Self {
            state: Mutex::new((*initial).clone()),
            initial,
            #[cfg(test)]
            oracle: if std::env::var_os("STRIATUM_SWEEP_SQL_ORACLE").is_some() {
                Some(Arc::new(store.private_account_copy_for_test(
                    crate::account_selection::now_ms(),
                )?))
            } else {
                None
            },
        })
    }

    pub(crate) fn published(&self) -> Published<'_> {
        #[cfg(test)]
        if let Some(oracle) = &self.oracle {
            return Published::Sql(oracle);
        }
        Published::State(Arc::new(self.state.lock().unwrap().clone()))
    }

    /// The route-state snapshot this sweep judges every evaluation against:
    /// the captured route rows with their measurements (pressure, latency,
    /// success, last error) as captured, and this copy's block overlay. A
    /// route-state write committed during the sweep changes neither, so
    /// every demand level (IQ) and every panel seat of one preview is judged
    /// against one state.
    pub(crate) fn route_snapshot(
        &self,
        now: f64,
    ) -> RouteStateResult<striatum_core::model_router::ModelRouteStateSnapshot> {
        // Probe clocks are evaluated at the wall clock, as the live
        // route-state snapshot evaluates them.
        self.state
            .lock()
            .unwrap()
            .blocks
            .route_state_snapshot(now, crate::account_selection::now_ms())
    }

    /// Release this copy's expired admission probes and continuation offers
    /// at `now`, exactly as a live reader's view (`AccountState::as_of`) and
    /// the writer's commands release them: probe clocks reopened one
    /// generation on as of the admission's deadline, offers stamped at
    /// theirs.
    pub(super) fn expire(&self, now: i64) {
        let mut state = self.state.lock().unwrap();
        let released = std::sync::Arc::new(state.clone()).as_of(now);
        *state = (*released).clone();
    }

    pub(super) fn listing(
        &self,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
        now: i64,
    ) -> Result<Vec<striatum_core::route_state_store::CredentialEntitlementFence>, String> {
        use crate::broca_credentials::ListingLookup;
        let mut state = self.state.lock().unwrap();
        let credentials = credentials.lock().map_err(|e| e.to_string())?;
        let mut live = Vec::new();
        let mut changes = Vec::new();
        for mut fence in state.blocks.entitlement_fences() {
            let signed_out = fence.code == striatum_core::route_state_store::SIGNED_OUT_FENCE_CODE;
            let lookup = if signed_out {
                credentials.signed_out_fingerprint(&fence.credential_id)
            } else {
                credentials.listing_fingerprint(&fence.credential_id)
            };
            let current = match lookup {
                ListingLookup::Unknown => {
                    live.push(fence);
                    continue;
                }
                ListingLookup::Absent => None,
                ListingLookup::Listed(fp) => Some(fp),
            };
            let inactive = signed_out
                && credentials
                    .listed_credentials()
                    .iter()
                    .any(|row| row.id == fence.credential_id && row.state != "active");
            let keep = inactive
                || match (&fence.listing_fingerprint, &current) {
                    (Some(old), Some(new)) => old == new,
                    (None, Some(_)) => true,
                    _ => false,
                };
            if !keep {
                let prefix = crate::credential_fences::quota_hold_prefix(&fence);
                for hold in state
                    .blocks
                    .active_pool_fences(&fence.provider_id)
                    .map_err(|e| e.to_string())?
                {
                    if hold.evidence.fence_id.starts_with(&prefix) {
                        state
                            .blocks
                            .dry_run_repair(
                                DryRunRepair::ClearFence(
                                    hold.evidence.fence_id,
                                    "vault_listing_changed".into(),
                                ),
                                now,
                            )
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
            changes.push((
                fence.provider_id.clone(),
                fence.credential_id.clone(),
                keep.then(|| current.clone()).flatten(),
            ));
            if keep {
                fence.listing_fingerprint = current;
                live.push(fence);
            }
        }
        let listings = credentials.listing_version().map(|_| {
            credentials
                .listed_credentials()
                .iter()
                .filter_map(|row| match credentials.listing_fingerprint(&row.id) {
                    ListingLookup::Listed(fp) => Some((row.id.clone(), fp)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        });
        state.blocks.dry_run_listing(listings.as_deref(), &changes);
        Ok(live)
    }

    pub(super) fn clear_size(&self, fact: &striatum_store::AccountSizeRefusal) {
        self.state.lock().unwrap().blocks.dry_run_clear_size(fact);
    }
    pub(super) fn clear_routes(&self, provider: &str, credential: &str, at: i64) {
        self.state
            .lock()
            .unwrap()
            .blocks
            .dry_run_clear_routes(provider, credential, at);
    }
    pub(super) fn clear_route(&self, provider: &str, credential: &str, model: &str, at: i64) {
        self.state
            .lock()
            .unwrap()
            .blocks
            .dry_run_clear_route(provider, credential, model, at);
    }
    pub(super) fn reconcile_route(&self, fence: &striatum_store::AccountRouteFence, until: i64) {
        self.state
            .lock()
            .unwrap()
            .blocks
            .dry_run_reconcile_route(fence, until);
    }
    pub(super) fn pool_states(
        &self,
        provider: &str,
        _now: i64,
    ) -> RouteStateResult<BTreeMap<String, striatum_core::key_pool::PoolKeyState>> {
        let mut states = BTreeMap::<String, striatum_core::key_pool::PoolKeyState>::new();
        for block in self.published().fence_rechecks()? {
            if block.provider != provider {
                continue;
            }
            let until = block.due_at();
            let Some(credential) = block.credential_id else {
                continue;
            };
            match block.kind.as_str() {
                "pool_billing" => {
                    states.entry(credential).or_default().walled_until_ms = Some(until)
                }
                "pool_rate" => {
                    states.entry(credential).or_default().cooldown_until_ms = Some(until)
                }
                _ => {}
            }
        }
        Ok(states)
    }
}

pub(super) fn published<'a>(
    store: &'a SqliteRouteStateStore,
    sweep: Option<&'a Sweep>,
    now_ms: i64,
) -> RouteStateResult<Published<'a>> {
    match sweep {
        Some(sweep) => Ok(sweep.published()),
        None => crate::account_state::published(store, now_ms),
    }
}

impl BlockReads for Sweep {
    fn fence_rechecks(&self) -> RouteStateResult<Vec<striatum_store::FenceRecheck>> {
        self.published().fence_rechecks()
    }
    fn active_fences(
        &self,
        provider: &str,
    ) -> RouteStateResult<Vec<striatum_store::AccountFenceRow>> {
        self.published().active_fences(provider)
    }
    fn active_pool_fences(
        &self,
        provider: &str,
    ) -> RouteStateResult<Vec<striatum_store::AccountPoolFenceRow>> {
        self.published().active_pool_fences(provider)
    }
    fn route_fence_latest_reports(&self) -> RouteStateResult<BTreeMap<String, i64>> {
        self.published().route_fence_latest_reports()
    }
    fn report_route_fences(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<striatum_store::AccountRouteFence>> {
        self.published().report_route_fences(now)
    }
    fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        quota: bool,
    ) -> RouteStateResult<(bool, bool)> {
        self.published()
            .credential_quota_classification(provider, credential, quota)
    }
    fn admission_exclusions(
        &self,
        principal: &str,
        admission: &str,
    ) -> RouteStateResult<Vec<striatum_store::AccountExclusionRow>> {
        self.published().admission_exclusions(principal, admission)
    }
    fn writer(&self) -> Option<&AccountTx<'_>> {
        None
    }
    fn repair(&self, repair: DryRunRepair, now: i64) -> RouteStateResult<bool> {
        self.state
            .lock()
            .unwrap()
            .blocks
            .dry_run_repair(repair, now)
    }
}

#[cfg(test)]
type Hook = Box<dyn FnMut(&Sweep, bool, usize) + Send>;
#[cfg(test)]
static HOOKS: Mutex<Vec<(std::path::PathBuf, Hook)>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) struct HookGuard(std::path::PathBuf);
#[cfg(test)]
impl Drop for HookGuard {
    fn drop(&mut self) {
        HOOKS.lock().unwrap().retain(|(path, _)| *path != self.0);
    }
}

impl Sweep {
    #[cfg(test)]
    pub(crate) fn hook(
        path: &std::path::Path,
        hook: impl FnMut(&Sweep, bool, usize) + Send + 'static,
    ) -> HookGuard {
        HOOKS
            .lock()
            .unwrap()
            .push((path.to_owned(), Box::new(hook)));
        HookGuard(path.to_owned())
    }

    pub(crate) fn after_evaluation(&self, path: &std::path::Path, panel: bool, point: usize) {
        #[cfg(test)]
        {
            let mut hooks = HOOKS.lock().unwrap();
            if let Some((_, hook)) = hooks.iter_mut().find(|(key, _)| key == path) {
                hook(self, panel, point);
            }
        }
        #[cfg(not(test))]
        let _ = (path, panel, point);
    }
}
