use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::broca_credentials::{BrocaCredentials, SharedBrocaCredentials};

const TTL_MS: i64 = 120_000;

struct Reservation {
    scope: Weak<Mutex<BrocaCredentials>>,
    provider: String,
    credential: String,
    owner: String,
    selection_owner: String,
    expires_at: i64,
}

fn reservations() -> &'static Mutex<Vec<Reservation>> {
    static RESERVATIONS: OnceLock<Mutex<Vec<Reservation>>> = OnceLock::new();
    RESERVATIONS.get_or_init(Default::default)
}

fn live(now: i64) -> std::sync::MutexGuard<'static, Vec<Reservation>> {
    let mut reservations = reservations()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    reservations.retain(|entry| entry.expires_at > now && entry.scope.strong_count() > 0);
    reservations
}

pub(super) fn blocked_until(
    scope: &Weak<Mutex<BrocaCredentials>>,
    provider: &str,
    credential: &str,
    owner: Option<&str>,
    now: i64,
) -> Option<i64> {
    live(now)
        .iter()
        .find(|entry| {
            entry.scope.ptr_eq(scope)
                && entry.provider == provider
                && entry.credential == credential
                && Some(entry.owner.as_str()) != owner
                && Some(entry.selection_owner.as_str()) != owner
        })
        .map(|entry| entry.expires_at)
}

/// Selection holds prospective probes while scoring, then binds only the
/// chosen credential to its decision. Dropping the scoring view releases the
/// other holds, including all holds when selection fails.
pub(super) struct Selection {
    scope: Weak<Mutex<BrocaCredentials>>,
    pub owner: String,
}

impl Selection {
    pub fn new(
        credentials: &SharedBrocaCredentials,
        probes: Vec<(String, String)>,
        now: i64,
    ) -> Self {
        let scope = Arc::downgrade(credentials);
        let owner = super::new_id("probe");
        let mut reservations = live(now);
        for (provider, credential) in probes {
            if !reservations.iter().any(|entry| {
                entry.scope.ptr_eq(&scope)
                    && entry.provider == provider
                    && entry.credential == credential
            }) {
                reservations.push(Reservation {
                    scope: scope.clone(),
                    provider,
                    credential,
                    owner: owner.clone(),
                    selection_owner: owner.clone(),
                    expires_at: now.saturating_add(TTL_MS),
                });
            }
        }
        Self { scope, owner }
    }

    pub fn bind(&self, provider: &str, credential: &str, decision: &str, now: i64) {
        if let Some(entry) = live(now).iter_mut().find(|entry| {
            entry.scope.ptr_eq(&self.scope)
                && entry.provider == provider
                && entry.credential == credential
                && entry.owner == self.owner
        }) {
            entry.owner = decision.to_owned();
            entry.expires_at = now.saturating_add(TTL_MS);
        }
    }
}

impl Drop for Selection {
    fn drop(&mut self) {
        reservations()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|entry| !entry.scope.ptr_eq(&self.scope) || entry.owner != self.owner);
    }
}

pub(super) fn claim(
    scope: &Weak<Mutex<BrocaCredentials>>,
    provider: &str,
    credential: &str,
    owner: Option<&str>,
    now: i64,
    claim: impl FnOnce() -> prefrontal_routing_core::route_state_store::RouteStateResult<
        prefrontal_routing_store::CredentialProbeClaim,
    >,
) -> prefrontal_routing_core::route_state_store::RouteStateResult<
    Option<prefrontal_routing_store::CredentialProbeClaim>,
> {
    let mut reservations = live(now);
    if reservations.iter().any(|entry| {
        entry.scope.ptr_eq(scope)
            && entry.provider == provider
            && entry.credential == credential
            && Some(entry.owner.as_str()) != owner
            && Some(entry.selection_owner.as_str()) != owner
    }) {
        return Ok(None);
    }
    let claimed = claim()?;
    if matches!(
        claimed,
        prefrontal_routing_store::CredentialProbeClaim::Claimed
    ) {
        reservations.retain(|entry| {
            !entry.scope.ptr_eq(scope)
                || entry.provider != provider
                || entry.credential != credential
        });
    }
    Ok(Some(claimed))
}
