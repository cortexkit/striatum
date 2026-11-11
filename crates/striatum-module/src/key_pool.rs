//! The key pool of API-key providers with no catalog rows of their own
//! (`anthropic-platform`): the routing operations core calls before every send
//! that starts a run.
//!
//! - `route.pool_acquire` picks a key for one session's next run, sticking to
//!   the session's prompt-cache workspace, and returns the launch target with
//!   that key's pin. A session whose live lease is still admissible keeps it.
//! - `route.pool_renew` keeps a live run's lease counting (and reinstates one
//!   that expired while its run was still live, on the same key).
//! - `route.pool_release` ends a run's lease, and optionally the session.
//! - `route.pool_wall` walls one key: out of credit, or a 429 with retry-after.
//!
//! A pool with no admissible key refuses `explicit_provider_pool_exhausted`
//! naming every key's reason. There is no fallback to the subscription logins.

use striatum_core::key_pool::{PoolBlockedKey, EXPLICIT_PROVIDER_POOL_EXHAUSTED};
use std::collections::BTreeMap;

use striatum_core::runner::RunnerModel;
use striatum_store::{
    PoolAcquire, PoolAcquireOutcome, PoolWallKind, SqliteRouteStateStore,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::broca_credentials::{SharedBrocaCredentials, EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED};

pub const POOL_ACQUIRE_OP: &str = "route.pool_acquire";
pub const POOL_RENEW_OP: &str = "route.pool_renew";
pub const POOL_RELEASE_OP: &str = "route.pool_release";
pub const POOL_WALL_OP: &str = "route.pool_wall";

/// A provider that is not a key-pool provider asked for a key.
pub const NOT_A_POOL_PROVIDER: &str = "not_a_pool_provider";

/// How long a lease counts as a run in flight without a renewal. Core renews
/// while the run lives; a lease from a core that died stops counting after this.
pub const LEASE_MS: i64 = 10 * 60_000;
/// How long an idle session keeps its workspace (the account-selection affinity
/// uses the same seven days).
pub const AFFINITY_IDLE_MS: i64 = 7 * 24 * 60 * 60_000;

pub fn handles(method: &str) -> bool {
    matches!(
        method,
        POOL_ACQUIRE_OP | POOL_RENEW_OP | POOL_RELEASE_OP | POOL_WALL_OP
    )
}

/// A typed refusal: the error frame's code, message and detail.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolRefusal {
    pub code: String,
    pub message: String,
    pub detail: Option<Value>,
}

fn refusal(code: &str, message: String) -> PoolRefusal {
    PoolRefusal {
        code: code.into(),
        message,
        detail: None,
    }
}

/// Whether `provider` is a key-pool provider: one with no catalog rows of its
/// own that borrows a catalog provider's rows through its credentials' `serves`
/// vendor (`catalog_targets`, from
/// [`crate::broca_credentials::BrocaCredentials::catalog_targets`]). Broca does
/// not know such a name; it is reached only as one of the vault keys mapped to
/// it, pinned on every send. Whether automatic selection may choose it is a
/// separate question, answered by `model_routing.exclude`.
pub fn is_pool_provider(catalog_targets: &BTreeMap<String, String>, provider: &str) -> bool {
    striatum_core::score_alias::alias_target(catalog_targets, provider).is_some()
}

/// One granted key and the launch target that pins it.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolGrant {
    pub lease_id: String,
    pub credential_id: String,
    pub workspace: String,
    pub valid_until_ms: i64,
    pub runner: RunnerModel,
}

impl PoolGrant {
    pub fn lease_json(&self) -> Value {
        json!({
            "leaseId": self.lease_id,
            "credentialId": self.credential_id,
            "workspace": self.workspace,
            "validUntilMs": self.valid_until_ms,
        })
    }
}

pub struct PoolCtx<'a> {
    pub config: Option<&'a crate::config_overrides::LiveConfig>,
    pub store: &'a SqliteRouteStateStore,
    pub credentials: &'a SharedBrocaCredentials,
    /// Each provider with no catalog rows that resolved to a catalog provider.
    pub catalog_targets: &'a BTreeMap<String, String>,
    pub now_ms: i64,
}

fn exhausted(provider: &str, blocked: &[PoolBlockedKey]) -> PoolRefusal {
    let keys = blocked
        .iter()
        .map(|block| match block.reset_at_ms {
            Some(at) => format!(
                "{} {} until {}",
                block.credential_id,
                block.reason,
                crate::account_selection::rfc3339(at)
            ),
            None => format!("{} {}", block.credential_id, block.reason),
        })
        .collect::<Vec<_>>();
    let earliest = blocked.iter().filter_map(|block| block.reset_at_ms).min();
    PoolRefusal {
        code: EXPLICIT_PROVIDER_POOL_EXHAUSTED.into(),
        message: format!(
            "no admissible key for {provider}: {}; the subscription logins are never used instead",
            keys.join("; ")
        ),
        detail: Some(json!({
            "provider": provider,
            "keys": blocked,
            "earliestResetAtMs": earliest,
        })),
    }
}

/// Lease a key of `provider` for the next run of `session` and build the launch
/// target that pins it.
pub fn acquire(
    ctx: &PoolCtx<'_>,
    provider: &str,
    model: &str,
    variant: Option<&str>,
    session: &str,
) -> Result<PoolGrant, PoolRefusal> {
    let keys = ctx
        .credentials
        .lock()
        .map_err(|_| {
            refusal(
                EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED,
                format!(
                    "no vault credential resolves provider {provider}: credential cache poisoned"
                ),
            )
        })?
        .pool_keys(provider, ctx.now_ms.max(0) as u64)
        .map_err(|(code, message)| refusal(&code, message));
    let Some(target) =
        striatum_core::score_alias::alias_target(ctx.catalog_targets, provider)
            .map(str::to_string)
    else {
        // Pool membership comes from the vault listing. When the listing is
        // unread, stale or maps no key to the provider, that is the refusal:
        // the provider may well resolve once the listing says so.
        keys?;
        return Err(refusal(
            NOT_A_POOL_PROVIDER,
            format!(
                "{provider} is not a key-pool provider: it has catalog rows of its own, or its credentials' serves vendors name no single catalog provider"
            ),
        ));
    };
    let keys = keys?;
    // A key that serves another provider than the one the provider borrows
    // rows from would price and route on one provider and send on another.
    if let Some(stray) = keys.iter().find(|key| {
        striatum_core::runner::RunnerAuthPin::from_credential_id(&key.credential_id)
            .is_none_or(|(served, _)| served != target)
    }) {
        return Err(refusal(
            EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED,
            format!(
                "provider {provider} maps to credential {}, which is not a method:{target}[:account] id; the provider borrows its rows from {target}",
                stray.credential_id
            ),
        ));
    }
    // A key the provider refused permanently (no entitlement, or no balance) is
    // fenced until its vault listing changes: only the pool's other keys are leased.
    let fences = crate::credential_fences::live(ctx.store, ctx.credentials)
        .map_err(|error| refusal("store_error", format!("key pool: {error}")))?;
    let clocks = ctx
        .store
        .fence_rechecks()
        .map_err(|error| refusal("store_error", error.to_string()))?;
    let classes = ctx
        .config
        .map(|config| {
            crate::account_selection::ClassContext::load(
                ctx.store,
                ctx.credentials,
                std::sync::Arc::new(config.clone()),
                crate::ACCOUNT_QUOTA.get(),
                ctx.now_ms,
            )
        })
        .transpose()
        .map_err(|error| refusal("store_error", error))?;
    let mut recovered = Vec::new();
    let accounts = classes
        .as_ref()
        .map(|classes| classes.probe_accounts())
        .unwrap_or_default();
    let (refused, keys): (Vec<_>, Vec<_>) = keys.into_iter().partition(|key| {
        if let Some(classes) = &classes {
            let account = accounts
                .get(&key.credential_id)
                .map_or(key.workspace.as_str(), String::as_str);
            if !classes.pool_fences_allow(&[provider, &target], model, account, &key.credential_id)
            {
                return true;
            }
            recovered.extend(classes.recovered_pool_origins(
                provider,
                model,
                account,
                &key.credential_id,
            ));
            recovered.extend(classes.recovered_pool_origins(
                &target,
                model,
                account,
                &key.credential_id,
            ));
            return false;
        }
        fences.iter().any(|fence| {
            fence.provider_id == provider
                && fence.credential_id == key.credential_id
                && clocks.iter().any(|clock| {
                    clock.kind == "credential"
                        && clock.provider == provider
                        && clock.credential_id.as_deref() == Some(&key.credential_id)
                        && clock.due_at() > ctx.now_ms
                })
        })
    });
    let refused: Vec<PoolBlockedKey> = refused
        .into_iter()
        .map(|key| PoolBlockedKey {
            reason: crate::credential_fences::rejection_reason(
                &fences,
                fences
                    .iter()
                    .find(|fence| {
                        fence.credential_id == key.credential_id
                            && (fence.provider_id == provider || fence.provider_id == target)
                    })
                    .map_or(provider, |fence| fence.provider_id.as_str()),
                std::slice::from_ref(&key.credential_id),
            )
            .into(),
            reset_at_ms: classes
                .as_ref()
                .and_then(|classes| {
                    classes.pool_recheck_hint(
                        &[provider, &target],
                        model,
                        accounts
                            .get(&key.credential_id)
                            .map_or(key.workspace.as_str(), String::as_str),
                        &key.credential_id,
                    )
                })
                .or_else(|| {
                    clocks
                        .iter()
                        .filter(|clock| {
                            [provider, target.as_str()].contains(&clock.provider.as_str())
                                && clock.applies(
                                    &clock.provider,
                                    &key.workspace,
                                    &key.credential_id,
                                    model,
                                )
                        })
                        .map(|clock| clock.due_at())
                        .filter(|at| *at > ctx.now_ms)
                        .max()
                }),
            credential_id: key.credential_id,
        })
        .collect();
    if keys.is_empty() && !refused.is_empty() {
        return Err(exhausted(provider, &refused));
    }
    let outcome = ctx
        .store
        .pool_acquire_for_route(
            &PoolAcquire {
                provider,
                session_id: session,
                keys: &keys,
                now_ms: ctx.now_ms,
                lease_ms: LEASE_MS,
                affinity_idle_ms: AFFINITY_IDLE_MS,
            },
            &[provider, &target],
            model,
            &recovered,
            &accounts,
        )
        .map_err(|error| refusal("store_error", format!("key pool: {error}")))?;
    let lease = match outcome {
        PoolAcquireOutcome::Leased(lease) => lease,
        PoolAcquireOutcome::Exhausted(mut blocked) => {
            blocked.extend(refused);
            return Err(exhausted(provider, &blocked));
        }
    };
    if let (Some(from), Some(reason)) = (&lease.moved_from, &lease.move_reason) {
        eprintln!(
            "[striatum] key pool {provider}: session {session} moved from workspace {from} to {} ({reason})",
            lease.workspace
        );
    }
    let mut runner = RunnerModel::pinned(&lease.credential_id, model, variant)
        .expect("pool keys were checked to parse as credential ids");
    runner.pool = Some(provider.to_string());
    runner.pool_session = Some(session.to_string());
    Ok(PoolGrant {
        lease_id: lease.lease_id,
        credential_id: lease.credential_id,
        workspace: lease.workspace,
        valid_until_ms: lease.valid_until_ms,
        runner,
    })
}

#[derive(Debug, Deserialize)]
struct Body<T> {
    params: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcquireParams {
    provider: String,
    model: String,
    #[serde(default)]
    variant: Option<String>,
    session: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenewParams {
    provider: String,
    session: String,
    lease_id: String,
    #[serde(default)]
    credential_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseParams {
    provider: String,
    session: String,
    #[serde(default)]
    lease_id: Option<String>,
    #[serde(default)]
    end_session: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WallParams {
    provider: String,
    credential_id: String,
    /// `billing_exhausted` or `rate_limited`.
    kind: String,
    until_ms: i64,
    #[serde(default)]
    reason: Option<String>,
}

fn decode<T: for<'de> Deserialize<'de>>(method: &str, body: &[u8]) -> Result<T, PoolRefusal> {
    serde_json::from_slice::<Body<T>>(body)
        .map(|body| body.params)
        .map_err(|error| {
            refusal(
                "invalid_request",
                format!("{method} params not decodable: {error}"),
            )
        })
}

/// Serve one pool operation: the `result` value, or a typed refusal.
pub fn handle(method: &str, body: &[u8], ctx: &PoolCtx<'_>) -> Result<Value, PoolRefusal> {
    match method {
        POOL_ACQUIRE_OP => {
            let params: AcquireParams = decode(method, body)?;
            let grant = acquire(
                ctx,
                &params.provider,
                &params.model,
                params.variant.as_deref(),
                &params.session,
            )?;
            let mut result = grant.lease_json();
            result["runner"] = json!(grant.runner);
            Ok(result)
        }
        POOL_RENEW_OP => {
            let params: RenewParams = decode(method, body)?;
            let renewed = ctx
                .store
                .pool_renew(
                    &params.provider,
                    &params.session,
                    &params.lease_id,
                    params.credential_id.as_deref(),
                    ctx.now_ms,
                    LEASE_MS,
                )
                .map_err(|error| refusal("store_error", format!("key pool: {error}")))?;
            Ok(json!({"live": renewed.is_some(), "validUntilMs": renewed}))
        }
        POOL_RELEASE_OP => {
            let params: ReleaseParams = decode(method, body)?;
            let released = ctx
                .store
                .pool_release(
                    &params.provider,
                    &params.session,
                    params.lease_id.as_deref(),
                    params.end_session,
                    ctx.now_ms,
                )
                .map_err(|error| refusal("store_error", format!("key pool: {error}")))?;
            Ok(json!({"released": released}))
        }
        POOL_WALL_OP => {
            let params: WallParams = decode(method, body)?;
            let kind = match params.kind.as_str() {
                striatum_core::key_pool::POOL_KEY_BILLING_EXHAUSTED => {
                    PoolWallKind::Billing
                }
                striatum_core::key_pool::POOL_KEY_RATE_LIMITED => {
                    PoolWallKind::RateLimited
                }
                other => {
                    return Err(refusal(
                        "invalid_request",
                        format!(
                        "{method} kind must be billing_exhausted or rate_limited, got {other:?}"
                    ),
                    ))
                }
            };
            ctx.store
                .pool_wall(
                    &params.provider,
                    &params.credential_id,
                    kind,
                    params.until_ms,
                    params.reason.as_deref().unwrap_or(&params.kind),
                    ctx.now_ms,
                )
                .map_err(|error| refusal("store_error", format!("key pool: {error}")))?;
            Ok(json!({"ok": true}))
        }
        other => Err(refusal(
            "unknown_method",
            format!("unknown method '{other}'"),
        )),
    }
}
