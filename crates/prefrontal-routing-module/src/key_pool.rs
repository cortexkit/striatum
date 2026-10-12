//! The key pool of explicit-only API-key providers (`anthropic-platform`): the
//! routing operations core calls before every send that starts a run.
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

use prefrontal_routing_core::key_pool::{PoolBlockedKey, EXPLICIT_PROVIDER_POOL_EXHAUSTED};
use prefrontal_routing_core::runner::RunnerModel;
use prefrontal_routing_core::ModelRoutingPolicyInput;
use prefrontal_routing_store::{
    PoolAcquire, PoolAcquireOutcome, PoolWallKind, SqliteRouteStateStore,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::broca_credentials::{SharedBrocaCredentials, EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED};

pub const POOL_ACQUIRE_OP: &str = "route.pool_acquire";
pub const POOL_RENEW_OP: &str = "route.pool_renew";
pub const POOL_RELEASE_OP: &str = "route.pool_release";
pub const POOL_WALL_OP: &str = "route.pool_wall";

/// A provider that is not an explicit-only pool provider asked for a key.
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

/// Whether `provider` is an explicit-only pool provider: listed in
/// `explicit_only_providers` and borrowing another provider's rows through
/// `score_alias`. Broca does not know such a name; it is reached only as one of
/// the vault keys mapped to it, pinned on every send.
pub fn is_pool_provider(policy: &ModelRoutingPolicyInput, provider: &str) -> bool {
    policy
        .explicit_only_providers
        .iter()
        .any(|entry| entry == provider)
        && prefrontal_routing_core::score_alias::alias_target(&policy.score_alias, provider)
            .is_some()
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
    pub store: &'a SqliteRouteStateStore,
    pub credentials: &'a SharedBrocaCredentials,
    pub policy: &'a ModelRoutingPolicyInput,
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
    if !is_pool_provider(ctx.policy, provider) {
        return Err(refusal(
            NOT_A_POOL_PROVIDER,
            format!("{provider} is not an explicit-only key-pool provider"),
        ));
    }
    let target =
        prefrontal_routing_core::score_alias::alias_target(&ctx.policy.score_alias, provider)
            .unwrap_or_default()
            .to_string();
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
        .map_err(|(code, message)| refusal(&code, message))?;
    // A key that serves another provider than the one the alias borrows rows
    // from would price and route on one provider and send on another.
    if let Some(stray) = keys.iter().find(|key| {
        prefrontal_routing_core::runner::RunnerAuthPin::from_credential_id(&key.credential_id)
            .is_none_or(|(served, _)| served != target)
    }) {
        return Err(refusal(
            EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED,
            format!(
                "provider {provider} maps to credential {}, which is not a method:{target}[:account] id; model_routing.score_alias borrows its rows from {target}",
                stray.credential_id
            ),
        ));
    }
    let outcome = ctx
        .store
        .pool_acquire(&PoolAcquire {
            provider,
            session_id: session,
            keys: &keys,
            now_ms: ctx.now_ms,
            lease_ms: LEASE_MS,
            affinity_idle_ms: AFFINITY_IDLE_MS,
        })
        .map_err(|error| refusal("store_error", format!("key pool: {error}")))?;
    let lease = match outcome {
        PoolAcquireOutcome::Leased(lease) => lease,
        PoolAcquireOutcome::Exhausted(blocked) => return Err(exhausted(provider, &blocked)),
    };
    if let (Some(from), Some(reason)) = (&lease.moved_from, &lease.move_reason) {
        eprintln!(
            "[prefrontal-routing] key pool {provider}: session {session} moved from workspace {from} to {} ({reason})",
            lease.workspace
        );
    }
    let mut runner = RunnerModel::pinned(&lease.credential_id, model, variant)
        .expect("pool keys were checked to parse as credential ids");
    runner.pool = Some(provider.to_string());
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
                prefrontal_routing_core::key_pool::POOL_KEY_BILLING_EXHAUSTED => {
                    PoolWallKind::Billing
                }
                prefrontal_routing_core::key_pool::POOL_KEY_RATE_LIMITED => {
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
