//! Account selection over the wire: `route.account_for` and the runner reports
//! that follow it (`route.account_observe`, `route.account_release`).
//! `route.account_status` reads quota headroom for cache warming without
//! admissions, decisions, store access or polling.
//!
//! Specification: `docs/designs/account-selection.md` ("Proposed protocol",
//! "Affinity, persistence and freeze semantics", "Operator pins and rollout").
//! The allocator is the pure `striatum_core::accounts::decide`;
//! this module builds its inputs (the roster joined from the vault listing and
//! Insula readings, and the stored affinity, fences, exclusions and demand),
//! commits its result in one store transaction and renders the reply.
//!
//! Account selection always serves. Every decision routing records is one it answered a
//! caller with (`mode = 'serve'`).

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::accounts::eligibility::{assess_reading, reserve_for};
use striatum_core::accounts::evidence::{
    confirm_exhaustion, header_window, Carrier, ConfirmationOutcome, Derivation, EvidenceScope,
    ExhaustionObservation, FrameOrigin, ObservationKind, ResetSource, WindowEvidence,
};
use striatum_core::accounts::{
    block_end, grown_recovery_set, recovery_set_from, validate_pool_name, AccountRef,
    AccountRequest, AccountStrategy, AccountView, AdmissionDemand, AdmissionState, AffinityOutcome,
    AllocatorPolicy, AllocatorState, ApplicableWindow, CallerKind, CandidateRef,
    ConfirmationSource, ConversationAffinity, ConversationKeySource, CredentialRecord, Decision,
    EpochMs, Fence, GeneralWindow, ModelFamily, ModelReach, ObservationSource, Outcome,
    PoolWindowReading, PrincipalExclusion, QuotaReading, QuotaWindow, RefusalCode, ResetTime,
    Roster, RunnerCandidates, ScopedList, ScopedWindow, StaleDisclosure,
};
use striatum_core::quota::RateWindow;
use striatum_core::{RouteStateResult, RouteStateStoreError};
use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord, AccountExclusionRow,
    AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow, AccountPoolFenceRow,
    AccountTx, SqliteRouteStateStore, DECISION_MODE_SERVE,
};

use crate::account_quota::{self, AccountEntry, AccountQuotaState, SharedAccountQuota};
use crate::account_refusal_log::{because, checking};
use crate::account_state::{AccountCommand, BlockReads};
use crate::config_overrides::AccountSelectionFlags;

/// Choose the account for one admission, renewal or reassignment.
pub const ACCOUNT_FOR_OP: &str = "route.account_for";
/// Read whether an account's quota has room for prompt-cache warming.
pub const ACCOUNT_STATUS_OP: &str = "route.account_status";
/// Report account-bound admission, dispatch and failure evidence.
pub const ACCOUNT_OBSERVE_OP: &str = "route.account_observe";
/// Release an admission that never started, or end a conversation's lease.
pub const ACCOUNT_RELEASE_OP: &str = "route.account_release";

/// The refusal every account operation answers while serving is off. This
/// code is intentionally preserved verbatim: it means the operator switched
/// account selection off for the whole module (the serving flag), and Broca
/// falls back to its static credential on it, so it must stay
/// distinguishable from quota or auth refusals. The contract declares it
/// (0.5.2) as untimed with empty detail, module-wide.
pub const DISABLED_CODE: &str = "account_selection_disabled";

/// A reservation that has not started within this long expires.
const ADMISSION_VALID_MS: i64 = 60_000;
/// How long a caller may dispatch on a pin without renewing it.
const LEASE_MS: i64 = 2 * 60_000;
/// Conversation affinity lasts this long idle.
const AFFINITY_IDLE_MS: i64 = 7 * 24 * 60 * 60_000;
/// Accepted use renews affinity at most this often.
const AFFINITY_TOUCH_MS: i64 = 60_000;
/// The longest hold a caller waits out when it does not say.
pub(crate) const DEFAULT_HOLD_WAIT_S: u64 = 900;
/// The principal a worker-launch preflight decides under. No caller can hold
/// it, so a preflight never reads or touches a real conversation.
const PREFLIGHT_PRINCIPAL: &str = "routing:preflight";

// ---------------------------------------------------------------------------
// Replies
// ---------------------------------------------------------------------------

/// What an account operation answers: a result, or a typed refusal whose
/// `detail` object repeats the refusal `code` and carries `retry_after_s`,
/// `decision_id`, the per-account `exclusions` and any fields specific to
/// that code (the error shape in the design's "Proposed protocol").
#[derive(Debug, Clone, PartialEq)]
pub enum OpReply {
    Result(Value),
    Error {
        code: String,
        message: String,
        detail: Option<Value>,
    },
}

impl OpReply {
    fn error(code: &str, message: impl Into<String>) -> Self {
        OpReply::Error {
            code: code.to_string(),
            message: message.into(),
            detail: None,
        }
    }

    pub(crate) fn typed(code: &str, message: impl Into<String>, detail: Value) -> Self {
        OpReply::Error {
            code: code.to_string(),
            message: message.into(),
            detail: Some(detail),
        }
    }

    #[cfg(test)]
    pub fn code(&self) -> Option<&str> {
        match self {
            OpReply::Result(_) => None,
            OpReply::Error { code, .. } => Some(code),
        }
    }
}

fn invalid(message: impl Into<String>) -> OpReply {
    OpReply::error("invalid_request", message)
}

/// Whether `method` is one of the account-selection operations.
pub fn handles(method: &str) -> bool {
    matches!(
        method,
        ACCOUNT_FOR_OP | ACCOUNT_STATUS_OP | ACCOUNT_OBSERVE_OP | ACCOUNT_RELEASE_OP
    )
}

/// The label a route's daemon-attested principal is recorded under. Only this
/// namespaces admissions, affinity and mutation authority; the request's
/// `caller` is audit metadata.
pub fn principal_label(principal: Option<&subc_protocol::Principal>) -> String {
    match principal {
        Some(subc_protocol::Principal::Reserved { module_id }) => format!("reserved:{module_id}"),
        Some(subc_protocol::Principal::Direct) => "direct".to_string(),
        Some(subc_protocol::Principal::Unverified) => "unverified".to_string(),
        None => "unattested".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Everything one operation reads besides its request.
pub struct Ctx<'a> {
    pub store: &'a SqliteRouteStateStore,
    /// The live per-account quota state and vault roster; `None` before boot
    /// restored it.
    pub accounts: Option<&'a SharedAccountQuota>,
    pub flags: AccountSelectionFlags,
    pub policy: &'a AllocatorPolicy,
    pub now_ms: i64,
    pub classes: Option<ClassContext>,
}

impl Ctx<'_> {
    fn command_context(&self) -> CommandContext {
        let concurrency_caps = self
            .classes
            .as_ref()
            .map(|classes| {
                let mut caps = classes.credentials.concurrency_caps.clone();
                // A config reload can precede the next credential publication.
                // Do not change the credential snapshot: its contents key the
                // serving-index cache, which observing caps must not invalidate.
                caps.publish_settings(classes.config.concurrency_caps());
                caps.resolved
            })
            .unwrap_or_default();
        CommandContext {
            flags: self.flags.clone(),
            policy: self.policy.clone(),
            now_ms: self.now_ms,
            classes: self.classes.clone(),
            concurrency_caps,
        }
    }

    fn provider_policy(&self, provider: &str) -> AllocatorPolicy {
        self.classes.as_ref().map_or_else(
            || self.policy.clone(),
            |classes| classes.config.allocator_policy(provider),
        )
    }
}

/// The immutable request inputs a command may use while holding the writer.
/// It deliberately holds no store handle, preventing nested writer acquisition.
#[derive(Clone)]
struct CommandContext {
    flags: AccountSelectionFlags,
    policy: AllocatorPolicy,
    now_ms: i64,
    classes: Option<ClassContext>,
    concurrency_caps: crate::concurrency_caps::ResolvedCaps,
}

impl CommandContext {
    fn provider_policy(&self, provider: &str) -> AllocatorPolicy {
        self.classes.as_ref().map_or_else(
            || self.policy.clone(),
            |classes| classes.config.allocator_policy(provider),
        )
    }
}

#[cfg(test)]
#[path = "account_selection/block_parity_tests.rs"]
mod block_parity_tests;

mod fact_index;
mod probe_reservations;
mod quota_route;
pub(crate) mod recovery_view;
mod retry_hints;

fn store_error(error: RouteStateStoreError) -> OpReply {
    OpReply::typed(
        "account_selection_unavailable",
        format!(
            "routing could not read or commit account state: {}",
            error.0
        ),
        json!({
            "code": "account_selection_unavailable",
            "retry_after_s": null,
            "decision_id": null,
            "exclusions": [],
        }),
    )
}

// ---------------------------------------------------------------------------
// Time, ids and hashing
// ---------------------------------------------------------------------------

/// UTC RFC 3339 with whole seconds for a Unix-millisecond instant.
pub fn rfc3339(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, mnt, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mnt:02}:{s:02}Z")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A process-random value, so ids and the salt differ between processes that
/// start at the same instant.
fn process_entropy() -> u64 {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    hasher.finish()
}

fn unique_digest(parts: &[&str]) -> String {
    #[cfg(test)]
    if let Some(inputs) = striatum_store::replay::current() {
        let mut hasher = Sha256::new();
        hasher.update(inputs.next_id().as_bytes());
        for part in parts {
            hasher.update(part.as_bytes());
            hasher.update([0]);
        }
        return hex(&hasher.finalize());
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let mut hasher = Sha256::new();
    hasher.update(nanos.to_le_bytes());
    hasher.update(NEXT_ID.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hasher.update(process_entropy().to_le_bytes());
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", &unique_digest(&[prefix])[..24])
}

fn fresh_salt() -> String {
    unique_digest(&["account-selection-salt"])
}

/// The stored form of a conversation key: salted and namespaced by principal,
/// so no row carries the caller's raw key and two principals never share one.
fn conversation_hash(salt: &str, principal: &str, conversation_key: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [salt, principal, conversation_key] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

pub(crate) fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn wire<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn wire_str<T: Serialize>(value: &T) -> String {
    match wire(value) {
        Value::String(text) => text,
        other => other.to_string(),
    }
}

fn decode_params(body: &[u8]) -> Result<Value, OpReply> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| invalid(format!("request body is not JSON: {error}")))?;
    Ok(value.get("params").cloned().unwrap_or(Value::Null))
}

/// Handle one account-selection operation. `body` is the whole request
/// (`{"method", "params"}`); `principal` is the route's attested principal.
#[cfg(test)]
pub fn handle(method: &str, body: &[u8], principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let params = decode_params(body).unwrap_or(Value::Null);
    let call = (method == ACCOUNT_FOR_OP).then(|| {
        call_log::AccountCall::legacy(
            &params,
            principal,
            ctx.classes.as_ref().map(|classes| classes.config.as_ref()),
            ctx.classes.as_ref().map(|classes| &classes.credentials),
            ctx.store,
        )
    });
    let reply = handle_with_call(method, body, principal, ctx, call.clone());
    if let Some(call) = call {
        call.finish();
    }
    reply
}

pub(crate) fn handle_with_call(
    method: &str,
    body: &[u8],
    principal: &str,
    ctx: &Ctx<'_>,
    call: Option<call_log::AccountCall>,
) -> OpReply {
    checking("account_selection_enabled");
    // This query bypasses the serving transactions and answers even while
    // serving is off. A warming check never reserves or charges quota.
    if method == ACCOUNT_STATUS_OP {
        return match decode_params(body) {
            Ok(params) => account_status(&params, ctx),
            Err(reply) => reply,
        };
    }
    if !ctx.flags.enabled {
        return OpReply::typed(
            DISABLED_CODE,
            format!("{method} is not served: account selection is disabled in routing policy"),
            json!({ "code": DISABLED_CODE }),
        );
    }
    checking("request_decode");
    let params = match decode_params(body) {
        Ok(params) => params,
        Err(reply) => return reply,
    };
    match method {
        ACCOUNT_FOR_OP => account_for(&params, principal, ctx, call),
        ACCOUNT_OBSERVE_OP => observe(&params, principal, ctx),
        ACCOUNT_RELEASE_OP => release(&params, principal, ctx),
        other => OpReply::error("unknown_method", format!("unknown method '{other}'")),
    }
}

mod continuation;
#[cfg(test)]
pub(crate) use continuation::continue_account;
pub(crate) use continuation::continue_account_with_call;
#[cfg(test)]
pub(crate) use continuation::inject_continuation_preparation_failure;
pub(crate) mod call_log;
mod concurrency_gate;
mod fork;

pub fn now_ms() -> i64 {
    crate::now_ms() as i64
}

mod dry_run;
pub(crate) use dry_run::Sweep;

pub(crate) mod class_context;
use class_context::*;
pub(crate) mod roster;
use roster::*;
pub(crate) mod block_recovery;
use block_recovery::*;
pub(crate) mod admission;
use admission::*;
mod admission_refusals;
use admission_refusals::*;
mod bound_tier;
use bound_tier::*;
pub(crate) mod status;
use status::*;
pub(crate) mod observations;
use observations::*;
#[cfg(test)]
mod tests;

pub(crate) use class_context::{ClassContext, ProbeBinding};
