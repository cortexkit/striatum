//! Account selection over the wire: `route.account_for`, the runner reports
//! that follow it (`route.account_observe`, `route.account_release`), shadow
//! decisions and their readback (`route.account_shadow_status`).
//!
//! Specification: `docs/designs/account-selection.md` ("Proposed protocol",
//! "Affinity, persistence and freeze semantics", "Operator pins and rollout"
//! step 3). The allocator is the pure `prefrontal_routing_core::accounts::decide`;
//! this module builds its inputs (the roster joined from the vault listing and
//! Insula readings, and the stored affinity, fences, exclusions and demand),
//! commits its result in one store transaction and renders the reply.
//!
//! Two flags in the routing file gate everything (`account_selection.enabled`
//! and `account_selection.shadow`), both off by default:
//! * With `enabled` off, every account operation refuses with
//!   `account_selection_disabled` and writes nothing to the serving tables.
//! * With `shadow` on, routing runs the allocator on a timer (a synthetic new
//!   cold session per model family), on each `route.account_for` it would
//!   have been asked while serving is off, and on each observation report. It
//!   records each decision and its explanation and answers nobody with it, and
//!   it charges no demand, issues no pin and changes no fence.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use prefrontal_routing_core::accounts::evidence::{
    confirm_exhaustion, header_window, Carrier, ConfirmationOutcome, Derivation, EvidenceScope,
    ExhaustionObservation, FrameOrigin, ObservationKind, ResetSource, WindowEvidence,
};
use prefrontal_routing_core::accounts::{
    decide, AccountRef, AccountRequest, AccountView, AdmissionDemand, AdmissionState,
    AffinityOutcome, AllocatorPolicy, AllocatorState, CallerKind, CandidateRef,
    ConversationAffinity, ConversationKeySource, CredentialRecord, Decision, Fence, GeneralWindow,
    ModelFamily, ModelReach, ObservationSource, Outcome, PrincipalExclusion, QuotaReading,
    QuotaWindow, RefusalCode, ResetTime, Roster, RunnerCandidates, ScopedList, ScopedWindow,
    StaleDisclosure,
};
use prefrontal_routing_core::quota::RateWindow;
use prefrontal_routing_core::{RouteStateResult, RouteStateStoreError};
use prefrontal_routing_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord, AccountExclusionRow,
    AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow, AccountTx,
    SqliteRouteStateStore, DECISION_MODE_SERVE, DECISION_MODE_SHADOW,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::account_quota::{
    self, AccountEntry, AccountQuotaState, ProviderAdapter, SharedAccountQuota, ACCOUNT_PROVIDERS,
};
use crate::config_overrides::AccountSelectionFlags;

/// Choose the account for one admission, renewal or reassignment.
pub const ACCOUNT_FOR_OP: &str = "route.account_for";
/// Report account-bound admission, dispatch and failure evidence.
pub const ACCOUNT_OBSERVE_OP: &str = "route.account_observe";
/// Release an admission that never started, or end a conversation's lease.
pub const ACCOUNT_RELEASE_OP: &str = "route.account_release";
/// Read the latest shadow decisions and why they came out as they did.
pub const ACCOUNT_SHADOW_STATUS_OP: &str = "route.account_shadow_status";

/// The refusal every account operation answers while serving is off.
pub const DISABLED_CODE: &str = "account_selection_disabled";

/// A reservation that has not started within this long expires.
const ADMISSION_VALID_MS: i64 = 60_000;
/// How long a caller may dispatch on a pin without renewing it.
const LEASE_MS: i64 = 2 * 60_000;
/// Conversation affinity lasts this long idle.
const AFFINITY_IDLE_MS: i64 = 7 * 24 * 60 * 60_000;
/// Accepted use renews affinity at most this often.
const AFFINITY_TOUCH_MS: i64 = 60_000;
/// The longest short hold a caller waits out when it does not say.
const DEFAULT_HOLD_WAIT_S: u64 = 900;
/// How often the shadow timer runs.
pub const SHADOW_INTERVAL: Duration = Duration::from_secs(60);
/// A timer shadow decision identical in outcome and exclusions to the last one
/// recorded for its provider and family is recorded again only after this
/// long, so an unchanged pool does not add three rows a minute forever.
const SHADOW_HEARTBEAT_MS: i64 = 15 * 60_000;
/// The principal shadow's synthetic requests are made under. No caller can
/// hold it, so synthetic decisions never touch a real conversation.
pub const SHADOW_PRINCIPAL: &str = "routing:shadow";
/// Readback returns at most this many shadow decisions.
const SHADOW_STATUS_MAX: usize = 200;

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

    fn typed(code: &str, message: impl Into<String>, detail: Value) -> Self {
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
        ACCOUNT_FOR_OP | ACCOUNT_OBSERVE_OP | ACCOUNT_RELEASE_OP | ACCOUNT_SHADOW_STATUS_OP
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
}

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

fn fingerprint(params: &Value) -> String {
    hex(&Sha256::digest(params.to_string().as_bytes()))
}

fn normalize(value: &str) -> String {
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

// ---------------------------------------------------------------------------
// Roster: vault identities joined with Insula readings
// ---------------------------------------------------------------------------

/// The allocator's roster for one provider plus what routing knows about its
/// gaps, for the explanation.
#[derive(Debug, Clone, PartialEq)]
pub struct RosterBuild {
    /// `None` when routing has never listed the vault (and has no persisted
    /// listing): new cold admissions are then unavailable.
    pub roster: Option<Roster>,
    /// Roster facts for the record: the listing's digest and age, whether it
    /// is past the 24-hour limit, the join gaps in both directions and the
    /// alarms. Emails are left out.
    pub context: Value,
}

fn adapter_for_provider(provider: &str) -> Option<&'static ProviderAdapter> {
    ACCOUNT_PROVIDERS
        .iter()
        .find(|adapter| adapter.routing_provider == provider)
}

fn is_default_record(credential_id: &str) -> bool {
    credential_id.split(':').count() == 2
}

fn reset_time(text: Option<&str>) -> ResetTime {
    match text {
        None => ResetTime::Missing,
        Some(text) => {
            account_quota::parse_rfc3339_ms(text).map_or(ResetTime::Unparsable, ResetTime::At)
        }
    }
}

fn quota_window(window: &RateWindow) -> Option<QuotaWindow> {
    let minutes = u32::try_from(window.window_minutes?).ok()?;
    Some(QuotaWindow {
        used_percent: window.used_percent,
        // Insula's `rawUsedPercent` is the poll's percent before the account's
        // saved (banked) resets are applied, not the unrounded utilization an
        // upstream response header carries. Only that header value may
        // confirm exhaustion, so a poll reading carries none.
        raw_utilization: None,
        resets_at: reset_time(window.resets_at.as_deref()),
        window_minutes: minutes,
        source: ObservationSource::Poll,
    })
}

/// One stored Insula entry as the allocator's reading. General windows are
/// found by `windowMinutes`, never slot name; scoped windows keep their
/// producer id and length.
pub fn quota_reading(entry: &Value) -> Option<QuotaReading> {
    let entry: AccountEntry = serde_json::from_value(entry.clone()).ok()?;
    let usage = entry.usage.as_ref();
    let general = |minutes: u32| {
        usage.and_then(|usage| {
            [&usage.primary, &usage.secondary, &usage.tertiary]
                .into_iter()
                .flatten()
                .filter_map(quota_window)
                .find(|window| window.window_minutes == minutes)
        })
    };
    let scoped = match usage.and_then(|usage| usage.extra_rate_windows.as_ref()) {
        None => ScopedList::Missing,
        Some(windows) => ScopedList::Listed(
            windows
                .iter()
                .filter_map(|extra| {
                    let window = extra.window.as_ref()?;
                    Some(ScopedWindow {
                        scope_id: extra.id.clone().unwrap_or_default(),
                        window_minutes: window
                            .window_minutes
                            .and_then(|minutes| u32::try_from(minutes).ok())
                            .unwrap_or(0),
                        used_percent: window.used_percent,
                        resets_at: reset_time(window.resets_at.as_deref()),
                    })
                })
                .collect(),
        ),
    };
    Some(QuotaReading {
        observed_at_ms: entry
            .fetched_at
            .as_deref()
            .and_then(account_quota::parse_rfc3339_ms),
        // An error without a class is still a degraded entry.
        error_class: entry.error_class.clone().or_else(|| {
            entry
                .error
                .as_ref()
                .map(|_| "unclassified_error".to_string())
        }),
        stale: entry.stale.as_ref().map(|stale| StaleDisclosure {
            since_ms: stale
                .since
                .as_deref()
                .and_then(account_quota::parse_rfc3339_ms),
            class: stale.class.clone(),
        }),
        five_hour: general(GeneralWindow::FiveHour.window_minutes()),
        seven_day: general(GeneralWindow::SevenDay.window_minutes()),
        scoped,
    })
}

/// Join the vault roster with Insula readings for one routing provider.
///
/// Candidates are the vault's identities: an Insula account the vault does not
/// list is a gap, never a candidate. A listed identity with no reading, or a
/// tombstoned one, is a candidate with no reading (unknown for cold work, and
/// after 15 minutes an alarm). A record whose vault state is not `active`
/// needs re-authorization. The 24-hour roster limit is the allocator's: the
/// roster is passed through with its listing time and the allocator refuses
/// new cold work once it is too old, while existing affinity continues.
pub fn allocator_roster(state: &AccountQuotaState, provider: &str, now_ms: i64) -> RosterBuild {
    let Some(adapter) = adapter_for_provider(provider) else {
        return RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "tracked": false }),
        };
    };
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        now_ms,
    );
    let insula_only: Vec<Value> = report
        .gaps
        .insula_only
        .iter()
        .filter(|gap| gap.provider == adapter.insula_provider)
        .map(|gap| json!({ "account_id": gap.account_id, "reason": gap.reason }))
        .collect();
    let vault_only: Vec<Value> = report
        .gaps
        .vault_only
        .iter()
        .filter(|gap| gap.refresh_adapter == adapter.refresh_adapter)
        .map(|gap| {
            json!({
                "account_id": gap.account_id,
                "credential_ids": gap.credential_ids,
                "first_seen_ms": gap.first_seen_ms,
                "first_reading_overdue": gap.first_reading_overdue,
            })
        })
        .collect();
    let roster = state.roster.as_ref().map(|vault| Roster {
        view_digest: vault.view.clone(),
        fetched_at_ms: vault.fetched_at_ms,
        accounts: vault
            .identities
            .iter()
            .filter(|identity| identity.refresh_adapter == adapter.refresh_adapter)
            .map(|identity| AccountView {
                account_id: identity.account_id.clone(),
                refresh_adapter: identity.refresh_adapter.clone(),
                credentials: identity
                    .credential_ids
                    .iter()
                    .map(|credential_id| CredentialRecord {
                        credential_id: credential_id.clone(),
                        default_record: is_default_record(credential_id),
                        needs_reauth: !identity.active_credential_ids.contains(credential_id),
                    })
                    .collect(),
                model_reach: ModelReach::All,
                quota: state
                    .readings
                    .readings
                    .get(&(
                        adapter.insula_provider.to_string(),
                        identity.account_id.clone(),
                    ))
                    .filter(|reading| reading.tombstoned_at_ms.is_none())
                    .and_then(|reading| quota_reading(&reading.entry)),
            })
            .collect(),
    });
    let context = json!({
        "provider": provider,
        "tracked": true,
        "view_digest": state.roster.as_ref().map(|vault| vault.view.clone()),
        "fetched_at_ms": state.roster.as_ref().map(|vault| vault.fetched_at_ms),
        "past_max_age": state.roster.as_ref().is_some_and(|vault| {
            now_ms - vault.fetched_at_ms >= account_quota::ROSTER_MAX_AGE_MS
        }),
        "poll": report.roster.poll,
        "account_ids": roster
            .as_ref()
            .map(|roster| roster.accounts.iter().map(|view| view.account_id.clone()).collect::<Vec<_>>())
            .unwrap_or_default(),
        "gaps": { "insula_only": insula_only, "vault_only": vault_only },
        "alarms": report.alarms,
    });
    RosterBuild { roster, context }
}

fn roster_for(ctx: &Ctx<'_>, provider: &str) -> RosterBuild {
    match ctx.accounts.map(|state| state.lock()) {
        Some(Ok(guard)) => allocator_roster(&guard, provider, ctx.now_ms),
        Some(Err(_)) => RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "error": "account state lock poisoned" }),
        },
        None => RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "error": "account state not restored yet" }),
        },
    }
}

// ---------------------------------------------------------------------------
// Stored rows as allocator inputs
// ---------------------------------------------------------------------------

fn conversation_affinity(row: &AccountAffinityRow, now_ms: i64) -> ConversationAffinity {
    ConversationAffinity {
        pin_id: row.pin_id.clone(),
        account_id: row.account_id.clone(),
        credential_id: row.credential_id.clone(),
        affinity_model: row.affinity_model.clone(),
        pin_generation: row.pin_generation,
        expires_at_ms: row.expires_at,
        lease_active: row.lease_valid_until.is_some_and(|until| until > now_ms),
    }
}

fn load_fences(tx: &AccountTx<'_>, provider: &str) -> RouteStateResult<Vec<Fence>> {
    Ok(tx
        .active_fences(provider)?
        .into_iter()
        .filter_map(|row| match serde_json::from_str::<Fence>(&row.fence_json) {
            Ok(fence) => Some(fence),
            Err(error) => {
                tracing::warn!(fence = %row.fence_id, error = %error, "stored fence undecodable; ignored");
                None
            }
        })
        .collect())
}

fn load_demand(tx: &AccountTx<'_>, provider: &str) -> RouteStateResult<Vec<AdmissionDemand>> {
    Ok(tx
        .demand(provider)?
        .into_iter()
        .filter_map(|row| {
            let state: AdmissionState = serde_json::from_value(json!(row.state)).ok()?;
            Some(AdmissionDemand {
                account_id: row.account_id,
                state,
                committed_at_ms: row.committed_at,
                request_bytes: row.request_bytes,
            })
        })
        .collect())
}

fn load_exclusions(
    tx: &AccountTx<'_>,
    principal: &str,
    admission_id: &str,
) -> RouteStateResult<Vec<PrincipalExclusion>> {
    Ok(tx
        .admission_exclusions(principal, admission_id)?
        .into_iter()
        .map(|row| PrincipalExclusion {
            principal: row.principal,
            credential_id: row.credential_id,
            runner_view_digest: row.runner_view_digest,
            roster_view_digest: row.roster_view_digest,
            recorded_at_ms: row.recorded_at,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Decision records
// ---------------------------------------------------------------------------

fn family_name(family: ModelFamily) -> String {
    wire_str(&family)
}

fn refusal_code(code: RefusalCode) -> &'static str {
    match code {
        RefusalCode::AccountSelectionUnavailable => "account_selection_unavailable",
        RefusalCode::AccountSelectionUnsupported => "account_selection_unsupported",
        RefusalCode::AccountNotAuthorized => "account_not_authorized",
        RefusalCode::AccountReauthRequired => "auth_required",
        RefusalCode::AccountQuotaUnknown => "account_quota_unknown",
        RefusalCode::AccountPoolReserveLimited => "account_pool_reserve_limited",
        RefusalCode::AccountPoolExhausted => "account_pool_exhausted",
    }
}

/// `(outcome, code, account_id, credential_id)` for the decision log.
fn outcome_columns(decision: &Decision) -> (&'static str, String, Option<String>, Option<String>) {
    match &decision.outcome {
        Outcome::Admit(admission) => (
            "admit",
            wire_str(&admission.reason),
            Some(admission.account_id.clone()),
            Some(admission.credential_id.clone()),
        ),
        Outcome::Hold(hold) => (
            "hold",
            "account_hold".to_string(),
            Some(hold.account_id.clone()),
            Some(hold.credential_id.clone()),
        ),
        Outcome::Refuse(refusal) => ("refuse", refusal_code(refusal.code).to_string(), None, None),
    }
}

struct RecordInput<'a> {
    decision_id: &'a str,
    mode: &'a str,
    trigger: &'a str,
    principal: &'a str,
    admission_id: Option<&'a str>,
    request_mode: Option<&'a str>,
    decision: &'a Decision,
    context: Value,
    now_ms: i64,
}

fn decision_record(input: RecordInput<'_>) -> AccountDecisionRecord {
    let (outcome, code, account_id, credential_id) = outcome_columns(input.decision);
    let explanation = &input.decision.explanation;
    AccountDecisionRecord {
        decision_id: input.decision_id.to_string(),
        mode: input.mode.to_string(),
        trigger: input.trigger.to_string(),
        principal: input.principal.to_string(),
        admission_id: input.admission_id.map(str::to_owned),
        request_mode: input.request_mode.map(str::to_owned),
        provider: explanation.provider.clone(),
        model: explanation.model.clone(),
        family: family_name(explanation.family),
        outcome: outcome.to_string(),
        code,
        account_id,
        credential_id,
        policy_version: explanation.policy_version,
        decision_json: wire(input.decision).to_string(),
        context_json: Some(input.context.to_string()),
        created_at: input.now_ms,
    }
}

/// Per-account exclusions as a refusal reports them: one entry per account
/// and reason, `{"account_id", "reason", ...}`, with a reserve exclusion split
/// into one entry per window it names (`"window": "seven_day"`).
fn wire_exclusions(decision: &Decision) -> Vec<Value> {
    let mut out = Vec::new();
    for note in &decision.explanation.accounts {
        for reason in &note.exclusions {
            let Value::Object(mut fields) = wire(reason) else {
                continue;
            };
            if let Some(Value::Array(windows)) = fields.remove("windows") {
                for window in windows {
                    let mut entry = fields.clone();
                    entry.insert("account_id".into(), json!(note.account_id));
                    entry.insert("window".into(), window);
                    out.push(Value::Object(entry));
                }
                continue;
            }
            fields.insert("account_id".into(), json!(note.account_id));
            out.push(Value::Object(fields));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// route.account_for
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum RequestMode {
    #[default]
    Admit,
    Renew,
    Reassign,
}

impl RequestMode {
    fn as_str(self) -> &'static str {
        match self {
            RequestMode::Admit => "admit",
            RequestMode::Renew => "renew",
            RequestMode::Reassign => "reassign",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CallerParams {
    runner: String,
    #[serde(default)]
    #[allow(dead_code)]
    harness: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct PinRef {
    id: String,
    generation: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct CandidateParams {
    account_id: String,
    credential_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CandidatesParams {
    view_digest: String,
    #[serde(default)]
    accounts: Vec<CandidateParams>,
}

#[derive(Debug, Clone, Deserialize)]
struct AccountRefParams {
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    credential_id: Option<String>,
}

impl AccountRefParams {
    fn to_ref(&self) -> Option<AccountRef> {
        match (&self.account_id, &self.credential_id) {
            (Some(account_id), _) => Some(AccountRef::AccountId(account_id.clone())),
            (None, Some(credential_id)) => Some(AccountRef::CredentialId(credential_id.clone())),
            (None, None) => None,
        }
    }
}

/// The parameters `route.account_for` accepts. Field names are the wire
/// names, snake_case like the rest of the account-selection schema.
#[derive(Debug, Clone, Deserialize)]
struct AccountForParams {
    provider: String,
    model: String,
    #[serde(default)]
    affinity_model: Option<String>,
    conversation_key: String,
    caller: CallerParams,
    admission_id: String,
    #[serde(default)]
    mode: RequestMode,
    #[serde(default)]
    expected_pin: Option<PinRef>,
    #[serde(default)]
    initial_input_bytes: u64,
    #[serde(default)]
    initial_input_estimated: bool,
    /// The id of the `route.select` decision that chose this model. Accepted
    /// for the record; account choice does not read it.
    #[serde(default)]
    #[allow(dead_code)]
    model_decision_id: Option<String>,
    #[serde(default)]
    candidates: Option<CandidatesParams>,
    #[serde(default)]
    requested_account: Option<AccountRefParams>,
    /// A plugin's one-time import of its existing local pin.
    #[serde(default)]
    preferred_existing_account: Option<AccountRefParams>,
    #[serde(default)]
    hold_wait_max_s: Option<u64>,
}

impl AccountForParams {
    fn validate(&self) -> Result<(), OpReply> {
        if self.admission_id.trim().is_empty() {
            return Err(invalid("`admission_id` is required and must not be empty"));
        }
        if self.conversation_key.trim().is_empty() {
            return Err(invalid("`conversation_key` must not be empty"));
        }
        if self.provider.trim().is_empty() || self.model.trim().is_empty() {
            return Err(invalid("`provider` and `model` must not be empty"));
        }
        if self.mode != RequestMode::Admit && self.expected_pin.is_none() {
            return Err(invalid(format!(
                "mode `{}` requires `expected_pin` as {{id, generation}}",
                self.mode.as_str()
            )));
        }
        Ok(())
    }

    fn caller_kind(&self) -> CallerKind {
        if self.caller.runner.eq_ignore_ascii_case("broca") {
            CallerKind::Broca
        } else {
            CallerKind::Plugin
        }
    }

    fn affinity_model(&self) -> &str {
        self.affinity_model.as_deref().unwrap_or(&self.model)
    }

    /// The allocator's request. `import_allowed` is true only when routing
    /// holds no affinity for this conversation, the one time an import hint
    /// may be considered.
    fn request(&self, principal: &str, import_allowed: bool) -> AccountRequest {
        AccountRequest {
            principal: principal.to_string(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            affinity_model: self.affinity_model().to_string(),
            conversation_key: self.conversation_key.clone(),
            // Broca falls back to `broca:<run_id>` when a direct caller sent no
            // conversation key; such a run has no affinity across sends.
            conversation_key_source: if self.conversation_key.starts_with("broca:") {
                ConversationKeySource::BrocaRunFallback
            } else {
                ConversationKeySource::Caller
            },
            caller: self.caller_kind(),
            // A model other than the affinity model was reached through
            // internal safety recovery, which is not direct work.
            via_safety_recovery: normalize(&self.model) != normalize(self.affinity_model()),
            initial_input_bytes: self.initial_input_bytes,
            initial_input_estimated: self.initial_input_estimated,
            candidates: self.candidates.as_ref().map(|candidates| RunnerCandidates {
                view_digest: candidates.view_digest.clone(),
                accounts: candidates
                    .accounts
                    .iter()
                    .map(|candidate| CandidateRef {
                        account_id: candidate.account_id.clone(),
                        credential_id: candidate.credential_id.clone(),
                    })
                    .collect(),
            }),
            requested_account: self
                .requested_account
                .as_ref()
                .and_then(AccountRefParams::to_ref),
            preferred_existing_account: if import_allowed {
                self.preferred_existing_account
                    .as_ref()
                    .and_then(AccountRefParams::to_ref)
            } else {
                None
            },
            hold_wait_max_s: self.hold_wait_max_s.unwrap_or(DEFAULT_HOLD_WAIT_S),
        }
    }
}

fn decode_params(body: &[u8]) -> Result<Value, OpReply> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| invalid(format!("request body is not JSON: {error}")))?;
    Ok(value.get("params").cloned().unwrap_or(Value::Null))
}

fn parse_account_for(params: &Value) -> Result<AccountForParams, OpReply> {
    let parsed: AccountForParams = serde_json::from_value(params.clone())
        .map_err(|error| invalid(format!("invalid route.account_for params: {error}")))?;
    parsed.validate()?;
    Ok(parsed)
}

/// Handle one account-selection operation. `body` is the whole request
/// (`{"method", "params"}`); `principal` is the route's attested principal.
pub fn handle(method: &str, body: &[u8], principal: &str, ctx: &Ctx<'_>) -> OpReply {
    if method == ACCOUNT_SHADOW_STATUS_OP {
        let params = decode_params(body).unwrap_or(Value::Null);
        return shadow_status(&params, principal, ctx);
    }
    if !ctx.flags.enabled {
        if ctx.flags.shadow {
            shadow_for_refused_op(method, body, principal, ctx);
        }
        return OpReply::typed(
            DISABLED_CODE,
            format!("{method} is not served: account selection is disabled in routing policy"),
            json!({ "code": DISABLED_CODE }),
        );
    }
    let params = match decode_params(body) {
        Ok(params) => params,
        Err(reply) => return reply,
    };
    match method {
        ACCOUNT_FOR_OP => account_for(&params, principal, ctx),
        ACCOUNT_OBSERVE_OP => {
            let reply = observe(&params, principal, ctx);
            if ctx.flags.shadow {
                if let Err(error) = shadow_pass(ctx, ShadowTrigger::Observation(&params)) {
                    tracing::warn!(error = %error.0, "shadow pass after an observation failed");
                }
            }
            reply
        }
        ACCOUNT_RELEASE_OP => release(&params, principal, ctx),
        other => OpReply::error("unknown_method", format!("unknown method '{other}'")),
    }
}

/// With serving off and shadow on, a refused operation still feeds shadow:
/// `route.account_for` is decided on the caller's own request, and an
/// observation triggers a pass whose record carries it. Only shadow decision
/// rows are written.
fn shadow_for_refused_op(method: &str, body: &[u8], principal: &str, ctx: &Ctx<'_>) {
    let Ok(params) = decode_params(body) else {
        return;
    };
    let result = match method {
        ACCOUNT_FOR_OP => match parse_account_for(&params) {
            Ok(parsed) => shadow_request(ctx, principal, &parsed, &params).map(|_| ()),
            Err(_) => Ok(()),
        },
        ACCOUNT_OBSERVE_OP => shadow_pass(ctx, ShadowTrigger::Observation(&params)).map(|_| ()),
        _ => Ok(()),
    };
    if let Err(error) = result {
        tracing::warn!(method, error = %error.0, "shadow decision not recorded");
    }
}

fn account_for(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed = match parse_account_for(params) {
        Ok(parsed) => parsed,
        Err(reply) => return reply,
    };
    let provider = normalize(&parsed.provider);
    if adapter_for_provider(&provider).is_none() {
        return OpReply::typed(
            "account_selection_unsupported",
            format!("routing selects no accounts for provider `{provider}`"),
            json!({
                "code": "account_selection_unsupported",
                "retry_after_s": null,
                "decision_id": null,
                "exclusions": [],
            }),
        );
    }
    // The roster is read before the transaction: no lock or fetch happens
    // while the selection transaction is open.
    let roster = roster_for(ctx, &provider);
    let request_fingerprint = fingerprint(params);
    ctx.store
        .account_transaction(|tx| {
            serve(
                tx,
                ctx,
                principal,
                &parsed,
                &provider,
                &roster,
                &request_fingerprint,
            )
        })
        .unwrap_or_else(store_error)
}

/// The whole admission inside one write transaction: idempotency, the
/// compare-and-set, the decision and its commit. The transaction commits
/// before the reply leaves, so a reply always describes a durable decision.
fn serve(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
    roster: &RosterBuild,
    request_fingerprint: &str,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    if let Some(existing) = tx.admission(principal, &params.admission_id)? {
        return replay(tx, principal, existing, request_fingerprint, now);
    }
    let salt = tx.salt(now, fresh_salt)?;
    let hash = conversation_hash(&salt, principal, &params.conversation_key);
    let affinity_row = tx.affinity(principal, &hash, provider)?;
    if let Some(expected) = &params.expected_pin {
        let current = affinity_row
            .as_ref()
            .map(|row| (row.pin_id.as_str(), row.pin_generation));
        if params.mode != RequestMode::Admit
            && current != Some((expected.id.as_str(), expected.generation))
        {
            return Ok(pin_conflict(affinity_row.as_ref(), expected));
        }
    }
    let state = AllocatorState {
        roster: roster.roster.clone(),
        affinity: affinity_row
            .as_ref()
            .map(|row| conversation_affinity(row, now)),
        fences: load_fences(tx, provider)?,
        principal_exclusions: load_exclusions(tx, principal, &params.admission_id)?,
        admissions: load_demand(tx, provider)?,
        operator_override: None,
        policy: ctx.policy.clone(),
    };
    let request = params.request(principal, affinity_row.is_none());
    let decision = decide(&request, &state, now);
    let decision_id = new_id("acd");
    tx.insert_decision(&decision_record(RecordInput {
        decision_id: &decision_id,
        mode: DECISION_MODE_SERVE,
        trigger: "request",
        principal,
        admission_id: Some(&params.admission_id),
        request_mode: Some(params.mode.as_str()),
        decision: &decision,
        context: json!({ "roster": roster.context }),
        now_ms: now,
    }))?;
    match &decision.outcome {
        Outcome::Admit(admission) => commit_admission(
            tx,
            ctx,
            principal,
            params,
            provider,
            &hash,
            affinity_row.as_ref(),
            &decision_id,
            &decision,
            admission,
            request_fingerprint,
        ),
        Outcome::Hold(hold) => {
            tx.insert_hold(&AccountHoldRow {
                decision_id: decision_id.clone(),
                principal: principal.to_string(),
                conversation_hash: hash.clone(),
                provider: provider.to_string(),
                pin_id: hold.pin_id.clone(),
                pin_generation: hold.pin_generation,
                account_id: hold.account_id.clone(),
                credential_id: hold.credential_id.clone(),
                basis: wire_str(&hold.basis),
                reset_at: hold.reset_at_ms,
                retry_after_s: hold.retry_after_s,
                created_at: now,
            })?;
            let refresh_adapter = refresh_adapter_for(provider);
            Ok(OpReply::typed(
                "account_hold",
                format!(
                    "account {} is in a short hold; retry after {} s",
                    hold.account_id, hold.retry_after_s
                ),
                json!({
                    "code": "account_hold",
                    "retry_after_s": hold.retry_after_s,
                    "decision_id": decision_id,
                    "account": {
                        "provider": provider,
                        "account_id": hold.account_id,
                        "credential_id": hold.credential_id,
                        "refresh_adapter": refresh_adapter,
                        "method": "oauth",
                    },
                    "pin": { "id": hold.pin_id, "generation": hold.pin_generation },
                    "reset_at": rfc3339(hold.reset_at_ms),
                    "basis": wire(&hold.basis),
                    "exclusions": wire_exclusions(&decision),
                }),
            ))
        }
        Outcome::Refuse(refusal) => {
            let code = refusal_code(refusal.code);
            Ok(OpReply::typed(
                code,
                format!("no account admitted: {code}"),
                json!({
                    "code": code,
                    "retry_after_s": refusal.retry_after_s,
                    "decision_id": decision_id,
                    "exclusions": wire_exclusions(&decision),
                }),
            ))
        }
    }
}

/// A duplicate admission id: the same parameters get the committed decision
/// back (marked `expired` once its start window has passed) and charge
/// nothing; different parameters are a conflict.
fn replay(
    tx: &AccountTx<'_>,
    principal: &str,
    existing: AccountAdmissionRow,
    request_fingerprint: &str,
    now: i64,
) -> RouteStateResult<OpReply> {
    if existing.request_fingerprint != request_fingerprint {
        return Ok(OpReply::typed(
            "admission_conflict",
            format!(
                "admission {} was already decided with different parameters",
                existing.admission_id
            ),
            json!({
                "code": "admission_conflict",
                "retry_after_s": null,
                "decision_id": existing.decision_id,
                "exclusions": [],
            }),
        ));
    }
    let mut state = existing.state.clone();
    if state == "reserved" && now >= existing.admission_valid_until {
        tx.mark_admission_expired(principal, &existing.admission_id)?;
        state = "expired".into();
    }
    let mut reply: Value = serde_json::from_str(&existing.reply_json).map_err(|error| {
        RouteStateStoreError(format!("stored admission reply undecodable: {error}"))
    })?;
    reply["admission"]["state"] = json!(state);
    Ok(OpReply::Result(reply))
}

fn pin_conflict(current: Option<&AccountAffinityRow>, expected: &PinRef) -> OpReply {
    OpReply::typed(
        "pin_generation_conflict",
        format!(
            "expected pin {} generation {} is not the conversation's current pin",
            expected.id, expected.generation
        ),
        json!({
            "code": "pin_generation_conflict",
            "retry_after_s": null,
            "decision_id": null,
            "exclusions": [],
            "current_pin": current.map(|row| json!({
                "id": row.pin_id,
                "generation": row.pin_generation,
                "account_id": row.account_id,
                "credential_id": row.credential_id,
            })),
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn commit_admission(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
    hash: &str,
    affinity_row: Option<&AccountAffinityRow>,
    decision_id: &str,
    decision: &Decision,
    admission: &prefrontal_routing_core::accounts::Admission,
    request_fingerprint: &str,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let pin_mode = match params.caller_kind() {
        CallerKind::Broca => "run_frozen",
        CallerKind::Plugin => "conversation_lease",
    };
    let lease_valid_until = now + LEASE_MS;
    let admission_valid_until = now + ADMISSION_VALID_MS;
    let expected = affinity_row.map(|row| (row.pin_id.as_str(), row.pin_generation));
    let (pin_id, pin_generation, affinity_expires_at, moved) = match &admission.affinity {
        AffinityOutcome::Retained {
            pin_id,
            pin_generation,
        } => {
            let row = affinity_row.ok_or_else(|| {
                RouteStateStoreError("allocator retained an affinity routing does not hold".into())
            })?;
            // Accepted use renews affinity at most once a minute; the lease
            // moves on every admission.
            let (last_used, expires_at) = if now - row.last_used_at >= AFFINITY_TOUCH_MS {
                (now, now + AFFINITY_IDLE_MS)
            } else {
                (row.last_used_at, row.expires_at)
            };
            if !tx.touch_affinity(
                principal,
                hash,
                provider,
                (pin_id, *pin_generation),
                last_used,
                expires_at,
                Some(lease_valid_until),
            )? {
                return Err(RouteStateStoreError(
                    "conversation pin changed inside the admission transaction".into(),
                ));
            }
            (pin_id.clone(), *pin_generation, expires_at, false)
        }
        AffinityOutcome::New {
            previous,
            pin_generation,
        } => {
            let pin_id = new_id("pin");
            tx.insert_pin(&AccountPinRow {
                pin_id: pin_id.clone(),
                principal: principal.to_string(),
                conversation_hash: hash.to_string(),
                provider: provider.to_string(),
                pin_generation: *pin_generation,
                affinity_model: normalize(params.affinity_model()),
                account_id: admission.account_id.clone(),
                credential_id: admission.credential_id.clone(),
                pin_mode: pin_mode.to_string(),
                previous_pin_id: previous.as_ref().map(|pin| pin.pin_id.clone()),
                decision_id: decision_id.to_string(),
                created_at: now,
            })?;
            let binding_revision = match affinity_row {
                Some(row) if row.credential_id == admission.credential_id => {
                    row.credential_binding_revision
                }
                Some(row) => row.credential_binding_revision + 1,
                None => 1,
            };
            let expires_at = now + AFFINITY_IDLE_MS;
            let applied = tx.put_affinity(
                &AccountAffinityRow {
                    principal: principal.to_string(),
                    conversation_hash: hash.to_string(),
                    provider: provider.to_string(),
                    affinity_model: normalize(params.affinity_model()),
                    pin_id: pin_id.clone(),
                    pin_generation: *pin_generation,
                    account_id: admission.account_id.clone(),
                    credential_id: admission.credential_id.clone(),
                    credential_binding_revision: binding_revision,
                    policy_version: decision.explanation.policy_version,
                    initial_demand_bytes: admission.request_bytes,
                    created_at: now,
                    last_used_at: now,
                    expires_at,
                    lease_valid_until: Some(lease_valid_until),
                },
                expected,
            )?;
            if !applied {
                return Err(RouteStateStoreError(
                    "conversation pin changed inside the admission transaction".into(),
                ));
            }
            (pin_id, *pin_generation, expires_at, true)
        }
    };
    // Admission charges its prompt once; a renewal charges nothing; a
    // reassignment charges only when it actually moved the conversation.
    let charged = match params.mode {
        RequestMode::Admit => true,
        RequestMode::Renew => false,
        RequestMode::Reassign => moved,
    };
    let refresh_adapter = refresh_adapter_for(provider);
    let reply = json!({
        "admission": { "id": params.admission_id, "state": "reserved" },
        "account": {
            "provider": provider,
            "account_id": admission.account_id,
            "credential_id": admission.credential_id,
            "refresh_adapter": refresh_adapter,
            "method": "oauth",
        },
        "pin": {
            "id": pin_id,
            "generation": pin_generation,
            "affinity_expires_at": rfc3339(affinity_expires_at),
            "admission_valid_until": rfc3339(admission_valid_until),
            "lease_valid_until": rfc3339(lease_valid_until),
            "mode": pin_mode,
        },
        "reason": {
            "code": wire(&admission.reason),
            "decision_id": decision_id,
            "policy_version": decision.explanation.policy_version,
            "quota_fetched_at": admission.quota_observed_at_ms.map(rfc3339),
            "quota_age_band": admission.quota_age_band.map(|band| wire(&band)),
        },
    });
    tx.insert_admission(&AccountAdmissionRow {
        principal: principal.to_string(),
        admission_id: params.admission_id.clone(),
        request_fingerprint: request_fingerprint.to_string(),
        request_mode: params.mode.as_str().to_string(),
        decision_id: decision_id.to_string(),
        conversation_hash: hash.to_string(),
        provider: provider.to_string(),
        model: normalize(&params.model),
        account_id: admission.account_id.clone(),
        credential_id: admission.credential_id.clone(),
        pin_id,
        pin_generation,
        state: "reserved".into(),
        charged,
        request_bytes: if charged { admission.request_bytes } else { 0 },
        request_bytes_estimated: admission.request_bytes_estimated,
        committed_at: now,
        admission_valid_until,
        lease_valid_until,
        reply_json: reply.to_string(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
    })?;
    Ok(OpReply::Result(reply))
}

/// The vault `refresh_adapter` of a provider's subscription credentials. The
/// roster admits only identities with this adapter, so every account routing
/// returns for the provider carries it.
fn refresh_adapter_for(provider: &str) -> String {
    adapter_for_provider(provider).map_or_else(
        || provider.to_string(),
        |adapter| adapter.refresh_adapter.to_string(),
    )
}

// ---------------------------------------------------------------------------
// route.account_observe
// ---------------------------------------------------------------------------

/// `route.account_observe` params. The whole params object is also kept
/// verbatim with the observation.
#[derive(Debug, Clone, Deserialize)]
struct ObserveParams {
    /// The runner's own id for this physical attempt or event; a repeat of
    /// the same id from the same runner is recognized and changes nothing.
    event_id: String,
    caller: CallerParams,
    /// `admission_started`, `run_finished`, `attempt`, `scoped_not_found` or
    /// `account_limit_exhausted`. Any other kind is kept as advisory.
    kind: String,
    #[serde(default)]
    admission_id: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    /// The account that actually served, when the runner knows it.
    #[serde(default)]
    served_account_id: Option<String>,
    #[serde(default)]
    credential_id: Option<String>,
    /// The runner's `list_scoped` view digest (scoped not-found).
    #[serde(default)]
    view_digest: Option<String>,
    /// The original observation time, never a forwarding time.
    #[serde(default)]
    observed_at_ms: Option<i64>,
    /// Which upstream frame the headers came from.
    #[serde(default)]
    upstream_origin: Option<FrameOrigin>,
    /// Every `anthropic-ratelimit-unified-*` header exactly as received.
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    limit_window: Option<String>,
    #[serde(default)]
    resets_at_ms: Option<i64>,
    #[serde(default)]
    resets_at_source: Option<ResetSource>,
}

const OBSERVATION_KINDS: &[&str] = &[
    "admission_started",
    "run_finished",
    "attempt",
    "scoped_not_found",
    "account_limit_exhausted",
];

fn observe(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed: ObserveParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_observe params: {error}")),
    };
    if parsed.event_id.trim().is_empty() {
        return invalid("`event_id` must not be empty");
    }
    // The roster digest a scoped not-found is recorded against.
    let roster_digests: BTreeMap<String, (String, BTreeSet<String>)> = ACCOUNT_PROVIDERS
        .iter()
        .map(|adapter| {
            let roster = roster_for(ctx, adapter.routing_provider).roster;
            let digest = roster
                .as_ref()
                .map(|r| r.view_digest.clone())
                .unwrap_or_default();
            let listed = roster
                .iter()
                .flat_map(|r| r.accounts.iter())
                .flat_map(|view| view.credentials.iter().map(|c| c.credential_id.clone()))
                .collect();
            (adapter.routing_provider.to_string(), (digest, listed))
        })
        .collect();
    ctx.store
        .account_transaction(|tx| {
            record_observation(tx, ctx, principal, &parsed, params, &roster_digests)
        })
        .unwrap_or_else(store_error)
}

fn record_observation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    parsed: &ObserveParams,
    raw: &Value,
    roster_digests: &BTreeMap<String, (String, BTreeSet<String>)>,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let runner = normalize(&parsed.caller.runner);
    if let Some(existing) = tx.observation_by_event(principal, &runner, &parsed.event_id)? {
        return Ok(OpReply::Result(json!({
            "observation": {
                "id": existing.observation_id,
                "duplicate": true,
                "bound": existing.bound,
                "effects": serde_json::from_str::<Value>(&existing.effects_json).unwrap_or(Value::Null),
            }
        })));
    }
    let observation_id = new_id("aco");
    // Mutation authorization: only an admission this principal was issued
    // binds the evidence, and only to the account that admission assigned.
    let admission = match &parsed.admission_id {
        Some(id) => tx.admission(principal, id)?,
        None => None,
    };
    tx.insert_observation(&AccountObservationRow {
        observation_id: observation_id.clone(),
        principal: principal.to_string(),
        runner: runner.clone(),
        event_id: parsed.event_id.clone(),
        kind: parsed.kind.clone(),
        admission_id: parsed.admission_id.clone(),
        bound: false,
        observation_json: raw.to_string(),
        effects_json: "[]".into(),
        received_at: now,
    })?;
    let mut effects: Vec<Value> = Vec::new();
    let mut bound = false;
    match &admission {
        None => effects.push(json!({
            "effect": "advisory",
            "why": "names no admission routing issued to this principal",
        })),
        Some(row) => {
            let mismatch = parsed
                .served_account_id
                .as_deref()
                .is_some_and(|served| served != row.account_id);
            if mismatch {
                tracing::warn!(
                    admission = %row.admission_id,
                    selected = %row.account_id,
                    served = ?parsed.served_account_id,
                    "account selection: served account differs from the admitted one"
                );
                effects.push(json!({
                    "effect": "served_account_mismatch",
                    "selected_account_id": row.account_id,
                    "served_account_id": parsed.served_account_id,
                    "pin_generation": row.pin_generation,
                }));
            } else {
                bound = true;
            }
        }
    }
    if !OBSERVATION_KINDS.contains(&parsed.kind.as_str()) {
        effects.push(json!({ "effect": "advisory", "why": "unknown observation kind" }));
    } else if let (true, Some(row)) = (bound, admission.as_ref()) {
        apply_bound_observation(
            tx,
            ctx,
            principal,
            parsed,
            row,
            &observation_id,
            roster_digests,
            &mut effects,
        )?;
    }
    let effects_value = Value::Array(effects);
    tx.set_observation_effects(&observation_id, bound, &effects_value.to_string())?;
    Ok(OpReply::Result(json!({
        "observation": {
            "id": observation_id,
            "duplicate": false,
            "bound": bound,
            "effects": effects_value,
        }
    })))
}

#[allow(clippy::too_many_arguments)]
fn apply_bound_observation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    parsed: &ObserveParams,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    roster_digests: &BTreeMap<String, (String, BTreeSet<String>)>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    let now = ctx.now_ms;
    let id = &admission.admission_id;
    match parsed.kind.as_str() {
        "admission_started" => {
            let Some(run_id) = parsed.run_id.as_deref() else {
                effects.push(json!({ "effect": "none", "why": "admission_started needs run_id" }));
                return Ok(());
            };
            let late = now >= admission.admission_valid_until;
            if tx.mark_admission_started(principal, id, run_id, now, late)? {
                effects.push(json!({ "effect": "admission_started", "late": late }));
            } else {
                effects.push(json!({ "effect": "none", "state": admission.state }));
            }
        }
        "run_finished" => {
            if tx.mark_admission_finished(principal, id, now)? {
                effects.push(json!({ "effect": "admission_finished" }));
            } else {
                effects.push(json!({ "effect": "none", "state": admission.state }));
            }
            header_fences(
                tx,
                ctx,
                principal,
                parsed,
                admission,
                observation_id,
                effects,
            )?;
        }
        "attempt" => header_fences(
            tx,
            ctx,
            principal,
            parsed,
            admission,
            observation_id,
            effects,
        )?,
        "scoped_not_found" => {
            let (Some(credential_id), Some(view_digest)) = (
                parsed.credential_id.as_deref(),
                parsed.view_digest.as_deref(),
            ) else {
                effects.push(json!({
                    "effect": "none",
                    "why": "scoped_not_found needs credential_id and view_digest",
                }));
                return Ok(());
            };
            let (roster_digest, listed) = roster_digests
                .get(&admission.provider)
                .cloned()
                .unwrap_or_default();
            tx.insert_exclusion(&AccountExclusionRow {
                principal: principal.to_string(),
                admission_id: id.clone(),
                credential_id: credential_id.to_string(),
                runner_view_digest: view_digest.to_string(),
                roster_view_digest: roster_digest,
                observation_id: Some(observation_id.to_string()),
                recorded_at: now,
            })?;
            effects.push(json!({
                "effect": "excluded_for_admission",
                "credential_id": credential_id,
            }));
            // Only the listing can drop a candidate; a credential refused again
            // while the vault still lists it is an alarm, not a revocation.
            if listed.contains(credential_id) && tx.exclusion_count(principal, credential_id)? > 1 {
                tracing::warn!(
                    principal,
                    credential_id,
                    "account selection: repeated scoped not-found for a credential the vault still lists"
                );
                effects.push(
                    json!({ "effect": "repeated_not_found_alarm", "credential_id": credential_id }),
                );
            }
        }
        "account_limit_exhausted" => {
            let observation = ExhaustionObservation {
                account_id: admission.account_id.clone(),
                bound: true,
                kind: ObservationKind::BrocaLimitExhausted {
                    provider: admission.provider.clone(),
                    limit_window: parsed.limit_window.clone(),
                    requested_family: ctx.policy.family_for_model(&admission.model),
                    resets_at_ms: parsed.resets_at_ms,
                    resets_at_source: parsed.resets_at_source,
                    observed_at_ms: parsed.observed_at_ms.unwrap_or(now),
                },
            };
            apply_confirmation(
                tx,
                ctx,
                principal,
                admission,
                observation_id,
                &observation,
                effects,
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// A response from the real upstream (a direct response, an upstream frame an
/// HTTP relay carried, or the WebSocket `response_start` frame; never headers
/// a relay made up) whose own raw 5h or 7d utilization header reads 1.0 or
/// more confirms that window is exhausted, without a usage poll. Everything
/// else a header says is kept with the observation and changes nothing shared.
fn header_fences(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    parsed: &ObserveParams,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    let headers: BTreeMap<String, &String> = parsed
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect();
    for (window, label) in [
        (GeneralWindow::FiveHour, "5h"),
        (GeneralWindow::SevenDay, "7d"),
    ] {
        let Some(raw) = headers.get(&format!("anthropic-ratelimit-unified-{label}-utilization"))
        else {
            continue;
        };
        let Some(frame) = parsed.upstream_origin else {
            effects.push(json!({
                "effect": "none",
                "window": wire(&window),
                "why": "header evidence without upstream_origin cannot confirm",
            }));
            continue;
        };
        let raw_utilization = raw.trim().parse::<f64>().unwrap_or(f64::NAN);
        let reset = headers
            .get(&format!("anthropic-ratelimit-unified-{label}-reset"))
            .map(|value| value.trim().parse::<f64>().unwrap_or(f64::NAN));
        let quota_window = header_window(window, raw_utilization, reset, ObservationSource::Header);
        let observed_at_ms = parsed.observed_at_ms.unwrap_or(ctx.now_ms);
        let observation = ExhaustionObservation {
            account_id: admission.account_id.clone(),
            bound: true,
            kind: ObservationKind::ServedHeader(WindowEvidence {
                scope: EvidenceScope::General(window),
                derivation: Derivation::Header {
                    frame,
                    raw_utilization: Some(raw_utilization).filter(|raw| raw.is_finite()),
                },
                carrier: Carrier::Direct,
                observed_at_ms,
                received_at_ms: ctx.now_ms,
                reset: quota_window.resets_at,
            }),
        };
        apply_confirmation(
            tx,
            ctx,
            principal,
            admission,
            observation_id,
            &observation,
            effects,
        )?;
    }
    Ok(())
}

fn apply_confirmation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    observation: &ExhaustionObservation,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    match confirm_exhaustion(observation) {
        ConfirmationOutcome::Confirmed { fence } => {
            let fence_id = new_id("acf");
            tx.insert_fence(&AccountFenceRow {
                fence_id: fence_id.clone(),
                provider: admission.provider.clone(),
                account_id: fence.account_id.clone(),
                scope_json: wire(&fence.scope).to_string(),
                fence_json: wire(&fence).to_string(),
                source: wire_str(&fence.source),
                observed_at: fence.observed_at_ms,
                principal: principal.to_string(),
                admission_id: Some(admission.admission_id.clone()),
                observation_id: Some(observation_id.to_string()),
                created_at: ctx.now_ms,
                cleared_at: None,
                cleared_reason: None,
            })?;
            effects.push(json!({ "effect": "fence", "fence_id": fence_id, "fence": wire(&fence) }));
        }
        other => effects.push(json!({ "effect": "not_confirmed", "evaluation": wire(&other) })),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// route.account_release
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ReleaseParams {
    admission_id: String,
}

fn release(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed: ReleaseParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_release params: {error}")),
    };
    let now = ctx.now_ms;
    ctx.store
        .account_transaction(|tx| {
            // Only this principal's own admission can be released; any other
            // id changes nothing.
            let Some(row) = tx.admission(principal, &parsed.admission_id)? else {
                return Ok(OpReply::Result(json!({
                    "release": {
                        "admission_id": parsed.admission_id,
                        "state": null,
                        "released": false,
                        "lease_ended": false,
                    }
                })));
            };
            let released = tx.mark_admission_released(principal, &row.admission_id, now)?;
            // Release ends the conversation's lease; it never erases affinity
            // and never cancels a request in flight.
            let lease_ended = tx.end_lease(&row.pin_id, now)?;
            let state = tx
                .admission(principal, &row.admission_id)?
                .map_or(row.state.clone(), |current| current.state);
            Ok(OpReply::Result(json!({
                "release": {
                    "admission_id": row.admission_id,
                    "state": state,
                    "released": released,
                    "lease_ended": lease_ended,
                }
            })))
        })
        .unwrap_or_else(store_error)
}

// ---------------------------------------------------------------------------
// Shadow
// ---------------------------------------------------------------------------

/// What made shadow run.
pub enum ShadowTrigger<'a> {
    /// The periodic pass: a synthetic new cold session per model family.
    Timer,
    /// An observation report, carried whole into the record.
    Observation(&'a Value),
}

/// One representative model per scoped family, in the policy's order, for the
/// synthetic cold requests.
fn representative_models(policy: &AllocatorPolicy) -> Vec<(ModelFamily, String)> {
    let mut seen = BTreeSet::new();
    policy
        .model_families
        .iter()
        .filter(|mapping| seen.insert(mapping.family))
        .map(|mapping| (mapping.family, mapping.model.clone()))
        .collect()
}

fn synthetic_request(provider: &str, model: &str, key: String) -> AccountRequest {
    AccountRequest {
        principal: SHADOW_PRINCIPAL.to_string(),
        provider: provider.to_string(),
        model: model.to_string(),
        affinity_model: model.to_string(),
        conversation_key: key,
        conversation_key_source: ConversationKeySource::Caller,
        // Broca is the first caller account selection will serve, and its
        // frozen runs need the stricter spendable floor.
        caller: CallerKind::Broca,
        via_safety_recovery: false,
        initial_input_bytes: 0,
        initial_input_estimated: true,
        candidates: None,
        requested_account: None,
        preferred_existing_account: None,
        hold_wait_max_s: DEFAULT_HOLD_WAIT_S,
    }
}

/// What makes two decisions "the same" for the timer's change-or-heartbeat
/// rule: the outcome, the chosen account or refusal code, and every
/// account's exclusion reasons. Weights and scores move every minute and are
/// left out.
fn decision_shape(decision: &Value) -> Value {
    let accounts: Vec<Value> = decision["explanation"]["accounts"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|note| {
            let reasons: Vec<Value> = note["exclusions"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|reason| reason["reason"].clone())
                .collect();
            json!([note["account_id"], reasons])
        })
        .collect();
    let outcome = &decision["outcome"];
    json!([
        outcome["kind"],
        outcome["account_id"],
        outcome["credential_id"],
        outcome["reason"],
        outcome["code"],
        accounts,
    ])
}

/// Run shadow once for every tracked provider. The timer decides a synthetic
/// new cold session per model family; an observation decides one synthetic
/// session for the observed model (Opus when it names none) and carries the
/// observation in the record. Returns the recorded decision ids.
pub fn shadow_pass(ctx: &Ctx<'_>, trigger: ShadowTrigger<'_>) -> RouteStateResult<Vec<String>> {
    let mut recorded = Vec::new();
    for adapter in ACCOUNT_PROVIDERS {
        let provider = adapter.routing_provider;
        let roster = roster_for(ctx, provider);
        let models: Vec<(ModelFamily, String)> = match &trigger {
            ShadowTrigger::Timer => representative_models(ctx.policy),
            ShadowTrigger::Observation(observation) => {
                let named = observation
                    .get("model")
                    .and_then(Value::as_str)
                    .map(normalize)
                    .filter(|model| ctx.policy.family_for_model(model) != ModelFamily::Other);
                let model = named.or_else(|| {
                    representative_models(ctx.policy)
                        .into_iter()
                        .find(|(family, _)| *family == ModelFamily::Opus)
                        .map(|(_, model)| model)
                });
                model
                    .map(|model| vec![(ctx.policy.family_for_model(&model), model)])
                    .unwrap_or_default()
            }
        };
        for (family, model) in models {
            let trigger_name = match &trigger {
                ShadowTrigger::Timer => "timer",
                ShadowTrigger::Observation(_) => "observation",
            };
            let request = synthetic_request(
                provider,
                &model,
                format!("shadow:{provider}:{}:{}", family_name(family), ctx.now_ms),
            );
            let observation = match &trigger {
                ShadowTrigger::Observation(observation) => Some((*observation).clone()),
                ShadowTrigger::Timer => None,
            };
            let id = ctx.store.account_transaction(|tx| {
                let state = AllocatorState {
                    roster: roster.roster.clone(),
                    affinity: None,
                    fences: load_fences(tx, provider)?,
                    principal_exclusions: Vec::new(),
                    admissions: load_demand(tx, provider)?,
                    operator_override: None,
                    policy: ctx.policy.clone(),
                };
                let decision = decide(&request, &state, ctx.now_ms);
                if matches!(trigger, ShadowTrigger::Timer) {
                    if let Some(last) = tx.latest_decision(
                        DECISION_MODE_SHADOW,
                        trigger_name,
                        provider,
                        &family_name(family),
                    )? {
                        let last_value: Value =
                            serde_json::from_str(&last.decision_json).unwrap_or(Value::Null);
                        if ctx.now_ms - last.created_at < SHADOW_HEARTBEAT_MS
                            && decision_shape(&last_value) == decision_shape(&wire(&decision))
                        {
                            return Ok(None);
                        }
                    }
                }
                let decision_id = new_id("acd");
                tx.insert_decision(&decision_record(RecordInput {
                    decision_id: &decision_id,
                    mode: DECISION_MODE_SHADOW,
                    trigger: trigger_name,
                    principal: SHADOW_PRINCIPAL,
                    admission_id: None,
                    request_mode: Some("admit"),
                    decision: &decision,
                    context: json!({
                        "synthetic": true,
                        "request": { "model": model, "caller": "broca", "cold": true },
                        "roster": roster.context,
                        "observation": observation,
                    }),
                    now_ms: ctx.now_ms,
                }))?;
                Ok(Some(decision_id))
            })?;
            recorded.extend(id);
        }
    }
    Ok(recorded)
}

/// Shadow a caller's own `route.account_for` while serving is off: the
/// caller's request and its stored affinity are read, nothing is written but
/// the shadow decision.
fn shadow_request(
    ctx: &Ctx<'_>,
    principal: &str,
    params: &AccountForParams,
    raw: &Value,
) -> RouteStateResult<Option<String>> {
    let provider = normalize(&params.provider);
    if adapter_for_provider(&provider).is_none() {
        return Ok(None);
    }
    let roster = roster_for(ctx, &provider);
    ctx.store.account_transaction(|tx| {
        let affinity_row = match tx.existing_salt()? {
            Some(salt) => tx.affinity(
                principal,
                &conversation_hash(&salt, principal, &params.conversation_key),
                &provider,
            )?,
            None => None,
        };
        let state = AllocatorState {
            roster: roster.roster.clone(),
            affinity: affinity_row
                .as_ref()
                .map(|row| conversation_affinity(row, ctx.now_ms)),
            fences: load_fences(tx, &provider)?,
            principal_exclusions: load_exclusions(tx, principal, &params.admission_id)?,
            admissions: load_demand(tx, &provider)?,
            operator_override: None,
            policy: ctx.policy.clone(),
        };
        let decision = decide(
            &params.request(principal, affinity_row.is_none()),
            &state,
            ctx.now_ms,
        );
        let decision_id = new_id("acd");
        tx.insert_decision(&decision_record(RecordInput {
            decision_id: &decision_id,
            mode: DECISION_MODE_SHADOW,
            trigger: "request",
            principal,
            admission_id: Some(&params.admission_id),
            request_mode: Some(params.mode.as_str()),
            decision: &decision,
            context: json!({ "synthetic": false, "request": raw, "roster": roster.context }),
            now_ms: ctx.now_ms,
        }))?;
        Ok(Some(decision_id))
    })
}

/// A one-line reason per account, derived from the recorded explanation.
fn why(decision: &Value) -> Vec<String> {
    decision["explanation"]["accounts"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|note| {
            let account = note["account_id"].as_str().unwrap_or("?");
            let reasons: Vec<String> = note["exclusions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|reason| reason["reason"].as_str().map(str::to_owned))
                .collect();
            if reasons.is_empty() {
                format!(
                    "{account}: eligible, weight {}, pending {} bytes, score {}",
                    note["weight"], note["pending_bytes"], note["score"]
                )
            } else {
                format!("{account}: excluded ({})", reasons.join(", "))
            }
        })
        .collect()
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ShadowStatusParams {
    #[serde(default)]
    limit: Option<usize>,
}

/// The latest shadow decisions and why. Synthetic decisions are routing's own
/// and shown to any caller; a decision shadowed from a caller's own request
/// is shown only to that caller's principal.
fn shadow_status(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed: ShadowStatusParams = serde_json::from_value(params.clone()).unwrap_or_default();
    let limit = parsed.limit.unwrap_or(20).clamp(1, SHADOW_STATUS_MAX);
    let rows = match ctx
        .store
        .account_decisions(DECISION_MODE_SHADOW, SHADOW_STATUS_MAX * 4)
    {
        Ok(rows) => rows,
        Err(error) => return store_error(error),
    };
    let decisions: Vec<Value> = rows
        .into_iter()
        .filter(|row| row.principal == SHADOW_PRINCIPAL || row.principal == principal)
        .take(limit)
        .map(|row| {
            let decision: Value = serde_json::from_str(&row.decision_json).unwrap_or(Value::Null);
            let context: Value = row
                .context_json
                .as_deref()
                .and_then(|text| serde_json::from_str(text).ok())
                .unwrap_or(Value::Null);
            json!({
                "decision_id": row.decision_id,
                "created_at": rfc3339(row.created_at),
                "trigger": row.trigger,
                "principal": row.principal,
                "provider": row.provider,
                "model": row.model,
                "family": row.family,
                "outcome": row.outcome,
                "code": row.code,
                "account_id": row.account_id,
                "credential_id": row.credential_id,
                "policy_version": row.policy_version,
                "why": why(&decision),
                "decision": decision,
                "context": context,
            })
        })
        .collect();
    OpReply::Result(json!({
        "shadow_enabled": ctx.flags.shadow,
        "serve_enabled": ctx.flags.enabled,
        "decisions": decisions,
    }))
}

/// The periodic shadow pass. It re-reads the flags every cycle, so turning
/// shadow on or off in the routing file takes effect within one interval, and
/// does nothing while shadow is off.
pub async fn shadow_loop(
    store: std::sync::Arc<SqliteRouteStateStore>,
    accounts: SharedAccountQuota,
    config_path: std::path::PathBuf,
) {
    let policy = AllocatorPolicy::default();
    loop {
        tokio::time::sleep(SHADOW_INTERVAL).await;
        let flags = crate::config_overrides::account_selection_flags(&config_path);
        if !flags.shadow {
            continue;
        }
        let ctx = Ctx {
            store: store.as_ref(),
            accounts: Some(&accounts),
            flags,
            policy: &policy,
            now_ms: now_ms(),
        };
        if let Err(error) = shadow_pass(&ctx, ShadowTrigger::Timer) {
            tracing::warn!(error = %error.0, "account selection shadow pass failed");
        }
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_quota::{apply_provider_reply, apply_roster_listing};
    use prefrontal_ckcred_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
    use prefrontal_routing_store::dev_descriptor;
    use std::sync::{Arc, Mutex};

    /// The fixed test clock, 2026-09-29T12:00:00Z.
    const NOW: i64 = 1_790_683_200_000;
    const ACCOUNT_A: &str = "00000000-0000-4000-8000-00000000000a";
    const ACCOUNT_B: &str = "00000000-0000-4000-8000-00000000000b";
    /// Insula reports it; the vault does not list it.
    const INSULA_ONLY: &str = "00000000-0000-4000-8000-00000000000c";
    /// The vault lists it; Insula has never reported it.
    const VAULT_ONLY: &str = "00000000-0000-4000-8000-00000000000d";
    const BROCA: &str = "reserved:broca";

    static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        dir: std::path::PathBuf,
        store: Option<SqliteRouteStateStore>,
        accounts: SharedAccountQuota,
        policy: AllocatorPolicy,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.store = None;
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn open_store(dir: &std::path::Path) -> SqliteRouteStateStore {
        let path = dir.join("store.db").to_string_lossy().into_owned();
        SqliteRouteStateStore::open(&dev_descriptor("prefrontal-routing", path))
            .expect("open store")
    }

    fn claude_entry(account: &str) -> Value {
        json!({
            "provider": "claude",
            "account": account,
            "accountInfo": { "email": format!("{account}@example.test") },
            "source": "vault",
            "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {
                "primary": { "usedPercent": 10.0, "windowMinutes": 300, "resetsAt": "2026-09-29T14:00:00Z" },
                "secondary": { "usedPercent": 40.0, "windowMinutes": 10080 },
                "extraRateWindows": [
                    { "id": "Fable", "title": "7 Day (Fable)", "window": { "usedPercent": 5.0, "windowMinutes": 10080 } }
                ]
            }
        })
    }

    fn listing() -> ScopedCredentialListing {
        let row = |id: &str, account: &str| ListedCredential {
            id: id.into(),
            categories: vec!["anthropic-native".into()],
            credential_type: "oauth".into(),
            refresh_adapter: Some("anthropic".into()),
            state: "active".into(),
            record_version: 1,
            operations: vec!["list".into()],
            account_id: Some(account.into()),
            email: Some(format!("{account}@example.test")),
        };
        ScopedCredentialListing {
            credentials: vec![
                row("oauth:anthropic", ACCOUNT_A),
                row("oauth:anthropic:ysample", ACCOUNT_B),
                row("oauth:anthropic:dsample", VAULT_ONLY),
            ],
            grant_tuples: vec![ListedGrant {
                selector_kind: "category".into(),
                selector: "anthropic-native".into(),
                operation: "list".into(),
            }],
            view: "vault-view-1".into(),
        }
    }

    fn fixture(label: &str) -> Fixture {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-account-selection-module-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut state = AccountQuotaState::default();
        // Listed 20 minutes ago, so the vault-only identity is overdue for its
        // first reading.
        apply_roster_listing(&mut state, &listing(), NOW - 20 * 60_000);
        let reply = serde_json::to_vec(&json!({
            "result": [claude_entry(ACCOUNT_A), claude_entry(ACCOUNT_B), claude_entry(INSULA_ONLY)],
            "completeProviders": ["claude"],
        }))
        .unwrap();
        apply_provider_reply(&mut state, "claude", &reply, NOW - 30_000).unwrap();
        Fixture {
            store: Some(open_store(&dir)),
            dir,
            accounts: Arc::new(Mutex::new(state)),
            policy: AllocatorPolicy::default(),
        }
    }

    impl Fixture {
        fn store(&self) -> &SqliteRouteStateStore {
            self.store.as_ref().unwrap()
        }

        fn reopen(&mut self) {
            self.store = None;
            self.store = Some(open_store(&self.dir));
        }

        fn ctx(&self, enabled: bool, shadow: bool, now_ms: i64) -> Ctx<'_> {
            Ctx {
                store: self.store(),
                accounts: Some(&self.accounts),
                flags: AccountSelectionFlags { enabled, shadow },
                policy: &self.policy,
                now_ms,
            }
        }

        fn call(&self, ctx: &Ctx<'_>, method: &str, principal: &str, params: Value) -> OpReply {
            let body = serde_json::to_vec(&json!({ "method": method, "params": params })).unwrap();
            handle(method, &body, principal, ctx)
        }

        fn shadow_rows(&self) -> Vec<AccountDecisionRecord> {
            self.store()
                .account_decisions(DECISION_MODE_SHADOW, 1000)
                .unwrap()
        }

        fn serve_rows(&self) -> Vec<AccountDecisionRecord> {
            self.store()
                .account_decisions(DECISION_MODE_SERVE, 1000)
                .unwrap()
        }
    }

    fn admit_params(admission_id: &str, key: &str, runner: &str) -> Value {
        json!({
            "provider": "anthropic",
            "model": "claude-opus-5-5",
            "affinity_model": "claude-opus-5-5",
            "conversation_key": key,
            "caller": { "runner": runner, "harness": "prefrontal-core" },
            "admission_id": admission_id,
            "mode": "admit",
            "expected_pin": null,
            "initial_input_bytes": 12000,
            "initial_input_estimated": false,
            "model_decision_id": "prior-route-decision-id",
            "candidates": null,
            "requested_account": null,
            "hold_wait_max_s": 900
        })
    }

    fn result(reply: OpReply) -> Value {
        match reply {
            OpReply::Result(value) => value,
            OpReply::Error { code, message, .. } => {
                panic!("expected a result, got {code}: {message}")
            }
        }
    }

    #[test]
    fn every_account_op_refuses_with_account_selection_disabled_and_writes_nothing() {
        let fixture = fixture("disabled");
        let ctx = fixture.ctx(false, false, NOW);
        let calls = [
            (
                ACCOUNT_FOR_OP,
                admit_params("adm-1", "consult:c:s:1", "broca"),
            ),
            (
                ACCOUNT_OBSERVE_OP,
                json!({ "event_id": "e1", "caller": { "runner": "broca" }, "kind": "admission_started",
                        "admission_id": "adm-1", "run_id": "run-1" }),
            ),
            (ACCOUNT_RELEASE_OP, json!({ "admission_id": "adm-1" })),
        ];
        for (method, params) in calls {
            let reply = fixture.call(&ctx, method, BROCA, params);
            assert_eq!(
                reply.code(),
                Some(DISABLED_CODE),
                "{method} must refuse while disabled"
            );
            let OpReply::Error { detail, .. } = &reply else {
                unreachable!()
            };
            assert_eq!(detail.as_ref().unwrap()["code"], DISABLED_CODE);
        }
        // Malformed params are refused the same way: nothing is even parsed.
        let reply = handle(
            ACCOUNT_FOR_OP,
            b"{\"method\":\"route.account_for\"}",
            BROCA,
            &ctx,
        );
        assert_eq!(reply.code(), Some(DISABLED_CODE));
        assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);
        assert!(fixture.shadow_rows().is_empty());
        assert!(fixture.serve_rows().is_empty());
    }

    #[test]
    fn a_shadow_pass_records_a_decision_with_an_explanation_and_serves_nothing() {
        let fixture = fixture("shadow");
        let ctx = fixture.ctx(false, true, NOW);
        let recorded = shadow_pass(&ctx, ShadowTrigger::Timer).unwrap();
        // One synthetic cold session per model family: Fable, Mythos, Opus.
        assert_eq!(recorded.len(), 3);
        let rows = fixture.shadow_rows();
        assert_eq!(rows.len(), 3);
        let opus = rows
            .iter()
            .find(|row| row.family == "opus")
            .expect("opus decision");
        assert_eq!(opus.principal, SHADOW_PRINCIPAL);
        assert_eq!(opus.trigger, "timer");
        assert_eq!(opus.outcome, "admit");
        assert_eq!(opus.code, "cold_scored");
        let decision: Value = serde_json::from_str(&opus.decision_json).unwrap();
        let notes = decision["explanation"]["accounts"].as_array().unwrap();
        let ids: Vec<&str> = notes
            .iter()
            .map(|note| note["account_id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&ACCOUNT_A) && ids.contains(&ACCOUNT_B) && ids.contains(&VAULT_ONLY));
        let unread = notes
            .iter()
            .find(|note| note["account_id"] == VAULT_ONLY)
            .unwrap();
        assert_eq!(unread["exclusions"][0]["reason"], "quota_missing");
        // Shadow charged no demand, issued no pin and admitted nothing.
        assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);

        // An unchanged pool a minute later adds no rows; a caller's request
        // while serving is off is refused, and shadowed.
        let later = fixture.ctx(false, true, NOW + 60_000);
        assert!(shadow_pass(&later, ShadowTrigger::Timer)
            .unwrap()
            .is_empty());
        let reply = fixture.call(
            &later,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-9", "consult:c:s:1", "broca"),
        );
        assert_eq!(
            reply.code(),
            Some(DISABLED_CODE),
            "shadow never answers with an account"
        );
        let OpReply::Error { detail, .. } = &reply else {
            unreachable!()
        };
        assert!(detail.as_ref().unwrap().get("account").is_none());
        let rows = fixture.shadow_rows();
        let request_row = rows
            .iter()
            .find(|row| row.trigger == "request")
            .expect("shadowed request");
        assert_eq!(request_row.principal, BROCA);
        assert_eq!(request_row.admission_id.as_deref(), Some("adm-9"));
        assert_eq!(request_row.outcome, "admit");
        assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);
        assert!(fixture.serve_rows().is_empty());

        // Readback shows the shadow decisions and why.
        let status = result(fixture.call(&later, ACCOUNT_SHADOW_STATUS_OP, "direct", json!({})));
        assert_eq!(status["shadow_enabled"], true);
        let decisions = status["decisions"].as_array().unwrap();
        // The request-shadowed decision belongs to Broca's principal only.
        assert_eq!(decisions.len(), 3);
        assert!(decisions
            .iter()
            .all(|row| !row["why"].as_array().unwrap().is_empty()));
        let broca_view = result(fixture.call(&later, ACCOUNT_SHADOW_STATUS_OP, BROCA, json!({})));
        assert_eq!(broca_view["decisions"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn with_serving_off_an_observation_writes_one_shadow_record_carrying_it() {
        let fixture = fixture("observe-shadow");
        let ctx = fixture.ctx(false, true, NOW);
        let observation = json!({
            "event_id": "attempt-1",
            "caller": { "runner": "broca" },
            "kind": "account_limit_exhausted",
            "admission_id": "adm-unknown",
            "model": "claude-opus-5-5",
            "limit_window": "seven_day",
            "observed_at_ms": NOW - 1_000,
        });
        let reply = fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, observation.clone());
        assert_eq!(reply.code(), Some(DISABLED_CODE));
        let rows = fixture.shadow_rows();
        assert_eq!(rows.len(), 1, "one shadow record per tracked provider");
        assert_eq!(rows[0].trigger, "observation");
        assert_eq!(rows[0].family, "opus");
        let context: Value =
            serde_json::from_str(rows[0].context_json.as_deref().unwrap()).unwrap();
        assert_eq!(context["observation"], observation);
        let counts = fixture.store().account_serving_counts().unwrap();
        assert_eq!(
            counts.total(),
            0,
            "no observation row, fence or exclusion: {counts:?}"
        );
    }

    #[test]
    fn a_roster_gap_is_reported_in_both_directions_without_emails() {
        let fixture = fixture("gap");
        let build = {
            let guard = fixture.accounts.lock().unwrap();
            allocator_roster(&guard, "anthropic", NOW)
        };
        let roster = build.roster.as_ref().expect("vault roster");
        let ids: Vec<&str> = roster
            .accounts
            .iter()
            .map(|view| view.account_id.as_str())
            .collect();
        assert_eq!(ids, vec![ACCOUNT_A, ACCOUNT_B, VAULT_ONLY]);
        assert!(
            !ids.contains(&INSULA_ONLY),
            "an account the vault does not list is never a candidate"
        );
        let unread = roster
            .accounts
            .iter()
            .find(|view| view.account_id == VAULT_ONLY)
            .unwrap();
        assert!(unread.quota.is_none());
        let gaps = &build.context["gaps"];
        assert_eq!(gaps["insula_only"][0]["account_id"], INSULA_ONLY);
        assert_eq!(gaps["vault_only"][0]["account_id"], VAULT_ONLY);
        assert_eq!(gaps["vault_only"][0]["first_reading_overdue"], true);
        let alarms = build.context["alarms"].as_array().unwrap();
        assert!(alarms
            .iter()
            .any(|alarm| alarm.as_str().unwrap().contains(VAULT_ONLY)));
        assert!(
            !build.context.to_string().contains("@example.test"),
            "emails stay hidden"
        );

        // The shadow record carries the same gaps.
        let ctx = fixture.ctx(false, true, NOW);
        shadow_pass(&ctx, ShadowTrigger::Timer).unwrap();
        let row = &fixture.shadow_rows()[0];
        let context: Value = serde_json::from_str(row.context_json.as_deref().unwrap()).unwrap();
        assert_eq!(
            context["roster"]["gaps"]["insula_only"][0]["account_id"],
            INSULA_ONLY
        );

        // A roster past 24 hours is still passed through, marked, so existing
        // affinity continues while new cold work is refused.
        let old = {
            let guard = fixture.accounts.lock().unwrap();
            allocator_roster(&guard, "anthropic", NOW + 24 * 60 * 60_000)
        };
        assert_eq!(old.context["past_max_age"], true);
        let ctx = fixture.ctx(true, false, NOW + 24 * 60 * 60_000);
        let reply = fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-old", "k-old", "broca"),
        );
        assert_eq!(reply.code(), Some("account_selection_unavailable"));
    }

    #[test]
    fn a_served_pin_survives_a_reopen_and_renewal_compare_and_sets_on_it() {
        let mut fixture = fixture("serve");
        let ctx = fixture.ctx(true, false, NOW);
        let params = admit_params("adm-1", "opencode:host:session-1", "anthropic-auth");
        let reply = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
        assert_eq!(
            reply["admission"],
            json!({ "id": "adm-1", "state": "reserved" })
        );
        assert_eq!(reply["account"]["provider"], "anthropic");
        assert_eq!(reply["account"]["refresh_adapter"], "anthropic");
        assert_eq!(reply["account"]["method"], "oauth");
        assert_eq!(reply["pin"]["generation"], 1);
        assert_eq!(reply["pin"]["mode"], "conversation_lease");
        assert_eq!(
            reply["pin"]["admission_valid_until"],
            "2026-09-29T12:01:00Z"
        );
        assert_eq!(reply["pin"]["lease_valid_until"], "2026-09-29T12:02:00Z");
        assert_eq!(reply["pin"]["affinity_expires_at"], "2026-10-06T12:00:00Z");
        assert_eq!(reply["reason"]["code"], "cold_scored");
        assert_eq!(reply["reason"]["policy_version"], 2);
        assert_eq!(reply["reason"]["quota_fetched_at"], "2026-09-29T11:59:00Z");
        assert_eq!(reply["reason"]["quota_age_band"], "full");
        let pin_id = reply["pin"]["id"].as_str().unwrap().to_string();
        let account = reply["account"]["account_id"].as_str().unwrap().to_string();

        // A transport retry gets the committed decision back; reusing the id
        // with other parameters is a conflict.
        let again = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
        assert_eq!(again, reply);
        let mut changed = params.clone();
        changed["initial_input_bytes"] = json!(99);
        let conflict = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, changed);
        assert_eq!(conflict.code(), Some("admission_conflict"));

        fixture.reopen();
        let later = NOW + 30_000;
        let ctx = fixture.ctx(true, false, later);
        let mut renew = admit_params("adm-2", "opencode:host:session-1", "anthropic-auth");
        renew["mode"] = json!("renew");
        renew["expected_pin"] = json!({ "id": pin_id, "generation": 1 });
        let renewed = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, renew.clone()));
        assert_eq!(
            renewed["pin"]["id"],
            json!(pin_id),
            "the pin survived the reopen"
        );
        assert_eq!(renewed["pin"]["generation"], 1);
        assert_eq!(renewed["account"]["account_id"], json!(account));
        assert_eq!(renewed["reason"]["code"], "sticky_hit");
        let renewal = fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, "adm-2"))
            .unwrap()
            .unwrap();
        assert!(!renewal.charged, "a renewal charges no demand");

        // A renewal naming an older generation gets the current pin back and
        // changes nothing.
        let mut stale = renew.clone();
        stale["admission_id"] = json!("adm-3");
        stale["expected_pin"] = json!({ "id": pin_id, "generation": 0 });
        let refused = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, stale);
        assert_eq!(refused.code(), Some("pin_generation_conflict"));
        let OpReply::Error { detail, .. } = refused else {
            unreachable!()
        };
        assert_eq!(detail.unwrap()["current_pin"]["id"], json!(pin_id));
        assert!(fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, "adm-3"))
            .unwrap()
            .is_none());

        // Another principal's conversation with the same key is its own.
        let other = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            "reserved:other",
            admit_params("adm-1", "opencode:host:session-1", "anthropic-auth"),
        ));
        assert_ne!(other["pin"]["id"], json!(pin_id));
    }

    #[test]
    fn observations_and_releases_act_only_on_the_principals_own_admission() {
        let fixture = fixture("observe");
        let ctx = fixture.ctx(true, false, NOW);
        let admitted = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-1", "broca:run-1", "broca"),
        ));
        let account = admitted["account"]["account_id"].clone();
        let exhausted = |event: &str| {
            json!({
                "event_id": event,
                "caller": { "runner": "broca" },
                "kind": "account_limit_exhausted",
                "admission_id": "adm-1",
                "limit_window": "seven_day",
                "resets_at_ms": NOW + 3_600_000,
                "resets_at_source": "header",
                "observed_at_ms": NOW + 1_000,
            })
        };
        // Another principal naming Broca's admission changes nothing shared.
        let foreign = result(fixture.call(
            &ctx,
            ACCOUNT_OBSERVE_OP,
            "reserved:plugin",
            exhausted("e-1"),
        ));
        assert_eq!(foreign["observation"]["bound"], false);
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
        let foreign_release = result(fixture.call(
            &ctx,
            ACCOUNT_RELEASE_OP,
            "reserved:plugin",
            json!({ "admission_id": "adm-1" }),
        ));
        assert_eq!(foreign_release["release"]["released"], false);

        let bound = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("e-2")));
        assert_eq!(bound["observation"]["bound"], true);
        assert_eq!(bound["observation"]["effects"][0]["effect"], "fence");
        assert_eq!(
            bound["observation"]["effects"][0]["fence"]["account_id"],
            account
        );
        // The same event again is recognized and changes nothing.
        let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("e-2")));
        assert_eq!(repeat["observation"]["duplicate"], true);
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);

        // A new cold admission now avoids the fenced account.
        let next = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-2", "broca:run-2", "broca"),
        ));
        assert_ne!(next["account"]["account_id"], account);

        // Release happens once, and never after the run started.
        let first = result(fixture.call(
            &ctx,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-2" }),
        ));
        assert_eq!(first["release"]["released"], true);
        assert_eq!(first["release"]["state"], "released");
        let second = result(fixture.call(
            &ctx,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-2" }),
        ));
        assert_eq!(second["release"]["released"], false);
        let late = fixture.ctx(true, false, NOW + 120_000);
        let started = result(fixture.call(
            &late,
            ACCOUNT_OBSERVE_OP,
            BROCA,
            json!({ "event_id": "s-1", "caller": { "runner": "broca" }, "kind": "admission_started",
                    "admission_id": "adm-1", "run_id": "run-1" }),
        ));
        assert_eq!(
            started["observation"]["effects"][0],
            json!({ "effect": "admission_started", "late": true })
        );
        let after_start = result(fixture.call(
            &late,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-1" }),
        ));
        assert_eq!(after_start["release"]["released"], false);
        assert_eq!(after_start["release"]["state"], "started");
    }

    #[test]
    fn rfc3339_formats_whole_seconds_in_utc() {
        assert_eq!(rfc3339(NOW), "2026-09-29T12:00:00Z");
        assert_eq!(
            rfc3339(NOW + 7 * 24 * 3_600_000 + 999),
            "2026-10-06T12:00:00Z"
        );
    }
}
