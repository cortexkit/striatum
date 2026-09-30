//! Inputs to the account allocator: the admission request, the per-account
//! views, conversation affinity, confirmed-exhaustion fences, per-principal
//! refusal exclusions, admission demand, the operator override and policy.
//!
//! Field names follow `docs/designs/account-selection.md` ("Proposed protocol",
//! "New-session rule", "Exhaustion is evidence", "Insula input"). Times are
//! epoch milliseconds on routing's clock unless a name says otherwise.

use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch.
pub type EpochMs = i64;

/// One of the two general subscription windows. Claude publishes both; the
/// ChatGPT (Codex) feed publishes only the weekly one, which maps to
/// `SevenDay` (design, "Insula input": Codex primary = 10080 minutes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneralWindow {
    FiveHour,
    SevenDay,
}

impl GeneralWindow {
    /// The window's own length, used as the weighting horizon when no future
    /// reset is known (design step 5).
    pub fn window_minutes(self) -> u32 {
        match self {
            GeneralWindow::FiveHour => 300,
            GeneralWindow::SevenDay => 10_080,
        }
    }
}

/// Model family for scoped quota. Scopes are per family, not per model
/// spelling: Fable 5 and 5.1 (including `[1m]` variants) share one Fable scope,
/// and Fable exhaustion never excludes Mythos or Opus (design step 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFamily {
    Fable,
    Mythos,
    Opus,
    /// Any model the policy's family map does not place in a scoped family.
    Other,
}

/// A reset time as the producer reported it. `Unparsable` and `Missing` stay
/// distinct so the explanation can say which; both behave as "no reset" for
/// weighting and both migrate rather than hold (design, short hold).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "at_ms")]
pub enum ResetTime {
    At(EpochMs),
    Unparsable,
    Missing,
}

impl ResetTime {
    pub fn at_ms(self) -> Option<EpochMs> {
        match self {
            ResetTime::At(at) => Some(at),
            ResetTime::Unparsable | ResetTime::Missing => None,
        }
    }
}

/// Where a window's value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationSource {
    /// Normalized from an upstream response's `anthropic-ratelimit-unified-*`
    /// headers and submitted by a runner, directly or through the passive feed.
    Header,
    /// An authoritative usage-endpoint poll (Insula's normal reading).
    Poll,
    /// A header-derived reading carried through Insula rather than polled.
    Insula,
}

/// One general window of an account's reading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaWindow {
    /// Normalized percent used, 0-100. For header readings this is the
    /// reference's rounded value, so a raw 0.995 reads as 100.
    pub used_percent: f64,
    /// The original raw utilization fraction before normalization, when the
    /// value is header-derived. Only this value can confirm exhaustion; the
    /// allocator itself never confirms from a view (see `evidence`).
    pub raw_utilization: Option<f64>,
    pub resets_at: ResetTime,
    pub window_minutes: u32,
    pub source: ObservationSource,
}

/// One producer-published scoped window (Insula `extraRateWindows[]`). It is
/// keyed by producer scope id plus `window_minutes`, never by title.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScopedWindow {
    pub scope_id: String,
    pub window_minutes: u32,
    /// Percent used. May be non-finite when the producer sent garbage; a
    /// non-finite value is not exhaustion and never joins the weight.
    pub used_percent: f64,
    pub resets_at: ResetTime,
}

/// The entry's scoped list. A missing list and an explicitly empty list both
/// pass the scope check, but they stay distinguishable in the explanation
/// (design step 3; Port fidelity, absent scope).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "windows")]
pub enum ScopedList {
    /// The entry carries no scoped metadata at all (for example a header-only
    /// reading).
    Missing,
    /// The entry lists its scoped windows; an empty vector is an explicit `[]`.
    Listed(Vec<ScopedWindow>),
}

/// Insula's `stale` disclosure: a preserved reading served through a transient
/// failure. It is not an entry-level `errorClass` and does not by itself
/// exclude the entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StaleDisclosure {
    pub since_ms: Option<EpochMs>,
    /// Uses the `errorClass` vocabulary; may be absent.
    pub class: Option<String>,
}

/// One account's quota reading as routing holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaReading {
    /// The account's observation point: the Insula entry's `fetchedAt`, or the
    /// time of a newer accepted complete header reading (design step 6). Age,
    /// the age band and pending demand are all measured from it. `None` when
    /// the producer sent no usable timestamp.
    pub observed_at_ms: Option<EpochMs>,
    /// Entry-level `errorClass`: a degraded entry with no usable reading.
    pub error_class: Option<String>,
    pub stale: Option<StaleDisclosure>,
    pub five_hour: Option<QuotaWindow>,
    pub seven_day: Option<QuotaWindow>,
    pub scoped: ScopedList,
}

impl QuotaReading {
    pub fn window(&self, window: GeneralWindow) -> Option<&QuotaWindow> {
        match window {
            GeneralWindow::FiveHour => self.five_hour.as_ref(),
            GeneralWindow::SevenDay => self.seven_day.as_ref(),
        }
    }
}

/// One vault credential record for an account. An account may have several.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRecord {
    /// Opaque vault credential id, returned verbatim (for example
    /// `oauth:anthropic:ysample`).
    pub credential_id: String,
    /// True for the provider's default record (`oauth:anthropic`), which wins
    /// the deterministic record binding when the runner does not name one.
    pub default_record: bool,
    pub needs_reauth: bool,
}

/// Which models an account can serve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "models")]
pub enum ModelReach {
    /// No restriction is known.
    All,
    /// Only these normalized model ids.
    Only(Vec<String>),
}

impl ModelReach {
    pub fn reaches(&self, model: &str) -> bool {
        match self {
            ModelReach::All => true,
            ModelReach::Only(models) => models.iter().any(|m| m == model),
        }
    }
}

/// An account's view: stable identity joined with its credential records and
/// its quota reading. Capacity is keyed by `(refresh_adapter, account_id)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountView {
    /// Opaque vault account UUID, compared for equality only.
    pub account_id: String,
    pub refresh_adapter: String,
    pub credentials: Vec<CredentialRecord>,
    pub model_reach: ModelReach,
    /// `None` when routing holds no reading for this account.
    pub quota: Option<QuotaReading>,
}

/// Routing's advisory roster: the last successful vault listing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Roster {
    pub view_digest: String,
    pub fetched_at_ms: EpochMs,
    pub accounts: Vec<AccountView>,
}

/// A conversation's affinity row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationAffinity {
    pub pin_id: String,
    pub account_id: String,
    /// The credential binding stored with the affinity.
    pub credential_id: String,
    /// The user-selected model this affinity is bound to. Internal safety
    /// recovery keeps it; a user model switch starts a new generation.
    pub affinity_model: String,
    pub pin_generation: u64,
    /// Idle expiry.
    pub expires_at_ms: EpochMs,
    /// An active conversation lease keeps affinity alive past idle expiry.
    pub lease_active: bool,
}

/// What a fence covers on its account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum FenceScope {
    /// A general window. Applies to every model family.
    General(GeneralWindow),
    /// A family's scoped window (Fable). Applies only to that family.
    Family(ModelFamily),
    /// `account_limit_exhausted` with no `limit_window`: blocks the requested
    /// family only (design, exhaustion table).
    UnknownScope(ModelFamily),
}

impl FenceScope {
    /// Whether this fence restricts work of `family`.
    pub fn applies_to(self, family: ModelFamily) -> bool {
        match self {
            FenceScope::General(_) => true,
            FenceScope::Family(f) | FenceScope::UnknownScope(f) => f == family,
        }
    }
}

/// Which confirmation source created a fence ("Exhaustion is evidence").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationSource {
    /// Source 1: Broca's typed `account_limit_exhausted` for the frozen account.
    BrocaAccountLimit,
    /// Source 2 with authoritative poll-sourced exhaustion observed after the 429.
    PostRateLimitPoll,
    /// Source 2 with raw-qualified header evidence observed after the 429.
    PostRateLimitHeader,
    /// Source 3: a genuine served response whose own raw utilization is >= 1.0.
    RawHeader,
}

/// Confirmed exhaustion on one scope of one account. Only bound, confirmed
/// evidence creates one (see `evidence::confirm_exhaustion`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fence {
    pub account_id: String,
    pub scope: FenceScope,
    /// Original observation time of the confirming evidence.
    pub observed_at_ms: EpochMs,
    /// A header reset. A Retry-After-derived time is never a reset: it is kept
    /// in `retry_hint_ms` and the fence has unknown recovery.
    pub reset: ResetTime,
    pub retry_hint_ms: Option<EpochMs>,
    pub source: ConfirmationSource,
}

/// A per-principal refusal exclusion of one credential, recorded when a
/// runner's own read was refused. It never affects other principals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalExclusion {
    pub principal: String,
    pub credential_id: String,
    /// The runner's `list_scoped` view digest reported with the refusal.
    pub runner_view_digest: String,
    /// Routing's own roster digest when the refusal was recorded.
    pub roster_view_digest: String,
    pub recorded_at_ms: EpochMs,
}

/// Lifecycle state of a committed admission (design, Admission lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionState {
    Reserved,
    Started,
    Released,
    Expired,
    Finished,
}

/// The demand a committed admission charged to its account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionDemand {
    pub account_id: String,
    pub state: AdmissionState,
    pub committed_at_ms: EpochMs,
    pub request_bytes: u64,
}

/// A reference to an account, as a caller or operator names one. Never a label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRef {
    AccountId(String),
    CredentialId(String),
}

/// The routing-owned operator override that applies to this request, already
/// resolved by provider (and caller, when scoped) by the caller of `decide`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorOverride {
    pub target: AccountRef,
}

/// One runner-listed credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateRef {
    pub account_id: String,
    pub credential_id: String,
}

/// The runner's own `list_scoped` view for this provider. It only narrows the
/// choice; it is never evidence of a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerCandidates {
    pub view_digest: String,
    pub accounts: Vec<CandidateRef>,
}

/// Who is asking. Broca runs are frozen for their whole life, so Broca cold
/// admissions need the extra spendable floor (design step 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallerKind {
    Broca,
    Plugin,
}

/// Whether the caller supplied the conversation key or Broca fell back to
/// `broca:<run_id>`, which has no affinity across sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationKeySource {
    Caller,
    BrocaRunFallback,
}

/// One `route.account_for` request, as the allocator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountRequest {
    /// The transport-authenticated principal.
    pub principal: String,
    pub provider: String,
    /// The model this admission will run.
    pub model: String,
    /// The user-selected model the affinity is bound to.
    pub affinity_model: String,
    pub conversation_key: String,
    pub conversation_key_source: ConversationKeySource,
    pub caller: CallerKind,
    /// True when `model` was reached through internal safety recovery rather
    /// than requested by the caller; such work is not "direct" (step 7).
    pub via_safety_recovery: bool,
    pub initial_input_bytes: u64,
    pub initial_input_estimated: bool,
    /// `None` means the runner sent no list: no narrowing.
    pub candidates: Option<RunnerCandidates>,
    pub requested_account: Option<AccountRef>,
    /// A plugin's one-time import hint; the caller of `decide` passes it only
    /// when an import is still permitted for this conversation.
    pub preferred_existing_account: Option<AccountRef>,
    pub hold_wait_max_s: u64,
}

/// Maps a producer scope id plus window length to a model family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeIdMapping {
    pub scope_id: String,
    pub window_minutes: u32,
    pub family: ModelFamily,
}

/// Maps a normalized API model id to its family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFamilyMapping {
    pub model: String,
    pub family: ModelFamily,
}

/// Routing policy for account selection. `Default` carries the design's
/// initial values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocatorPolicy {
    pub policy_version: u32,
    /// Reserve on the general 5h window, in percentage points (live sidecar value 1).
    pub reserve_five_hour: f64,
    /// Reserve on the general 7d window, in percentage points.
    pub reserve_seven_day: f64,
    /// Reserve on a family's scoped window (0, as the reference outside its killswitch).
    pub reserve_scoped: f64,
    /// Points that must stay spendable on every applicable general window for
    /// a Broca cold admission.
    pub broca_floor_points: f64,
    /// Request bytes charged when the caller could not measure its prompt.
    pub estimated_request_bytes: u64,
    /// How long upstream usage figures trail consumption.
    pub reflection_lag_ms: i64,
    /// Divisor applied to a direct-Opus score on a freshly Fable-exhausted account.
    pub fable_preservation_factor: f64,
    /// A honoured `requested_account` must score within this factor of the best.
    pub requested_account_factor: f64,
    /// Principals granted `account_request` (none by default).
    pub account_request_principals: Vec<String>,
    /// Tie-break order, before account UUID byte order.
    pub account_order: Vec<String>,
    /// Longest confirmed-5h reset distance that still holds.
    pub short_hold_max_ms: i64,
    /// Readings younger than this are in the full band.
    pub full_band_ms: i64,
    /// Readings at least this old are in the excluded band.
    pub excluded_band_ms: i64,
    /// Age factor at the end of the discounted band.
    pub discounted_floor_factor: f64,
    /// A timestamp further ahead of routing's clock than this is clock-invalid.
    pub future_skew_ms: i64,
    /// How long a persisted roster serves cold admissions through a vault outage.
    pub roster_max_age_ms: i64,
    /// Cap on a per-principal refusal exclusion.
    pub principal_exclusion_max_ms: i64,
    /// Base Retry-After for `account_quota_unknown`, before jitter.
    pub quota_unknown_retry_s: u64,
    /// Added to the earliest relevant reset for pool refusals.
    pub pool_retry_margin_s: u64,
    /// Pool refusal Retry-After when no reset is known.
    pub pool_retry_fallback_s: u64,
    pub model_families: Vec<ModelFamilyMapping>,
    pub scope_ids: Vec<ScopeIdMapping>,
}

impl Default for AllocatorPolicy {
    fn default() -> Self {
        let family = |model: &str, family| ModelFamilyMapping {
            model: model.to_string(),
            family,
        };
        Self {
            policy_version: 2,
            reserve_five_hour: 1.0,
            reserve_seven_day: 1.0,
            reserve_scoped: 0.0,
            broca_floor_points: 5.0,
            estimated_request_bytes: 32 * 1024,
            reflection_lag_ms: 60_000,
            fable_preservation_factor: 2.0,
            requested_account_factor: 2.0,
            account_request_principals: Vec::new(),
            account_order: Vec::new(),
            short_hold_max_ms: 15 * 60_000,
            full_band_ms: 5 * 60_000,
            excluded_band_ms: 20 * 60_000,
            discounted_floor_factor: 0.25,
            future_skew_ms: 60_000,
            roster_max_age_ms: 24 * 60 * 60_000,
            principal_exclusion_max_ms: 24 * 60 * 60_000,
            quota_unknown_retry_s: 60,
            pool_retry_margin_s: 60,
            pool_retry_fallback_s: 300,
            model_families: vec![
                family("claude-fable-5", ModelFamily::Fable),
                family("claude-fable-5[1m]", ModelFamily::Fable),
                family("claude-fable-5-1", ModelFamily::Fable),
                family("claude-fable-5-1[1m]", ModelFamily::Fable),
                family("claude-mythos-5-1", ModelFamily::Mythos),
                family("claude-mythos-5-1[1m]", ModelFamily::Mythos),
                family("claude-opus-5-5", ModelFamily::Opus),
                family("claude-opus-5-5[1m]", ModelFamily::Opus),
            ],
            // Insula publishes the Fable window's id as upstream
            // `limits[].scope.model.id` when available, else the display name,
            // which today is `Fable`.
            scope_ids: vec![ScopeIdMapping {
                scope_id: "Fable".to_string(),
                window_minutes: 10_080,
                family: ModelFamily::Fable,
            }],
        }
    }
}

impl AllocatorPolicy {
    pub fn family_for_model(&self, model: &str) -> ModelFamily {
        self.model_families
            .iter()
            .find(|mapping| mapping.model == model)
            .map(|mapping| mapping.family)
            .unwrap_or(ModelFamily::Other)
    }

    /// The family a producer scope window belongs to, or `None` when routing
    /// cannot map its id.
    pub fn family_for_scope(&self, window: &ScopedWindow) -> Option<ModelFamily> {
        self.scope_ids
            .iter()
            .find(|m| m.scope_id == window.scope_id && m.window_minutes == window.window_minutes)
            .map(|m| m.family)
    }

    /// Whether any scope mapping exists for `family`.
    pub fn family_has_scope(&self, family: ModelFamily) -> bool {
        self.scope_ids.iter().any(|m| m.family == family)
    }
}

/// Everything routing holds that the decision reads. The caller of `decide`
/// loads it inside the selection transaction; `decide` performs no I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocatorState {
    /// `None` at a boot with no persisted roster.
    pub roster: Option<Roster>,
    /// This conversation's affinity under this caller and affinity model, if any.
    pub affinity: Option<ConversationAffinity>,
    pub fences: Vec<Fence>,
    pub principal_exclusions: Vec<PrincipalExclusion>,
    pub admissions: Vec<AdmissionDemand>,
    pub operator_override: Option<OperatorOverride>,
    pub policy: AllocatorPolicy,
}
