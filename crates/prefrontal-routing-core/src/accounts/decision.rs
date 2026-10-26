//! The allocator's output: an admission, a short hold or a typed refusal,
//! always with a machine-readable explanation of why this account was chosen
//! and what was excluded and why.

use serde::{Deserialize, Serialize};

use super::pools::PoolKey;
use super::types::{CandidateRef, ConversationKeySource, EpochMs, GeneralWindow, ModelFamily};

/// Reading age band (design, "Insula input and conservative degraded behavior").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgeBand {
    /// Under 5 minutes: age factor 1.
    Full,
    /// 5 to 20 minutes: age factor falls linearly from 1 to 0.25.
    Discounted,
    /// 20 minutes or more: unknown for cold work.
    Excluded,
}

/// A window that contributes to an account's weight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicableWindow {
    FiveHour,
    SevenDay,
    Reported(u32),
    /// The requested family's scoped window (Fable).
    FamilyScope,
}

impl From<GeneralWindow> for ApplicableWindow {
    fn from(window: GeneralWindow) -> Self {
        match window {
            GeneralWindow::FiveHour => ApplicableWindow::FiveHour,
            GeneralWindow::SevenDay => ApplicableWindow::SevenDay,
            GeneralWindow::Reported(minutes) => ApplicableWindow::Reported(minutes),
        }
    }
}

/// Whether the entry's scoped list was missing, explicitly empty or present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopedListState {
    Missing,
    ExplicitEmpty,
    Present,
}

/// The step-6 tier an exclusion belongs to, used to pick the no-candidate
/// reply code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionTier {
    /// Model reach: the account cannot serve the requested model.
    Reach,
    /// The runner's `candidates` list does not name the account.
    Candidacy,
    Principal,
    Reauth,
    Fence,
    QuotaUnknown,
    Reserve,
}

/// Why one account was excluded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum ExclusionReason {
    UnsupportedModel,
    NotInRunnerCandidates,
    /// Every runner-listed record is refused for this principal.
    PrincipalExcluded {
        credential_ids: Vec<String>,
    },
    /// Every remaining record needs re-authorization.
    ReauthRequired,
    /// A standing block on a pool the requested model draws on; `pool` is
    /// `None` for a block on the whole account.
    ConfirmedFence {
        pool: Option<PoolKey>,
    },
    QuotaMissing,
    EntryErrorClass {
        error_class: String,
    },
    /// A `stale.class` other than `upstream_failed`, which routing's mapping
    /// does not cover yet.
    StaleClassUnknown {
        class: String,
    },
    TimestampMissing,
    /// `observedAt` more than the allowed skew ahead of routing's clock.
    ClockInvalid {
        observed_at_ms: EpochMs,
    },
    AgeExcluded {
        age_ms: i64,
    },
    GeneralWindowMissing {
        window: GeneralWindow,
    },
    /// A percentage outside 0-100 (or non-finite) makes the entry invalid.
    InvalidPercent {
        window: ApplicableWindow,
    },
    /// The old period ended; its usage says nothing about the current period.
    WindowResetElapsed {
        window: ApplicableWindow,
    },
    /// The family's known scope id is absent and an unmapped scoped window is
    /// present, so the family's scope is unknown.
    ScopeUnmapped {
        family: ModelFamily,
        unmapped_scope_ids: Vec<String>,
    },
    /// The family's scoped window reads 0 or less remaining (a reading, not a fence).
    ScopeExhausted {
        family: ModelFamily,
    },
    /// Zero weight: nothing spendable above the reserve on these windows.
    BelowReserve {
        windows: Vec<ApplicableWindow>,
    },
    /// A Broca cold admission needs `broca_floor_points` spendable.
    BelowBrocaFloor {
        window: GeneralWindow,
        spendable: f64,
    },
}

impl ExclusionReason {
    pub fn tier(&self) -> ExclusionTier {
        use ExclusionReason::*;
        match self {
            UnsupportedModel => ExclusionTier::Reach,
            NotInRunnerCandidates => ExclusionTier::Candidacy,
            PrincipalExcluded { .. } => ExclusionTier::Principal,
            ReauthRequired => ExclusionTier::Reauth,
            ConfirmedFence { .. } => ExclusionTier::Fence,
            QuotaMissing
            | EntryErrorClass { .. }
            | StaleClassUnknown { .. }
            | TimestampMissing
            | ClockInvalid { .. }
            | AgeExcluded { .. }
            | GeneralWindowMissing { .. }
            | InvalidPercent { .. }
            | WindowResetElapsed { .. }
            | ScopeUnmapped { .. } => ExclusionTier::QuotaUnknown,
            ScopeExhausted { .. } | BelowReserve { .. } | BelowBrocaFloor { .. } => {
                ExclusionTier::Reserve
            }
        }
    }
}

/// One window's contribution to an account's weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowWeight {
    pub window: ApplicableWindow,
    pub used_percent: f64,
    pub reserve: f64,
    pub spendable: f64,
    pub horizon_hours: f64,
    pub weight: f64,
    /// The reset had passed but the reading predates it: the old spendable is
    /// spread over one full window, which can only undercount headroom.
    pub reset_passed: bool,
}

/// Everything the allocator determined about one account.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountNote {
    pub account_id: String,
    /// The record this admission would bind, when one is usable.
    pub credential_id: Option<String>,
    pub exclusions: Vec<ExclusionReason>,
    pub scoped_list: Option<ScopedListState>,
    pub age_band: Option<AgeBand>,
    pub age_factor: Option<f64>,
    pub window_weights: Vec<WindowWeight>,
    pub weight: Option<f64>,
    pub pending_bytes: Option<u64>,
    /// `(pending_bytes + request_bytes) / weight`, after any Fable preservation.
    pub score: Option<f64>,
    /// The score was divided by the Fable preservation factor (step 7).
    pub fable_preserved: bool,
}

impl AccountNote {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            credential_id: None,
            exclusions: Vec::new(),
            scoped_list: None,
            age_band: None,
            age_factor: None,
            window_weights: Vec::new(),
            weight: None,
            pending_bytes: None,
            score: None,
            fable_preserved: false,
        }
    }
}

/// Why an established conversation left its account.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum InvalidationReason {
    UserModelChange,
    IdleExpired,
    /// The account or credential is gone from the roster.
    IdentityWithdrawn,
    /// The credential id now belongs to a different account.
    CredentialRebound,
    UnsupportedModel,
    NotInRunnerCandidates,
    PrincipalExcluded,
    ReauthRequired,
    /// A confirmed block on an applicable pool (`None`: the whole account)
    /// whose reset is unknown or too far away to hold for.
    ConfirmedExhaustion {
        pool: Option<PoolKey>,
    },
    /// A full-band reading shows an applicable window at 100% used. This is a
    /// conversation-local move; it creates no fence.
    FreshFullReading {
        window: ApplicableWindow,
    },
    /// A confirmed short hold whose jittered Retry-After exceeds the caller's
    /// `hold_wait_max_s`.
    HoldExceedsWait {
        retry_after_s: u64,
    },
    /// An operator override named another account.
    OperatorOverride,
}

/// What happened to the conversation's existing affinity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AffinityNote {
    None,
    Retained {
        account_id: String,
        /// Quota reasons the account would be excluded for cold work; affinity
        /// continues under them with this degraded reason.
        degraded: Vec<ExclusionReason>,
    },
    Held {
        account_id: String,
    },
    Invalidated {
        account_id: String,
        reason: InvalidationReason,
    },
}

/// The outcome of a `preferred_existing_account` import.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ImportNote {
    Accepted {
        account_id: String,
    },
    Rejected {
        reasons: Vec<ExclusionReason>,
        /// True when the named account is not in routing's usable roster.
        not_in_roster: bool,
    },
}

/// The outcome of the operator override.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OverrideNote {
    Applied {
        account_id: String,
    },
    /// Failed a hard filter, visibly; no other account was picked.
    Rejected {
        reasons: Vec<ExclusionReason>,
        not_in_roster: bool,
    },
}

/// Why a `requested_account` was ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedAccountIgnored {
    /// The principal has no `account_request` grant.
    NotGranted,
    NotEligible,
    /// Its score is more than the policy factor times the best score.
    OutsideFactor,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RequestedAccountNote {
    Honoured { account_id: String },
    Ignored { reason: RequestedAccountIgnored },
}

/// Routing's roster as this decision saw it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RosterNote {
    pub view_digest: String,
    pub fetched_at_ms: EpochMs,
    /// Older than the policy's `roster_max_age_ms`: it no longer serves cold
    /// admissions, though existing affinity continues.
    pub expired: bool,
    pub account_ids: Vec<String>,
}

/// The machine-readable explanation every decision carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Explanation {
    pub policy_version: u32,
    pub provider: String,
    pub model: String,
    pub affinity_model: String,
    pub family: ModelFamily,
    /// Direct Opus work: an Opus-family model the caller requested, not one
    /// reached through safety recovery (step 7).
    pub direct_opus: bool,
    pub conversation_key_source: ConversationKeySource,
    pub request_bytes: u64,
    pub request_bytes_estimated: bool,
    pub roster: Option<RosterNote>,
    pub runner_view_digest: Option<String>,
    pub runner_candidates: Option<Vec<CandidateRef>>,
    pub affinity: AffinityNote,
    /// The conversation's pin before this decision, unchanged. When a move
    /// fails (a refusal), this stays the conversation's historical identity
    /// and credential provenance; it is not authorization to serve, and no
    /// new generation is issued.
    pub previous_pin: Option<PreviousPin>,
    pub import: Option<ImportNote>,
    pub operator_override: Option<OverrideNote>,
    pub requested_account: Option<RequestedAccountNote>,
    /// Per-account evaluation, in roster order.
    pub accounts: Vec<AccountNote>,
}

/// Why the chosen account was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmitReason {
    /// Existing affinity retained.
    StickyHit,
    /// A validated `preferred_existing_account` import became the affinity.
    ImportedAffinity,
    /// Cold scoring (steps 4-7).
    ColdScored,
    RequestedAccount,
    OperatorOverride,
    /// The override admitted an account whose quota is unknown.
    OperatorOverrideUnknownQuota,
}

/// The pin before this decision, when the conversation had one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviousPin {
    pub pin_id: String,
    pub account_id: String,
    pub credential_id: String,
    pub pin_generation: u64,
}

/// What the store should do with the conversation's affinity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AffinityOutcome {
    Retained {
        pin_id: String,
        pin_generation: u64,
    },
    /// A new pin generation: first admission, a move, or a user model switch.
    New {
        previous: Option<PreviousPin>,
        pin_generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Admission {
    pub account_id: String,
    pub credential_id: String,
    pub reason: AdmitReason,
    pub affinity: AffinityOutcome,
    /// Demand this admission charges: `max(1, initial_input_bytes)`, or the
    /// policy estimate when the caller could not measure.
    pub request_bytes: u64,
    pub request_bytes_estimated: bool,
    pub quota_observed_at_ms: Option<EpochMs>,
    pub quota_age_band: Option<AgeBand>,
}

/// `account_hold`: stay on the same account and retry after the reset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hold {
    pub account_id: String,
    pub credential_id: String,
    pub pin_id: String,
    pub pin_generation: u64,
    pub reset_at_ms: EpochMs,
    pub base_retry_after_s: u64,
    /// Base plus the conversation's jitter.
    pub retry_after_s: u64,
    pub basis: HoldBasis,
}

/// What justified a short hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldBasis {
    /// A confirmed block on the account (shared state).
    ConfirmedFence,
    /// A full-band reading with a window at 100% used (possibly a rounded raw
    /// 0.995).
    /// Soft and local to this conversation: it is not confirmation, creates
    /// no fence and grants no post-reset probe; each later admission
    /// re-evaluates it against the newest reading.
    ProactiveReading,
}

/// Typed non-success reply codes the allocator can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalCode {
    AccountSelectionUnavailable,
    AccountSelectionUnsupported,
    AccountNotAuthorized,
    AccountReauthRequired,
    AccountQuotaUnknown,
    AccountPoolReserveLimited,
    AccountPoolExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub code: RefusalCode,
    pub retryable: bool,
    /// `None` when the design names no retry time for the code.
    pub retry_after_s: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Outcome {
    Admit(Admission),
    Hold(Hold),
    Refuse(Refusal),
}

/// The allocator's whole answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub outcome: Outcome,
    pub explanation: Explanation,
}

impl Decision {
    pub fn admission(&self) -> Option<&Admission> {
        match &self.outcome {
            Outcome::Admit(admission) => Some(admission),
            _ => None,
        }
    }

    pub fn hold(&self) -> Option<&Hold> {
        match &self.outcome {
            Outcome::Hold(hold) => Some(hold),
            _ => None,
        }
    }

    pub fn refusal(&self) -> Option<&Refusal> {
        match &self.outcome {
            Outcome::Refuse(refusal) => Some(refusal),
            _ => None,
        }
    }

    pub fn account_note(&self, account_id: &str) -> Option<&AccountNote> {
        self.explanation
            .accounts
            .iter()
            .find(|note| note.account_id == account_id)
    }
}
