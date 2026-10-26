//! Shared builders for the account allocator tests. Every test uses the golden
//! fixtures' fixed clock, 2026-09-29T12:00:00Z, and fake vault identities.

#![allow(dead_code)]

use prefrontal_routing_core::accounts::{
    AccountRequest, AccountView, AdmissionDemand, AdmissionState, AllocatorPolicy, AllocatorState,
    CallerKind, CandidateRef, ConversationAffinity, ConversationKeySource, CredentialRecord,
    EpochMs, Fence, FenceScope, GeneralWindow, ModelReach, ObservationSource, QuotaReading,
    ModelFamily, ModelFamilyMapping, QuotaWindow, ResetTime, Roster, RunnerCandidates, ScopedList,
    ScopedWindow,
};

/// The golden fixtures' clock: 2026-09-29T12:00:00Z.
pub const NOW: EpochMs = 1_790_683_200_000;
pub const MINUTE: i64 = 60_000;
pub const HOUR: i64 = 60 * MINUTE;

pub fn credential_id(account: &str) -> String {
    format!("oauth:anthropic:{account}")
}

pub fn window(used_percent: f64, resets_at: ResetTime, minutes: u32) -> QuotaWindow {
    QuotaWindow {
        used_percent,
        raw_utilization: None,
        resets_at,
        window_minutes: minutes,
        window_kind: None,
        source: ObservationSource::Poll,
    }
}

/// A full-band reading observed at `NOW` with the 5h window resetting in two
/// hours and the 7d window in three days.
pub fn reading(five_used: f64, seven_used: f64) -> QuotaReading {
    QuotaReading {
        observed_at_ms: Some(NOW),
        error_class: None,
        stale: None,
        five_hour: Some(window(five_used, ResetTime::At(NOW + 2 * HOUR), 300)),
        seven_day: Some(window(seven_used, ResetTime::At(NOW + 72 * HOUR), 10_080)),
        general_windows: None,
        scoped: ScopedList::Missing,
        pool_windows: None,
    }
}

pub fn fable_window(used_percent: f64) -> ScopedWindow {
    ScopedWindow {
        scope_id: "Fable".into(),
        window_minutes: 10_080,
        used_percent,
        resets_at: ResetTime::At(NOW + 96 * HOUR),
    }
}

pub fn account(id: &str, quota: Option<QuotaReading>) -> AccountView {
    AccountView {
        account_id: id.into(),
        routing_provider: "anthropic".into(),
        refresh_adapter: "anthropic".into(),
        credentials: vec![CredentialRecord {
            credential_id: credential_id(id),
            default_record: false,
            needs_reauth: false,
        }],
        model_reach: ModelReach::All,
        quota,
    }
}

pub fn state(accounts: Vec<AccountView>) -> AllocatorState {
    AllocatorState {
        roster: Some(Roster {
            view_digest: "roster-1".into(),
            fetched_at_ms: NOW - MINUTE,
            accounts,
        }),
        affinity: None,
        fences: Vec::new(),
        principal_exclusions: Vec::new(),
        admissions: Vec::new(),
        operator_override: None,
        policy: policy(),
    }
}

/// The default policy with the model-scoped quota families operator config
/// carries (`accounts.model_families`, written by the config converter).
pub fn policy() -> AllocatorPolicy {
    let family = |model: &str, family| ModelFamilyMapping {
        model: model.to_string(),
        family,
    };
    AllocatorPolicy {
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
        ..AllocatorPolicy::default()
    }
}

pub fn request() -> AccountRequest {
    AccountRequest {
        principal: "plugin:anthropic-auth:host-a".into(),
        provider: "anthropic".into(),
        model: "claude-sonnet-5".into(),
        affinity_model: "claude-sonnet-5".into(),
        conversation_key: "ses-golden-a".into(),
        conversation_key_source: ConversationKeySource::Caller,
        caller: CallerKind::Plugin,
        via_safety_recovery: false,
        initial_input_bytes: 1000,
        initial_input_estimated: false,
        candidates: None,
        requested_account: None,
        preferred_existing_account: None,
        hold_wait_max_s: 900,
        pool: None,
    }
}

pub fn with_model(mut request: AccountRequest, model: &str) -> AccountRequest {
    request.model = model.into();
    request.affinity_model = model.into();
    request
}

pub fn affinity(account: &str, model: &str, generation: u64) -> ConversationAffinity {
    ConversationAffinity {
        pin_id: format!("pin-{account}"),
        account_id: account.into(),
        credential_id: credential_id(account),
        affinity_model: model.into(),
        pin_generation: generation,
        expires_at_ms: NOW + 7 * 24 * HOUR,
        lease_active: false,
    }
}

/// A block keyed by the pool a pre-pool fence scope migrates to, confirmed at
/// `NOW`. A reading observed at the same instant never lifts it, so the
/// builders' readings (observed at `NOW` or earlier) leave it standing.
pub fn fence(account: &str, scope: FenceScope, reset: ResetTime) -> Fence {
    Fence {
        account_id: account.into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW,
        reset,
        retry_hint_ms: None,
        source: prefrontal_routing_core::accounts::ConfirmationSource::BrocaAccountLimit,
    }
}

pub fn five_hour_fence(account: &str, reset: ResetTime) -> Fence {
    fence(account, FenceScope::General(GeneralWindow::FiveHour), reset)
}

pub fn candidates(pairs: &[&str]) -> RunnerCandidates {
    RunnerCandidates {
        view_digest: "runner-view-1".into(),
        accounts: pairs
            .iter()
            .map(|id| CandidateRef {
                account_id: (*id).into(),
                credential_id: credential_id(id),
            })
            .collect(),
    }
}

pub fn demand(
    account: &str,
    state: AdmissionState,
    committed_at_ms: EpochMs,
    bytes: u64,
) -> AdmissionDemand {
    AdmissionDemand {
        account_id: account.into(),
        state,
        committed_at_ms,
        request_bytes: bytes,
    }
}
