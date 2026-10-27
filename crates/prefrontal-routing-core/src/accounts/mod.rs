//! Account selection: the pure allocator behind `route.account_for`.
//!
//! Specification: `docs/designs/account-selection.md` ("New-session rule:
//! sticky-balanced first", "Exhaustion is evidence, not a status-word
//! shortcut", "Insula input and conservative degraded behavior"). This module
//! holds only pure decisions. SQLite transactions and migrations belong to
//! routing-store; the wire, inventory, polling and observations belong to
//! routing-module.
//!
//! - [`decide`] picks the account for one admission and explains why.
//! - [`evidence::confirm_exhaustion`] decides whether one observation confirms
//!   exhaustion (and so may become a shared [`Fence`], a block keyed by
//!   account and [`PoolKey`]).
//! - [`pools::block_end`] decides when a block stops restricting its account.
//! - [`evidence::hold_until`] and [`evidence::retry_after_with_jitter`] are
//!   the hold rule's reset check and the reference's jitter.

pub mod decide;
pub mod decision;
pub mod eligibility;
pub mod evidence;
pub mod policy;
pub mod pools;
pub mod types;

#[cfg(test)]
mod policy_tests;

pub use decide::{decide, decide_with_context};
pub use decision::{
    AccountNote, Admission, AdmitReason, AffinityNote, AffinityOutcome, AgeBand, ApplicableWindow,
    Decision, ExclusionReason, ExclusionTier, Explanation, Hold, HoldBasis, ImportNote,
    InvalidationReason, Outcome, OverrideNote, PreviousPin, Refusal, RefusalCode,
    RequestedAccountIgnored, RequestedAccountNote, RosterNote, ScopedListState, WindowWeight,
};
pub use policy::{
    input_size_class, AccountStrategy, AllocationContext, CreditReading, PoolFloor, SpendReading,
};
pub use pools::{
    block_end, grown_recovery_set, recovery_set_from, validate_pool_name, AllowedAccountClass,
    BlockEnd, PoolKey, PoolScope, PoolWindowReading, POOL_NAME_MAX_LEN,
};
pub use types::{
    AccountRef, AccountRequest, AccountView, AdmissionDemand, AdmissionState, AllocatorPolicy,
    AllocatorState, CallerKind, CandidateRef, ConfirmationSource, ConversationAffinity,
    ConversationKeySource, CredentialRecord, EpochMs, Fence, FenceScope, GeneralWindow,
    ModelFamily, ModelFamilyMapping, ModelReach, ObservationSource, OperatorOverride,
    PrincipalExclusion, QuotaReading, QuotaWindow, ResetTime, Roster, RunnerCandidates,
    ScopeIdMapping, ScopedList, ScopedWindow, StaleDisclosure,
};
