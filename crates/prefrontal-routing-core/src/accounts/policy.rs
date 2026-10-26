//! Provider-independent account policy and per-admission facts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{EpochMs, PoolKey};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStrategy {
    #[default]
    Sticky,
    RoundRobin,
    Hybrid,
    Ordered,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolFloor {
    pub key: PoolKey,
    pub remaining_percent: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreditReading {
    pub observed_at_ms: EpochMs,
    pub spendable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpendReading {
    pub used: f64,
    pub reset_at_ms: Option<EpochMs>,
}

/// Facts that vary by admission, rather than operator policy. Unknown facts
/// are left absent; in particular no report fabricates a credit balance.
#[derive(Debug, Clone, Default)]
pub struct AllocationContext {
    pub hold_ok: Option<bool>,
    pub parent_account_id: Option<String>,
    pub credits: BTreeMap<String, CreditReading>,
    pub spend: BTreeMap<String, SpendReading>,
}

/// Input-size bands are powers of two in tokens. A refusal at 169k tokens
/// therefore restricts 128k..256k, never a small prompt or the entire pool.
pub fn input_size_class(tokens: u64) -> u32 {
    63 - tokens.max(1).leading_zeros()
}
