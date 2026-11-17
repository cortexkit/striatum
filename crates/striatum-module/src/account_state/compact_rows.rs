//! State-only row representations; persistence and replies keep owned text.

use super::conversation_hash::ConversationHash;
use super::shared_text::SharedText;
use striatum_store::{AccountAdmissionRow, AccountAffinityRow, LatestAdmissionRow};

pub(crate) type Admission = AccountAdmissionRow<SharedText, ConversationHash>;
pub(crate) type Affinity = AccountAffinityRow<SharedText, ConversationHash>;
pub(crate) type Latest = LatestAdmissionRow<SharedText, ConversationHash>;

pub(crate) fn admission(row: AccountAdmissionRow) -> Admission {
    row.map_text(SharedText::from, ConversationHash::from)
}
pub(crate) fn affinity(row: AccountAffinityRow) -> Affinity {
    row.map_text(SharedText::from, ConversationHash::from)
}
pub(crate) fn latest(row: LatestAdmissionRow) -> Latest {
    row.map_text(SharedText::from, ConversationHash::from)
}
pub(crate) fn expand_admission(row: &Admission) -> AccountAdmissionRow {
    row.clone().map_text(|s| s.to_string(), |h| h.to_string())
}
pub(crate) fn expand_affinity(row: &Affinity) -> AccountAffinityRow {
    row.clone().map_text(|s| s.to_string(), |h| h.to_string())
}

#[cfg(test)]
#[path = "compact_rows_tests.rs"]
mod tests;
