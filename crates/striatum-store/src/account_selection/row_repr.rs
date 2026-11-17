//! Field-preserving representation conversions; SQL and replies use the String defaults.
use super::{AccountAdmissionRow, AccountAffinityRow, LatestAdmissionRow};
impl<Text, Hash> AccountAdmissionRow<Text, Hash> {
    /// Change only the representation of repeated text and conversation hashes.
    pub fn map_text<T, H>(
        self,
        mut text: impl FnMut(Text) -> T,
        hash: impl FnOnce(Hash) -> H,
    ) -> AccountAdmissionRow<T, H> {
        AccountAdmissionRow {
            principal: text(self.principal),
            admission_id: self.admission_id,
            request_fingerprint: self.request_fingerprint,
            request_mode: text(self.request_mode),
            decision_id: self.decision_id,
            conversation_hash: hash(self.conversation_hash),
            provider: text(self.provider),
            model: text(self.model),
            account_id: text(self.account_id),
            credential_id: text(self.credential_id),
            pin_id: self.pin_id,
            pin_generation: self.pin_generation,
            state: text(self.state),
            charged: self.charged,
            request_bytes: self.request_bytes,
            request_bytes_estimated: self.request_bytes_estimated,
            committed_at: self.committed_at,
            admission_valid_until: self.admission_valid_until,
            lease_valid_until: self.lease_valid_until,
            reply_json: self.reply_json,
            run_id: self.run_id,
            started_at: self.started_at,
            started_late: self.started_late,
            released_at: self.released_at,
            finished_at: self.finished_at,
            last_observed_at: self.last_observed_at,
        }
    }
}
impl<Text, Hash> AccountAffinityRow<Text, Hash> {
    /// Change only the representation of repeated text and conversation hashes.
    pub fn map_text<T, H>(
        self,
        mut text: impl FnMut(Text) -> T,
        hash: impl FnOnce(Hash) -> H,
    ) -> AccountAffinityRow<T, H> {
        AccountAffinityRow {
            principal: text(self.principal),
            conversation_hash: hash(self.conversation_hash),
            provider: text(self.provider),
            affinity_model: text(self.affinity_model),
            pin_id: self.pin_id,
            pin_generation: self.pin_generation,
            account_id: text(self.account_id),
            credential_id: text(self.credential_id),
            credential_binding_revision: self.credential_binding_revision,
            policy_version: self.policy_version,
            initial_demand_bytes: self.initial_demand_bytes,
            created_at: self.created_at,
            last_used_at: self.last_used_at,
            expires_at: self.expires_at,
            lease_valid_until: self.lease_valid_until,
        }
    }
}
impl<Text, Hash> LatestAdmissionRow<Text, Hash> {
    /// Change only the representation of repeated text and conversation hashes.
    pub fn map_text<T, H>(
        self,
        mut text: impl FnMut(Text) -> T,
        hash: impl FnOnce(Hash) -> H,
    ) -> LatestAdmissionRow<T, H> {
        LatestAdmissionRow {
            principal: text(self.principal),
            conversation_hash: hash(self.conversation_hash),
            provider: text(self.provider),
            admission_id: self.admission_id,
            committed_at: self.committed_at,
            rowid: self.rowid,
            pin_id: self.pin_id,
            pin_generation: self.pin_generation,
            state: text(self.state),
            admission_valid_until: self.admission_valid_until,
            lease_valid_until: self.lease_valid_until,
            account_id: text(self.account_id),
        }
    }
}
