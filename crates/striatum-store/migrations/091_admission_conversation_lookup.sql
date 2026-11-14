-- Retention rechecks two things under the account writer after deleting rows:
-- a conversation's next newest admission, once its newest one is deleted, and
-- an admission's next newest continuation offer that a run used to move to
-- another account, once the newest such offer is deleted. Without these indexes
-- each lookup scans every admission or offer of the principal: about 0.3 s per
-- pruned conversation on a live store.
CREATE INDEX account_admission_by_conversation
  ON account_admission (principal, conversation_hash, provider, committed_at);
CREATE INDEX account_continue_by_admission
  ON account_continue (principal, admission_id);
