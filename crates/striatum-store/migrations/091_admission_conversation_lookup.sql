-- Retention's maintenance recheck finds a conversation's next newest admission
-- after deleting its newest one, and the next newest used continuation of an
-- admission. Without these, each lookup scans every admission of the principal
-- under the account writer: about 0.3 s per pruned conversation on a live store.
CREATE INDEX account_admission_by_conversation
  ON account_admission (principal, conversation_hash, provider, committed_at);
CREATE INDEX account_continue_by_admission
  ON account_continue (principal, admission_id);
