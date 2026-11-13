-- Curated auth_methods restrict one model, not a provider's whole sign-in.
-- Existing rows have no proven coverage source; the next catalog poll marks
-- only rows disclosed in login_methods. Keep this distinction across restarts.
ALTER TABLE model_facts ADD COLUMN login_methods_documented INTEGER NOT NULL DEFAULT 0
    CHECK (login_methods_documented IN (0, 1));
