-- Normalize the historical family spellings before family provenance becomes authoritative.
-- Unknown non-null values remain visible so the store opener can refuse with row identities.
ALTER TABLE model_facts ADD COLUMN family_source TEXT NOT NULL DEFAULT 'absent'
  CHECK (family_source IN ('override', 'rule', 'curated', 'absent'));

UPDATE model_facts
   SET family = CASE family
     WHEN 'anthropic' THEN 'claude'
     WHEN 'openai' THEN 'gpt'
     WHEN 'openai-gpt' THEN 'gpt'
     WHEN 'google-gemini' THEN 'gemini'
     WHEN 'xai-grok' THEN 'grok'
     ELSE family
   END;

UPDATE model_facts
   SET family_source = CASE WHEN family IS NULL THEN 'absent' ELSE 'curated' END;
