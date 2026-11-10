-- Evidence and permission to retry have independent lifetimes. A probe advances
-- only the schedule; it must not make an old refusal look newly observed.
CREATE TABLE fence_recheck (
  kind TEXT NOT NULL,
  origin_key TEXT NOT NULL,
  provider TEXT NOT NULL,
  account_id TEXT,
  credential_id TEXT,
  model TEXT,
  size_class INTEGER,
  principal TEXT,
  admission_id TEXT,
  evidence TEXT NOT NULL,
  evidence_at INTEGER NOT NULL,
  quota INTEGER NOT NULL CHECK (quota IN (0,1)),
  classified INTEGER NOT NULL DEFAULT 0,
  next_recheck_at INTEGER NOT NULL,
  reset_check_at INTEGER,
  last_claimed_at INTEGER,
  cleared_at INTEGER,
  PRIMARY KEY (kind, origin_key)
);
CREATE INDEX fence_recheck_scope ON fence_recheck(provider, credential_id, account_id, model);

-- Independent quota and non-quota refusals must not overwrite each other.
CREATE TABLE account_non_quota_route_fence (
 provider TEXT NOT NULL, credential_id TEXT NOT NULL, model TEXT NOT NULL,
 until_ms INTEGER NOT NULL, decision_id TEXT NOT NULL, recorded_at INTEGER NOT NULL,
 last_reported_at INTEGER NOT NULL, PRIMARY KEY(provider,credential_id,model)
);
INSERT INTO account_non_quota_route_fence
 SELECT provider,credential_id,model,until_ms,decision_id,recorded_at,COALESCE(last_reported_at,recorded_at)
 FROM account_route_fence WHERE wall_class='transient';

-- JSON arrays make compound identities unambiguous even when ids contain ':'
-- or '/'. Provider cooldown fanout is represented by its provider origin only.
CREATE VIEW fence_origin AS
SELECT 'account' AS kind, json_array(fence_id) AS origin_key, provider, account_id,
       NULL AS credential_id, NULL AS model, NULL AS size_class,
       NULL AS principal, NULL AS admission_id, fence_json AS evidence,
       observed_at AS evidence_at, 1 AS quota, NULL AS deadline,
       json_extract(fence_json, '$.reset.at_ms') AS reset_at
FROM account_fence WHERE cleared_at IS NULL
UNION ALL
SELECT 'credential', json_array(provider_id,credential_id), provider_id,NULL,credential_id,NULL,NULL,NULL,NULL,
       json_array(code,listing_fingerprint,observed_at_ms),observed_at_ms,0,NULL,NULL
FROM credential_entitlement_fences
UNION ALL
SELECT 'account_route',json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
       json_array(wall_class,until_ms,COALESCE(last_reported_at,recorded_at)),COALESCE(last_reported_at,recorded_at),
       wall_class='quota',until_ms,CASE WHEN wall_class='quota' THEN until_ms END
FROM account_route_fence WHERE wall_class='quota'
UNION ALL
SELECT 'non_quota_route',json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
       json_array(until_ms,last_reported_at),last_reported_at,0,until_ms,NULL
FROM account_non_quota_route_fence
UNION ALL
SELECT 'pool_billing',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
       json_array(walled_until,wall_reason),updated_at,0,walled_until,NULL
FROM pool_key_state WHERE walled_until IS NOT NULL
UNION ALL
SELECT 'pool_rate',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
       json_array(cooldown_until,cooldown_reason),updated_at,0,cooldown_until,NULL
FROM pool_key_state WHERE cooldown_until IS NOT NULL
UNION ALL
SELECT 'provider',json_array(provider_key),provider_key,NULL,NULL,NULL,NULL,NULL,NULL,
       json_array(cooldown_until,quota_exhausted_until,last_error_at),COALESCE(last_error_at,updated_at),
       COALESCE(wall_class='quota',0),MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),
       CASE WHEN wall_class='quota' THEN quota_exhausted_until END
FROM provider_cooldown_state WHERE cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL
UNION ALL
SELECT 'route',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
       json_array(cooldown_until,quota_exhausted_until,last_error_at),COALESCE(last_error_at,updated_at),
       COALESCE(wall_class='quota',0),MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),
       CASE WHEN wall_class='quota' THEN quota_exhausted_until END
FROM model_route_state r WHERE (cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL)
 AND NOT EXISTS (SELECT 1 FROM provider_cooldown_state p WHERE p.provider_key=r.provider_id
   AND COALESCE(p.cooldown_until,0)>=COALESCE(r.cooldown_until,0)
   AND COALESCE(p.quota_exhausted_until,0)>=COALESCE(r.quota_exhausted_until,0))
UNION ALL
SELECT 'failure',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
       json_array(failure_backoff_until,failure_backoff_reason),COALESCE(last_error_at,updated_at),0,failure_backoff_until,NULL
FROM model_route_state WHERE failure_backoff_until IS NOT NULL
UNION ALL
SELECT 'size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
       json_array(observed_at,expires_at),observed_at,1,expires_at,expires_at
FROM account_size_refusal
UNION ALL
SELECT 'principal',json_array(principal,admission_id,credential_id),'',NULL,credential_id,NULL,NULL,principal,admission_id,
       json_array(recorded_at,runner_view_digest,roster_view_digest),recorded_at,0,NULL,NULL
FROM account_principal_exclusion
UNION ALL
SELECT 'observation',json_array(observation_id,e.key),
       json_extract(e.value,'$.provider'),json_extract(e.value,'$.account_id'),NULL,NULL,NULL,NULL,NULL,
       e.value,received_at,0,json_extract(e.value,'$.retry_at_ms'),NULL
FROM account_observation o,json_each(CASE WHEN o.kind='transient_backoff' AND o.bound=1
 AND json_valid(o.effects_json) THEN o.effects_json ELSE '[]' END) e
WHERE o.kind='transient_backoff' AND o.bound=1
 AND o.received_at >= CAST(strftime('%s','now') AS INTEGER)*1000-86400000
 AND json_extract(e.value,'$.effect')='transient_backoff';

-- Capture one clock value for the entire conversion. Reopening the store does
-- not repeat the conversion or move an existing retry time forward.
CREATE TEMP TABLE fence_migration_clock(at INTEGER NOT NULL);
INSERT INTO fence_migration_clock VALUES (CAST(strftime('%s','now') AS INTEGER)*1000);
INSERT INTO fence_recheck
 (kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
  evidence,evidence_at,quota,next_recheck_at,reset_check_at)
SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
 evidence,evidence_at,COALESCE(quota,0),
 CASE WHEN quota=1 THEN at ELSE MIN(COALESCE(deadline,at+3600000),at+3600000) END,
 CASE WHEN quota=1 AND reset_at>at THEN reset_at END
FROM fence_origin CROSS JOIN fence_migration_clock;
DROP TABLE fence_migration_clock;
