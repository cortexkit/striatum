-- Evidence and permission to retry have independent lifetimes. A probe advances
-- only the schedule; it must not make an old refusal look newly observed.
CREATE TEMP TABLE fence_migration_clock(at INTEGER NOT NULL);
INSERT INTO fence_migration_clock VALUES (CAST(strftime('%s','now') AS INTEGER)*1000);
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
  claim_owner TEXT,
  cleared_at INTEGER,
  PRIMARY KEY (kind, origin_key)
);
CREATE INDEX fence_recheck_scope ON fence_recheck(provider, credential_id, account_id, model);
-- A completed probe may heal exactly the evidence its launch was authorized
-- to test, even when that evidence names no pool or another credential for
-- the same account.
CREATE TABLE fence_probe_authorization (
 owner TEXT NOT NULL,kind TEXT NOT NULL,origin_key TEXT NOT NULL,
 evidence TEXT NOT NULL,evidence_at INTEGER NOT NULL,claimed_at INTEGER NOT NULL,
 PRIMARY KEY(owner,kind,origin_key)
);
-- Clearing one route must not leave it blocked by a provider fanout origin.
-- The exception matches evidence, so a later provider refusal fences it again.
CREATE TABLE fence_recheck_route_clear (
 kind TEXT NOT NULL,origin_key TEXT NOT NULL,provider TEXT NOT NULL,model TEXT NOT NULL,evidence TEXT NOT NULL,
 PRIMARY KEY(kind,origin_key,provider,model)
);
-- An ordinary success proves recovery only for the pools it actually used.
-- A probe instead uses its exact evidence authorization above as its scope.
CREATE TABLE fence_success_pool (
 fence_id TEXT NOT NULL,
 pool_key_json TEXT NOT NULL,evidence_at INTEGER NOT NULL,healed_at INTEGER NOT NULL,
 PRIMARY KEY(fence_id,pool_key_json)
);
-- Observation history stays append-only. Its active backoff is one origin per
-- account, so a probe refusal replaces that origin's retry rather than leaving
-- an older claimed observation to block the new, shorter retry.
CREATE TABLE account_transient_backoff (
 provider TEXT NOT NULL,account_id TEXT NOT NULL,retry_at_ms INTEGER NOT NULL,
 evidence_at INTEGER NOT NULL,evidence_key TEXT NOT NULL,effects_json TEXT NOT NULL,
 PRIMARY KEY(provider,account_id)
);
INSERT INTO account_transient_backoff
SELECT json_extract(e.value,'$.provider'),json_extract(e.value,'$.account_id'),
 json_extract(e.value,'$.retry_at_ms'),o.received_at,json_array(o.observation_id,e.key),e.value
FROM account_observation o,json_each(CASE WHEN o.kind='transient_backoff' AND o.bound=1
 AND json_valid(o.effects_json) THEN o.effects_json ELSE '[]' END) e
WHERE o.kind='transient_backoff' AND o.bound=1
 AND o.received_at>=(SELECT at FROM fence_migration_clock)-86400000
 AND json_extract(e.value,'$.effect')='transient_backoff'
ORDER BY o.received_at,o.observation_id,e.key
ON CONFLICT(provider,account_id) DO UPDATE SET retry_at_ms=excluded.retry_at_ms,
 evidence_at=excluded.evidence_at,evidence_key=excluded.evidence_key,effects_json=excluded.effects_json;
ALTER TABLE pool_key_state ADD COLUMN wall_observed_at INTEGER;
ALTER TABLE pool_key_lease ADD COLUMN fence_probe INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_admission ADD COLUMN input_size_class INTEGER;
ALTER TABLE account_admission ADD COLUMN model_decision_id TEXT;
CREATE INDEX account_admission_model_decision ON account_admission(model_decision_id)
 WHERE model_decision_id IS NOT NULL;
ALTER TABLE pool_key_state ADD COLUMN cooldown_observed_at INTEGER;
UPDATE pool_key_state SET wall_observed_at=updated_at WHERE walled_until IS NOT NULL;
UPDATE pool_key_state SET cooldown_observed_at=updated_at WHERE cooldown_until IS NOT NULL;
ALTER TABLE model_route_state ADD COLUMN wall_observed_at INTEGER;
ALTER TABLE model_route_state ADD COLUMN wall_unscoped INTEGER NOT NULL DEFAULT 0;
ALTER TABLE model_route_state ADD COLUMN failure_observed_at INTEGER;
ALTER TABLE provider_cooldown_state ADD COLUMN wall_observed_at INTEGER;
ALTER TABLE provider_cooldown_state ADD COLUMN wall_unscoped INTEGER NOT NULL DEFAULT 0;
UPDATE model_route_state SET wall_observed_at=COALESCE(last_error_at,updated_at),failure_observed_at=COALESCE(last_error_at,updated_at);
UPDATE provider_cooldown_state SET wall_observed_at=COALESCE(last_error_at,updated_at);
CREATE TRIGGER model_route_state_inherit_fence_scope AFTER INSERT ON model_route_state
WHEN EXISTS (SELECT 1 FROM provider_cooldown_state p WHERE p.provider_key=NEW.provider_id
 AND COALESCE(p.cooldown_until,0)>=COALESCE(NEW.cooldown_until,0)
 AND COALESCE(p.quota_exhausted_until,0)>=COALESCE(NEW.quota_exhausted_until,0))
BEGIN
 UPDATE model_route_state SET
  wall_unscoped=(SELECT wall_unscoped FROM provider_cooldown_state WHERE provider_key=NEW.provider_id),
  wall_observed_at=(SELECT wall_observed_at FROM provider_cooldown_state WHERE provider_key=NEW.provider_id)
 WHERE route_key=NEW.route_key;
END;

-- Independent quota and non-quota refusals must not overwrite each other.
CREATE TABLE account_non_quota_route_fence (
 provider TEXT NOT NULL, credential_id TEXT NOT NULL, model TEXT NOT NULL,
 until_ms INTEGER NOT NULL, decision_id TEXT NOT NULL, recorded_at INTEGER NOT NULL,
 last_reported_at INTEGER NOT NULL, wall_kind TEXT NOT NULL,
 PRIMARY KEY(provider,credential_id,model,wall_kind)
);
CREATE TABLE account_non_quota_size_refusal (
 provider TEXT NOT NULL,credential_id TEXT NOT NULL,model TEXT NOT NULL,
 size_class INTEGER NOT NULL,expires_at INTEGER NOT NULL,observed_at INTEGER NOT NULL,
 PRIMARY KEY(provider,credential_id,model,size_class)
);
INSERT INTO account_non_quota_route_fence
 SELECT provider,credential_id,model,until_ms,decision_id,recorded_at,COALESCE(last_reported_at,recorded_at),'transient'
 FROM account_route_fence WHERE wall_class='transient';

-- JSON arrays make compound identities unambiguous even when ids contain ':'
-- or '/'. Provider cooldown fanout is represented by its provider origin only.
CREATE VIEW fence_origin AS
SELECT 'account' AS kind, json_array(fence_id) AS origin_key, provider, account_id,
       NULL AS credential_id, NULL AS model, NULL AS size_class,
       NULL AS principal, NULL AS admission_id, fence_json AS evidence,
       MIN(observed_at,created_at) AS evidence_at, 1 AS quota, NULL AS deadline,
       json_extract(fence_json, '$.reset.at_ms') AS reset_at
FROM account_fence WHERE cleared_at IS NULL
UNION ALL
SELECT 'credential', json_array(provider_id,credential_id), provider_id,NULL,credential_id,NULL,NULL,NULL,NULL,
       json_array(code,observed_at_ms),observed_at_ms,
       EXISTS(SELECT 1 FROM account_fence f WHERE f.cleared_at IS NULL AND
         substr(f.fence_id,1,length('credential-quota:'||json_array(c.provider_id,c.credential_id)||':'))='credential-quota:'||json_array(c.provider_id,c.credential_id)||':'),
       observed_at_ms+21600000,NULL
FROM credential_entitlement_fences c
UNION ALL
SELECT 'account_route',json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
       json_array(wall_class,until_ms,COALESCE(last_reported_at,recorded_at)),COALESCE(last_reported_at,recorded_at),
       wall_class='quota',until_ms,CASE WHEN wall_class='quota' THEN until_ms END
FROM account_route_fence WHERE wall_class='quota'
UNION ALL
SELECT CASE WHEN wall_kind='transient' THEN 'transient_route' ELSE 'non_quota_route' END,json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
       json_array(until_ms,last_reported_at),last_reported_at,0,until_ms,NULL
FROM account_non_quota_route_fence
UNION ALL
SELECT 'pool_billing',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
       json_array(walled_until,wall_reason,COALESCE(wall_observed_at,updated_at)),COALESCE(wall_observed_at,updated_at),0,walled_until,NULL
FROM pool_key_state WHERE walled_until IS NOT NULL
UNION ALL
SELECT 'pool_rate',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
       json_array(cooldown_until,cooldown_reason,COALESCE(cooldown_observed_at,updated_at)),COALESCE(cooldown_observed_at,updated_at),0,cooldown_until,NULL
FROM pool_key_state WHERE cooldown_until IS NOT NULL
UNION ALL
SELECT 'provider',json_array(provider_key),provider_key,NULL,NULL,NULL,NULL,NULL,NULL,
       json_array(cooldown_until,quota_exhausted_until,wall_observed_at),COALESCE(wall_observed_at,last_error_at,updated_at),
       0,MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),NULL
FROM provider_cooldown_state WHERE cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL
UNION ALL
SELECT 'route',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
       json_array(cooldown_until,quota_exhausted_until,wall_observed_at),COALESCE(wall_observed_at,last_error_at,updated_at),
       0,MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),NULL
FROM model_route_state r WHERE (cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL)
 AND NOT EXISTS (SELECT 1 FROM provider_cooldown_state p WHERE p.provider_key=r.provider_id
   AND p.last_error IS r.last_error
   AND COALESCE(p.wall_observed_at,p.last_error_at,p.updated_at)=COALESCE(r.wall_observed_at,r.last_error_at,r.updated_at)
   AND COALESCE(p.cooldown_until,0)>=COALESCE(r.cooldown_until,0)
   AND COALESCE(p.quota_exhausted_until,0)>=COALESCE(r.quota_exhausted_until,0))
UNION ALL
SELECT 'failure',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
       json_array(failure_backoff_until,failure_backoff_reason,failure_observed_at),COALESCE(failure_observed_at,last_error_at,updated_at),0,failure_backoff_until,NULL
FROM model_route_state WHERE failure_backoff_until IS NOT NULL
UNION ALL
SELECT 'size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
       json_array(observed_at,expires_at),observed_at,1,expires_at,expires_at
FROM account_size_refusal
UNION ALL
SELECT 'non_quota_size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
       json_array(observed_at,expires_at),observed_at,0,expires_at,NULL
FROM account_non_quota_size_refusal
UNION ALL
SELECT 'principal',json_array(principal,admission_id,credential_id),'',NULL,credential_id,NULL,NULL,principal,admission_id,
       json_array(recorded_at,runner_view_digest,roster_view_digest),recorded_at,0,recorded_at+86400000,NULL
FROM account_principal_exclusion
UNION ALL
SELECT 'observation',json_array(provider,account_id),provider,account_id,NULL,NULL,NULL,NULL,NULL,
       json_array(evidence_key,effects_json),evidence_at,0,retry_at_ms,NULL
FROM account_transient_backoff;

-- Capture one clock value for the entire conversion. Reopening the store does
-- not repeat the conversion or move an existing retry time forward.
INSERT INTO fence_recheck
 (kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
  evidence,evidence_at,quota,next_recheck_at,reset_check_at)
SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
 evidence,evidence_at,COALESCE(quota,0),
 CAST(CASE WHEN quota=1 THEN at ELSE MIN(COALESCE(deadline,at+3600000),at+3600000) END AS INTEGER),
 CASE WHEN quota=1 AND reset_at>at THEN reset_at END
FROM fence_origin CROSS JOIN fence_migration_clock;
DROP TABLE fence_migration_clock;
