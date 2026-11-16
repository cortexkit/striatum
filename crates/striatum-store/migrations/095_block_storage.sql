-- Enforcement moves in one transaction; the old evidence remains a frozen
-- audit copy. Refusals, permission to test them and per-pool success stay separate.
CREATE TABLE block (
  kind TEXT NOT NULL CHECK (kind IN ('account','credential','account_route',
    'non_quota_route','transient_route','pool_billing','pool_rate','provider',
    'route','failure','size','non_quota_size','principal','observation')),
  origin_key TEXT NOT NULL CHECK (json_valid(origin_key)),
  provider TEXT NOT NULL,
  account_id TEXT,
  credential_id TEXT,
  model TEXT,
  size_class INTEGER,
  principal TEXT,
  admission_id TEXT,
  observation_id TEXT,
  pool_key_json TEXT CHECK (pool_key_json IS NULL OR json_valid(pool_key_json)),
  listing_fingerprint TEXT,
  recovery_set_json TEXT CHECK (recovery_set_json IS NULL OR json_valid(recovery_set_json)),
  evidence TEXT NOT NULL,
  evidence_at INTEGER NOT NULL,
  quota INTEGER NOT NULL CHECK (quota IN (0,1)),
  classified INTEGER NOT NULL DEFAULT 0 CHECK (classified IN (0,1)),
  deadline INTEGER CHECK (kind NOT IN ('pool_billing','pool_rate')
    OR typeof(deadline) IN ('integer','null')),
  reset_at INTEGER,
  next_recheck_at INTEGER NOT NULL,
  reset_check_at INTEGER,
  last_claimed_at INTEGER,
  claim_generation INTEGER NOT NULL DEFAULT 0,
  claim_owner TEXT,
  cleared_at INTEGER,
  cleared_by TEXT CHECK (cleared_by IN ('kept','removed','retired')),
  cleared_reason TEXT,
  cleared_models TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(cleared_models)),
  source_present INTEGER NOT NULL DEFAULT 1 CHECK (source_present IN (0,1)),
  managed INTEGER NOT NULL DEFAULT 0 CHECK (managed IN (0,1)),
  source_order INTEGER NOT NULL DEFAULT 0,
  observed_at INTEGER,
  created_at INTEGER,
  recorded_at INTEGER,
  last_reported_at INTEGER,
  cooldown_until INTEGER,
  quota_exhausted_until INTEGER,
  reason TEXT,
  code TEXT,
  wall_class TEXT,
  -- Account-route records of other classes keep ordering/transient data but
  -- are not origins. Claims and counts share this stored generated flag.
  is_origin INTEGER GENERATED ALWAYS AS (kind!='account_route' OR wall_class IS 'quota') STORED,
  wall_unscoped INTEGER NOT NULL DEFAULT 0 CHECK (wall_unscoped IN (0,1)),
  scope_error TEXT,
  meta TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(meta)),
  PRIMARY KEY (kind, origin_key)
);
CREATE INDEX block_scope ON block(provider,credential_id,account_id,model);
CREATE INDEX block_source_order ON block(kind,source_order);
CREATE INDEX block_retired_time ON block(cleared_at) WHERE cleared_by='retired' AND is_origin=1;
CREATE INDEX block_exclusions ON block(principal,admission_id,credential_id)
  WHERE kind='principal';
CREATE INDEX block_observation ON block(observation_id) WHERE observation_id IS NOT NULL;
CREATE INDEX block_pool ON block(pool_key_json) WHERE pool_key_json IS NOT NULL;

CREATE TABLE probe_authorization (
  owner TEXT NOT NULL, kind TEXT NOT NULL, origin_key TEXT NOT NULL,
  evidence TEXT NOT NULL, evidence_at INTEGER NOT NULL, claimed_at INTEGER NOT NULL,
  claim_generation INTEGER NOT NULL,
  PRIMARY KEY(owner,kind,origin_key)
);
INSERT INTO probe_authorization SELECT * FROM fence_probe_authorization;

CREATE TABLE success_proof (
  fence_id TEXT NOT NULL, pool_key_json TEXT NOT NULL,
  evidence_at INTEGER NOT NULL, healed_at INTEGER NOT NULL,
  PRIMARY KEY(fence_id,pool_key_json)
);
INSERT INTO success_proof SELECT * FROM fence_success_pool;

-- Keep retirement rows per evidence generation, not as one stored scope-wide
-- watermark. Observation writes also reject receipt times no newer than any
-- retired generation of that same origin.
CREATE TABLE retirement_tombstone (
  kind TEXT NOT NULL, origin_key TEXT NOT NULL, evidence TEXT NOT NULL,
  evidence_at INTEGER NOT NULL, retired_at INTEGER NOT NULL,
  PRIMARY KEY(kind,origin_key,evidence)
);
CREATE INDEX retirement_tombstone_time ON retirement_tombstone(retired_at);
CREATE INDEX observation_retirement_guard ON retirement_tombstone(kind,origin_key,evidence_at);
INSERT INTO retirement_tombstone SELECT * FROM fence_retired_origin;

CREATE TEMP TABLE block_migration_now(at INTEGER NOT NULL);
INSERT INTO block_migration_now VALUES (CAST(strftime('%s','now') AS INTEGER)*1000);
CREATE TEMP TABLE block_migration_quarantine(kind TEXT NOT NULL,origin_key TEXT NOT NULL,
  PRIMARY KEY(kind,origin_key));
INSERT INTO block_migration_quarantine
SELECT 'provider',json_array(provider_key) FROM provider_cooldown_state
WHERE block_migration_quarantined(wall_unscoped,last_error,wall_class)
UNION ALL
SELECT 'route',json_array(route_key) FROM model_route_state
WHERE block_migration_quarantined(wall_unscoped,last_error,wall_class);

INSERT INTO block(kind,origin_key,provider,account_id,credential_id,model,size_class,
  principal,admission_id,evidence,evidence_at,quota,deadline,reset_at,next_recheck_at,reset_check_at)
SELECT o.kind,o.origin_key,o.provider,o.account_id,o.credential_id,o.model,o.size_class,
  o.principal,o.admission_id,o.evidence,o.evidence_at,COALESCE(o.quota,0),o.deadline,o.reset_at,
  CAST(CASE WHEN o.quota=1 THEN o.evidence_at+900000
    ELSE MIN(COALESCE(o.deadline,o.evidence_at+3600000),o.evidence_at+3600000) END AS INTEGER),
  CASE WHEN o.quota=1 AND o.reset_at>o.evidence_at THEN o.reset_at END
FROM fence_origin o
WHERE NOT EXISTS (SELECT 1 FROM block_migration_quarantine q
  WHERE q.kind=o.kind AND q.origin_key=o.origin_key);

-- Provider-wide refusal evidence can hide a route's shorter cooldown deadline.
-- Store both deadlines; routing suppresses the duplicate route origin at read.
INSERT INTO block(kind,origin_key,provider,model,evidence,evidence_at,quota,deadline,
  next_recheck_at)
SELECT 'route',json_array(r.route_key),r.provider_id,r.model_id,
  json_array(r.cooldown_until,r.quota_exhausted_until,r.wall_observed_at),
  COALESCE(r.wall_observed_at,r.last_error_at,r.updated_at),0,
  MAX(COALESCE(r.cooldown_until,0),COALESCE(r.quota_exhausted_until,0)),
  CAST(MIN(MAX(COALESCE(r.cooldown_until,0),COALESCE(r.quota_exhausted_until,0)),
    COALESCE(r.wall_observed_at,r.last_error_at,r.updated_at)+3600000) AS INTEGER)
FROM model_route_state r
WHERE (r.cooldown_until IS NOT NULL OR r.quota_exhausted_until IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM block b WHERE b.kind='route' AND b.origin_key=json_array(r.route_key))
  AND NOT EXISTS (SELECT 1 FROM block_migration_quarantine q
    WHERE q.kind='route' AND q.origin_key=json_array(r.route_key));

-- Older route-fence rows of a non-quota class still supply report ordering
-- and keyed source metadata, although fence_origin excludes them.
INSERT INTO block(kind,origin_key,provider,credential_id,model,evidence,evidence_at,
  quota,deadline,next_recheck_at)
SELECT 'account_route',json_array(provider,credential_id,model),provider,credential_id,model,
  json_array(wall_class,until_ms,COALESCE(last_reported_at,recorded_at)),
  COALESCE(last_reported_at,recorded_at),0,until_ms,
  CAST(MIN(until_ms,COALESCE(last_reported_at,recorded_at)+3600000) AS INTEGER)
FROM account_route_fence WHERE wall_class IS NOT 'quota';

-- Account fences keep their minted ids and observation references even after
-- clearing. They are audit tombstones, not live records in the loader.
INSERT INTO block(kind,origin_key,provider,account_id,evidence,evidence_at,quota,
  next_recheck_at,cleared_at,cleared_by,cleared_reason,source_present)
SELECT 'account',json_array(fence_id),provider,account_id,fence_json,
  MIN(observed_at,created_at),1,CAST(MIN(observed_at,created_at)+900000 AS INTEGER),
  cleared_at,'removed',cleared_reason,0
FROM account_fence WHERE cleared_at IS NOT NULL;

-- Metadata contains only details decoded by Rust. Fields named by SQL indexes,
-- ordering, retention or compare-and-set predicates have their own columns.
UPDATE block SET
  (source_order,observed_at,created_at,observation_id,pool_key_json,cleared_reason,recovery_set_json,meta) =
  (SELECT f.rowid,f.observed_at,f.created_at,f.observation_id,f.pool_key_json,f.cleared_reason,f.recovery_set_json,
    json_object('scope_json',f.scope_json,'fence_json',f.fence_json,'source',f.source,
      'principal',f.principal,'admission_id',f.admission_id)
   FROM account_fence f WHERE block.origin_key=json_array(f.fence_id))
WHERE kind='account';
UPDATE block SET (source_order,code,observed_at,listing_fingerprint,meta) =
  (SELECT c.rowid,c.code,c.observed_at_ms,c.listing_fingerprint,'{}'
   FROM credential_entitlement_fences c
   WHERE block.origin_key=json_array(c.provider_id,c.credential_id))
WHERE kind='credential';
UPDATE block SET (source_order,recorded_at,last_reported_at,wall_class,meta) =
  (SELECT f.rowid,f.recorded_at,f.last_reported_at,f.wall_class,json_object('decision_id',f.decision_id)
   FROM account_route_fence f WHERE block.origin_key=json_array(f.provider,f.credential_id,f.model))
WHERE kind='account_route';
UPDATE block SET (source_order,recorded_at,last_reported_at,wall_class,meta) =
  (SELECT f.rowid,f.recorded_at,f.last_reported_at,f.wall_kind,json_object('decision_id',f.decision_id)
   FROM account_non_quota_route_fence f
   WHERE block.origin_key=json_array(f.provider,f.credential_id,f.model)
     AND ((block.kind='transient_route' AND f.wall_kind='transient')
       OR (block.kind='non_quota_route' AND f.wall_kind!='transient')))
WHERE kind IN ('non_quota_route','transient_route');
UPDATE block SET (source_order,observed_at) =
  (SELECT s.rowid,s.observed_at FROM account_size_refusal s
   WHERE block.origin_key=json_array(s.provider,s.credential_id,s.model,s.size_class))
WHERE kind='size';
UPDATE block SET (source_order,observed_at) =
  (SELECT s.rowid,s.observed_at FROM account_non_quota_size_refusal s
   WHERE block.origin_key=json_array(s.provider,s.credential_id,s.model,s.size_class))
WHERE kind='non_quota_size';
UPDATE block SET (source_order,recorded_at,observation_id,meta) =
  (SELECT x.rowid,x.recorded_at,x.observation_id,
    json_object('runner_view_digest',x.runner_view_digest,'roster_view_digest',x.roster_view_digest)
   FROM account_principal_exclusion x
   WHERE block.origin_key=json_array(x.principal,x.admission_id,x.credential_id))
WHERE kind='principal';
UPDATE block SET (source_order,observation_id) =
  (SELECT t.rowid,CASE WHEN json_valid(t.evidence_key) THEN json_extract(t.evidence_key,'$[0]') END FROM account_transient_backoff t
   WHERE block.origin_key=json_array(t.provider,t.account_id))
WHERE kind='observation';
UPDATE block SET (source_order,reason,observed_at) =
  (SELECT p.rowid,CASE block.kind WHEN 'pool_billing' THEN p.wall_reason ELSE p.cooldown_reason END,
    CASE block.kind WHEN 'pool_billing' THEN COALESCE(p.wall_observed_at,p.updated_at) ELSE COALESCE(p.cooldown_observed_at,p.updated_at) END
   FROM pool_key_state p WHERE block.origin_key=json_array(p.provider,p.credential_id))
WHERE kind IN ('pool_billing','pool_rate');
UPDATE block SET (source_order,cooldown_until,quota_exhausted_until,wall_class,wall_unscoped,scope_error,observed_at) =
  (SELECT p.rowid,p.cooldown_until,p.quota_exhausted_until,p.wall_class,p.wall_unscoped,p.last_error,COALESCE(p.wall_observed_at,p.last_error_at,p.updated_at)
   FROM provider_cooldown_state p WHERE block.origin_key=json_array(p.provider_key))
WHERE kind='provider';
UPDATE block SET (source_order,cooldown_until,quota_exhausted_until,wall_class,wall_unscoped,scope_error,observed_at) =
  (SELECT r.rowid,r.cooldown_until,r.quota_exhausted_until,r.wall_class,r.wall_unscoped,r.last_error,COALESCE(r.wall_observed_at,r.last_error_at,r.updated_at)
   FROM model_route_state r WHERE block.origin_key=json_array(r.route_key))
WHERE kind='route';
UPDATE block SET (source_order,reason,wall_class,wall_unscoped,scope_error,observed_at) =
  (SELECT r.rowid,r.failure_backoff_reason,r.wall_class,r.wall_unscoped,r.last_error,COALESCE(r.failure_observed_at,r.last_error_at,r.updated_at)
   FROM model_route_state r WHERE block.origin_key=json_array(r.route_key))
WHERE kind='failure';

-- Matching evidence keeps its class, classification and exact claimed
-- generation. A hold created or cleared afterwards does not reclassify it.
-- Its clock evidence time can predate the current source metadata. Probe
-- history uses evidence_at; provider coverage uses the separate observed_at.
UPDATE block SET (evidence_at,quota,classified,next_recheck_at,reset_check_at,last_claimed_at,
  claim_generation,claim_owner,cleared_at,cleared_by,managed) =
  (SELECT r.evidence_at,r.quota,r.classified,r.next_recheck_at,r.reset_check_at,r.last_claimed_at,
    r.claim_generation,r.claim_owner,r.cleared_at,
    CASE WHEN r.cleared_at IS NOT NULL THEN 'kept' END,1
   FROM fence_recheck r WHERE r.kind=block.kind AND r.origin_key=block.origin_key
     AND r.evidence=block.evidence)
WHERE EXISTS (SELECT 1 FROM fence_recheck r WHERE r.kind=block.kind
  AND r.origin_key=block.origin_key AND r.evidence=block.evidence);

-- New evidence preserves a previous probe's time, but not its owner or
-- generation. Clock advancement never makes an old refusal newly observed.
UPDATE block SET
  last_claimed_at=(SELECT r.last_claimed_at FROM fence_recheck r
    WHERE r.kind=block.kind AND r.origin_key=block.origin_key),
  next_recheck_at=MAX(next_recheck_at,COALESCE((SELECT r.next_recheck_at FROM fence_recheck r
    WHERE r.kind=block.kind AND r.origin_key=block.origin_key AND block.evidence_at<r.last_claimed_at),next_recheck_at)),
  reset_check_at=CASE WHEN reset_check_at>(SELECT COALESCE(r.last_claimed_at,-9223372036854775808)
    FROM fence_recheck r WHERE r.kind=block.kind AND r.origin_key=block.origin_key) THEN reset_check_at END
WHERE managed=0 AND EXISTS (SELECT 1 FROM fence_recheck r
  WHERE r.kind=block.kind AND r.origin_key=block.origin_key);

UPDATE block SET cleared_models=COALESCE((SELECT json_group_array(c.model)
  FROM (SELECT model FROM fence_recheck_route_clear c
    WHERE c.kind=block.kind AND c.origin_key=block.origin_key AND c.evidence=block.evidence
    ORDER BY c.rowid) c),'[]');

-- Observation clocks outlive the sparse observation lookback. Other orphaned
-- clocks remain as stored tombstones but are not loaded as active evidence.
INSERT INTO block(kind,origin_key,provider,account_id,credential_id,model,size_class,
  principal,admission_id,evidence,evidence_at,quota,classified,next_recheck_at,
  reset_check_at,last_claimed_at,claim_generation,claim_owner,cleared_at,cleared_by,source_present,managed)
SELECT r.kind,r.origin_key,r.provider,r.account_id,r.credential_id,r.model,r.size_class,
  r.principal,r.admission_id,r.evidence,r.evidence_at,r.quota,r.classified,r.next_recheck_at,
  r.reset_check_at,r.last_claimed_at,r.claim_generation,r.claim_owner,
  CASE WHEN r.kind='observation' THEN r.cleared_at ELSE COALESCE(r.cleared_at,(SELECT at FROM block_migration_now)) END,
  CASE WHEN r.kind='observation' AND r.cleared_at IS NULL THEN NULL ELSE 'removed' END,0,1
FROM fence_recheck r
WHERE NOT EXISTS (SELECT 1 FROM block b WHERE b.kind=r.kind AND b.origin_key=r.origin_key)
  AND NOT EXISTS (SELECT 1 FROM block_migration_quarantine q WHERE q.kind=r.kind AND q.origin_key=r.origin_key);

INSERT INTO retirement_tombstone
SELECT kind,origin_key,evidence,evidence_at,(SELECT at FROM block_migration_now)
FROM block WHERE managed=0 AND quota=0 AND deadline<(SELECT at FROM block_migration_now)
  AND is_origin=1 AND EXISTS (SELECT 1 FROM fence_origin o
    WHERE o.kind=block.kind AND o.origin_key=block.origin_key AND o.evidence=block.evidence)
ON CONFLICT(kind,origin_key,evidence) DO NOTHING;
UPDATE block SET cleared_at=(SELECT at FROM block_migration_now),cleared_by='retired',source_present=0
WHERE managed=0 AND quota=0 AND deadline<(SELECT at FROM block_migration_now)
  AND is_origin=1 AND EXISTS (SELECT 1 FROM fence_origin o
    WHERE o.kind=block.kind AND o.origin_key=block.origin_key AND o.evidence=block.evidence);

CREATE TRIGGER retirement_tombstone_enforce AFTER INSERT ON retirement_tombstone
BEGIN
  UPDATE block SET cleared_at=NEW.retired_at,cleared_by='retired',source_present=0,claim_owner=NULL
  WHERE kind=NEW.kind AND origin_key=NEW.origin_key AND evidence=NEW.evidence;
END;

-- A retirement remains effective if delayed evidence re-creates its source.
-- These triggers cover the exact (kind, origin_key, evidence) signature.
-- Observation writes check their receipt time separately before insertion;
-- other new evidence for the same origin is not retired by these triggers.
CREATE TRIGGER block_retirement_insert AFTER INSERT ON block
WHEN EXISTS (SELECT 1 FROM retirement_tombstone t
  WHERE t.kind=NEW.kind AND t.origin_key=NEW.origin_key AND t.evidence=NEW.evidence)
BEGIN
  UPDATE block SET
    cleared_at=(SELECT retired_at FROM retirement_tombstone
      WHERE kind=NEW.kind AND origin_key=NEW.origin_key AND evidence=NEW.evidence),
    cleared_by='retired',source_present=0,claim_owner=NULL
  WHERE kind=NEW.kind AND origin_key=NEW.origin_key;
END;
CREATE TRIGGER block_retirement_update AFTER UPDATE OF kind,origin_key,evidence ON block
WHEN EXISTS (SELECT 1 FROM retirement_tombstone t
  WHERE t.kind=NEW.kind AND t.origin_key=NEW.origin_key AND t.evidence=NEW.evidence)
BEGIN
  UPDATE block SET
    cleared_at=(SELECT retired_at FROM retirement_tombstone
      WHERE kind=NEW.kind AND origin_key=NEW.origin_key AND evidence=NEW.evidence),
    cleared_by='retired',source_present=0,claim_owner=NULL
  WHERE kind=NEW.kind AND origin_key=NEW.origin_key;
END;

-- Earlier successful observation probes only cleared their clocks. Retire
-- that exact evidence and spend every authorization for it in the successors,
-- without changing the retained source, clock or authorization audit tables.
INSERT INTO retirement_tombstone(kind,origin_key,evidence,evidence_at,retired_at)
SELECT kind,origin_key,evidence,evidence_at,cleared_at FROM block
WHERE kind='observation' AND is_origin=1 AND managed=1 AND source_present=1
  AND cleared_by='kept' AND cleared_at IS NOT NULL
ON CONFLICT(kind,origin_key,evidence) DO NOTHING;
DELETE FROM probe_authorization AS p
WHERE p.kind='observation' AND EXISTS (
  SELECT 1 FROM retirement_tombstone t
  WHERE t.kind=p.kind AND t.origin_key=p.origin_key AND t.evidence=p.evidence
    AND t.evidence_at=p.evidence_at);

DROP TRIGGER model_route_state_inherit_fence_scope;
DROP TRIGGER model_facts_inherit_provider_cooldown;
CREATE TRIGGER model_facts_inherit_provider_cooldown AFTER INSERT ON model_facts
BEGIN
  INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,variant,
    quota_pressure,last_error,last_error_at,updated_at)
  SELECT NEW.route_key,NEW.capacity_key,NEW.provider_id,NEW.model_id,NEW.variant,
    p.quota_pressure,p.last_error,p.last_error_at,NEW.updated_at
  FROM (SELECT 1) LEFT JOIN provider_cooldown_state p ON p.provider_key=NEW.provider_id
  WHERE 1
  ON CONFLICT(route_key) DO NOTHING;
END;

DROP TABLE block_migration_now;
DROP TABLE block_migration_quarantine;
