//! Shared SQL fixture for adapter and request-evaluator differential tests.

use crate::{fence_rechecks, SqliteRouteStateStore};
use rusqlite::params;

impl SqliteRouteStateStore {
    pub fn assert_block_parity_fixture_coverage_for_test(&self, now: i64) {
        use std::collections::BTreeSet;
        use striatum_core::blocks::{Cause, LifetimeClass, SOURCE_KINDS};
        let loaded = self.load_blocks(now).unwrap();
        assert_eq!(
            loaded
                .blocks
                .iter()
                .map(|b| b.kind.as_str())
                .collect::<BTreeSet<_>>(),
            SOURCE_KINDS.into_iter().collect()
        );
        assert_eq!(
            loaded
                .blocks
                .iter()
                .map(|b| b.lifetime)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([LifetimeClass::Quota, LifetimeClass::NonQuota])
        );
        assert!(
            loaded
                .blocks
                .iter()
                .any(|b| b.claim_owner.is_some() && b.claim_generation > 0),
            "claimed block"
        );
        assert!(
            loaded.blocks.iter().any(|b| b.next_recheck_at < now),
            "due block"
        );
        assert!(
            loaded.blocks.iter().any(|b| !b.cleared_models.is_empty()),
            "partial route clear"
        );
        assert!(!loaded.cleared_evidence.is_empty(), "cleared evidence");
        let size = loaded.blocks.iter().find(|b| b.kind == "size").unwrap();
        let other = loaded
            .blocks
            .iter()
            .find(|b| b.kind == "non_quota_size")
            .unwrap();
        assert_eq!(size.scope, other.scope);
        assert_eq!(size.origin_key, other.origin_key);
        assert_eq!(size.cause, Cause::SizeRefusal);
        assert_eq!(size.cause, other.cause);
        assert_eq!(size.lifetime, LifetimeClass::Quota);
        assert_eq!(other.lifetime, LifetimeClass::NonQuota);
        self.with_reader_for_test(|conn| {
            let source_kinds: BTreeSet<String> = conn.prepare("SELECT DISTINCT kind FROM fence_origin")?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
            assert_eq!(source_kinds,SOURCE_KINDS.into_iter().map(String::from).collect());
            assert!(conn.query_row("SELECT count(*) FROM fence_retired_origin",[],|r|r.get::<_,i64>(0))? > 0,"retired origin");
            let quarantined = fence_rechecks::quarantined_origins(conn)?;
            assert!(quarantined.iter().any(|(kind,_)|kind=="route"),"quarantined route");
            assert!(quarantined.iter().all(|(kind,key)|!loaded.blocks.iter().any(|b|&b.kind==kind && &b.origin_key==key)));
            for kind in ["provider","route"] {
                assert!(conn.query_row(&format!("SELECT count(*) FROM {} WHERE quota_exhausted_until IS NOT NULL",if kind=="provider" {"provider_cooldown_state"} else {"model_route_state"}),[],|r|r.get::<_,i64>(0))? > 0);
                assert!(loaded.blocks.iter().any(|b|b.kind==kind && b.lifetime==LifetimeClass::NonQuota));
            }
            let (view_quota,stored_quota,classified): (bool,bool,bool) = conn.query_row("SELECT o.quota,c.quota,c.classified FROM fence_origin o JOIN fence_recheck c USING(kind,origin_key) WHERE o.kind='credential' AND o.credential_id='oauth:cpu-0'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            assert_ne!(view_quota,stored_quota,"credential stored class differs from view");
            assert!(classified);
            assert!(loaded.blocks.iter().any(|b|b.kind=="credential" && b.classified && (b.lifetime==LifetimeClass::Quota)==stored_quota));
            Ok(())
        }).unwrap();
    }

    pub fn seed_block_parity_fixture_for_test(&self, now: i64) {
        self.with_writer_for_test(|conn| seed_block_parity_fixture(conn, now))
            .unwrap();
    }
}

/// The fixture's statements, for a caller that runs them on its own
/// connection or inside an account command.
pub(crate) fn seed_block_parity_fixture(
    conn: &rusqlite::Connection,
    now: i64,
) -> rusqlite::Result<()> {
    let at = now - 1_000;
    let until = now + 60_000;
    let fence = striatum_core::accounts::Fence {
        account_id: "account-0".into(),
        pool: None,
        recovery_set: Vec::new(),
        observed_at_ms: at,
        reset: striatum_core::accounts::ResetTime::Missing,
        retry_hint_ms: None,
        source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
    };
    conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json) VALUES ('parity-fence','cpu-provider-0','account-0','{}',?1,'broca_account_limit',?2,'parity',?2,NULL,'[]')", params![serde_json::to_string(&fence).unwrap(),at])?;
    conn.execute_batch(&format!(r#"
                INSERT OR REPLACE INTO credential_entitlement_fences VALUES ('cpu-provider-0','oauth:cpu-0','billing_exhausted',NULL,{at});
                INSERT INTO account_route_fence VALUES ('cpu-provider-0','oauth:cpu-0','model-0',{until},'parity',{at},{at},'quota');
                INSERT INTO account_non_quota_route_fence VALUES ('cpu-provider-0','oauth:cpu-0','model-0',{until},'parity',{at},{at},'billing');
                INSERT INTO account_non_quota_route_fence VALUES ('cpu-provider-0','oauth:cpu-0','model-0',{until},'parity',{at},{at},'transient');
                INSERT INTO pool_key_state(provider,credential_id,walled_until,cooldown_until,wall_observed_at,cooldown_observed_at,updated_at) VALUES ('cpu-provider-0','oauth:cpu-0',{until},{until},{at},{at},{at});
                INSERT INTO provider_cooldown_state(provider_key,quota_exhausted_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('cpu-provider-0',{until},{at},1,'provider capacity',{at});
                INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,quota_exhausted_until,wall_observed_at,wall_unscoped,last_error,updated_at,failure_backoff_until,failure_observed_at)
                VALUES ('parity/route','parity/route','cpu-provider-0','model-0',{until},{at},1,'route capacity',{at},{until},{at})
                ON CONFLICT(route_key) DO UPDATE SET quota_exhausted_until=excluded.quota_exhausted_until;
                INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,wall_class,last_error,updated_at)
                VALUES ('parity/quarantined','parity/quarantined','cpu-provider-0','quarantined',{until},{at},0,'quota','{{"provider_code":"quota_exhausted","account_ref":"account-0"}}',{at});
                INSERT INTO account_size_refusal VALUES ('cpu-provider-0','oauth:cpu-0','model-0',17,{until},{at});
                INSERT INTO account_non_quota_size_refusal VALUES ('cpu-provider-0','oauth:cpu-0','model-0',17,{until},{at});
                INSERT INTO account_principal_exclusion VALUES ('parity-principal','parity-admission','oauth:cpu-0','runner','roster',NULL,{at});
                INSERT INTO account_transient_backoff VALUES ('cpu-provider-0','account-0',{until},{at},'parity-observation','[]');
                INSERT INTO credential_entitlement_fences VALUES ('parity-retired','retired','billing_exhausted',NULL,{at});
                INSERT INTO fence_retired_origin SELECT kind,origin_key,evidence,evidence_at,{now} FROM fence_origin WHERE provider='parity-retired';
            "#))?;
    fence_rechecks::sync(conn)?;
    conn.execute_batch(&format!(r#"
                UPDATE fence_recheck SET quota=1,classified=1,claim_generation=7 WHERE kind='credential' AND credential_id='oauth:cpu-0';
                UPDATE fence_recheck SET claim_owner='parity-owner',claim_generation=3,last_claimed_at={now},next_recheck_at={now}+900000 WHERE kind='account_route';
                UPDATE fence_recheck SET next_recheck_at={now}-1 WHERE kind='pool_rate';
                INSERT INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'model-1',evidence FROM fence_recheck WHERE kind='provider';
                INSERT INTO fence_recheck(kind,origin_key,provider,evidence,evidence_at,quota,next_recheck_at,cleared_at,claim_generation)
                VALUES ('credential','["parity-cleared","cleared"]','parity-cleared','cleared-evidence',{at},0,{until},{now},2);
            "#))?;
    Ok(())
}
