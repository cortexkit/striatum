use super::*;

fn migrated_provider_stamp(route_clock: bool) -> (Rig, i64, i64, i64) {
    let name = if route_clock {
        "provider-stamp-clocked-route"
    } else {
        "provider-stamp-unclocked-route"
    };
    let old = legacy_sync_rig(name);
    let now = crate::now_ms() + 60_000;
    let clock_at = now - 2000;
    let source_at = now - 1000;
    let admitted = now + 60_001;
    old.store.with_writer_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT max(version) FROM cortexkit_schema_version WHERE namespace='route_state'",[],|r|r.get::<_,i64>(0))?,95);
        for (model,error) in [("inherited","provider capacity"),("independent","independent capacity")] {
            conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_unscoped,last_error,last_error_at,updated_at)
                VALUES (?1,?1,'stamp-p',?2,?3,1,?4,?5,?5)",params![format!("stamp-p/{model}"),model,now+60000,error,clock_at])?;
        }
        if route_clock { crate::fence_rechecks::legacy::sync_at(conn,now)?; }
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_unscoped,last_error,last_error_at,updated_at)
            VALUES ('stamp-p',?1,1,'provider capacity',?2,?2)",params![now+60000,clock_at])?;
        crate::fence_rechecks::legacy::register(conn,"provider","[\"stamp-p\"]")?;
        conn.execute("UPDATE provider_cooldown_state SET last_error_at=?1,updated_at=?1 WHERE provider_key='stamp-p'",[source_at])?;
        conn.execute("UPDATE model_route_state SET last_error_at=?1,updated_at=?1 WHERE route_key='stamp-p/inherited'",[source_at])?;
        conn.execute("UPDATE fence_recheck SET claim_owner='stamp-owner',claim_generation=7,last_claimed_at=?1,next_recheck_at=?2 WHERE kind='provider' AND provider='stamp-p'",params![admitted,admitted+60000])?;
        conn.execute("INSERT INTO fence_probe_authorization SELECT 'stamp-owner',kind,origin_key,evidence,evidence_at,?1,claim_generation FROM fence_recheck WHERE kind='provider' AND provider='stamp-p'",[admitted])?;
        Ok(())
    }).unwrap();
    (upgrade_sync_rig(old, name), now, source_at, admitted)
}

fn heal_migrated_provider_stamp(route_clock: bool) {
    let (rig, now, source_at, admitted) = migrated_provider_stamp(route_clock);
    let provider_key = "[\"stamp-p\"]";
    let route_key = "[\"stamp-p/inherited\"]";
    let before = rig.published();
    let provider = before.block("provider", provider_key).unwrap();
    assert_eq!(provider.observed_at, Some(source_at));
    assert_ne!(provider.block.evidence_at, source_at);
    let route = before.block("route", route_key).unwrap();
    assert_eq!(route.observed_at, Some(source_at));
    assert_eq!(route.managed, route_clock);
    assert_eq!(
        route.block.evidence_at,
        if route_clock {
            provider.block.evidence_at
        } else {
            source_at
        }
    );
    assert_eq!(
        before
            .authorizations()
            .filter(|proof| proof.owner == "stamp-owner")
            .count(),
        1
    );
    rig.store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.heal_authorized_probe("stamp-owner", "stamp-p", "inherited", admitted + 1)
        })
        .unwrap();
    let after = rig.published();
    assert_eq!(after, rig.fresh());
    assert!(!after
        .block("provider", provider_key)
        .is_some_and(|row| row.source_present));
    assert!(
        !after
            .block("route", route_key)
            .is_some_and(|row| row.source_present),
        "provider recovery cannot unhide the inherited route it was covering"
    );
    assert!(
        after
            .block("route", "[\"stamp-p/independent\"]")
            .is_some_and(|row| row.source_present),
        "a different cause never inherits the provider's recovery"
    );
    assert!(after
        .fence_rechecks(now)
        .unwrap()
        .iter()
        .all(|origin| !(origin.kind == "route" && origin.origin_key == route_key)));
    let walls = rig.store.diagnostic_route_walls(now).unwrap();
    assert!(walls
        .iter()
        .all(|wall| wall.route_key != "stamp-p/inherited"));
    assert!(walls
        .iter()
        .any(|wall| wall.route_key == "stamp-p/independent"));
    rig.reload();
    assert_eq!(rig.published(), after);
}

#[test]
fn provider_probe_heal_uses_source_stamp_when_covered_route_has_no_clock() {
    heal_migrated_provider_stamp(false);
}

#[test]
fn provider_probe_heal_uses_source_stamp_in_the_route_compare_and_set() {
    heal_migrated_provider_stamp(true);
}

fn unmanaged_provider_clear() -> (Rig, i64) {
    let old = legacy_sync_rig("unmanaged-provider-clear");
    let now = crate::now_ms() + 60_000;
    old.store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at)
            VALUES ('clear-p',?1,?2,1,'provider capacity',?2,?2)",params![now+60000,now-1000])?;
        for model in ["cleared","sibling"] {
            conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at)
                VALUES (?1,?1,'clear-p',?2,'{}','curated',?3)",params![format!("clear-p/{model}"),model,now])?;
        }
        assert_eq!(conn.query_row("SELECT count(*) FROM fence_recheck",[],|r|r.get::<_,i64>(0))?,0);
        Ok(())
    }).unwrap();
    let rig = upgrade_sync_rig(old, "unmanaged-provider-clear");
    let row = rig.published();
    assert!(!row.block("provider", "[\"clear-p\"]").unwrap().managed);
    let clear = rig
        .store
        .clear_route_cooldowns("clear-p/cleared")
        .unwrap()
        .unwrap();
    assert_eq!(clear.routes.len(), 1);
    assert_eq!(clear.routes[0].1.cooldown_until, Some((now + 60000) as f64));
    assert_eq!(
        rig.published()
            .block("provider", "[\"clear-p\"]")
            .unwrap()
            .cleared_models,
        vec!["cleared"]
    );
    (rig, now)
}

fn assert_provider_model_clear(rig: &Rig, now: i64) {
    let memory = rig.published();
    assert_eq!(memory, rig.fresh());
    let origins = memory.fence_rechecks(now).unwrap();
    assert!(
        !origins
            .iter()
            .any(|origin| origin.applies("clear-p", "", "", "cleared")),
        "the operator clear must be visible before any registration Tick"
    );
    assert!(
        origins
            .iter()
            .any(|origin| origin.applies("clear-p", "", "", "sibling")),
        "one model clear never removes its sibling's provider pressure"
    );
    let walls = rig.store.diagnostic_route_walls(now).unwrap();
    assert!(walls.iter().all(|wall| wall.route_key != "clear-p/cleared"));
    assert!(walls.iter().any(|wall| wall.route_key == "clear-p/sibling"));
}

#[test]
fn migrated_unmanaged_provider_model_clear_agrees_with_diagnostics_immediately() {
    let (rig, now) = unmanaged_provider_clear();
    assert_provider_model_clear(&rig, now);
}

#[test]
fn migrated_unmanaged_provider_model_clear_survives_the_first_tick_and_reload() {
    let (rig, now) = unmanaged_provider_clear();
    rig.store
        .account_command(AccountCommandKind::Tick, move |tx| tx.tick(now))
        .unwrap();
    assert_eq!(
        rig.published()
            .block("provider", "[\"clear-p\"]")
            .unwrap()
            .cleared_models,
        vec!["cleared"],
        "registration cannot erase an explicit clear of this same provider evidence"
    );
    assert_provider_model_clear(&rig, now);
    let after = rig.published();
    rig.reload();
    assert_eq!(rig.published(), after);
    assert_provider_model_clear(&rig, now);
}
