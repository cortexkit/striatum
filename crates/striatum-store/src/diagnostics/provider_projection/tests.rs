use super::*;

fn stores(
    label: &str,
) -> (
    cortexkit_test_support::ScratchDir,
    SqliteRouteStateStore,
    SqliteRouteStateStore,
) {
    let scratch = cortexkit_test_support::ScratchDir::new(label);
    let old = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(label, scratch.path().join("old.db").to_string_lossy()),
        95,
    )
    .unwrap();
    let native = SqliteRouteStateStore::open(&crate::dev_descriptor(
        label,
        scratch.path().join("native.db").to_string_lossy(),
    ))
    .unwrap();
    (scratch, old, native)
}

fn insert_fact(conn: &Connection, model: &str) -> rusqlite::Result<()> {
    conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at,source)
        VALUES (?1,?1,'projection-p',?2,'{}','curated',0,'seed')",params![format!("projection-p/{model}"),model])?;
    Ok(())
}

// Read the original diagnostic fields directly from the real old schema.
// The old model_facts_inherit_provider_cooldown trigger copies provider
// deadlines, class and error fields into each newly inserted catalogue route.
fn old_wall(store: &SqliteRouteStateStore, key: &str) -> DiagnosticRouteWall {
    store.with_reader_for_test(|conn|conn.query_row("SELECT route_key,capacity_key,provider_id,model_id,cooldown_until,
        quota_exhausted_until,failure_backoff_until,failure_backoff_reason,last_error,last_error_at,wall_class
        FROM model_route_state WHERE route_key=?1",[key],|row|Ok(DiagnosticRouteWall {
            route_key:row.get(0)?,capacity_key:row.get(1)?,provider_id:row.get(2)?,model_id:row.get(3)?,
            cooldown_until:row.get(4)?,quota_exhausted_until:row.get(5)?,failure_backoff_until:row.get(6)?,failure_backoff_reason:row.get(7)?,
            last_error:row.get(8)?,last_error_at:row.get(9)?,wall_class:crate::row_wall_class(row,10)?,
        }))).unwrap()
}

fn provider_then_catalogue(old: &SqliteRouteStateStore, native: &SqliteRouteStateStore, now: i64) {
    old.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,quota_exhausted_until,wall_class,wall_unscoped,last_error,last_error_at,updated_at,wall_observed_at)
            VALUES ('projection-p',?1,?2,'billing',1,'provider capacity',?3,?3,?3)",params![now+60000,now+120000,now])?;
        insert_fact(conn,"new")
    }).unwrap();
    native
        .record_provider_cooldown(
            "projection-p",
            "projection-p/existing",
            "projection-p/existing",
            &crate::RouteStateUpdate {
                observed_at_ms: Some(now),
                unscoped_wall: true,
                cooldown_until: Some((now + 60000) as f64),
                quota_exhausted_until: Some((now + 120000) as f64),
                wall_class: Some(RouteWallClass::Billing),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    native
        .with_writer_for_test(|conn| insert_fact(conn, "new"))
        .unwrap();
}

#[test]
fn provider_only_catalogue_wall_diagnostics_equal_the_old_trigger_result() {
    let (_scratch, old, native) = stores("diagnostic-provider-catalogue");
    let now = crate::now_ms();
    provider_then_catalogue(&old, &native, now);
    let expected = old_wall(&old, "projection-p/new");
    assert_eq!(expected.cooldown_until, Some((now + 60000) as f64));
    let actual = native
        .diagnostic_route_walls(now)
        .unwrap()
        .into_iter()
        .find(|row| row.route_key == "projection-p/new");
    assert_eq!(
        actual,
        Some(expected),
        "every diagnostic field must retain main's inherited wall"
    );
}

#[test]
fn operator_clear_returns_the_provider_wall_it_actually_clears_for_a_new_route() {
    let (_scratch, old, native) = stores("clear-provider-catalogue");
    let now = crate::now_ms();
    provider_then_catalogue(&old, &native, now);
    let expected = old_wall(&old, "projection-p/new");
    let cleared = native
        .clear_route_cooldowns("projection-p/new")
        .unwrap()
        .unwrap();
    assert_eq!(
        cleared.routes,
        vec![(
            "projection-p/new".into(),
            crate::CooldownState {
                cooldown_until: expected.cooldown_until,
                quota_exhausted_until: expected.quota_exhausted_until,
            }
        )],
        "operator output must name the inherited pressure, not null horizons"
    );
    assert!(
        native
            .diagnostic_route_walls(now)
            .unwrap()
            .iter()
            .all(|row| row.route_key != "projection-p/new"),
        "the provider's explicit model clear suppresses inheritance"
    );
}

#[test]
fn failure_only_wall_keeps_the_original_billing_class_and_all_diagnostic_fields() {
    let (_scratch, old, native) = stores("diagnostic-failure-only-class");
    let now = crate::now_ms();
    old.with_writer_for_test(|conn| {
        insert_fact(conn,"failure")?;
        conn.execute("UPDATE model_route_state SET failure_backoff_until=?1,failure_backoff_reason='send failure',wall_class='billing',
            wall_unscoped=1,last_error='send error',last_error_at=?2 WHERE route_key='projection-p/failure'",params![now+60000,now])?;
        Ok(())
    }).unwrap();
    native.with_writer_for_test(|conn| {
        insert_fact(conn,"failure")?;
        conn.execute("UPDATE model_route_state SET last_error='send error',last_error_at=?1 WHERE route_key='projection-p/failure'",[now])?;
        let row=crate::block_storage::failure_source(conn,crate::block_storage::FailureSource {
            key:"projection-p/failure",provider:"projection-p",model:"failure",deadline:Some((now+60000) as f64),reason:Some("send failure"),
            observed_at:now,error:Some("send error"),class:Some("billing"),unscoped:true,
        })?.unwrap();
        crate::block_storage::write_source(conn,&row)?;
        Ok(())
    }).unwrap();
    assert_eq!(
        native.diagnostic_route_walls(now).unwrap(),
        vec![old_wall(&old, "projection-p/failure")]
    );
}
