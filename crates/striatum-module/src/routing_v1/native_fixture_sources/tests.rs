#![cfg(test)]

/// Seed a failure deadline/reason before serving a fixture request. No request
/// owns a probe on this row (`claim_owner` defaults to null). Stored
/// `model_route_state.failure_backoff_until` and `failure_backoff_reason` stay null.
pub(super) fn failure_backoff(
    store: &striatum_store::SqliteRouteStateStore,
    key: &str,
    until: f64,
    reason: Option<&str>,
) {
    store.with_writer_for_test(|conn| {
        let inserted=conn.execute(
            "INSERT INTO block(kind,origin_key,provider,model,evidence,evidence_at,quota,deadline,
                next_recheck_at,observed_at,scope_error,wall_class,wall_unscoped,reason,source_order)
             SELECT 'failure',json_array(m.route_key),m.provider_id,m.model_id,
                json_array(?2,?3,COALESCE(m.failure_observed_at,m.last_error_at,m.updated_at)),
                COALESCE(m.failure_observed_at,m.last_error_at,m.updated_at),0,?2,
                MIN(?2,COALESCE(m.failure_observed_at,m.last_error_at,m.updated_at)+3600000),
                COALESCE(m.failure_observed_at,m.last_error_at,m.updated_at),m.last_error,
                r.wall_class,COALESCE(r.wall_unscoped,0),?3,m.rowid
             FROM model_route_state m LEFT JOIN block r
                ON r.kind='route' AND r.origin_key=json_array(m.route_key) AND r.source_present=1
             WHERE m.route_key=?1",
            rusqlite::params![key,until,reason],
        )?;
        assert_eq!(inserted,1,"the fixture requires an existing route and one new native failure source");
        let frozen:(Option<f64>,Option<String>)=conn.query_row(
            "SELECT failure_backoff_until,failure_backoff_reason FROM model_route_state WHERE route_key=?1",
            [key],|row|Ok((row.get(0)?,row.get(1)?)),
        )?;
        assert_eq!(frozen,(None,None),"post-migration failure inputs must not populate frozen columns");
        Ok(())
    }).unwrap();
}
