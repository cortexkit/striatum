//! Route-snapshot fixtures keep raw telemetry and block sources separate.
use super::*;
use crate::block_storage::{self, FailureSource, WallSource};

#[derive(Clone, Debug)]
pub(super) struct Input {
    pub route: String,
    pub provider: String,
    pub model: String,
    pub cooldown: String,
    pub quota: String,
    pub failure: String,
    pub pressure: String,
    pub class: String,
    pub error: String,
    pub unscoped: bool,
    pub at: i64,
}

fn literal(value: &str) -> Option<&str> {
    (value != "NULL").then(|| &value[1..value.len() - 1])
}
fn number(value: &str) -> Option<f64> {
    value.parse().ok()
}
impl Input {
    fn quarantined(&self) -> bool {
        fence_rechecks::quarantined_row(self.unscoped, literal(&self.error), literal(&self.class))
    }
    fn update(&self) -> RouteStateUpdate {
        RouteStateUpdate {
            observed_at_ms: Some(self.at),
            cooldown_until: number(&self.cooldown),
            quota_exhausted_until: number(&self.quota),
            quota_pressure: number(&self.pressure),
            last_error: literal(&self.error).map(str::to_owned),
            wall_class: literal(&self.class)
                .map(|value| striatum_core::RouteWallClass::parse(value).unwrap()),
            unscoped_wall: self.unscoped,
            ..Default::default()
        }
    }
}

fn replace_wall(conn: &Connection, input: &Input, kind: &str, key: &str) -> rusqlite::Result<()> {
    if !input.quarantined() {
        if let Some(mut row) = block_storage::wall_source(
            conn,
            WallSource {
                kind,
                key,
                provider: &input.provider,
                model: (kind == "route").then_some(input.model.as_str()),
                cooldown: number(&input.cooldown),
                exhausted: number(&input.quota),
                wall_observed: Some(input.at),
                observed_at: input.at,
                error: literal(&input.error),
                class: literal(&input.class),
                unscoped: input.unscoped,
            },
        )? {
            let (table, column) = if kind == "route" {
                ("model_route_state", "route_key")
            } else {
                ("provider_cooldown_state", "provider_key")
            };
            row.source_order = conn.query_row(
                &format!("SELECT rowid FROM {table} WHERE {column}=?1"),
                [key],
                |stored| stored.get(0),
            )?;
            return block_storage::write_source(conn, &row).map(|_| ());
        }
    }
    conn.execute(
        "DELETE FROM block WHERE kind=?1 AND origin_key=json_array(?2)",
        rusqlite::params![kind, key],
    )
    .map(|_| ())
}

pub(super) fn route(rig: &Rig, input: Input) -> bool {
    let row = input.clone();
    rig.run(move |conn| {
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,quota_pressure,
            wall_observed_at,last_error,last_error_at,updated_at,failure_observed_at)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?6,?6,?6)
            ON CONFLICT(route_key) DO UPDATE SET quota_pressure=excluded.quota_pressure,
            wall_observed_at=excluded.wall_observed_at,last_error=excluded.last_error,failure_observed_at=excluded.failure_observed_at",
            rusqlite::params![row.route,format!("{}/{}",row.provider,row.model),row.provider,row.model,number(&row.pressure),row.at,literal(&row.error)])?;
        replace_wall(conn, &row, "route", &row.route)?;
        if let Some(mut source) = block_storage::failure_source(conn, FailureSource {
            key: &row.route, provider: &row.provider, model: &row.model,
            deadline: number(&row.failure), reason: Some("error"), observed_at: row.at,
            error: literal(&row.error), class: literal(&row.class), unscoped: row.unscoped,
        })? {
            source.source_order = conn.query_row("SELECT rowid FROM model_route_state WHERE route_key=?1", [&row.route], |stored| stored.get(0))?;
            block_storage::write_source(conn, &source)?;
        } else {
            conn.execute("DELETE FROM block WHERE kind='failure' AND origin_key=json_array(?1)", [&row.route])?;
        }
        Ok(())
    });
    quarantine_write(rig, &input, "route")
}

pub(super) fn provider(rig: &Rig, input: Input) -> bool {
    let row = input.clone();
    rig.run(move |conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,wall_observed_at,last_error,last_error_at,updated_at)
            VALUES (?1,?2,?3,?2,?2) ON CONFLICT(provider_key) DO UPDATE SET
            wall_observed_at=excluded.wall_observed_at,last_error=excluded.last_error",
            rusqlite::params![row.provider,row.at,literal(&row.error)])?;
        replace_wall(conn, &row, "provider", &row.provider)
    });
    quarantine_write(rig, &input, "provider")
}

/// Fixture preparation reproduces the old raw row, including any independent
/// failure backoff. It is not a producer witness. A quarantined signal must
/// subsequently enter the real producer, which must leave its source absent.
fn quarantine_write(rig: &Rig, input: &Input, kind: &str) -> bool {
    if !input.quarantined() {
        return false;
    }
    let trace = crate::writer_entry_trace::Trace::default();
    let installed = trace.install();
    let (key, expected) = if kind == "route" {
        rig.store
            .update_route_state(
                &input.route,
                &format!("{}/{}", input.provider, input.model),
                &input.update(),
            )
            .unwrap();
        (
            input.route.as_str(),
            "account_tx::route_state::update_route_state",
        )
    } else {
        rig.store
            .record_provider_cooldown(
                &input.provider,
                &input.route,
                &format!("{}/{}", input.provider, input.model),
                &input.update(),
            )
            .unwrap();
        (
            input.provider.as_str(),
            "account_tx::route_state::record_provider_cooldown",
        )
    };
    drop(installed);
    let entered = trace
        .entries()
        .into_iter()
        .any(|entry| entry.name == expected && entry.account_rows_written.is_some());
    assert!(
        entered,
        "quarantine coverage requires the actual returned producer entry: {expected}"
    );
    let absent: bool = rig.store.with_reader_for_test(|conn| conn.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM block WHERE kind=?1 AND origin_key=json_array(?2) AND source_present=1)",
        rusqlite::params![kind,key], |row| row.get(0),
    )).unwrap();
    // Never remove a source after the producer: a faulty import must remain
    // visible to this assertion instead of being erased by fixture cleanup.
    assert!(
        absent,
        "quarantined {kind} signal created a native source: {input:?}"
    );
    eprintln!("quarantine producer witness: kind={kind} key={key} cooldown={} quota={} error={} class={} observed={}",
        input.cooldown,input.quota,input.error,input.class,input.at);
    entered && input.quarantined() && absent
}

pub(super) fn delete_route(rig: &Rig, route: String) {
    rig.run(move |conn| {
        conn.execute("DELETE FROM model_route_state WHERE route_key=?1", [&route])?;
        conn.execute(
            "DELETE FROM block WHERE kind IN ('route','failure') AND origin_key=json_array(?1)",
            [&route],
        )?;
        Ok(())
    });
}
