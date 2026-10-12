//! Rehearse read-side account-wall quarantine on a fresh VACUUM INTO copy.
//! The source is opened read-only; normal store startup runs only on the copy.
use prefrontal_routing_core::{account_wall::account_scoped_wall, RouteStateStore, RouteWallClass};
use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
use rusqlite::{Connection, OpenFlags};
use std::{collections::HashMap, path::PathBuf};

type Wall = (
    String,
    Option<f64>,
    Option<f64>,
    Option<String>,
    Option<RouteWallClass>,
);

fn walls(conn: &Connection) -> Vec<Wall> {
    conn.prepare("SELECT route_key,cooldown_until,quota_exhausted_until,last_error,wall_class FROM model_route_state ORDER BY route_key").unwrap()
        .query_map([],|row| {
            let class: Option<String> = row.get(4)?;
            Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,class.map(|class| RouteWallClass::parse(&class).unwrap())))
        }).unwrap().collect::<Result<_,_>>().unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "usage: account_wall_rehearsal SOURCE_DB NEW_COPY_DB"
    );
    let source = PathBuf::from(&args[1]).canonicalize().unwrap();
    let copy = PathBuf::from(&args[2]);
    assert!(
        !copy.exists(),
        "copy path must be new; existing databases are never opened for writing"
    );
    std::fs::create_dir_all(copy.parent().unwrap()).unwrap();
    let source_conn =
        Connection::open_with_flags(&source, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let before = walls(&source_conn);
    let eligible = before
        .iter()
        .filter(|(_, cooldown, quota, error, class)| {
            (cooldown.is_some() || quota.is_some()) && account_scoped_wall(error.as_deref(), *class)
        })
        .collect::<Vec<_>>();
    println!(
        "before copy: {} explicitly account-scoped wall row(s)",
        eligible.len()
    );
    for (route, cooldown, quota, _, class) in &eligible {
        println!(
            "quarantine candidate: {route} class={class:?} cooldown={cooldown:?} quota={quota:?}"
        );
    }
    // SQLite gives a consistent copy of the live WAL-backed database without
    // granting the rehearsal any write permission to the source database.
    source_conn
        .execute("VACUUM INTO ?1", [copy.to_string_lossy().as_ref()])
        .unwrap();
    let copy_conn = Connection::open_with_flags(&copy, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(walls(&copy_conn), before, "VACUUM copied all wall evidence");
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        "account-wall-rehearsal",
        copy.to_string_lossy(),
    ))
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;
    let snapshot = store.build_snapshot(now).unwrap();
    for (route, cooldown, quota, error, class) in &before {
        let state = &snapshot.by_route[route];
        let scoped = account_scoped_wall(error.as_deref(), *class);
        assert_eq!(
            state.cooldown_until,
            if scoped { None } else { *cooldown },
            "{route}"
        );
        assert_eq!(
            state.quota_exhausted_until,
            if scoped { None } else { *quota },
            "{route}"
        );
    }
    assert_eq!(
        walls(&copy_conn),
        before,
        "snapshot quarantine must not rewrite evidence"
    );
    let facts = store.get_facts().unwrap();
    let mut admitted_snapshot = snapshot;
    if let Some(quota) = store.quota_snapshot().unwrap() {
        let windows = serde_json::from_str::<Vec<prefrontal_routing_core::ProviderUsage>>(
            &quota.windows_json,
        )
        .unwrap();
        prefrontal_routing_core::merge_quota_overlay(
            &mut admitted_snapshot,
            &windows,
            &facts,
            &HashMap::new(),
        );
    }
    for (route, _, _, _, _) in eligible {
        let panel =
            prefrontal_routing_core::select_panel(prefrontal_routing_core::SelectPanelInput {
                package_tokens: None,
                panel_spec: serde_json::from_value(
                    serde_json::json!({"size":1,"iqFloor":90,"class":"SPEC","diversity":"none"}),
                )
                .unwrap(),
                selection: prefrontal_routing_core::PanelSelection::Roster,
                roster: serde_json::from_value(serde_json::json!([{"model":route}])).unwrap(),
                facts: facts.clone(),
                runtime_snapshot: Some(admitted_snapshot.clone()),
                exclude_route_keys: vec![],
                operator_exclude_keys: vec![],
                explicit_only_providers: Vec::new(),
                required_capabilities: None,
                available_models: [route.clone()].into_iter().collect(),
                availability_substrate: prefrontal_routing_core::AvailabilitySubstrate::Broca,
                is_subscription_route: None,
            })
            .unwrap();
        println!(
            "rehearsed seating: {route} seats={} shortfall={:?}",
            panel.members.len(),
            panel.shortfall
        );
    }
    println!("rehearsal: 3 checks passed (copy matches, snapshot quarantines only scoped walls, raw evidence unchanged); {} rows checked",before.len());
}
