use super::*;
use striatum_core::RouteStateStore;

/// Keep the real pre-migration insert trigger and frozen SQL reader separate
/// from the native store. Only the native store runs the block migration.
struct Pair {
    legacy: crate::SqliteRouteStateStore,
    native: crate::SqliteRouteStateStore,
    now: i64,
    deadline: i64,
    _scratch: cortexkit_test_support::ScratchDir,
}

impl Pair {
    fn new(expired: bool) -> Self {
        Self::with_prior_catalogue_route(expired, false)
    }

    fn with_prior_catalogue_route(expired: bool, prior_route: bool) -> Self {
        let scratch = cortexkit_test_support::ScratchDir::new("derived-provider-snapshot");
        let old = crate::dev_descriptor(
            "old-provider-snapshot",
            scratch.path().join("old.db").to_string_lossy(),
        );
        let new = crate::dev_descriptor(
            "native-provider-snapshot",
            scratch.path().join("new.db").to_string_lossy(),
        );
        let migrations = crate::route_state_migrations();
        let block = migrations
            .iter()
            .position(|migration| migration.statements == block_storage::MIGRATION)
            .unwrap();
        let previous = migrations[..block].last().unwrap().version;
        let legacy =
            crate::SqliteRouteStateStore::open_at_version_for_test(&old, previous).unwrap();
        let staged =
            crate::SqliteRouteStateStore::open_at_version_for_test(&new, previous).unwrap();
        let now = crate::now_ms();
        let evidence_at = if expired { now - 120_000 } else { now - 100 };
        let deadline = if expired { now - 60_000 } else { now + 60_000 };
        for store in [&legacy, &staged] {
            store.with_writer_for_test(|conn| {
                if prior_route {
                    conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at)
                        VALUES ('derived/before','derived/before','derived','before','{}','curated',?1)", [now-200])?;
                }
                conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,quota_exhausted_until,wall_class,wall_unscoped,wall_observed_at,last_error,last_error_at,updated_at)
                    VALUES ('derived',?1,?2,'billing',1,?3,'provider-capacity',?3,?3)", rusqlite::params![deadline, deadline + 10_000, evidence_at])?;
                conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_unscoped,wall_observed_at,last_error,last_error_at,updated_at)
                    VALUES ('derived/independent','derived/independent','derived','independent',?1,1,?2,'independent-capacity',?2,?2)", rusqlite::params![now+120_000, now-50])?;
                // Create the probe retry before the refusal's deadline passes;
                // inserting already-expired evidence would just archive it instead.
                fence_rechecks::legacy::sync_at(conn, if expired { evidence_at + 1 } else { now })
            }).unwrap();
        }
        drop(staged);
        let native = crate::SqliteRouteStateStore::open(&new).unwrap();
        Self {
            legacy,
            native,
            now,
            deadline,
            _scratch: scratch,
        }
    }

    fn insert_catalogue(&self) {
        let insert = |conn: &rusqlite::Connection| {
            conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at)
                VALUES ('derived/new','derived/new','derived','new','{}','curated',?1)", [self.now]).map(|_| ())
        };
        self.legacy.with_writer_for_test(insert).unwrap();
        let now = self.now;
        self.native.account_command(crate::AccountCommandKind::Maintenance, move |tx| tx.pool_sql(|conn| {
            conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at)
                VALUES ('derived/new','derived/new','derived','new','{}','curated',?1)", [now]).map(|_| ())
        })).unwrap();
    }

    fn native_snapshot(&self, now: i64) -> striatum_core::model_router::ModelRouteStateSnapshot {
        let sql = self
            .native
            .build_snapshot_at_for_test(now as f64, now)
            .unwrap();
        let memory = self
            .native
            .with_reader_for_test(BlockState::load)
            .unwrap()
            .route_state_snapshot(now as f64, now)
            .unwrap();
        assert_eq!(
            memory, sql,
            "native memory and native SQL use the same public view"
        );
        sql
    }

    fn compare_frozen(&self, now: i64) -> striatum_core::model_router::ModelRouteStateSnapshot {
        let expected = self
            .legacy
            .with_reader_for_test(|conn| {
                Ok(crate::route_snapshot::legacy_read_snapshot(
                    conn, now as f64, now,
                ))
            })
            .unwrap()
            .unwrap();
        let actual = self.native_snapshot(now);
        assert_eq!(actual, expected, "the native public snapshot matches the untouched frozen SQL reader and old insert trigger");
        actual
    }

    fn assert_no_stored_inheritance(&self) {
        self.native.with_reader_for_test(|conn| {
            let frozen: (Option<f64>, Option<f64>, Option<String>, bool) = conn.query_row(
                "SELECT cooldown_until,quota_exhausted_until,wall_class,wall_unscoped FROM model_route_state WHERE route_key='derived/new'", [],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?;
            assert_eq!(frozen, (None,None,None,false));
            assert_eq!(conn.query_row("SELECT count(*) FROM block WHERE kind='route' AND origin_key=json_array('derived/new')", [], |row| row.get::<_,i64>(0))?, 0);
            Ok(())
        }).unwrap();
    }
}

#[test]
fn native_catalogue_snapshot_matches_frozen_fields_after_registered_migration() {
    let pair = Pair::new(false);
    pair.insert_catalogue();
    let snapshot = pair.compare_frozen(pair.now);
    let new = &snapshot.by_route["derived/new"];
    assert_eq!(new.cooldown_until, Some(pair.deadline as f64));
    assert_eq!(
        new.quota_exhausted_until,
        Some((pair.deadline + 10_000) as f64)
    );
    assert_eq!(new.wall_class, Some(striatum_core::RouteWallClass::Billing));
    pair.assert_no_stored_inheritance();
}

#[test]
fn native_catalogue_provider_projection_preserves_longer_independent_evidence() {
    let pair = Pair::new(false);
    pair.insert_catalogue();
    let snapshot = pair.compare_frozen(pair.now);
    let independent = &snapshot.by_route["derived/independent"];
    assert_eq!(
        independent.cooldown_until,
        Some((pair.now + 120_000) as f64)
    );
    assert_eq!(independent.quota_exhausted_until, None);
    assert_eq!(independent.wall_class, None);
    pair.assert_no_stored_inheritance();
}

#[test]
fn native_catalogue_provider_projection_disappears_on_a_real_heal() {
    let pair = Pair::new(false);
    pair.insert_catalogue();
    let before = pair.compare_frozen(pair.now);
    assert_eq!(
        before.by_route["derived/new"].cooldown_until,
        Some(pair.deadline as f64)
    );
    pair.native
        .account_transaction(|tx| {
            tx.heal_serving_fences(
                "derived",
                "account",
                "credential",
                "new",
                None,
                pair.now + 1,
                pair.now + 2,
            )
        })
        .unwrap();
    let after = pair.native_snapshot(pair.now + 2);
    let new = &after.by_route["derived/new"];
    assert_eq!(new.cooldown_until, None);
    assert_eq!(new.quota_exhausted_until, None);
    assert_eq!(new.wall_class, None);
    assert_eq!(
        after.by_route["derived/independent"].cooldown_until,
        Some((pair.now + 120_000) as f64)
    );
    pair.assert_no_stored_inheritance();
}

#[test]
fn native_catalogue_provider_projection_keeps_the_claimed_provider_term_raw() {
    let pair = Pair::new(false);
    pair.insert_catalogue();
    pair.compare_frozen(pair.now);
    let provider = pair
        .native
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .find(|row| row.kind == "provider" && row.provider == "derived")
        .unwrap();
    let claimed_at = pair.deadline + 10_001;
    let query = claimed_at + 1;
    assert!(pair
        .native
        .account_transaction(|tx| tx.grant_fence_rechecks(
            std::slice::from_ref(&provider),
            claimed_at,
            Some("derived-probe"),
        ))
        .unwrap());
    let raw = pair
        .native
        .get_provider_cooldown("derived")
        .unwrap()
        .unwrap();
    assert_eq!(raw.cooldown_until, Some(pair.deadline as f64));
    assert_eq!(
        raw.quota_exhausted_until,
        Some((pair.deadline + 10_000) as f64)
    );
    let empty = striatum_core::model_router::ModelRouteStateSnapshot::default();
    assert!(!striatum_core::route_state_store::route_cooled(
        Some(&raw),
        &empty,
        "derived/new",
        "derived/new",
        query as f64
    ));
    let snapshot = pair.native_snapshot(query);
    assert!(snapshot.by_route["derived/new"]
        .cooldown_until
        .is_some_and(|until| until > query as f64));
    assert!(striatum_core::route_state_store::route_cooled(
        Some(&raw),
        &snapshot,
        "derived/new",
        "derived/new",
        query as f64
    ));
    pair.assert_no_stored_inheritance();
}

#[test]
fn route_minted_after_provider_expiry_is_not_blocked() {
    let pair = Pair::new(true);
    let provider = pair
        .native
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .find(|row| row.kind == "provider")
        .expect("the previously registered provider clock still exists");
    assert!(provider.due_at() < pair.now);
    let raw = pair
        .native
        .get_provider_cooldown("derived")
        .unwrap()
        .unwrap();
    assert!(raw
        .cooldown_until
        .is_some_and(|until| until < pair.now as f64));
    pair.insert_catalogue();
    let snapshot = pair.compare_frozen(pair.now);
    let new = &snapshot.by_route["derived/new"];
    // The old SQL snapshot keeps expired deadlines and the billing class.
    // Preserve those fields exactly; without a later probe claim they must
    // not cool this newly minted route.
    assert_eq!(new.cooldown_until, Some(pair.deadline as f64));
    assert_eq!(
        new.quota_exhausted_until,
        Some((pair.deadline + 10_000) as f64)
    );
    assert_eq!(new.wall_class, Some(striatum_core::RouteWallClass::Billing));
    let empty = striatum_core::model_router::ModelRouteStateSnapshot::default();
    assert!(!striatum_core::route_state_store::route_cooled(
        Some(&raw),
        &empty,
        "derived/new",
        "derived/new",
        pair.now as f64
    ));
    assert!(!striatum_core::route_state_store::route_cooled(
        Some(&raw),
        &snapshot,
        "derived/new",
        "derived/new",
        pair.now as f64
    ));
    pair.assert_no_stored_inheritance();
}

#[test]
fn provider_projection_does_not_retroactively_inherit_an_existing_catalogue_row() {
    let pair = Pair::with_prior_catalogue_route(false, true);
    pair.insert_catalogue();
    let snapshot = pair.compare_frozen(pair.now);
    let before = &snapshot.by_route["derived/before"];
    assert_eq!(before.cooldown_until, None);
    assert_eq!(before.quota_exhausted_until, None);
    assert_eq!(before.wall_class, None);
    assert_eq!(
        snapshot.by_route["derived/new"].cooldown_until,
        Some(pair.deadline as f64)
    );
    pair.assert_no_stored_inheritance();
}
