use super::*;

#[path = "credential/tests.rs"]
mod credential;

#[path = "decisions/tests.rs"]
mod decisions;
#[path = "frozen/tests.rs"]
mod frozen;
#[path = "observation/tests.rs"]
mod observation;
#[path = "operator/tests.rs"]
mod operator;
#[path = "pool/tests.rs"]
mod pool;
#[path = "provider/tests.rs"]
mod provider;
#[path = "real/tests.rs"]
mod real;
#[path = "retention/tests.rs"]
mod retention;
#[test]
fn migrated_state_readers_match_legacy_state_without_virtual_source_rows() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-state-parity");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-state-parity",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.assert_block_parity_fixture_coverage_for_test(now);
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit',?1,?2,1,'capacity',?2)",rusqlite::params![now+2000,now-1])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit/old','inherit/old','inherit','old',?1,?2,1,'capacity',?2)",rusqlite::params![now+1000,now-1])?;
        conn.execute("INSERT INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'zzz',evidence FROM fence_recheck WHERE kind='provider'",[])?;
        conn.execute("INSERT INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'aaa',evidence FROM fence_recheck WHERE kind='provider'",[])?;
        Ok(())
    }).unwrap();
    let times = store.block_parity_times(now).unwrap();
    store
        .with_writer_for_test(|conn| {
            let orphaned_cleared: i64 = conn.query_row("SELECT count(*) FROM fence_recheck r WHERE r.kind!='observation' AND r.cleared_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fence_origin o WHERE o.kind=r.kind AND o.origin_key=r.origin_key)",[],|row| row.get(0))?;
            assert_eq!(orphaned_cleared,1,"the fixture exercises a cleared orphan before normalization");
            fence_rechecks::sync_at(conn,now)?;
            conn.execute("INSERT INTO fence_probe_authorization SELECT claim_owner,kind,origin_key,evidence,evidence_at,COALESCE(last_claimed_at,evidence_at),claim_generation FROM fence_recheck WHERE claim_owner IS NOT NULL",[])?;
            let legacy = crate::block_state::legacy::BlockState::load(conn)?;
            block_storage::install_migration_function(conn)?;
            conn.execute_batch(block_storage::MIGRATION)?;
            let state = BlockState::load(conn)?;
            assert!(state.enabled());
            assert!(state.shares_storage_with(&state.clone()));
            assert_eq!(state.eligibility(), legacy.eligibility());
            assert_eq!(state.size_refusals(), legacy.size_refusals());
            assert_eq!(
                state.route_fence_latest_reports(),
                legacy.route_fence_latest_reports()
            );
            assert_eq!(state.entitlement_fences(), legacy.entitlement_fences());
            assert_eq!(
                state.active_fence_providers(),
                legacy.active_fence_providers()
            );
            assert_eq!(state.authorization_owners(), legacy.authorization_owners());
            assert_eq!(state.clock_rechecks(), legacy.clock_rechecks());
            assert_eq!(state.live_clocks(),legacy.live_clocks());
            assert!(!state.needs_sync(), "migration leaves no orphaned clocks to reconcile");
            for class in ["quota", "transient", "billing", "unknown"] {
                assert_eq!(
                    state.route_fences(class),
                    legacy.route_fences(class),
                    "{class}"
                );
            }
            let quarantined: std::collections::HashSet<_> = legacy.quarantined().into_iter().collect();
            let kept = |origin: &ViewOrigin| !quarantined.contains(&(origin.kind.clone(),origin.origin_key.clone()));
            let origins: Vec<_> = legacy.view_origins().into_iter().filter(kept).collect();
            assert_eq!(state.view_origins(), origins);
            assert_eq!(
                state.view_origins_in_scan_order(),
                legacy.view_origins_in_scan_order().into_iter().filter(kept).collect::<Vec<_>>()
            );
            for at in times {
                let before = legacy.blocks(at).unwrap();
                let after = state.blocks(at).unwrap();
                assert_eq!(after.blocks, before.blocks, "blocks at {at}");
                assert_eq!(
                    after.cleared_evidence, before.cleared_evidence,
                    "cleared at {at}"
                );
                assert_eq!(
                    state.fence_rechecks(at).unwrap(),
                    legacy.fence_rechecks(at).unwrap(),
                    "scan order at {at}"
                );
                let before = legacy.route_state_snapshot(at as f64, at).unwrap();
                let after = state.route_state_snapshot(at as f64, at).unwrap();
                assert_eq!(after.now,before.now);
                for (before,after) in [(&before.by_route,&after.by_route),(&before.by_capacity,&after.by_capacity)] {
                    assert_eq!(after.keys().collect::<std::collections::BTreeSet<_>>(),before.keys().collect::<std::collections::BTreeSet<_>>());
                    for (key,row) in before { assert_eq!(after.get(key),Some(row),"route/capacity {key} at {at}"); }
                }
            }
            for origin in origins {
                let provider = origin.provider.as_str();
                let credential = origin.credential_id.as_deref().unwrap_or("missing");
                let model = origin.model.as_deref().unwrap_or("missing");
                assert_eq!(
                    state.entitlement_fence(provider, credential),
                    legacy.entitlement_fence(provider, credential)
                );
                assert_eq!(
                    state.active_fences(provider),
                    legacy.active_fences(provider)
                );
                assert_eq!(
                    state.active_pool_fences(provider).unwrap(),
                    legacy.active_pool_fences(provider).unwrap()
                );
                for proposed in [false, true] {
                    assert_eq!(
                        state.credential_quota_classification(provider, credential, proposed),
                        legacy.credential_quota_classification(provider, credential, proposed)
                    );
                }
                let principal = origin.principal.as_deref().unwrap_or("missing");
                let admission = origin.admission_id.as_deref().unwrap_or("missing");
                assert_eq!(
                    state.admission_exclusions(principal, admission),
                    legacy.admission_exclusions(principal, admission)
                );
                assert_eq!(
                    state.exclusion_count(principal, credential),
                    legacy.exclusion_count(principal, credential)
                );
                assert_eq!(
                    state.source_origin(&origin.kind, &origin.origin_key),
                    legacy
                        .source_origin(&origin.kind, &origin.origin_key)
                        .cloned()
                );
                assert_eq!(
                    state.account_route_fence(provider, credential, model),
                    legacy
                        .account_route_fence(provider, credential, model)
                        .map(|(row, class, at)| (row.clone(), class, at))
                );
                for class in ["billing", "transient"] {
                    assert_eq!(
                        state.non_quota_route_fence(provider, credential, model, class),
                        legacy
                            .non_quota_route_fence(provider, credential, model, class)
                            .map(|(row, at)| (row.clone(), at))
                    );
                }
                if let Some(size) = origin.size_class {
                    for quota in [false, true] {
                        assert_eq!(
                            state.size_refusal(quota, provider, credential, model, size),
                            legacy
                                .size_refusal(quota, provider, credential, model, size)
                                .cloned()
                        );
                    }
                }
                if !provider.is_empty() {
                    assert_eq!(
                        state.provider_cooldown(provider).unwrap(),
                        legacy.provider_cooldown(provider).unwrap()
                    );
                    let mut before = legacy.pool_key_rows(provider)?;
                    let mut after = state.pool_key_rows(provider)?;
                    before.sort();
                    after.sort();
                    assert_eq!(after, before);
                }
                state.check_source(conn, &origin.kind, &origin.origin_key, provider)?;
                state.check_clock(conn, &origin.kind, &origin.origin_key)?;
                state.check_provider_rows(conn, provider)?;
                state.check_exclusions(conn, principal, admission)?;
                let mut before = legacy.routes_stamped_by_provider(provider,origin.evidence_at,now+1);
                let mut after = state.routes_stamped_by_provider(provider,origin.evidence_at,now+1);
                before.sort_by(|a,b| a.0.cmp(&b.0)); after.sort_by(|a,b| a.0.cmp(&b.0)); assert_eq!(after,before);
            }
            for fence in legacy.active_fences("cpu-provider-0") {
                assert_eq!(
                    state.account_fence(&fence.fence_id),
                    legacy
                        .account_fence(&fence.fence_id)
                        .map(|(row, pool, recovery)| (row.clone(), pool, recovery))
                );
            }
            for owner in legacy.authorization_owners() {
                let mut before = legacy.clocks_claimed_by(&owner);
                let mut after = state.clocks_claimed_by(&owner);
                before.sort_by(|a,b| (&a.0.kind,&a.0.origin_key).cmp(&(&b.0.kind,&b.0.origin_key)));
                after.sort_by(|a,b| (&a.0.kind,&a.0.origin_key).cmp(&(&b.0.kind,&b.0.origin_key)));
                assert_eq!(after,before);
                assert_eq!(state.probe_release(&owner), legacy.probe_release(&owner));
                let mut before = legacy.clone();
                let mut after = state.clone();
                before.apply_probe_release(&owner, now);
                after.apply_probe_release(&owner, now);
                assert_eq!(
                    after.blocks(now).unwrap().blocks,
                    before.blocks(now).unwrap().blocks
                );
                assert_eq!(after.authorization_owners(), before.authorization_owners());
            }
            assert_eq!(
                state
                    .table_sizes()
                    .iter()
                    .find(|(name, _)| *name == "retirement_tombstone")
                    .unwrap()
                    .1,
                0
            );
            assert_eq!(state.block_rows().count(), state.tables[0].rows.len);
            Ok(())
        })
        .unwrap();
}

#[test]
fn captured_physical_rows_equal_reload_after_replace_move_rollback_and_retirement() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-state-capture");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-state-capture",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        conn.execute("INSERT INTO probe_authorization SELECT 'capture-owner',kind,origin_key,evidence,evidence_at,evidence_at,claim_generation FROM block WHERE kind='credential' AND source_present=1",[])?;
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let changed = Arc::clone(&captured);
        conn.update_hook(Some(move |_, database: &str, table: &str, rowid: i64| {
            if database == "main" {
                if let Some(index) = TABLES.iter().position(|name| *name == table) {
                    changed.lock().unwrap().push((index as u8,rowid));
                }
            }
        }));
        conn.authorizer(Some(|context: rusqlite::hooks::AuthContext<'_>| {
            if matches!(context.action,rusqlite::hooks::AuthAction::Delete { table_name } if TABLES.contains(&table_name)) {
                rusqlite::hooks::Authorization::Ignore
            } else { rusqlite::hooks::Authorization::Allow }
        }));
        let mut state = BlockState::load(conn)?;
        let original = state.clone();
        assert!(state.shares_storage_with(&original));
        let persisted=conn.prepare("SELECT name FROM pragma_table_xinfo('block') WHERE hidden=0 ORDER BY cid")?
            .query_map([],|row|row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?.join(",");
        let replace=format!("INSERT OR REPLACE INTO block({persisted}) SELECT {persisted} FROM block WHERE kind='credential'");
        let commands = [
            "UPDATE block SET claim_generation=claim_generation+1,claim_owner='claimed',cleared_models='[\"partial/model\"]' WHERE kind='credential'",
            replace.as_str(),
            "UPDATE block SET wall_class='transient' WHERE kind='account_route'",
            "UPDATE block SET wall_class='quota' WHERE kind='account_route'",
            "UPDATE block SET origin_key='[\"moved/route\"]' WHERE kind='failure'",
            "SAVEPOINT rejected; UPDATE block SET claim_owner='rolled-back' WHERE kind='account'; ROLLBACK TO rejected; RELEASE rejected",
            "DELETE FROM block WHERE kind='non_quota_size'",
            "DELETE FROM probe_authorization",
            "UPDATE pool_key_state SET last_used_at=last_used_at+1",
            "UPDATE provider_cooldown_state SET quota_pressure=0.75",
            "UPDATE model_route_state SET latency_ema_ms=123",
            "INSERT OR REPLACE INTO success_proof VALUES ('parity-fence','{}',17,19)",
            "INSERT OR REPLACE INTO account_eligibility(provider,credential_id,model,listing_fingerprint,code,observed_at) VALUES ('new-provider','new-credential','new-model','fingerprint','known',21)",
            "INSERT INTO retirement_tombstone SELECT kind,origin_key,evidence,evidence_at,evidence_at+1 FROM block WHERE kind='size'",
        ];
        for (step,command) in commands.into_iter().enumerate() {
            captured.lock().unwrap().clear();
            conn.execute_batch(command)?;
            let changes = captured.lock().unwrap().clone();
            assert!(!changes.is_empty(),"command {step} exercises the update hook");
            let changes = read_back(conn,&changes)?;
            state.apply(&changes);
            let loaded = BlockState::load(conn)?;
            assert_eq!(state,loaded,"memory equals load after command {step}: {command}");
            assert_eq!(state.fingerprints(),loaded.fingerprints());
            if command.contains("wall_class='transient'") {
                let records:Vec<_>=state.block_rows().filter(|row|row.block.kind=="account_route").collect();
                assert!(!records.is_empty());assert!(records.iter().all(|row|!row.is_origin()));
            }
            if command.contains("wall_class='quota'") {
                let records:Vec<_>=state.block_rows().filter(|row|row.block.kind=="account_route").collect();
                assert!(!records.is_empty());assert!(records.iter().all(|row|row.is_origin()));
            }
            for table in &state.tables { assert_eq!(table.rows.len,table.keys.len); }
            assert!(!state.shares_storage_with(&original));
        }
        assert!(state.block_rows().any(|row| row.block.kind == "credential" && !row.cleared_models.is_empty()));
        assert!(!state.block_rows().any(|row| row.block.kind == "size"));
        assert!(state.tables[1].rows.len == 0,"whole-table deletion reports every authorization");
        assert!(state.block("failure","[\"moved/route\"]").is_some());
        Ok(())
    }).unwrap();
}

#[test]
fn only_principal_blocks_have_the_empty_provider_in_both_scope_indexes() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-empty-provider-scopes");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-empty-provider-scopes",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    store
        .with_writer_for_test(|conn| {
            block_storage::install_migration_function(conn)?;
            conn.execute_batch(block_storage::MIGRATION)
        })
        .unwrap();
    let now = crate::now_ms();
    store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.pool_wall(
                "scope-provider",
                "other-credential",
                crate::key_pool::PoolWallKind::Billing,
                now + 1000,
                "billing",
                now,
            )?;
            tx.insert_exclusion_with_recheck(
                &crate::AccountExclusionRow {
                    principal: "scope-principal".into(),
                    admission_id: "scope-admission".into(),
                    credential_id: "scope-credential".into(),
                    runner_view_digest: "runner".into(),
                    roster_view_digest: "roster".into(),
                    observation_id: None,
                    recorded_at: now,
                },
                1000,
            )?;
            Ok(())
        })
        .unwrap();
    store.with_writer_for_test(|conn| {
        let state=BlockState::load(conn)?;
        assert!(state.block_rows().any(|row| row.block.kind!="principal"),"the real writer also creates a non-principal block");
        let physical=conn.prepare("SELECT kind,origin_key FROM block WHERE provider='' ORDER BY kind,origin_key")?
            .query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(physical,vec![("principal".into(),serde_json::json!(["scope-principal","scope-admission","scope-credential"]).to_string())]);
        let provider:Vec<_>=state.tables[0].scoped(&provider_scope("")).collect();
        let admission:Vec<_>=state.tables[0].scoped(&exclusion_scope("scope-principal","scope-admission")).collect();
        assert_eq!(provider.len(),1,"the principal row is in the empty-provider clock scope");
        assert_eq!(admission.len(),1,"the principal row is in the principal/admission scope");
        assert_eq!(provider[0].0,admission[0].0,"both indexes reach the same physical row");
        assert!(matches!(provider[0].1,Record::Block(row) if row.block.kind=="principal" && row.block.scope.provider.is_empty()));
        assert!(matches!(admission[0].1,Record::Block(row) if row.block.kind=="principal" && row.block.scope.principal.as_deref()==Some("scope-principal") && row.block.scope.admission_id.as_deref()==Some("scope-admission")));
        for table in [4,5,6] { assert_eq!(state.tables[table].scoped(&provider_scope("")).count(),0,"no telemetry row has the empty provider"); }
        state.check_provider_rows(conn,"")?;
        state.check_exclusions(conn,"scope-principal","scope-admission")?;
        conn.execute("UPDATE block SET claim_generation=claim_generation+1 WHERE kind='principal'",[])?;
        assert!(state.check_provider_rows(conn,"").is_err(),"the real empty-provider decision check rejects a stale principal clock");
        Ok(())
    }).unwrap();
}

#[test]
fn dry_run_clear_route_preserves_siblings_and_latest_report_boundary() {
    for (recorded_at, reported_at, clears) in [
        (1, Some(9), true),
        (1, Some(10), false),
        (1, Some(11), false),
        (9, None, true),
        (10, None, false),
    ] {
        let mut state = BlockState {
            enabled: true,
            tables: vec![Table::default(); TABLES.len()],
        };
        for (index, (kind, provider, credential, model)) in [
            ("account_route", "p", "c", "target"),
            ("account_route", "p", "c", "sibling"),
            ("account_route", "p", "other", "target"),
            ("account_route", "other", "c", "target"),
            ("non_quota_route", "p", "c", "target"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut row = block_storage::account_route_source(
                &crate::AccountRouteFence {
                    provider: provider.into(),
                    credential_id: credential.into(),
                    model: model.into(),
                    until_ms: 100,
                    decision_id: "decision".into(),
                    recorded_at: if index == 0 { recorded_at } else { 1 },
                },
                kind,
                if kind == "account_route" {
                    "quota"
                } else {
                    "billing"
                },
                if index == 0 { reported_at } else { Some(1) },
            )
            .unwrap();
            row.rowid = index as i64 + 1;
            let key = serde_json::json!([kind, row.block.origin_key]).to_string();
            state.tables[0].put(row.rowid, key, Some(Arc::new(Record::Block(row))));
        }
        state.dry_run_clear_route("p", "c", "target", 10);
        assert_eq!(
            state.source_record("account_route", r#"["p","c","target"]"#).is_none(),
            clears,
            "recorded={recorded_at}, reported={reported_at:?}: only evidence strictly before 10 clears"
        );
        for (kind, key) in [
            ("account_route", r#"["p","c","sibling"]"#),
            ("account_route", r#"["p","other","target"]"#),
            ("account_route", r#"["other","c","target"]"#),
            ("non_quota_route", r#"["p","c","target"]"#),
        ] {
            assert!(
                state.source_record(kind, key).is_some(),
                "unrelated {kind} {key} stays"
            );
        }
    }
}

#[test]
fn private_sweep_repairs_match_legacy_classification_heals_listing_and_replacement() {
    use crate::block_state::legacy::DryRunRepair as OldRepair;
    let scratch = cortexkit_test_support::ScratchDir::new("block-private-repair-parity");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-private-repair-parity",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(move |conn| {
        conn.execute("INSERT INTO credential_entitlement_fences(provider_id,credential_id,code,observed_at_ms) VALUES ('private-provider','private-credential','refused',?1)",[now-1])?;
        fence_rechecks::sync_at(conn,now)?;
        let mut old=crate::block_state::legacy::BlockState::load(conn)?;
        let fence=old.active_pool_fences("cpu-provider-0").unwrap().into_iter().next().unwrap();
        let route=old.route_fences("quota").into_iter().next().unwrap();
        let size=old.size_refusals().into_iter().next().unwrap();
        block_storage::install_migration_function(conn)?;conn.execute_batch(block_storage::MIGRATION)?;
        let mut state=BlockState::load(conn)?;
        let compare=|label:&str,state:&BlockState,old:&crate::block_state::legacy::BlockState| {
            let before=old.blocks(now).unwrap();let after=state.blocks(now).unwrap();
            assert_eq!(after.blocks.len(),before.blocks.len(),"block multiplicity after {label}");
            let keyed=|rows:Vec<Block>| rows.into_iter().map(|row|((row.kind.clone(),row.origin_key.clone()),row)).collect::<BTreeMap<_,_>>();
            let before_blocks=keyed(before.blocks);let after_blocks=keyed(after.blocks);
            assert_eq!(after_blocks.keys().collect::<Vec<_>>(),before_blocks.keys().collect::<Vec<_>>(),"block identities after {label}");
            for (key,block) in before_blocks { assert_eq!(after_blocks.get(&key),Some(&block),"block {key:?} after {label}"); }
            assert_eq!(after.cleared_evidence,before.cleared_evidence,"cleared evidence after {label}");
            assert_eq!(state.fence_rechecks(now).unwrap(),old.fence_rechecks(now).unwrap(),"scan order after {label}");
        };
        for quota in [true,false] {
            assert_eq!(state.dry_run_repair(DryRunRepair::Classify("private-provider".into(),"private-credential".into(),quota),now).unwrap(),old.dry_run_repair(OldRepair::Classify("private-provider".into(),"private-credential".into(),quota),now).unwrap());
            compare("sticky classification",&state,&old);
        }
        let changes=vec![("private-provider".into(),"private-credential".into(),Some("new-fingerprint".into()))];
        state.dry_run_listing(None,&changes);old.dry_run_listing(None,&changes);compare("listing",&state,&old);
        state.dry_run_reconcile_route(&route,now+20_000);old.dry_run_reconcile_route(&route,now+20_000);compare("reconcile route",&state,&old);
        state.dry_run_clear_size(&size);old.dry_run_clear_size(&size);compare("clear size",&state,&old);
        state.dry_run_clear_routes(&route.provider,&route.credential_id,now+1);old.dry_run_clear_routes(&route.provider,&route.credential_id,now+1);compare("clear routes",&state,&old);
        state.dry_run_repair(DryRunRepair::ClearHeld(fence.evidence.clone(),"recovered".into()),now).unwrap();old.dry_run_repair(OldRepair::ClearHeld(fence.evidence.clone(),"recovered".into()),now).unwrap();compare("clear held fence before replacement",&state,&old);
        let mut inserted=fence;inserted.evidence.fence_id="sweep-new-fence".into();
        state.dry_run_repair(DryRunRepair::InsertFence(inserted.clone()),now).unwrap();old.dry_run_repair(OldRepair::InsertFence(inserted),now).unwrap();compare("insert fence",&state,&old);
        state.dry_run_repair(DryRunRepair::ClearCredential("private-provider".into(),"private-credential".into()),now).unwrap();old.dry_run_repair(OldRepair::ClearCredential("private-provider".into(),"private-credential".into()),now).unwrap();compare("clear credential",&state,&old);
        Ok(())
    }).unwrap();
}

#[test]
fn native_pool_writer_keeps_frozen_deadlines_null_and_preserves_claim_history() {
    let scratch = cortexkit_test_support::ScratchDir::new("native-pool-block-writer");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "native-pool-block-writer",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store
        .with_writer_for_test(|conn| {
            block_storage::install_migration_function(conn)?;
            conn.execute_batch(block_storage::MIGRATION)
        })
        .unwrap();
    store
        .pool_wall(
            "native-pool",
            "credential",
            crate::key_pool::PoolWallKind::Billing,
            now + 1000,
            "first",
            now,
        )
        .unwrap();
    store.with_writer_for_test(move |conn| {
        let state=BlockState::load(conn)?;
        let key=serde_json::json!(["native-pool","credential"]).to_string();
        let row=state.block("pool_billing",&key).unwrap();
        assert!(row.managed);assert_eq!(row.block.evidence_at,now);assert_eq!(row.block.next_recheck_at,now+1000);
        conn.execute("UPDATE block SET last_claimed_at=?1,next_recheck_at=?2,claim_generation=7,claim_owner='held',classified=1,cleared_models='[\"model\"]' WHERE kind='pool_billing'",rusqlite::params![now+2000,now+10_000])?;
        Ok(())
    }).unwrap();
    store
        .pool_wall(
            "native-pool",
            "credential",
            crate::key_pool::PoolWallKind::Billing,
            now + 1000,
            "first",
            now,
        )
        .unwrap();
    store
        .with_reader_for_test(move |conn| {
            let state = BlockState::load(conn)?;
            let row = state
                .block(
                    "pool_billing",
                    &serde_json::json!(["native-pool", "credential"]).to_string(),
                )
                .unwrap();
            assert_eq!(row.block.claim_owner.as_deref(), Some("held"));
            assert_eq!(row.block.claim_generation, 7);
            assert!(row.block.classified);
            assert_eq!(row.cleared_models, vec!["model"]);
            Ok(())
        })
        .unwrap();
    store
        .pool_wall(
            "native-pool",
            "credential",
            crate::key_pool::PoolWallKind::Billing,
            now + 2000,
            "new",
            now + 1,
        )
        .unwrap();
    store
        .pool_wall(
            "native-pool",
            "credential",
            crate::key_pool::PoolWallKind::Billing,
            now + 3000,
            "delayed old",
            now,
        )
        .unwrap();
    store.with_reader_for_test(move |conn| {
        let frozen:(Option<i64>,Option<i64>,Option<String>,Option<String>,i64)=conn.query_row("SELECT walled_until,cooldown_until,wall_reason,cooldown_reason,wall_observed_at FROM pool_key_state WHERE provider='native-pool'",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
        assert_eq!(frozen,(None,None,None,None,now+1));
        let state=BlockState::load(conn)?;
        let row=state.block("pool_billing",&serde_json::json!(["native-pool","credential"]).to_string()).unwrap();
        assert_eq!(row.reason.as_deref(),Some("new"));assert_eq!(row.deadline,Some(crate::block_storage::Deadline::Integer(now+2000)));
        assert_eq!(row.block.last_claimed_at,Some(now+2000));assert_eq!(row.block.next_recheck_at,now+10_000);
        assert_eq!(row.block.claim_generation,7);assert_eq!(row.block.claim_owner,None);assert!(!row.block.classified);assert!(row.cleared_models.is_empty());
        assert!(row.managed);
        Ok(())
    }).unwrap();
}
