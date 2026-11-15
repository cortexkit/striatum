use super::*;

#[test]
fn consumer_names_migration_preserves_policy_rows_and_history() {
    let scratch = cortexkit_test_support::ScratchDir::new("consumer-names-migration");
    let descriptor = dev_descriptor(
        "consumer-names",
        scratch.path().join("store.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 93).unwrap();
    let pairs = [
        ("mason", "prefrontal-core.mason"),
        ("sidekick.explore", "prefrontal-core.sidekick.explore"),
        ("athena", "prefrontal-core.athena"),
        ("athena.gather.child", "prefrontal-core.athena.gather.child"),
        ("synthesizer", "prefrontal-core.athena.synthesizer"),
        ("lab", "prefrontal-core.lab"),
        ("core", "prefrontal-core.wake"),
        ("codemode.classify", "basal.codemode.classify"),
        ("flow", "basal.flow"),
        ("mc_summary", "magic-context.summary"),
        ("mc_dream", "magic-context.dream"),
        ("mc_dream_classify", "magic-context.dream.classify-memories"),
        ("mc_dream_curate", "magic-context.dream.curate"),
        (
            "mc_dream_compress_cues",
            "magic-context.dream.compress-cues",
        ),
        (
            "mc_dream_review_user",
            "magic-context.dream.review-user-memories",
        ),
        ("mc_dream_map", "magic-context.dream.map-memories"),
        ("mc_dream_primers", "magic-context.dream.refresh-primers"),
        (
            "mc_dream_retrospective",
            "magic-context.dream.reflect-events",
        ),
        ("mc_dream_verify.child", "magic-context.dream.verify.child"),
        ("head", "head"),
        ("media.smoke", "media.smoke"),
        ("runners", "runners"),
        ("masonry", "masonry"),
        ("mc_dream_docs", "mc_dream_docs"),
    ];
    old.store.with_conn(|conn| {
        for (i, (consumer, _)) in pairs.iter().enumerate() {
            conn.execute("INSERT INTO routing_v1_decision
                (decision_id, principal, consumer, provider, model, runner_module, affinity,
                 created_at_ms, expires_at_ms, allowed_account_class, selection_json)
                VALUES (?1, 'owner', ?2, 'p', 'm', 'runner', ?1, 100, 999999,
                        'priced_per_token_only', '{\"consumer\":\"mason\"}')",
                params![i.to_string(), consumer])?;
            conn.execute("INSERT INTO routing_v1_affinity VALUES ('owner', ?1, ?2, ?2, 100, NULL)",
                params![consumer, i.to_string()])?;
            let state = serde_json::json!({"request":{"consumer":consumer}, "answer":{"consumer":"mason"}}).to_string();
            conn.execute("INSERT INTO routing_v1_panel VALUES (?1, 'owner', ?2, 100)",
                params![i.to_string(), state])?;
        }
        conn.execute_batch("INSERT INTO model_route_decisions
            (id, target_agent, consumer, requirements_json, score_json, rejected_json, anchor_version, created_at)
            VALUES ('legacy', 'sidekick.map', 'sidekick', '{}', '{}', '[]', 1, 100),
                   ('0', 'mason', 'mason', '{}', '{}', '[]', 1, 100);
            INSERT INTO routing_v1_report VALUES ('0','report','{\"consumer\":\"mason\"}','answer',100);
            INSERT INTO routing_v1_admission VALUES ('runner','admission','body','answer',100);
            INSERT INTO routing_v1_panel VALUES ('invalid', 'owner', 'not-json', 100);")?;
        Ok(())
    }).unwrap();
    drop(old);
    let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
    for (i, (old_name, new_name)) in pairs.iter().enumerate() {
        let decision = upgraded.v1_decision(&i.to_string()).unwrap().unwrap();
        assert_eq!(decision.consumer, *new_name, "{old_name}");
        assert_eq!(
            decision.allowed_account_class,
            striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly
        );
        assert_eq!(decision.expires_at_ms, 999999);
        assert_eq!(
            upgraded
                .v1_affinity_decision("owner", new_name, &i.to_string())
                .unwrap()
                .unwrap()
                .decision_id,
            i.to_string()
        );
        upgraded
            .store
            .with_conn(|conn| {
                let state: String = conn.query_row(
                    "SELECT state_json FROM routing_v1_panel WHERE panel_id=?1",
                    [i.to_string()],
                    |r| r.get(0),
                )?;
                let state: serde_json::Value = serde_json::from_str(&state).unwrap();
                assert_eq!(state["request"]["consumer"], *new_name);
                assert_eq!(
                    state["answer"]["consumer"], "mason",
                    "answer history stays unchanged"
                );
                Ok(())
            })
            .unwrap();
    }
    upgraded
        .store
        .with_conn(|conn| {
            let ledger: (String, String) = conn.query_row(
                "SELECT target_agent, consumer FROM model_route_decisions WHERE id='legacy'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!(
                ledger,
                ("prefrontal-core.sidekick.map".into(), "sidekick".into())
            );
            let history: (String, String) = conn.query_row(
                "SELECT target_agent, consumer FROM model_route_decisions WHERE id='0'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!(history, ("prefrontal-core.mason".into(), "mason".into()));
            assert_eq!(
                conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                    .get::<_, i64>(
                    0
                ))?,
                0
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(
        upgraded.v1_selection_json("0").unwrap().unwrap(),
        "{\"consumer\":\"mason\"}"
    );
    assert_eq!(
        upgraded.v1_report("0").unwrap().unwrap().body_json,
        "{\"consumer\":\"mason\"}"
    );
    assert_eq!(
        upgraded
            .v1_admission("runner", "admission")
            .unwrap()
            .unwrap()
            .answer_json,
        "answer"
    );
    drop(upgraded);
    let reopened = SqliteRouteStateStore::open(&descriptor).unwrap();
    assert_eq!(
        reopened.v1_decision("0").unwrap().unwrap().consumer,
        "prefrontal-core.mason"
    );
}

#[test]
fn consumer_names_migration_affinity_collisions_keep_latest_end_marker_and_canonical_ties() {
    let scratch = cortexkit_test_support::ScratchDir::new("consumer-affinity-collisions");
    let descriptor = dev_descriptor(
        "consumer-affinity",
        scratch.path().join("store.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 93).unwrap();
    old.store.with_conn(|conn| {
        for (key, old_time, new_time, old_end, new_end) in [
            ("old-newer", 200, 100, Some(201), None),
            ("new-newer", 100, 200, None, Some(201)),
            ("tie", 200, 200, None, Some(201)),
        ] {
            conn.execute("INSERT INTO routing_v1_affinity VALUES ('owner','mason',?1,'old',?2,?3)", params![key, old_time, old_end])?;
            conn.execute("INSERT INTO routing_v1_affinity VALUES ('owner','prefrontal-core.mason',?1,'canonical',?2,?3)", params![key, new_time, new_end])?;
        }
        Ok(())
    }).unwrap();
    drop(old);
    let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
    upgraded.store.with_conn(|conn| {
        for (key, expected) in [("old-newer", "old"), ("new-newer", "canonical"), ("tie", "canonical")] {
            let row: (String, i64, Option<i64>) = conn.query_row("SELECT decision_id, updated_at_ms, ended_at_ms FROM routing_v1_affinity WHERE principal='owner' AND consumer='prefrontal-core.mason' AND affinity=?1", [key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            assert_eq!(row, (expected.into(), 200, Some(201)), "{key}");
        }
        assert_eq!(conn.query_row("SELECT count(*) FROM routing_v1_affinity", [], |r| r.get::<_, i64>(0))?, 3);
        Ok(())
    }).unwrap();
}
