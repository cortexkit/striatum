//! Dispatch tests check the control path, binding epochs, and isolation of blocking work.

use super::*;

#[tokio::test]
async fn undecodable_control_request_is_refused_without_stopping_serving() {
    let rig = Rig::new("undecodable-control-request");
    let store = Arc::new(rig.open_store());
    let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));
    let catalog_health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));
    let min_band = Arc::new(Mutex::new(None));
    let broca_credentials = empty_broca_credentials();
    let bind = json!({
        "op": "route.bind",
        "route_channel": 317,
        "epoch": 42,
        "target": { "kind": "management_surface", "module_id": DEFAULT_MODULE_ID },
        "identity": {
            "project_root": cortexkit_test_support::scratch_root(),
            "harness": "control-tolerance-test",
            "session": "session-1"
        }
    });
    let mut unknown_scope_field = bind.clone();
    unknown_scope_field["scope"] = json!({
        "owner": { "kind": "reserved", "module_id": "prefrontal-core" },
        "ref": "head-scope",
        "scope_epoch": 3,
        "kind": "head",
        "owner_authorized": true,
        "attributes": { "unknown_scope_authority": true }
    });

    for undecodable_body in [
        b"{not-json".to_vec(),
        serde_json::to_vec(&json!({ "op": "unknown.control.operation" })).unwrap(),
        serde_json::to_vec(&unknown_scope_field).unwrap(),
    ] {
        let (writer, mut replies) = mpsc::channel(3);
        let mut route_epochs = RouteEpochs::from([(317, 41)]);
        // Exercise the same dispatch and continuation decision as the live
        // read loop: a refusal, a valid bind, then a health request.
        for (corr, body) in [
            (11, undecodable_body),
            (12, serde_json::to_vec(&bind).unwrap()),
            (13, br#"{"op":"health.check"}"#.to_vec()),
        ] {
            let frame =
                Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body).unwrap();
            let ver = frame.header.ver;
            assert!(handle_frame(
                frame,
                &writer,
                &quota_cache,
                &store,
                &mut route_epochs,
                &min_band,
                &rig.dir.join("routing.jsonc"),
                &rig.dir.join("operator.jsonc"),
                &broca_credentials,
                &catalog_health,
            )
            .await
            .expect("an undecodable control request must not end the frame loop"));
            let reply = replies.try_recv().expect("each request must get a reply");
            assert_eq!(reply.header.ver, ver);
            assert_eq!(reply.header.channel, 0);
            assert_eq!(reply.header.epoch, 0);
            assert_eq!(reply.header.corr, corr);
            match corr {
                11 => {
                    assert_eq!(reply.header.ty, FrameType::Error);
                    let error: ErrorBody = serde_json::from_slice(&reply.body).unwrap();
                    assert_eq!(error.code, "invalid_request");
                    assert!(error.message.starts_with("invalid control request: "));
                    assert_eq!(route_epochs.get(&317), Some(&41));
                }
                12 => {
                    assert_eq!(reply.header.ty, FrameType::Response);
                    assert_eq!(
                        serde_json::from_slice::<ModuleControlResponse>(&reply.body).unwrap(),
                        ModuleControlResponse::RouteBindAck {}
                    );
                    assert_eq!(route_epochs.get(&317), Some(&42));
                }
                13 => {
                    assert_eq!(reply.header.ty, FrameType::Response);
                    assert!(matches!(
                        serde_json::from_slice::<ModuleControlResponse>(&reply.body).unwrap(),
                        ModuleControlResponse::HealthCheck { .. }
                    ));
                    assert_eq!(route_epochs.get(&317), Some(&42));
                }
                _ => unreachable!(),
            }
            assert!(replies.try_recv().is_err(), "only one reply per request");
        }
    }
}

#[tokio::test]
async fn route_bind_replaces_the_epoch_for_a_reused_channel() {
    let (writer, mut replies) = mpsc::channel(2);
    let mut route_epochs = RouteEpochs::new();
    let catalog_health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));

    for (corr, epoch) in [(1, 41), (2, 42)] {
        let body = serde_json::to_vec(&json!({
            "op": "route.bind",
            "route_channel": 7,
            "epoch": epoch,
            "target": { "kind": "management_surface", "module_id": DEFAULT_MODULE_ID },
            "identity": {
                "project_root": cortexkit_test_support::scratch_root(),
                "harness": "epoch-rebind-test",
                "session": "session-1"
            }
        }))
        .unwrap();
        let frame = Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body).unwrap();

        handle_control_request(
            frame,
            &writer,
            &mut route_epochs,
            &catalog_health,
            &empty_broca_credentials(),
            &Arc::new(Mutex::new(QuotaCache::default())),
        )
        .await
        .unwrap();
        let ack = replies.recv().await.unwrap();
        assert_eq!(ack.header.channel, 0);
        assert_eq!(ack.header.epoch, 0);
        assert_eq!(route_epochs.get(&7), Some(&epoch));
    }

    clear_route_epoch(&mut route_epochs, 7, 41);
    assert_eq!(route_epochs.get(&7), Some(&42));
    clear_route_epoch(&mut route_epochs, 7, 42);
    assert!(!route_epochs.contains_key(&7));
}

/// Wire tests for failures that used to be dropped without a reply or a log line.
mod select_wire_error_reporting {
    use super::*;
    use rusqlite::OptionalExtension;

    fn rig_sql(rig: &Rig, sql: &str) {
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute_batch(sql)
            .unwrap();
    }

    fn route_frame(corr: u64, request: serde_json::Value) -> Frame {
        Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            corr,
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap()
    }

    fn error_body(frame: &Frame) -> ErrorBody {
        assert_eq!(frame.header.ty, FrameType::Error);
        serde_json::from_slice(&frame.body).unwrap()
    }

    /// A route request whose handler fails before replying is answered with
    /// `internal_error` through the live frame dispatch, and the failure is
    /// logged. The handler failure here is real: the decision's remembered
    /// serving identity is unreadable when a `completed` outcome reads it.
    #[tokio::test]
    async fn a_route_handler_failing_before_its_reply_answers_internal_error() {
        let rig = Rig::new("route-handler-error-reply");
        let store = Arc::new(rig.open_store());
        let decision_id = store
            .record_decision(&DecisionInput {
                target_agent: "mason".into(),
                ..Default::default()
            })
            .unwrap();
        rig_sql(
                &rig,
                &format!(
                    "UPDATE model_route_decisions SET provider_identity_json = '{{' WHERE id = '{decision_id}'"
                ),
            );
        let (writer, mut replies) = mpsc::channel(4);
        let mut route_epochs = RouteEpochs::from([(7, 1)]);
        take_operator_warnings();
        assert!(handle_frame(
            route_frame(
                55,
                json!({
                    "method": ROUTE_SET_DECISION_OUTCOME_OP,
                    "params": { "decisionID": decision_id, "outcome": "completed" }
                }),
            ),
            &writer,
            &Arc::new(Mutex::new(QuotaCache::default())),
            &store,
            &mut route_epochs,
            &Arc::new(Mutex::new(None)),
            &rig.dir.join("routing.jsonc"),
            &rig.dir.join("operator.jsonc"),
            &empty_broca_credentials(),
            &Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
        )
        .await
        .unwrap());
        let reply = tokio::time::timeout(Duration::from_secs(10), replies.recv())
            .await
            .expect("a failed route handler must still answer its caller")
            .unwrap();
        assert_eq!(
            (reply.header.channel, reply.header.epoch, reply.header.corr),
            (7, 1, 55)
        );
        let error = error_body(&reply);
        assert_eq!(error.code, "internal_error");
        assert!(
            error
                .message
                .starts_with("routing failed before replying: "),
            "{}",
            error.message
        );
        let warnings = take_operator_warnings();
        assert!(
            warnings
                .iter()
                .any(|line| line.contains("route request on channel 7 (corr 55) failed: ")),
            "{warnings:?}"
        );
        assert!(replies.try_recv().is_err(), "exactly one reply");
    }

    fn reply_frame(corr: u64) -> Frame {
        Frame::build(
            FrameType::Response,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            corr,
            br#"{"result":{}}"#.to_vec(),
        )
        .unwrap()
    }

    /// A handler that already replied gets no second answer when it then
    /// fails; a handler that succeeds has its reply forwarded unchanged.
    #[tokio::test]
    async fn a_handler_failing_after_its_reply_sends_no_second_frame() {
        let (writer, mut replies) = mpsc::channel(4);
        take_operator_warnings();
        answer_route_request(&writer, 1, 7, 1, 61, |reply| async move {
            send(&reply, reply_frame(61)).await?;
            Err(ModuleError::Message("after the reply".into()))
        })
        .await;
        let forwarded = replies.try_recv().unwrap();
        assert_eq!(
            (forwarded.header.ty, forwarded.header.corr),
            (FrameType::Response, 61)
        );
        assert!(replies.try_recv().is_err(), "no second frame");
        let warnings = take_operator_warnings();
        assert!(
            warnings
                .iter()
                .any(|line| line.contains("failed after replying: after the reply")),
            "{warnings:?}"
        );

        answer_route_request(&writer, 1, 7, 1, 62, |reply| async move {
            send(&reply, reply_frame(62)).await
        })
        .await;
        assert_eq!(replies.try_recv().unwrap().header.corr, 62);
        assert!(replies.try_recv().is_err());
        assert!(take_operator_warnings().is_empty());
    }

    /// With the connection gone no reply can be delivered; the failure is
    /// still logged and serving does not stall on the closed writer.
    #[tokio::test]
    async fn a_failure_with_the_connection_closed_is_only_logged() {
        let (writer, replies) = mpsc::channel(4);
        drop(replies);
        take_operator_warnings();
        answer_route_request(&writer, 1, 7, 1, 63, |_reply| async move {
            Err(ModuleError::Message("egress channel closed".into()))
        })
        .await;
        let warnings = take_operator_warnings();
        assert!(
            warnings
                .iter()
                .any(|line| line.contains("(corr 63) failed: egress channel closed")),
            "{warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .any(|line| line.contains("(corr 63) got no failure reply")),
            "{warnings:?}"
        );
    }

    async fn record_cooldown(store: &SharedStore, params: serde_json::Value) -> Frame {
        run_route_wire_request(
            store,
            json!({ "method": ROUTE_RECORD_COOLDOWN_OP, "params": params }),
        )
        .await
    }

    fn stored_pressure(rig: &Rig, route_key: &str) -> Option<Option<f64>> {
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .query_row(
                "SELECT quota_pressure FROM model_route_state WHERE route_key = ?1",
                [route_key],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    }

    /// A route-scoped cooldown names its own capacity and a 0..=1 pressure;
    /// anything else is refused before it reaches the store.
    #[tokio::test]
    async fn record_cooldown_refuses_a_foreign_capacity_and_out_of_range_pressure() {
        let rig = Rig::new("record-cooldown-validation");
        let store = Arc::new(rig.open_store());
        for (route_key, capacity_key, pressure) in [
            ("p/m/high", "q/m", 0.5),
            ("p/m-long/high", "p/m", 0.5),
            ("p/m/high", "", 0.5),
            ("p/m/high", "p/m", 1.5),
            ("p/m/high", "p/m", -0.1),
        ] {
            let reply = record_cooldown(
                &store,
                json!({
                    "routeKey": route_key,
                    "capacityKey": capacity_key,
                    "cooldownUntil": 1.0e15,
                    "quotaPressure": pressure,
                }),
            )
            .await;
            let error = error_body(&reply);
            assert_eq!(
                error.code, "invalid_request",
                "{route_key} {capacity_key} {pressure}: {}",
                error.message
            );
            assert_eq!(stored_pressure(&rig, route_key), None, "{route_key}");
        }
        // Pressure out of range is refused on a provider-wide write too.
        let reply = record_cooldown(
            &store,
            json!({
                "routeKey": "p/m/high",
                "capacityKey": "p/m",
                "providerWide": true,
                "cooldownUntil": 1.0e15,
                "quotaPressure": 2.0,
            }),
        )
        .await;
        assert_eq!(error_body(&reply).code, "invalid_request");
        assert_eq!(stored_pressure(&rig, "p/m/high"), None);

        // The same route with its own capacity and an in-range pressure is
        // stored, for a variant route and for a variantless one.
        for (route_key, capacity_key) in [("p/m/high", "p/m"), ("p/m", "p/m")] {
            let reply = record_cooldown(
                &store,
                json!({
                    "routeKey": route_key,
                    "capacityKey": capacity_key,
                    "cooldownUntil": 1.0e15,
                    "quotaPressure": 0.4,
                }),
            )
            .await;
            assert_eq!(reply.header.ty, FrameType::Response, "{route_key}");
            assert_eq!(stored_pressure(&rig, route_key), Some(Some(0.4)));
        }
    }
}

/// Route handlers call the SQLite store synchronously. While one handler's
/// store write waits on another connection's write lock, the rest of the
/// runtime must keep running.
mod route_handler_isolation {
    use super::*;
    use rusqlite::OptionalExtension;

    struct StoreContention {
        waiting: tokio::sync::oneshot::Sender<()>,
        released: std::sync::mpsc::Receiver<()>,
    }

    static STORE_CONTENTION: Mutex<Option<StoreContention>> = Mutex::new(None);

    fn wait_for_lock_release(_attempt: i32) -> bool {
        let Some(contention) = STORE_CONTENTION.lock().unwrap().take() else {
            return false;
        };
        contention.waiting.send(()).is_ok() && contention.released.recv().is_ok()
    }

    /// On a current-thread runtime, a handler run on the runtime's only
    /// thread would stall every other task until its store call returned.
    /// Progress is shown by message order: the other task answers while the
    /// lock is still held, and the handler can only finish after release.
    #[tokio::test(flavor = "current_thread")]
    async fn a_route_handler_waiting_on_the_store_does_not_stall_other_tasks() {
        let rig = Rig::new("route-handler-store-wait");
        let store = Arc::new(rig.open_store());
        let db = rig.dir.join("store.db");
        // Observe actual SQLite contention, then keep the handler waiting
        // until COMMIT regardless of how long the async runtime is delayed.
        // The production busy timeout must not race the test's lock release.
        let (waiting, waited) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        *STORE_CONTENTION.lock().unwrap() = Some(StoreContention { waiting, released });
        store
            .set_busy_handler_for_test(Some(wait_for_lock_release))
            .unwrap();
        // A second connection holds the database's write lock, so the
        // handler's cooldown write waits inside the store call.
        let lock_holder = rusqlite::Connection::open(&db).unwrap();
        lock_holder.execute_batch("BEGIN EXCLUSIVE").unwrap();

        let (writer, mut replies) = mpsc::channel(4);
        let mut route_epochs = RouteEpochs::from([(7, 1)]);
        let request = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            71,
            serde_json::to_vec(&json!({
                "method": ROUTE_RECORD_COOLDOWN_OP,
                "params": {
                    "routeKey": "p/m",
                    "capacityKey": "p/m",
                    "cooldownUntil": 1.0e15,
                    "quotaPressure": 0.4,
                }
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(handle_frame(
            request,
            &writer,
            &Arc::new(Mutex::new(QuotaCache::default())),
            &store,
            &mut route_epochs,
            &Arc::new(Mutex::new(None)),
            &rig.dir.join("routing.jsonc"),
            &rig.dir.join("operator.jsonc"),
            &empty_broca_credentials(),
            &Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
        )
        .await
        .unwrap());

        tokio::select! {
            waiting = waited => waiting.expect("the handler reaches SQLite's busy wait"),
            reply = replies.recv() => panic!(
                "the handler must wait for the writer, not answer early: {}",
                String::from_utf8_lossy(&reply.unwrap().body)
            ),
        }

        // Only start the ping once the handler is inside the store call.
        // Another task on the same runtime must still be able to answer.
        let (ping, pinged) = tokio::sync::oneshot::channel::<()>();
        let (pong, ponged) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            pinged.await.unwrap();
            pong.send(()).unwrap();
        });
        ping.send(()).unwrap();
        ponged
            .await
            .expect("the other task answers while the handler waits on the store");
        assert!(
            replies.try_recv().is_err(),
            "the handler cannot have finished while the write lock is held"
        );

        lock_holder.execute_batch("COMMIT").unwrap();
        release.send(()).unwrap();
        let reply = replies.recv().await.unwrap();
        assert_eq!(
            (reply.header.ty, reply.header.corr),
            (FrameType::Response, 71),
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
        let pressure: Option<Option<f64>> = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row(
                "SELECT quota_pressure FROM model_route_state WHERE route_key = 'p/m'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(
            pressure,
            Some(Some(0.4)),
            "the write lands once the lock is free"
        );
    }
}
