use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn refused_plane_retries_at_unchanged_version_but_no_coverage_stays_cached() {
    let rig = Rig::new("plane-retry");
    let store = Arc::new(rig.store());
    let health = Arc::new(Mutex::new(CatalogReachHealth::new(60_000)));
    let facts = [
        crate::tests::fact("openai", "example"),
        crate::tests::fact("anthropic", "example"),
    ];
    let credentials = Arc::new(Mutex::new(
        crate::broca_credentials::BrocaCredentials::api_test_credentials(&facts),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let openai_calls = Arc::new(AtomicUsize::new(0));
    let absent_calls = Arc::new(AtomicUsize::new(0));
    let peer_task = tokio::spawn({
        let calls = openai_calls.clone();
        let absent = absent_calls.clone();
        async move {
            while let Some(frame) = read_frame(&mut peer).await.unwrap() {
                if frame.header.ty == FrameType::Goodbye {
                    break;
                }
                let request: Value = serde_json::from_slice(&frame.body).unwrap();
                let (kind, body) = match request["arguments"]["provider_id"].as_str() {
                    Some("openai") => match calls.fetch_add(1, Ordering::SeqCst) {
                        1 => (FrameType::Error, json!({"code":"unavailable","message":"temporary failure"})),
                        n => (FrameType::Response, json!({
                            "plane":{"provider_id":"openai","auth_method":"apikey","kind":"metered"},
                            "models":{"openai/example":{"rate.output":{"state":"priced","currency":"USD","units":10 + n,"exponent":0}}}
                        })),
                    },
                    Some("anthropic") => {
                        absent.fetch_add(1, Ordering::SeqCst);
                        (FrameType::Error, json!({"code":"no_coverage"}))
                    }
                    other => {
                        assert_eq!(other, None);
                        assert_eq!(request["name"], CATALOG_STATUS);
                        (FrameType::Response, json!({"catalog_version":42}))
                    }
                };
                let reply = Frame::build(
                    kind, Flags::new(false, Priority::Interactive, false), frame.header.channel,
                    frame.header.epoch, frame.header.corr, serde_json::to_vec(&body).unwrap(),
                ).unwrap();
                write_frame(&mut peer, &reply).await.unwrap();
            }
        }
    });
    let mut client = FusiformCatalogConsumer {
        stream, credentials: Some(credentials.clone()), route_channel: 1, route_epoch: 1, next_corr: 1,
    };
    let mut snapshot = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
    Arc::make_mut(&mut snapshot).cursor.catalog_version = 41;
    client.refresh_planes(Some(&snapshot)).await.unwrap();
    let rate = || credentials.lock().unwrap().billing_catalog.for_route(
        "openai/example", &crate::broca_credentials::ServingMethod {
            auth_method: Some("apikey".into()), reason: "test".into(),
        }, false,
    ).rate("rate.output");
    assert_eq!(rate(), Some(10.0));

    Arc::make_mut(&mut snapshot).cursor.catalog_version = 42;
    client.refresh_planes(Some(&snapshot)).await.unwrap();
    let while_failed = rate();
    client.refresh_planes(None).await.unwrap();
    assert_eq!(openai_calls.load(Ordering::SeqCst), 2, "retry fired before its deadline");
    store.apply_fusiform_catalog(&snapshot.facts, snapshot.cursor).unwrap();
    tokio::time::sleep(RETRY_INTERVAL).await;
    let outcome = poll_cycle(&mut client, &store, &health, None, None).await.unwrap();
    peer_task.await.unwrap();
    assert_eq!(outcome.get_calls, 0);
    assert_eq!(openai_calls.load(Ordering::SeqCst), 3, "transient plane refusal was cached past its retry deadline");
    assert_eq!(absent_calls.load(Ordering::SeqCst), 2, "no_coverage must stay cached at the same version");
    assert_eq!(while_failed, Some(10.0), "the failed refresh discarded the last good plane");
    assert_eq!(rate(), Some(12.0));
}
