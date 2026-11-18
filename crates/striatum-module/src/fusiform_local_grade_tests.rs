struct LocalGradeWire {
    credentials: crate::broca_credentials::SharedBrocaCredentials,
    version: i64,
    reads: usize,
    next: Option<Result<media_grades::LocalGrades, FusiformCatalogError>>,
}

impl CatalogWire for LocalGradeWire {
    fn credentials(&self) -> Option<crate::broca_credentials::SharedBrocaCredentials> {
        Some(self.credentials.clone())
    }
    async fn catalog_get(&mut self) -> Result<Arc<CatalogSnapshot>, FusiformCatalogError> {
        decode_catalog_get(serde_json::to_vec(&json!({
            "catalog_version":self.version,"resolved_at_ms":self.version,"models":{}
        })).unwrap())
    }
    async fn catalog_status(&mut self) -> Result<CatalogStatusReply, FusiformCatalogError> {
        Ok(CatalogStatusReply { catalog_version: self.version })
    }
    async fn local_grades(&mut self) -> Result<media_grades::LocalGrades, FusiformCatalogError> {
        self.reads += 1;
        self.next.take().expect("one local-grade read per refresh")
    }
    async fn close(&mut self) -> Result<(), FusiformCatalogError> { Ok(()) }
}

fn local_grades(grade: f64) -> media_grades::LocalGrades {
    media_grades::LocalGrades::from_test_grade("local:apple/phone-transcriber", "transcription", grade)
}

#[tokio::test]
async fn local_grade_wire_uses_one_prefix_only_tool_call_and_counts_it() {
    let sample: Value = serde_json::from_slice(include_bytes!("../fixtures/quality-grades-prefix-v1/local-two-entries.json")).unwrap();
    let (mut client, peer) = paged_wire(vec![(sample["request"].clone(), FrameType::Response, sample["response"].clone())]).await;
    let (grades, calls) = count_calls(client.local_grades()).await;
    let grades = grades.unwrap();
    assert_eq!(grades, media_grades::LocalGrades::from_test_entries(&[
        ("local:apple/phone-transcriber", "transcription", 59.0),
        ("local:example/offline-speaker", "speech", 72.5),
    ]));
    assert_eq!(calls, 1, "counter must observe a real Fusiform tool call");
    client.close().await.unwrap();
    paged_finish(peer, 1).await;
}

#[tokio::test]
async fn local_grade_wire_bad_request_is_unsupported_but_other_errors_are_failures() {
    let sample: Value = serde_json::from_slice(include_bytes!("../fixtures/quality-grades-prefix-v1/prefix-unsupported-older.json")).unwrap();
    let (mut client, peer) = paged_wire(vec![(sample["request"].clone(), FrameType::Error, sample["refusal"].clone())]).await;
    assert_eq!(client.local_grades().await.unwrap(), media_grades::LocalGrades::unsupported_prefix());
    client.close().await.unwrap();
    paged_finish(peer, 1).await;
    let (mut client, peer) = paged_wire(vec![(sample["request"].clone(), FrameType::Error, json!({"code":"unavailable","message":"temporarily offline"}))]).await;
    assert!(matches!(client.local_grades().await, Err(FusiformCatalogError::Refused { code, .. }) if code == "unavailable"));
    client.close().await.unwrap();
    paged_finish(peer, 1).await;
}

#[tokio::test]
async fn local_grades_refresh_with_catalog_changes_and_keep_last_good_on_failure() {
    let rig = Rig::new("local-grade-refresh");
    let store = Arc::new(rig.store());
    let health = Arc::new(Mutex::new(CatalogReachHealth::new(1000)));
    let credentials = Arc::new(Mutex::new(crate::broca_credentials::BrocaCredentials::default()));
    let mut wire = LocalGradeWire { credentials: credentials.clone(), version: 1, reads: 0, next: Some(Ok(local_grades(59.0))) };
    bootstrap_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    let first = credentials.lock().unwrap().catalog_snapshot.clone().unwrap();
    assert_eq!(first.local_media_grade("transcribe", "apple/phone-transcriber"), Some(59.0));
    assert_eq!(first.local_media_grade("synthesize", "apple/phone-transcriber"), None);
    assert_eq!(wire.reads, 1);
    poll_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    assert_eq!(wire.reads, 1, "an unchanged status poll is not a catalog refresh");
    wire.version = 2;
    wire.next = Some(Ok(local_grades(63.0)));
    poll_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    let changed = credentials.lock().unwrap().catalog_snapshot.clone().unwrap();
    assert_eq!(changed.cursor.catalog_version, 2);
    assert_eq!(changed.local_media_grade("transcribe", "apple/phone-transcriber"), Some(63.0));
    assert_eq!(first.local_media_grade("transcribe", "apple/phone-transcriber"), Some(59.0));
    assert_eq!(wire.reads, 2);
    for reason in [error("network timeout"), FusiformCatalogError::Refused { code: "unknown_tool".into(), message: "quality.grades".into() }] {
        wire.version += 1;
        wire.next = Some(Err(reason));
        poll_cycle(&mut wire, &store, &health, None, None).await.unwrap();
        let latest = credentials.lock().unwrap().catalog_snapshot.clone().unwrap();
        assert_eq!(latest.cursor.catalog_version, wire.version);
        assert_eq!(latest.local_media_grade("transcribe", "apple/phone-transcriber"), Some(63.0));
    }
    assert_eq!(wire.reads, 4);
    wire.version += 1;
    wire.next = Some(Ok(Default::default()));
    poll_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    assert_eq!(credentials.lock().unwrap().catalog_snapshot.as_ref().unwrap().local_media_grade("transcribe", "apple/phone-transcriber"), None);
    assert_eq!(wire.reads, 5);
}

#[tokio::test]
async fn older_fusiform_unknown_prefix_clears_local_grades_without_retrying() {
    let rig = Rig::new("local-grade-old-prefix");
    let store = Arc::new(rig.store());
    let health = Arc::new(Mutex::new(CatalogReachHealth::new(1000)));
    let credentials = Arc::new(Mutex::new(crate::broca_credentials::BrocaCredentials::default()));
    let mut wire = LocalGradeWire { credentials: credentials.clone(), version: 1, reads: 0, next: Some(Ok(local_grades(59.0))) };
    bootstrap_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    wire.version = 2;
    wire.next = Some(Ok(media_grades::LocalGrades::unsupported_prefix()));
    poll_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    assert_eq!(credentials.lock().unwrap().catalog_snapshot.as_ref().unwrap().local_media_grade("transcribe", "apple/phone-transcriber"), None);
    assert_eq!(wire.reads, 2);
    wire.next = None;
    bootstrap_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    assert_eq!(wire.reads, 2);
    wire.version = 3;
    wire.next = Some(Ok(local_grades(59.0)));
    poll_cycle(&mut wire, &store, &health, None, None).await.unwrap();
    assert_eq!(wire.reads, 3);
    assert_eq!(credentials.lock().unwrap().catalog_snapshot.as_ref().unwrap().local_media_grade("transcribe", "apple/phone-transcriber"), Some(59.0));
}
