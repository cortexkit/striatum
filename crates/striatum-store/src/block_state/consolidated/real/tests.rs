use super::*;

fn synthetic_state(value: f64) -> BlockState {
    let real = SqlReal::from(value);
    let mut state = BlockState {
        enabled: true,
        tables: vec![Table::default(); TABLES.len()],
    };
    let mut source = StoredBlock::from_origin(ViewOrigin {
        kind: "provider".into(),
        origin_key: "[\"float-provider\"]".into(),
        provider: "float-provider".into(),
        evidence: "synthetic".into(),
        evidence_at: 1,
        quota: Some(false),
        ..Default::default()
    })
    .unwrap();
    source.deadline = Some(block_storage::Deadline::Real(real));
    source.reset_at = Some(real);
    source.cooldown_until = Some(real);
    source.quota_exhausted_until = Some(real);
    state.tables[0].put(
        1,
        "[\"provider\",\"[\\\"float-provider\\\"]\"]".into(),
        Some(Arc::new(Record::Block(source))),
    );
    state.tables[4].put(
        1,
        "[\"float-provider\",\"credential\"]".into(),
        Some(Arc::new(Record::Pool(PoolTelemetry {
            provider: "float-provider".into(),
            credential_id: "credential".into(),
            last_used_at: SqlValue::Real(real),
        }))),
    );
    state.tables[5].put(
        1,
        "[\"float-provider\"]".into(),
        Some(Arc::new(Record::Provider(ProviderTelemetry {
            provider_key: "float-provider".into(),
            quota_pressure: Some(real),
            last_error: None,
        }))),
    );
    state.tables[6].put(
        1,
        "[\"float-provider/model\"]".into(),
        Some(Arc::new(Record::Route {
            provider: "float-provider".into(),
            row: RouteTelemetry {
                route_key: "float-provider/model".into(),
                capacity_key: "float-provider/model".into(),
                quota_pressure: Some(real),
                latency_ema_ms: Some(real),
                success_ema: Some(real),
                last_error: None,
            },
        })),
    );
    state
}

#[test]
fn all_native_real_fields_share_the_bitwise_state_equality() {
    fn requires_eq<T: Eq>(_: &T) {}
    // Synthetic NaNs exercise the Eq contract itself. SQL normalization of
    // NaN to NULL is checked separately in sql_real::tests.
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let state = synthetic_state(nan);
    requires_eq(&state);
    assert_eq!(state, synthetic_state(nan));
    assert_ne!(
        state,
        synthetic_state(f64::from_bits(0x7ff8_0000_0000_0002))
    );
    assert_ne!(synthetic_state(0.0), synthetic_state(-0.0));
}
