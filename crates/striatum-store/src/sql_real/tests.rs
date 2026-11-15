use super::*;

#[test]
fn equality_is_reflexive_for_nan_and_distinguishes_signed_zero() {
    let nan = SqlReal::from(f64::from_bits(0x7ff8_0000_0000_0001));
    assert_eq!(nan, nan);
    assert_eq!(nan, SqlReal::from(f64::from_bits(0x7ff8_0000_0000_0001)));
    assert_ne!(nan, SqlReal::from(f64::from_bits(0x7ff8_0000_0000_0002)));
    assert_ne!(SqlReal::from(0.0), SqlReal::from(-0.0));
    assert_eq!(SqlValue::Real(nan), SqlValue::Real(nan));
}

#[test]
fn real_round_trip_uses_the_value_sqlite_actually_stores() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE values_to_reload (value REAL)")
        .unwrap();
    let original = SqlReal::from(1.234_567_890_123_456_7);
    conn.execute("INSERT INTO values_to_reload VALUES (?1)", [original])
        .unwrap();
    let loaded: SqlReal = conn
        .query_row("SELECT value FROM values_to_reload", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loaded, original);
    assert_eq!(
        loaded.as_f64().to_bits(),
        1.234_567_890_123_456_7_f64.to_bits()
    );
    conn.execute(
        "INSERT INTO values_to_reload VALUES (?1)",
        [SqlReal::from(f64::NAN)],
    )
    .unwrap();
    // SQLite turns a bound NaN into NULL. The captured optional value must
    // therefore be None, not a NaN assumed to survive a database round trip.
    let loaded: Option<SqlReal> = conn
        .query_row(
            "SELECT value FROM values_to_reload WHERE rowid=2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(loaded, None);
    assert_eq!(
        conn.query_row(
            "SELECT typeof(value) FROM values_to_reload WHERE rowid=2",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "null"
    );
}
