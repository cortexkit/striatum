//! Apply routing migrations to an already cloned live-store snapshot, never the live DB.

use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
use std::time::Instant;

fn main() {
    let path = std::env::args().nth(1).expect("cloned snapshot path");
    let started = Instant::now();
    let store =
        SqliteRouteStateStore::open(&dev_descriptor("report-rehearsal", path.clone())).unwrap();
    println!("migration wall time: {:?}", started.elapsed());
    drop(store);
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    println!("integrity_check: {integrity}");
    assert_eq!(integrity, "ok");
    let foreign_keys: i64 = conn
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })
        .unwrap();
    println!("foreign_key_check: {foreign_keys} violations");
    assert_eq!(foreign_keys, 0);
}
