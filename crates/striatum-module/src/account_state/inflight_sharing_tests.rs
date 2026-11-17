use super::*;

#[test]
fn account_bucket_forks_share_immutable_entries_and_preserve_old_membership() {
    let entry = Arc::new(InflightEntry {
        principal: "p".into(),
        conversation_hash: "hash".into(),
        model: "model".into(),
        run_id: None,
        renewal: false,
        started: false,
        started_at: None,
        until_ms: 100,
    });
    let first: RowKey = ("p".into(), "a".into());
    let second: RowKey = ("p".into(), "b".into());
    let mut bucket = Arc::new(BTreeMap::from([
        (Arc::new(first.clone()), entry.clone()),
        (Arc::new(second.clone()), entry.clone()),
    ]));
    let old = bucket.clone();
    assert!(Arc::make_mut(&mut bucket).remove(&first).is_some());
    assert!(Arc::ptr_eq(
        bucket.get(&second).unwrap(),
        old.get(&second).unwrap()
    ));
    assert!(Arc::ptr_eq(
        bucket.get_key_value(&second).unwrap().0,
        old.get_key_value(&second).unwrap().0,
    ));
    assert_eq!(old.len(), 2);
    assert!(old.contains_key(&first));
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket.get(&second).unwrap().until_ms, 100);
}
