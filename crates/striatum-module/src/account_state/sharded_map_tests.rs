use super::*;

#[test]
fn sharded_map_removal_shrinks_without_changing_content_or_snapshots() {
    let keys: Vec<_> = (0_u64..)
        .filter(|key| shard_of(key) == 0)
        .take(128)
        .collect();
    let mut map = ShardedMap::default();
    for key in &keys {
        map.insert(*key, *key);
    }
    let snapshot = map.clone();
    let capacity = snapshot.groups[0][0].capacity();
    for key in &keys[..108] {
        assert_eq!(map.remove(key), Some(*key));
    }
    assert!(map.groups[0][0].capacity() < capacity / 2);
    assert_eq!(snapshot.len(), 128);
    assert_eq!(snapshot.groups[0][0].capacity(), capacity);
    let mut fresh = ShardedMap::default();
    for key in &keys[108..] {
        fresh.insert(*key, *key);
    }
    assert_eq!(map, fresh);
    for key in &keys[108..] {
        assert_eq!(map.remove(key), Some(*key));
    }
    assert_eq!(map.groups[0][0].capacity(), 0);
    assert_eq!(map, ShardedMap::default());
}
