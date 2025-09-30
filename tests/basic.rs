use flatmap_rs::FlatMap;

#[test]
fn test_insert_get_remove_string() {
    let m: FlatMap<String, String> = FlatMap::with_capacity(16);
    assert!(m.is_empty());

    // insert new
    let old = m.insert("a".to_string(), "1".to_string());
    assert!(old.is_none());
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(&"a".to_string()), Some("1".to_string()));

    // insert another
    let _ = m.insert("b".to_string(), "2".to_string());
    assert_eq!(m.len(), 2);
    assert_eq!(m.get(&"b".to_string()), Some("2".to_string()));

    // update existing
    let old = m.insert("a".to_string(), "10".to_string());
    assert_eq!(old, Some("1".to_string()));
    assert_eq!(m.get(&"a".to_string()), Some("10".to_string()));

    // remove existing
    let old = m.remove("b".to_string());
    assert_eq!(old, Some("2".to_string()));
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(&"b".to_string()), None);
}

#[test]
fn test_get_or_insert_with() {
    let m: FlatMap<u64, String> = FlatMap::new();

    // insert new via get_or_insert_with
    let (v, existed) = m.get_or_insert_with(42, || "hello".to_string());
    assert_eq!(v, "hello");
    assert!(!existed);
    assert_eq!(m.len(), 1);

    // get existing via get_or_insert_with
    let (v2, existed2) = m.get_or_insert_with(42, || "world".to_string());
    assert_eq!(v2, "hello");
    assert!(existed2);
    assert_eq!(m.len(), 1);
}

#[test]
fn test_for_each_traversal() {
    let m: FlatMap<u64, u64> = FlatMap::with_capacity(8);
    for i in 0..50u64 {
        let _ = m.insert(i, i * 2);
    }
    assert_eq!(m.len(), 50);

    let mut sum_keys = 0u64;
    let mut sum_vals = 0u64;

    for (k, v) in m.iter() {
        sum_keys += k;
        sum_vals += v;
    }

    assert_eq!(sum_keys, (0..50).sum());
    assert_eq!(sum_vals, (0..50).map(|i| i * 2).sum());
}

#[test]
fn test_basic_integer_keys() {
    let m: FlatMap<i32, i32> = FlatMap::new();
    assert_eq!(m.get(&1), None);
    assert_eq!(m.insert(1, 7), None);
    assert_eq!(m.get(&1), Some(7));
    assert_eq!(m.insert(1, 9), Some(7));
    assert_eq!(m.get(&1), Some(9));
    assert_eq!(m.remove(1), Some(9));
    assert_eq!(m.get(&1), None);
}

#[test]
fn test_edge_cases_strings() {
    use flatmap_rs::FlatMap;
    let m: FlatMap<String, String> = FlatMap::new();

    // Empty string key
    let (v, existed) = m.get_or_insert_with("".to_string(), || "empty_key_value".to_string());
    assert_eq!(v, "empty_key_value");
    assert!(!existed);
    assert_eq!(m.get(&"".to_string()), Some("empty_key_value".to_string()));

    // Very long key
    let mut long_key = String::with_capacity(1000);
    for _ in 0..1000 {
        long_key.push('a');
    }
    let (v2, existed2) = m.get_or_insert_with(long_key.clone(), || "long_key_value".to_string());
    assert_eq!(v2, "long_key_value");
    assert!(!existed2);
    assert_eq!(m.get(&long_key), Some("long_key_value".to_string()));

    // Ensure previous data intact
    assert_eq!(m.get(&"".to_string()), Some("empty_key_value".to_string()));
}

#[test]
fn test_multiple_keys_and_deletions() {
    use flatmap_rs::FlatMap;
    let m: FlatMap<i32, String> = FlatMap::new();

    // Insert multiple keys
    for i in 0..100 {
        let _ = m.insert(i, format!("value_{}", i));
    }

    // Verify all keys
    for i in 0..100 {
        let expected = format!("value_{}", i);
        assert_eq!(m.get(&i), Some(expected));
    }

    // Delete even keys
    for i in (0..100).step_by(2) {
        let _ = m.remove(i);
    }

    // Verify deletions and remaining keys
    for i in 0..100 {
        if i % 2 == 0 {
            assert_eq!(m.get(&i), None);
        } else {
            let expected = format!("value_{}", i);
            assert_eq!(m.get(&i), Some(expected));
        }
    }
}

#[test]
fn test_size_and_is_empty_semantics() {
    use flatmap_rs::FlatMap;
    let m: FlatMap<i32, String> = FlatMap::new();

    // Initially empty
    assert!(m.is_empty());
    assert_eq!(m.len(), 0);

    // Add items and check length
    for i in 0..10 {
        let _ = m.insert(i, format!("value_{}", i));
        assert_eq!(m.len(), (i + 1) as usize);
        assert!(!m.is_empty());
    }

    // Delete items and check length
    for i in 0..10 {
        let _ = m.remove(i);
        assert_eq!(m.len(), (9 - i) as usize);
    }
    assert!(m.is_empty());
}

#[test]
fn test_for_each_early_termination() {
    use flatmap_rs::FlatMap;
    let m: FlatMap<i32, i32> = FlatMap::new();
    for i in 0..20 {
        let _ = m.insert(i, i * 3);
    }

    let mut count = 0;
    for _ in m.keys() {
        count += 1;
        if count >= 5 {
            break;
        }
    }
    assert_eq!(count, 5);
}

#[test]
fn test_iter_consistency() {
    use flatmap_rs::FlatMap;
    let m: FlatMap<i32, String> = FlatMap::new();
    for i in 0..10 {
        let _ = m.insert(i, format!("v{}", i));
    }

    let mut collected = m.iter().collect::<Vec<(i32, String)>>();
    collected.sort_by_key(|(k, _)| *k);
    assert_eq!(collected.len(), 10);
    for i in 0..10 {
        assert_eq!(collected[i as usize].0, i);
        assert_eq!(collected[i as usize].1, format!("v{}", i));
    }
}
