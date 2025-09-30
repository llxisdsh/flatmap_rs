use flatmap_rs::FlatMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_range_process_meta_based_iteration() {
    let map = FlatMap::new();

    // Insert entries to ensure we have data in multiple buckets
    for i in 0..100 {
        map.insert(i, i * 10);
    }

    // Use range_process to increment all values by 1
    let mut processed_count = 0;
    map.retain(|_k, v| {
        processed_count += 1;
        *v += 1;
        true
    });

    // Verify all entries were processed
    assert_eq!(processed_count, 100);

    // Verify all values were updated correctly
    for i in 0..100 {
        assert_eq!(map.get(&i), Some(i * 10 + 1));
    }
}

#[test]
fn test_range_process_in_lock_processing() {
    let map = FlatMap::new();

    // Insert test data
    for i in 0..50 {
        map.insert(i, i);
    }

    // Track which keys were processed
    let mut processed_keys = Vec::new();

    map.retain(|k, v| {
        processed_keys.push(*k);
        if *k % 2 == 0 {
            false
        } else {
            *v *= 2;
            true
        }
    });

    // Verify processing happened in-lock (all keys should be processed)
    assert_eq!(processed_keys.len(), 50);

    // Verify deletions and updates
    for i in 0..50 {
        if i % 2 == 0 {
            assert_eq!(map.get(&i), None, "Even key {} should be deleted", i);
        } else {
            assert_eq!(map.get(&i), Some(i * 2), "Odd key {} should be doubled", i);
        }
    }
}

#[test]
fn test_range_process_meta_mask_correctness() {
    let map = FlatMap::new();

    // Insert sparse data to test meta mask handling
    let keys = vec![1, 7, 15, 23, 31, 47, 63, 79, 95];
    for &key in &keys {
        map.insert(key, key * 100);
    }

    let mut found_keys = Vec::new();
    map.retain(|k, _v| {
        found_keys.push(*k);
        true
    });

    // Sort both vectors for comparison
    found_keys.sort();
    let mut expected_keys = keys.clone();
    expected_keys.sort();

    assert_eq!(
        found_keys, expected_keys,
        "Meta-based iteration should find all keys"
    );
}

#[test]
fn test_range_process_concurrent_with_resize() {
    let map = Arc::new(FlatMap::new());
    let barrier = Arc::new(Barrier::new(3));

    // Pre-populate with some data
    for i in 0..20 {
        map.insert(i, i);
    }

    let map1 = Arc::clone(&map);
    let barrier1 = Arc::clone(&barrier);
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        // Continuously add entries to trigger resize
        for i in 20..200 {
            map1.insert(i, i);
            if i % 10 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        }
    });

    let map2 = Arc::clone(&map);
    let barrier2 = Arc::clone(&barrier);
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        // Continuously remove entries to trigger shrink
        for i in 0..20 {
            map2.remove(i);
            thread::sleep(Duration::from_millis(2));
        }
    });

    let map3 = Arc::clone(&map);
    let barrier3 = Arc::clone(&barrier);
    let handle3 = thread::spawn(move || {
        barrier3.wait();
        // Perform range_process operations during resize
        for _ in 0..10 {
            let mut count = 0;
            map3.retain(|_k, v| {
                count += 1;
                *v += 1;
                true
            });
            thread::sleep(Duration::from_millis(5));
        }
    });

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();

    // Verify map is still consistent
    let mut final_count = 0;
    map.retain(|_k, _v| {
        final_count += 1;
        true
    });

    // Should have some entries remaining
    assert!(
        final_count > 0,
        "Map should have entries after concurrent operations"
    );
}

#[test]
fn test_range_process_delete_all_meta_update() {
    let map = FlatMap::new();

    // Insert data
    for i in 0..30 {
        map.insert(i, i);
    }

    // Delete all entries using range_process
    map.retain(|_k, _v| false);

    // Verify all entries are deleted
    for i in 0..30 {
        assert_eq!(map.get(&i), None, "Key {} should be deleted", i);
    }

    // Verify map is empty
    assert!(map.is_empty());
    assert_eq!(map.len(), 0);
}

#[test]
fn test_range_process_mixed_operations_consistency() {
    let map = FlatMap::new();

    // Insert initial data
    for i in 0..60 {
        map.insert(i, i);
    }

    // Perform mixed operations: delete multiples of 3, update multiples of 5, cancel others
    map.retain(|k, v| {
        if *k % 3 == 0 {
            false
        } else if *k % 5 == 0 {
            *v *= 10;
            true
        } else {
            true
        }
    });

    // Verify results
    for i in 0..60 {
        if i % 3 == 0 {
            assert_eq!(
                map.get(&i),
                None,
                "Key {} (multiple of 3) should be deleted",
                i
            );
        } else if i % 5 == 0 {
            assert_eq!(
                map.get(&i),
                Some(i * 10),
                "Key {} (multiple of 5) should be updated",
                i
            );
        } else {
            assert_eq!(map.get(&i), Some(i), "Key {} should remain unchanged", i);
        }
    }
}

#[test]
fn test_range_process_restart_on_table_swap() {
    let map = Arc::new(FlatMap::new());

    // Pre-populate
    for i in 0..50 {
        map.insert(i, i);
    }

    let map_clone = Arc::clone(&map);
    let handle = thread::spawn(move || {
        // Trigger resize by adding many entries
        for i in 50..500 {
            map_clone.insert(i, i);
        }
    });

    // Perform range_process while resize is happening
    let mut total_processed = 0;
    map.retain(|_k, v| {
        total_processed += 1;
        *v += 1;
        true
    });

    handle.join().unwrap();

    // Should have processed some entries (exact count depends on timing)
    assert!(total_processed > 0, "Should have processed some entries");

    // Verify map consistency
    let mut final_count = 0;
    map.retain(|_k, _v| {
        final_count += 1;
        true
    });

    assert!(
        final_count >= 50,
        "Should have at least the original entries"
    );
}

#[test]
fn test_range_process_empty_map() {
    let map: FlatMap<i32, i32> = FlatMap::new();

    let mut processed_count = 0;
    map.retain(|_k, _v| {
        processed_count += 1;
        true
    });

    assert_eq!(processed_count, 0, "Empty map should process 0 entries");
}

#[test]
fn test_range_process_single_entry() {
    let map = FlatMap::new();
    map.insert(42, 100);

    let mut processed_count = 0;
    map.retain(|k, v| {
        processed_count += 1;
        assert_eq!(*k, 42);
        assert_eq!(*v, 100);
        *v *= 2;
        true
    });

    assert_eq!(processed_count, 1);
    assert_eq!(map.get(&42), Some(200));
}
