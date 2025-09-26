use flatmapof::{FlatMap, Op};
use std::sync::Arc;
use std::thread;

#[test]
fn test_process_update_existing() {
    let map = FlatMap::new();
    map.insert("key1".to_string(), 10);
    
    let (old_val, new_val) = map.process("key1".to_string(), |old| {
        match old {
            Some(v) => (Op::Update, Some(v + 5)),
            None => (Op::Cancel, None),
        }
    });
    
    assert_eq!(old_val, Some(10));
    assert_eq!(new_val, Some(15));
    assert_eq!(map.get(&"key1".to_string()), Some(15));
}

#[test]
fn test_process_insert_new() {
    let map = FlatMap::new();
    
    let (old_val, new_val) = map.process("key1".to_string(), |old| {
        match old {
            Some(_) => (Op::Cancel, None),
            None => (Op::Update, Some(42)),
        }
    });
    
    assert_eq!(old_val, None);
    assert_eq!(new_val, Some(42));
    assert_eq!(map.get(&"key1".to_string()), Some(42));
}

#[test]
fn test_process_delete() {
    let map = FlatMap::new();
    map.insert("key1".to_string(), 10);
    
    let (old_val, new_val) = map.process("key1".to_string(), |old| {
        match old {
            Some(_v) => (Op::Delete, None),
            None => (Op::Cancel, None),
        }
    });
    
    assert_eq!(old_val, Some(10));
    assert_eq!(new_val, None);
    assert_eq!(map.get(&"key1".to_string()), None);
}

#[test]
fn test_process_cancel() {
    let map = FlatMap::new();
    map.insert("key1".to_string(), 10);
    
    let (old_val, new_val) = map.process("key1".to_string(), |_old| {
        (Op::Cancel, None)
    });
    
    assert_eq!(old_val, Some(10));
    assert_eq!(new_val, Some(10)); // Cancel returns the old value
    assert_eq!(map.get(&"key1".to_string()), Some(10)); // Unchanged
}

#[test]
fn test_range_process_update_all() {
    let map = FlatMap::new();
    map.insert("key1".to_string(), 1);
    map.insert("key2".to_string(), 2);
    map.insert("key3".to_string(), 3);
    
    map.range_process(|_k, v| {
        (Op::Update, Some(v * 2))
    });
    
    assert_eq!(map.get(&"key1".to_string()), Some(2));
    assert_eq!(map.get(&"key2".to_string()), Some(4));
    assert_eq!(map.get(&"key3".to_string()), Some(6));
}

#[test]
fn test_range_process_delete_some() {
    let map = FlatMap::new();
    map.insert("key1".to_string(), 1);
    map.insert("key2".to_string(), 2);
    map.insert("key3".to_string(), 3);
    map.insert("key4".to_string(), 4);
    
    map.range_process(|_k, v| {
        if *v % 2 == 0 {
            (Op::Delete, None)
        } else {
            (Op::Cancel, None)
        }
    });
    
    assert_eq!(map.get(&"key1".to_string()), Some(1));
    assert_eq!(map.get(&"key2".to_string()), None);
    assert_eq!(map.get(&"key3".to_string()), Some(3));
    assert_eq!(map.get(&"key4".to_string()), None);
}

#[test]
fn test_concurrent_process() {
    let map = Arc::new(FlatMap::new());
    
    // Insert initial values
    for i in 0..100 {
        map.insert(i, i);
    }
    
    let mut handles = vec![];
    
    // Spawn threads that use process to increment values
    for _ in 0..4 {
        let map_clone: Arc<FlatMap<i32, i32>> = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                map_clone.process(i, |old| {
                    match old {
                        Some(v) => (Op::Update, Some(v + 1)),
                        None => (Op::Update, Some(1)),
                    }
                });
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Each value should have been incremented 4 times
    for i in 0..100 {
        assert_eq!(map.get(&i), Some(i + 4));
    }
}