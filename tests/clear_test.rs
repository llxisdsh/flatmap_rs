use flatmapof::FlatMap;
use std::sync::Arc;
use std::thread;

#[test]
fn test_clear_basic() {
    let m: FlatMap<u64, String> = FlatMap::with_capacity(16);
    
    // Insert some data
    m.insert(1, "one".to_string());
    m.insert(2, "two".to_string());
    m.insert(3, "three".to_string());
    assert_eq!(m.len(), 3);
    assert!(!m.is_empty());
    
    // Clear the map
    m.clear();
    
    // Verify map is empty
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
    
    // Verify all keys are gone
    assert_eq!(m.get(&1), None);
    assert_eq!(m.get(&2), None);
    assert_eq!(m.get(&3), None);
}

#[test]
fn test_clear_empty_map() {
    let m: FlatMap<u64, String> = FlatMap::new();
    
    // Clear an already empty map
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
    
    m.clear();
    
    // Should still be empty
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
}

#[test]
fn test_clear_and_reinsert() {
    let m: FlatMap<u64, String> = FlatMap::with_capacity(32);
    
    // Insert initial data
    for i in 0..10 {
        m.insert(i, format!("value_{}", i));
    }
    assert_eq!(m.len(), 10);
    
    // Clear the map
    m.clear();
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
    
    // Reinsert data
    for i in 0..5 {
        m.insert(i + 100, format!("new_value_{}", i));
    }
    assert_eq!(m.len(), 5);
    
    // Verify new data is accessible
    for i in 0..5 {
        assert_eq!(m.get(&(i + 100)), Some(format!("new_value_{}", i)));
    }
    
    // Verify old data is gone
    for i in 0..10 {
        assert_eq!(m.get(&i), None);
    }
}

#[test]
fn test_clear_large_map() {
    let m: FlatMap<u64, u64> = FlatMap::with_capacity(1024);
    
    // Insert a large amount of data
    for i in 0..1000 {
        m.insert(i, i * 2);
    }
    assert_eq!(m.len(), 1000);
    
    // Clear the map
    m.clear();
    
    // Verify it's empty
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
    
    // Spot check that data is gone
    assert_eq!(m.get(&0), None);
    assert_eq!(m.get(&500), None);
    assert_eq!(m.get(&999), None);
}

#[test]
fn test_clear_concurrent_access() {
    let m = Arc::new(FlatMap::<u64, u64>::with_capacity(128));
    
    // Insert initial data
    for i in 0..100 {
        m.insert(i, i * 2);
    }
    assert_eq!(m.len(), 100);
    
    let m_clone = Arc::clone(&m);
    
    // Spawn a thread that clears the map
    let clear_handle = thread::spawn(move || {
        m_clone.clear();
    });
    
    // Wait for clear to complete
    clear_handle.join().unwrap();
    
    // Verify map is empty
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
}

#[test]
fn test_clear_concurrent_operations() {
    let m = Arc::new(FlatMap::<u64, u64>::with_capacity(256));
    
    // Insert initial data
    for i in 0..50 {
        m.insert(i, i);
    }
    
    let m1 = Arc::clone(&m);
    let m2 = Arc::clone(&m);
    let m3 = Arc::clone(&m);
    
    let handles: Vec<_> = vec![
        // Thread 1: Clear the map
        thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(10));
            m1.clear();
        }),
        // Thread 2: Try to insert during clear
        thread::spawn(move || {
            for i in 100..150 {
                m2.insert(i, i * 3);
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }),
        // Thread 3: Try to read during clear
        thread::spawn(move || {
            for i in 0..50 {
                let _ = m3.get(&i);
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }),
    ];
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // The map should be in a consistent state
    // Either completely clear or with some new insertions from thread 2
    let final_len = m.len();
    println!("Final length after concurrent operations: {}", final_len);
    
    // Verify consistency - all entries should be from thread 2 (100-149 range)
    let mut found_old_entries = false;
    for i in 0..50 {
        if m.get(&i).is_some() {
            found_old_entries = true;
            break;
        }
    }
    
    // Old entries should be gone after clear
    assert!(!found_old_entries, "Found old entries that should have been cleared");
}

#[test]
fn test_clear_preserves_capacity_behavior() {
    let m: FlatMap<u64, String> = FlatMap::with_capacity(64);
    
    // Fill the map
    for i in 0..50 {
        m.insert(i, format!("value_{}", i));
    }
    assert_eq!(m.len(), 50);
    
    // Clear the map
    m.clear();
    assert_eq!(m.len(), 0);
    
    // The map should still be able to efficiently handle insertions
    // (this tests that the internal structure is properly reset)
    for i in 0..100 {
        m.insert(i, format!("new_value_{}", i));
    }
    assert_eq!(m.len(), 100);
    
    // Verify all new data is accessible
    for i in 0..100 {
        assert_eq!(m.get(&i), Some(format!("new_value_{}", i)));
    }
}

#[test]
fn test_clear_multiple_times() {
    let m: FlatMap<u64, u64> = FlatMap::with_capacity(32);
    
    for round in 0..5 {
        // Insert data
        for i in 0..20 {
            m.insert(i + round * 100, i * round);
        }
        assert_eq!(m.len(), 20);
        
        // Clear
        m.clear();
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
    }
    
    // Final verification
    assert_eq!(m.len(), 0);
    assert!(m.is_empty());
}