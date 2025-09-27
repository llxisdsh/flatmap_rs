use flatmap_rs::{FlatMap, Op};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_simple_range_process() {
    let map = FlatMap::new();
    
    // Insert some data
    for i in 0..10 {
        map.insert(i, format!("value_{}", i));
    }
    
    println!("Starting range_process...");
    
    // Simple range_process that should complete quickly
    map.range_process(|k, _v| {
        println!("Processing key: {}", k);
        if *k % 2 == 0 {
            (Op::Delete, None)
        } else {
            (Op::Cancel, None)
        }
    });
    
    println!("range_process completed!");
    
    // Check remaining entries
    let remaining = map.len();
    println!("Remaining entries: {}", remaining);
    assert_eq!(remaining, 5); // Should have 5 odd-numbered entries left
}

#[test]
fn test_concurrent_insert_range_process() {
    let map = Arc::new(FlatMap::new());
    
    // Insert initial data
    for i in 0..20 {
        map.insert(i, format!("value_{}", i));
    }
    
    let map_clone = Arc::clone(&map);
    
    // Start a thread that does range_process
    let range_handle = thread::spawn(move || {
        println!("Starting range_process in thread...");
        map_clone.range_process(|k, _v| {
            println!("Processing key: {}", k);
            thread::sleep(Duration::from_millis(1)); // Small delay to increase chance of race
            if *k % 3 == 0 {
                (Op::Delete, None)
            } else {
                (Op::Cancel, None)
            }
        });
        println!("range_process completed in thread!");
    });
    
    // Give range_process a moment to start
    thread::sleep(Duration::from_millis(5));
    
    // Try to insert while range_process is running
    println!("Inserting new data...");
    for i in 100..110 {
        map.insert(i, format!("new_value_{}", i));
        thread::sleep(Duration::from_millis(1));
    }
    
    range_handle.join().unwrap();
    println!("Test completed successfully!");
}