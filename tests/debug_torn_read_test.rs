use flatmap_rs::FlatMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn debug_torn_read_test() {
    let map: Arc<FlatMap<i32, String>> = Arc::new(FlatMap::new());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(3));
    let torn_read_count = Arc::new(AtomicUsize::new(0));
    let debug_count = Arc::new(AtomicUsize::new(0));

    // Initialize with some values
    for i in 0..10 {
        // Smaller range for debugging
        map.insert(i, format!("value_{}", i));
    }

    let map_clone1: Arc<FlatMap<i32, String>> = Arc::clone(&map);
    let barrier_clone1 = Arc::clone(&barrier);
    let stop_flag_clone1 = Arc::clone(&stop_flag);

    // Writer thread: continuously update values using range_process
    let writer = thread::spawn(move || {
        barrier_clone1.wait();
        let mut counter = 0;
        while !stop_flag_clone1.load(Ordering::Relaxed) {
            map_clone1.retain(|k, v| {
                let new_value = format!("updated_{}_{}", k, counter);
                *v = new_value;
                true
            });
            counter += 1;
            if counter > 5 {
                // Limit iterations for debugging
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    });

    let map_clone2: Arc<FlatMap<i32, String>> = Arc::clone(&map);
    let barrier_clone2 = Arc::clone(&barrier);
    let stop_flag_clone2 = Arc::clone(&stop_flag);
    let torn_read_count_clone = Arc::clone(&torn_read_count);
    let debug_count_clone = Arc::clone(&debug_count);

    // Reader thread: continuously read values and check for consistency
    let reader = thread::spawn(move || {
        barrier_clone2.wait();
        while !stop_flag_clone2.load(Ordering::Relaxed) {
            for i in 0..10 {
                if let Some(value) = map_clone2.get(&i) {
                    let count = debug_count_clone.fetch_add(1, Ordering::Relaxed);
                    if count < 20 {
                        // Print first 20 reads for debugging
                        println!("Read key={}, value='{}'", i, value);
                    }

                    // Check if the value is consistent (not a torn read)
                    let value_str = value.as_str();
                    if !value_str.starts_with("value_") && !value_str.starts_with("updated_") {
                        println!("TORN READ: Invalid prefix for key={}, value='{}'", i, value);
                        torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    // Additional check: if it's an updated value, it should have the correct format
                    if value_str.starts_with("updated_") {
                        let parts: Vec<&str> = value_str.split('_').collect();
                        if parts.len() != 3 {
                            println!(
                                "TORN READ: Wrong part count for key={}, value='{}', parts={:?}",
                                i, value, parts
                            );
                            torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                        } else if parts[1] != i.to_string() {
                            println!("TORN READ: Wrong key in value for key={}, value='{}', expected_key={}, actual_key={}", i, value, i, parts[1]);
                            torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
    });

    // Main thread waits and then stops the test
    barrier.wait();
    thread::sleep(Duration::from_millis(100));
    stop_flag.store(true, Ordering::Relaxed);

    writer.join().unwrap();
    reader.join().unwrap();

    println!("Total reads: {}", debug_count.load(Ordering::Relaxed));
    println!(
        "Torn reads detected: {}",
        torn_read_count.load(Ordering::Relaxed)
    );

    // Should have no torn reads
    assert_eq!(
        torn_read_count.load(Ordering::Relaxed),
        0,
        "Detected torn reads during concurrent range_process updates"
    );
}
