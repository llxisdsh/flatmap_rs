use flatmap_rs::{FlatMap, Op};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn stress_torn_read_test() {
    let map: Arc<FlatMap<i32, String>> = Arc::new(FlatMap::new());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(3));
    let torn_read_count = Arc::new(AtomicUsize::new(0));
    let debug_count = Arc::new(AtomicUsize::new(0));

    // Initialize with some values (same as original test)
    for i in 0..100 {
        map.insert(i, format!("value_{}", i));
    }

    let map_clone1: Arc<FlatMap<i32, String>> = Arc::clone(&map);
    let barrier_clone1 = Arc::clone(&barrier);
    let stop_flag_clone1 = Arc::clone(&stop_flag);

    // Writer thread: continuously update values using range_process (same as original)
    let writer = thread::spawn(move || {
        barrier_clone1.wait();
        let mut counter = 0;
        while !stop_flag_clone1.load(Ordering::Relaxed) {
            map_clone1.range_process(|k, _v| {
                let new_value = format!("updated_{}_{}", k, counter);
                (Op::Update, Some(new_value))
            });
            counter += 1;
            thread::sleep(Duration::from_micros(1)); // Same as original
        }
    });

    let map_clone2: Arc<FlatMap<i32, String>> = Arc::clone(&map);
    let barrier_clone2 = Arc::clone(&barrier);
    let stop_flag_clone2 = Arc::clone(&stop_flag);
    let torn_read_count_clone = Arc::clone(&torn_read_count);
    //let debug_count_clone = Arc::clone(&debug_count);

    // Reader thread: continuously read values and check for consistency (same as original)
    let reader = thread::spawn(move || {
        barrier_clone2.wait();
        while !stop_flag_clone2.load(Ordering::Relaxed) {
            for i in 0..100 {
                if let Some(value) = map_clone2.get(&i) {
                    //let count = debug_count_clone.fetch_add(1, Ordering::Relaxed);

                    // Check if the value is consistent (not a torn read)
                    let value_str = value.as_str();
                    if !value_str.starts_with("value_") && !value_str.starts_with("updated_") {
                        let torn_count = torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                        if torn_count < 10 {
                            // Print first few torn reads for debugging
                            println!("TORN READ: Invalid prefix for key={}, value='{}'", i, value);
                        }
                    }
                    // Additional check: if it's an updated value, it should have the correct format
                    if value_str.starts_with("updated_") {
                        let parts: Vec<&str> = value_str.split('_').collect();
                        if parts.len() != 3 {
                            let torn_count = torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                            if torn_count < 10 {
                                println!("TORN READ: Wrong part count for key={}, value='{}', parts={:?}", i, value, parts);
                            }
                        } else if parts[1] != i.to_string() {
                            let torn_count = torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                            if torn_count < 10 {
                                println!("TORN READ: Wrong key in value for key={}, value='{}', expected_key={}, actual_key={}", i, value, i, parts[1]);
                            }
                        }
                    }
                }
            }
            thread::sleep(Duration::from_micros(1)); // Same as original
        }
    });

    // Main thread waits and then stops the test (same as original)
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
