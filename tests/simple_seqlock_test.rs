use flatmapof::{FlatMap, Op};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn test_simple_seqlock_protection() {
    let map: Arc<FlatMap<i32, i32>> = Arc::new(FlatMap::new());
    
    // Initialize with some values
    for i in 0..10 {
        map.insert(i, i * 10);
    }
    
    let barrier = Arc::new(Barrier::new(3));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let torn_read_count = Arc::new(AtomicUsize::new(0));
    
    let map_clone1: Arc<FlatMap<i32, i32>> = Arc::clone(&map);
    let barrier_clone1 = Arc::clone(&barrier);
    let stop_flag_clone1 = Arc::clone(&stop_flag);
    
    // Writer thread: continuously update values using range_process
    let writer = thread::spawn(move || {
        barrier_clone1.wait();
        let mut counter = 0;
        while !stop_flag_clone1.load(Ordering::Relaxed) {
            map_clone1.range_process(|k, _v| {
                // Update to a predictable value: key * 1000 + counter
                let new_value = k * 1000 + counter;
                (Op::Update, Some(new_value))
            });
            counter += 1;
            if counter > 1000 {
                counter = 0; // Reset to avoid overflow
            }
            thread::sleep(Duration::from_micros(1));
        }
    });
    
    let map_clone2: Arc<FlatMap<i32, i32>> = Arc::clone(&map);
    let barrier_clone2 = Arc::clone(&barrier);
    let stop_flag_clone2 = Arc::clone(&stop_flag);
    let torn_read_count_clone = Arc::clone(&torn_read_count);
    
    // Reader thread: continuously read values and check for consistency
    let reader = thread::spawn(move || {
        barrier_clone2.wait();
        while !stop_flag_clone2.load(Ordering::Relaxed) {
            for i in 0..10 {
                if let Some(value) = map_clone2.get(&i) {
                    // Check if the value is consistent
                    // Value should be either:
                    // 1. Original value: i * 10
                    // 2. Updated value: i * 1000 + counter (where counter is 0-1000)
                    
                    if value == i * 10 {
                        // Original value, this is fine
                        continue;
                    }
                    
                    // Check if it's a valid updated value
                    let expected_base = i * 1000;
                    if value < expected_base || value > expected_base + 1000 {
                        // This is a torn read - value is not in expected range
                        torn_read_count_clone.fetch_add(1, Ordering::Relaxed);
                        println!("Torn read detected: key={}, value={}, expected_base={}", i, value, expected_base);
                    }
                }
            }
            thread::sleep(Duration::from_micros(1));
        }
    });
    
    // Main thread waits and then stops the test
    barrier.wait();
    thread::sleep(Duration::from_millis(50)); // Shorter test duration
    stop_flag.store(true, Ordering::Relaxed);
    
    writer.join().unwrap();
    reader.join().unwrap();
    
    // Should have no torn reads
    let torn_reads = torn_read_count.load(Ordering::Relaxed);
    assert_eq!(torn_reads, 0, "Detected {} torn reads during concurrent range_process updates", torn_reads);
}