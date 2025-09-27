use flatmapof::{FlatMap, Op};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn test_seqlock_protection_during_deletes() {
    let map: Arc<FlatMap<i32, String>> = Arc::new(FlatMap::new());
    let torn_read_count = Arc::new(AtomicUsize::new(0));
    
    // Insert initial data
    for i in 0..50 {
        map.insert(i, format!("value_{}", i));
    }
    
    let map_clone1 = Arc::clone(&map);
    let map_clone2 = Arc::clone(&map);
    let torn_count_clone = Arc::clone(&torn_read_count);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone1 = Arc::clone(&stop_flag);
    let stop_clone2 = Arc::clone(&stop_flag);
    
    // Writer thread: perform range_process operations
    let writer = thread::spawn(move || {
        let mut cycle = 0;
        while !stop_clone1.load(Ordering::Relaxed) && cycle < 5 {
            println!("Writer cycle: {}", cycle);
            
            // Delete and re-insert in batches
            map_clone1.range_process(|k, _v| {
                if k % 10 == cycle % 10 {
                    (Op::Delete, None)
                } else {
                    (Op::Cancel, None)
                }
            });
            
            // Re-insert the deleted keys
            for i in 0..50 {
                if i % 10 == cycle % 10 {
                    map_clone1.insert(i, format!("updated_{}_{}", i, cycle));
                }
            }
            
            cycle += 1;
            thread::sleep(Duration::from_millis(5));
        }
        println!("Writer completed {} cycles", cycle);
    });
    
    // Reader thread: check for consistency
    let reader = thread::spawn(move || {
        let mut total_reads = 0;
        let mut check_count = 0;
        
        thread::sleep(Duration::from_millis(1)); // Let writer start first
        
        while !stop_clone2.load(Ordering::Relaxed) && check_count < 20 {
            check_count += 1;
            println!("Reader check: {}", check_count);
            
            for i in 0..50 {
                if let Some(value) = map_clone2.get(&i) {
                    total_reads += 1;
                    
                    // Simple consistency check
                    let is_valid = if value.starts_with("value_") {
                        value == format!("value_{}", i)
                    } else if value.starts_with("updated_") {
                        let parts: Vec<&str> = value.split('_').collect();
                        parts.len() == 3 && parts[1] == i.to_string()
                    } else {
                        false
                    };
                    
                    if !is_valid {
                        torn_count_clone.fetch_add(1, Ordering::Relaxed);
                        println!("Torn read: key={}, value='{}'", i, value);
                    }
                }
            }
            thread::sleep(Duration::from_millis(2));
        }
        println!("Reader completed {} reads in {} checks", total_reads, check_count);
    });
    
    // Let threads run for a short time
    thread::sleep(Duration::from_millis(30));
    stop_flag.store(true, Ordering::Relaxed);
    
    writer.join().unwrap();
    reader.join().unwrap();
    
    let torn_reads = torn_read_count.load(Ordering::Relaxed);
    println!("Total torn reads detected: {}", torn_reads);
    
    // Should have no torn reads due to seqlock protection
    assert_eq!(torn_reads, 0, "Detected {} torn reads during concurrent operations", torn_reads);
}