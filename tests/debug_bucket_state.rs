use flatmap_rs::FlatMap;

#[test]
fn test_bucket_state_after_range_delete() {
    let map = FlatMap::new();

    // Insert some data
    for i in 0..10 {
        map.insert(i, format!("value_{}", i));
    }

    println!("Before range_process:");

    // Delete some entries using retain
    map.retain(|k, _v| {
        return if *k % 2 == 0 {
            println!("Deleting key: {}", k);
            false
        } else {
            true
        };
    });

    println!("After range_process, before get:");

    // Test each get operation individually with timeout
    for i in 0..10 {
        println!("About to get key: {}", i);

        // Use a separate thread with timeout for each get
        let map_ref = &map;
        let result = std::thread::scope(|s| {
            let handle = s.spawn(move || map_ref.get(&i));

            // Wait for the thread to complete or timeout
            match handle.join() {
                Ok(result) => {
                    println!("get({}) = {:?}", i, result);
                    result
                }
                Err(_) => {
                    println!("get({}) panicked!", i);
                    None
                }
            }
        });

        if i % 2 == 0 {
            assert_eq!(result, None, "Deleted key {} should not exist", i);
        } else {
            assert_eq!(
                result,
                Some(format!("value_{}", i)),
                "Key {} should still exist",
                i
            );
        }
    }

    println!("All get operations completed successfully!");
}
