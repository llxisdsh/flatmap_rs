use flatmap_rs::FlatMap;

#[test]
fn test_get_after_range_delete() {
    let map = FlatMap::new();

    // Insert some data
    for i in 0..10 {
        map.insert(i, format!("value_{}", i));
    }

    println!("Initial data inserted");

    // Delete some entries using range_process
    map.retain(|k, _v| {
        if *k % 2 == 0 {
            println!("Deleting key: {}", k);
            false
        } else {
            true
        }
    });

    println!("retain completed");

    // Try to get the deleted entries
    for i in 0..10 {
        println!("Getting key: {}", i);
        let result = map.get(&i);
        println!("get({}) = {:?}", i, result);

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

    println!("Test completed successfully!");
}
