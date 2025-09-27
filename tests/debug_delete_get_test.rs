use flatmap_rs::{FlatMap, Op};

#[test]
fn test_get_after_range_delete() {
    let map = FlatMap::new();
    
    // Insert some data
    for i in 0..10 {
        map.insert(i, format!("value_{}", i));
    }
    
    println!("Initial data inserted");
    
    // Delete some entries using range_process
    map.range_process(|k, _v| {
        if *k % 2 == 0 {
            println!("Deleting key: {}", k);
            (Op::Delete, None)
        } else {
            (Op::Cancel, None)
        }
    });
    
    println!("range_process completed");
    
    // Try to get the deleted entries
    for i in 0..10 {
        println!("Getting key: {}", i);
        let result = map.get(&i);
        println!("get({}) = {:?}", i, result);
        
        if i % 2 == 0 {
            assert_eq!(result, None, "Deleted key {} should not exist", i);
        } else {
            assert_eq!(result, Some(format!("value_{}", i)), "Key {} should still exist", i);
        }
    }
    
    println!("Test completed successfully!");
}