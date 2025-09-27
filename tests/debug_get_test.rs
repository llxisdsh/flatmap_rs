use flatmapof::FlatMap;

#[test]
fn test_simple_get() {
    let map = FlatMap::new();
    
    // Insert some data
    for i in 0..10 {
        map.insert(i, format!("value_{}", i));
    }
    
    // Test get operations
    for i in 0..10 {
        let result = map.get(&i);
        println!("get({}) = {:?}", i, result);
        assert_eq!(result, Some(format!("value_{}", i)));
    }
    
    // Test get for non-existent key
    let result = map.get(&999);
    println!("get(999) = {:?}", result);
    assert_eq!(result, None);
    
    println!("Simple get test completed!");
}

#[test]
fn test_get_after_delete() {
    let map = FlatMap::new();
    
    // Insert data
    map.insert(1, "value1".to_string());
    map.insert(2, "value2".to_string());
    
    // Delete one entry
    map.remove(1);
    
    // Test get operations
    assert_eq!(map.get(&1), None);
    assert_eq!(map.get(&2), Some("value2".to_string()));
    
    println!("Get after delete test completed!");
}