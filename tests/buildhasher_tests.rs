use flatmap_rs::FlatMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

// Custom hasher for testing
#[derive(Default, Clone)]
struct TestHasher {
    value: u64,
}

impl Hasher for TestHasher {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.value = self.value.wrapping_mul(31).wrapping_add(byte as u64);
        }
    }
}

#[derive(Default, Clone)]
struct TestBuildHasher;

impl BuildHasher for TestBuildHasher {
    type Hasher = TestHasher;

    fn build_hasher(&self) -> Self::Hasher {
        TestHasher::default()
    }
}

#[test]
fn test_with_hasher() {
    let map: FlatMap<String, i32, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    map.insert("key1".to_string(), 100);
    map.insert("key2".to_string(), 200);
    
    assert_eq!(map.get(&"key1".to_string()), Some(100));
    assert_eq!(map.get(&"key2".to_string()), Some(200));
    assert_eq!(map.len(), 2);
}

#[test]
fn test_with_capacity_and_hasher() {
    let map: FlatMap<String, i32, TestBuildHasher> = 
        FlatMap::with_capacity_and_hasher(100, TestBuildHasher);
    
    map.insert("test".to_string(), 42);
    assert_eq!(map.get(&"test".to_string()), Some(42));
}

#[test]
fn test_contains_key_with_custom_hasher() {
    let map: FlatMap<String, i32, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    map.insert("exists".to_string(), 1);
    
    assert!(map.contains_key(&"exists".to_string()));
    assert!(!map.contains_key(&"not_exists".to_string()));
}

#[test]
fn test_keys_with_custom_hasher() {
    let map: FlatMap<String, i32, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    map.insert("key1".to_string(), 1);
    map.insert("key2".to_string(), 2);
    map.insert("key3".to_string(), 3);
    
    let keys: Vec<String> = map.keys().collect();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"key1".to_string()));
    assert!(keys.contains(&"key2".to_string()));
    assert!(keys.contains(&"key3".to_string()));
}

#[test]
fn test_values_with_custom_hasher() {
    let map: FlatMap<String, i32, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    map.insert("a".to_string(), 10);
    map.insert("b".to_string(), 20);
    map.insert("c".to_string(), 30);
    
    let values: Vec<i32> = map.values().collect();
    assert_eq!(values.len(), 3);
    assert!(values.contains(&10));
    assert!(values.contains(&20));
    assert!(values.contains(&30));
}

#[test]
fn test_default_trait() {
    // Test default with RandomState
    let map1: FlatMap<String, i32> = FlatMap::default();
    map1.insert("test".to_string(), 42);
    assert_eq!(map1.get(&"test".to_string()), Some(42));
    
    // Test default with custom hasher
    let map2: FlatMap<String, i32, TestBuildHasher> = FlatMap::default();
    map2.insert("test".to_string(), 42);
    assert_eq!(map2.get(&"test".to_string()), Some(42));
}

#[test]
fn test_into_iterator() {
    let map: FlatMap<String, i32, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);
    
    let items: Vec<(String, i32)> = (&map).into_iter().collect();
    assert_eq!(items.len(), 3);
    
    // Check that all items are present
    let mut found_a = false;
    let mut found_b = false;
    let mut found_c = false;
    
    for (key, value) in items {
        match key.as_str() {
            "a" => { assert_eq!(value, 1); found_a = true; }
            "b" => { assert_eq!(value, 2); found_b = true; }
            "c" => { assert_eq!(value, 3); found_c = true; }
            _ => panic!("Unexpected key: {}", key),
        }
    }
    
    assert!(found_a && found_b && found_c);
}

#[test]
fn test_from_iterator() {
    let data = vec![
        ("key1".to_string(), 1),
        ("key2".to_string(), 2),
        ("key3".to_string(), 3),
    ];
    
    let map: FlatMap<String, i32, TestBuildHasher> = data.into_iter().collect();
    
    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"key1".to_string()), Some(1));
    assert_eq!(map.get(&"key2".to_string()), Some(2));
    assert_eq!(map.get(&"key3".to_string()), Some(3));
}

#[test]
fn test_extend() {
    let mut map: FlatMap<String, i32, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    map.insert("initial".to_string(), 0);
    
    let additional_data = vec![
        ("key1".to_string(), 1),
        ("key2".to_string(), 2),
    ];
    
    map.extend(additional_data);
    
    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"initial".to_string()), Some(0));
    assert_eq!(map.get(&"key1".to_string()), Some(1));
    assert_eq!(map.get(&"key2".to_string()), Some(2));
}

#[test]
fn test_random_state_compatibility() {
    // Test that RandomState still works as default
    let map1 = FlatMap::new();
    map1.insert("test".to_string(), 42);
    assert_eq!(map1.get(&"test".to_string()), Some(42));
    
    let map2 = FlatMap::with_capacity(10);
    map2.insert("test".to_string(), 42);
    assert_eq!(map2.get(&"test".to_string()), Some(42));
    
    // Test explicit RandomState
    let map3: FlatMap<String, i32, RandomState> = FlatMap::with_hasher(RandomState::new());
    map3.insert("test".to_string(), 42);
    assert_eq!(map3.get(&"test".to_string()), Some(42));
}

#[test]
fn test_complex_operations_with_custom_hasher() {
    let map: FlatMap<String, Vec<i32>, TestBuildHasher> = FlatMap::with_hasher(TestBuildHasher);
    
    // Insert some complex data
    map.insert("list1".to_string(), vec![1, 2, 3]);
    map.insert("list2".to_string(), vec![4, 5, 6]);
    
    // Test get
    assert_eq!(map.get(&"list1".to_string()), Some(vec![1, 2, 3]));
    
    // Test remove
    let removed = map.remove("list1".to_string());
    assert_eq!(removed, Some(vec![1, 2, 3]));
    assert_eq!(map.len(), 1);
    
    // Test clear
    map.clear();
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
}

#[test]
fn test_concurrent_operations() {
    use std::sync::Arc;
    use std::thread;
    
    let map: Arc<FlatMap<i32, String, TestBuildHasher>> = 
        Arc::new(FlatMap::with_hasher(TestBuildHasher));
    
    let mut handles = vec![];
    
    // Spawn multiple threads to insert data
    for i in 0..10 {
        let map_clone: Arc<FlatMap<i32, String, TestBuildHasher>> = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for j in 0..10 {
                let key = i * 10 + j;
                let value = format!("value_{}", key);
                map_clone.insert(key, value);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all data was inserted
    assert_eq!(map.len(), 100);
    
    for i in 0..100 {
        let expected = format!("value_{}", i);
        assert_eq!(map.get(&i), Some(expected));
    }
}