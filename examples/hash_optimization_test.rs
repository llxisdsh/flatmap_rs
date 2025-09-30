use flatmap_rs::FlatMap;

fn main() {
    // Test numeric type optimization
    let map_u64: FlatMap<u64, String> = FlatMap::new();
    map_u64.insert(42u64, "forty-two".to_string());
    println!("u64 key test: {:?}", map_u64.get(&42u64));

    let map_i32: FlatMap<i32, String> = FlatMap::new();
    map_i32.insert(-123i32, "negative".to_string());
    println!("i32 key test: {:?}", map_i32.get(&-123i32));

    // Test string type optimization
    let map_string: FlatMap<String, i32> = FlatMap::new();
    map_string.insert("hello".to_string(), 100);
    println!("String key test: {:?}", map_string.get(&"hello".to_string()));

    // Test generic type (fallback to standard hasher)
    #[derive(Hash, Eq, PartialEq, Clone)]
    struct CustomKey(u32, String);
    
    let map_custom: FlatMap<CustomKey, String> = FlatMap::new();
    let key = CustomKey(1, "test".to_string());
    map_custom.insert(key.clone(), "custom".to_string());
    println!("Custom key test: {:?}", map_custom.get(&key));

    println!("Hash optimization test completed successfully!");
}