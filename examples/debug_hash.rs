use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn hash_key(key: &i32) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn main() {
    println!("=== 检查key的hash值 ===");
    let test_keys = [1, 2, 11, 12, 27, 28, 31, 32];
    
    for &key in &test_keys {
        let hash = hash_key(&key);
        let h1 = hash as usize;
        let h2 = (hash >> 32) as u8;
        println!("key {}: hash={:016x}, h1={}, h2={}", key, hash, h1, h2);
    }
    
    println!("\n=== 检查bucket分布 ===");
    // 假设初始table大小为16 (2^4)
    let table_size = 16;
    for &key in &test_keys {
        let hash = hash_key(&key);
        let h1 = hash as usize;
        let bucket_index = h1 % table_size;
        println!("key {}: bucket_index={}", key, bucket_index);
    }
    
    println!("=== 检查完成 ===");
}