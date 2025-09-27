use ahash::AHasher;
use flatmap_rs::FlatMap;
use std::hash::BuildHasherDefault;
use std::hash::{BuildHasher, Hash, Hasher};

fn main() {
    let map = FlatMap::new();

    // Print HASH_INIT_FLAG value
    let hash_init_flag = 0x8000_0000_0000_0000u64;
    println!("HASH_INIT_FLAG: 0x{:016x}", hash_init_flag);

    // Test with the same hasher as FlatMap uses
    let hasher = BuildHasherDefault::<AHasher>::default();

    // Test with a simple key
    let key = 'a';
    let hash = {
        let mut h = hasher.build_hasher();
        key.hash(&mut h);
        h.finish()
    };

    println!("Hash for 'a': 0x{:016x}", hash);
    println!("Hash for 'a' with flag: 0x{:016x}", hash | hash_init_flag);
    println!("Hash highest bit set: {}", (hash & hash_init_flag) != 0);

    // Test multiple keys to see hash distribution
    for i in 0..10 {
        let key = format!("key_{}", i);
        let hash = {
            let mut h = hasher.build_hasher();
            key.hash(&mut h);
            h.finish()
        };
        println!(
            "Hash for '{}': 0x{:016x}, highest bit: {}",
            key,
            hash,
            (hash & hash_init_flag) != 0
        );
    }

    println!("\n--- Testing map operations ---");

    // Insert and retrieve multiple key-value pairs
    for i in 0..6 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);

        println!("Inserting: {} -> {}", key, value);
        map.insert(key.clone(), value.clone());

        let retrieved = map.get(&key);
        println!("Retrieved: {:?}", retrieved);

        if retrieved.is_none() {
            println!("ERROR: Failed to retrieve just inserted key: {}", key);
        }
    }

    println!("\nFinal map length: {}", map.len());

    // Test all keys again
    println!("\n--- Final verification ---");
    for i in 0..6 {
        let key = format!("key_{}", i);
        let retrieved = map.get(&key);
        println!("Key '{}': {:?}", key, retrieved);
    }
}
