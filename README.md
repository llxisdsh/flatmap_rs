# FlatMap - A High-Performance Concurrent Hash Map for Rust

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

FlatMap is a lock-free, high-performance concurrent hash map implementation in Rust, designed for scenarios with heavy read workloads and mixed read-write operations.

## ⚠️ Early Version Notice

This is an **initial version** of FlatMap and has not undergone extensive optimization. The implementation focuses on correctness and basic performance characteristics. Future versions will include more sophisticated optimizations and features.

## 🚀 Key Features

### Technical Characteristics

- **Lock-Free Design**: Uses atomic operations and sequence locks for thread-safe operations without traditional locking
- **Read-Optimized**: Optimized for scenarios with frequent read operations
- **Generic Support**: Full support for custom `BuildHasher` implementations
- **Memory Efficient**: Flat memory layout for better cache performance
- **Concurrent Safe**: Thread-safe operations for both reads and writes
- **Zero-Copy Reads**: Read operations don't require data copying in most cases

### Core Components

- **Atomic Operations**: Leverages Rust's atomic primitives for thread safety
- **Sequence Locks**: Implements seqlock mechanism for consistent reads during concurrent writes
- **Custom Hashing**: Supports pluggable hash functions via `BuildHasher` trait
- **Dynamic Resizing**: Automatic capacity expansion when load factor exceeds threshold

## 📊 Performance Benchmarks

Comprehensive benchmarks comparing FlatMap with `std::collections::HashMap` and `DashMap`:

### Single-Threaded Performance

| Operation Type | FlatMap | HashMap | DashMap | Winner |
|----------------|---------|---------|---------|---------|
| **Mixed Ops** (50k insert/get/remove) | 12.84ms | **6.08ms** | 6.72ms | HashMap |
| **Read Heavy** (50k reads) | **800µs** | 897µs | 1.67ms | **FlatMap** |

### Concurrent Performance (4 threads)

| Operation Type | FlatMap | DashMap | Winner |
|----------------|---------|---------|---------|
| **Mixed Read/Write** (70% read, 30% write) | **1.11ms** | 1.18ms | **FlatMap** |
| **Write Heavy** (pure writes) | 1.82ms | **1.46ms** | DashMap |

### Performance Summary

- ✅ **FlatMap excels in read-heavy scenarios** - 12% faster than HashMap, 52% faster than DashMap
- ✅ **Superior concurrent mixed workloads** - 6% faster than DashMap in realistic read/write scenarios
- ⚠️ **HashMap dominates single-threaded mixed operations** - Traditional HashMap is fastest for single-threaded use
- ⚠️ **DashMap leads in write-heavy concurrent scenarios** - 20% faster than FlatMap in pure write workloads

## 🛠️ Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
flatmap_rs = "0.1.0"
```

### Basic Example

```rust
use flatmap_rs::FlatMap;

fn main() {
    // Create a new FlatMap
    let map = FlatMap::new();
    
    // Insert key-value pairs
    map.insert(1, "hello");
    map.insert(2, "world");
    
    // Read values
    if let Some(value) = map.get(&1) {
        println!("Key 1: {}", value);
    }
    
    // Remove values
    map.remove(1);
    
    println!("Map length: {}", map.len());
}
```

### With Custom Hasher

```rust
use flatmap_rs::FlatMap;
use ahash::RandomState;

fn main() {
    // Use AHash for better performance
    let map = FlatMap::with_hasher(RandomState::new());
    
    map.insert("key1", 42);
    map.insert("key2", 84);
    
    assert_eq!(map.get("key1"), Some(42));
}
```

### Concurrent Usage

```rust
use flatmap_rs::FlatMap;
use std::sync::Arc;
use std::thread;

fn main() {
    let map = Arc::new(FlatMap::new());
    
    // Spawn multiple threads for concurrent access
    let handles: Vec<_> = (0..4).map(|i| {
        let map = map.clone();
        thread::spawn(move || {
            // Each thread operates on different key ranges
            let start = i * 1000;
            let end = start + 1000;
            
            for j in start..end {
                map.insert(j, j * 2);
            }
            
            for j in start..end {
                assert_eq!(map.get(&j), Some(j * 2));
            }
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!("Final map size: {}", map.len());
}
```

## 🎯 Use Cases

FlatMap is particularly well-suited for:

- **Read-Heavy Applications**: Caches, configuration stores, lookup tables
- **Concurrent Web Services**: Session stores, request routing tables
- **Real-Time Systems**: Where consistent read performance is critical
- **Mixed Workloads**: Applications with more reads than writes (typical in most systems)

## 🔧 Features

- `default`: Enables `ahash` and `std` features
- `ahash`: Use AHash as the default hasher (recommended for performance)
- `std`: Enable standard library features

## 🧪 Running Benchmarks

```bash
# Run all benchmarks
cargo bench --bench flatmap

# View detailed HTML reports
# Reports will be generated in target/criterion/
```

## 🤝 Contributing

Contributions are welcome! This is an early-stage project with room for significant improvements:

- Performance optimizations
- Additional features
- Better documentation
- More comprehensive tests

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


---

**Note**: This is an experimental implementation. While functional and tested, it's recommended to thoroughly evaluate performance characteristics for your specific use case before production deployment.