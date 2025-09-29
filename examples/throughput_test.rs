use dashmap::DashMap;
use flatmap_rs::FlatMap;
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TEST_DURATION: Duration = Duration::from_secs(1);
const WARMUP_OPERATIONS: usize = 10_000;

// 生成测试数据
fn generate_test_data(size: usize) -> Vec<(u64, u64)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size).map(|_| (rng.gen(), rng.gen())).collect()
}

// 性能测试结果结构
#[derive(Debug)]
struct PerformanceResult {
    name: String,
    operations_per_second: usize,
    total_operations: usize,
}

impl PerformanceResult {
    fn new(name: String, total_operations: usize, duration: Duration) -> Self {
        let ops_per_sec = (total_operations as f64 / duration.as_secs_f64()) as usize;
        Self {
            name,
            operations_per_second: ops_per_sec,
            total_operations,
        }
    }
}

// 单线程插入测试
fn test_single_thread_insert() -> Vec<PerformanceResult> {
    println!("=== 单线程插入吞吐量测试 ===");
    let test_data = generate_test_data(1_000_000);
    let mut results = Vec::new();

    // FlatMap 测试
    {
        let flatmap = FlatMap::new();

        // 预热
        for (k, v) in test_data.iter().take(WARMUP_OPERATIONS) {
            flatmap.insert(*k, *v);
        }

        let start = Instant::now();
        let mut operations = 0;
        let mut data_iter = test_data.iter().cycle();

        while start.elapsed() < TEST_DURATION {
            for _ in 0..1000 {
                if let Some((k, v)) = data_iter.next() {
                    flatmap.insert(*k, *v);
                    operations += 1;
                }
            }
        }

        let duration = start.elapsed();
        results.push(PerformanceResult::new(
            "FlatMap".to_string(),
            operations,
            duration,
        ));
    }

    // HashMap 测试
    {
        let mut hashmap = HashMap::new();

        // 预热
        for (k, v) in test_data.iter().take(WARMUP_OPERATIONS) {
            hashmap.insert(*k, *v);
        }

        let start = Instant::now();
        let mut operations = 0;
        let mut data_iter = test_data.iter().cycle();

        while start.elapsed() < TEST_DURATION {
            for _ in 0..1000 {
                if let Some((k, v)) = data_iter.next() {
                    hashmap.insert(*k, *v);
                    operations += 1;
                }
            }
        }

        let duration = start.elapsed();
        results.push(PerformanceResult::new(
            "HashMap".to_string(),
            operations,
            duration,
        ));
    }

    // DashMap 测试
    {
        let dashmap = DashMap::new();

        // 预热
        for (k, v) in test_data.iter().take(WARMUP_OPERATIONS) {
            dashmap.insert(*k, *v);
        }

        let start = Instant::now();
        let mut operations = 0;
        let mut data_iter = test_data.iter().cycle();

        while start.elapsed() < TEST_DURATION {
            for _ in 0..1000 {
                if let Some((k, v)) = data_iter.next() {
                    dashmap.insert(*k, *v);
                    operations += 1;
                }
            }
        }

        let duration = start.elapsed();
        results.push(PerformanceResult::new(
            "DashMap".to_string(),
            operations,
            duration,
        ));
    }

    results
}

// 单线程读取测试
fn test_single_thread_read() -> Vec<PerformanceResult> {
    println!("=== 单线程读取吞吐量测试 ===");
    let test_data = generate_test_data(100_000);
    let mut results = Vec::new();

    // FlatMap 测试
    {
        let flatmap = FlatMap::new();

        // 预填充数据
        for (k, v) in &test_data {
            flatmap.insert(*k, *v);
        }

        let start = Instant::now();
        let mut operations = 0;
        let mut data_iter = test_data.iter().cycle();

        while start.elapsed() < TEST_DURATION {
            for _ in 0..1000 {
                if let Some((k, _)) = data_iter.next() {
                    let _ = flatmap.get(k);
                    operations += 1;
                }
            }
        }

        let duration = start.elapsed();
        results.push(PerformanceResult::new(
            "FlatMap".to_string(),
            operations,
            duration,
        ));
    }

    // HashMap 测试
    {
        let mut hashmap = HashMap::new();

        // 预填充数据
        for (k, v) in &test_data {
            hashmap.insert(*k, *v);
        }

        let start = Instant::now();
        let mut operations = 0;
        let mut data_iter = test_data.iter().cycle();

        while start.elapsed() < TEST_DURATION {
            for _ in 0..1000 {
                if let Some((k, _)) = data_iter.next() {
                    let _ = hashmap.get(k);
                    operations += 1;
                }
            }
        }

        let duration = start.elapsed();
        results.push(PerformanceResult::new(
            "HashMap".to_string(),
            operations,
            duration,
        ));
    }

    // DashMap 测试
    {
        let dashmap = DashMap::new();

        // 预填充数据
        for (k, v) in &test_data {
            dashmap.insert(*k, *v);
        }

        let start = Instant::now();
        let mut operations = 0;
        let mut data_iter = test_data.iter().cycle();

        while start.elapsed() < TEST_DURATION {
            for _ in 0..1000 {
                if let Some((k, _)) = data_iter.next() {
                    let _ = dashmap.get(k);
                    operations += 1;
                }
            }
        }

        let duration = start.elapsed();
        results.push(PerformanceResult::new(
            "DashMap".to_string(),
            operations,
            duration,
        ));
    }

    results
}

// 多线程插入测试
fn test_multi_thread_insert() -> Vec<PerformanceResult> {
    let num_threads = num_cpus::get();
    println!("=== 多线程插入吞吐量测试 ({} 线程) ===", num_threads);
    let test_data = Arc::new(generate_test_data(1_000_000));
    let mut results = Vec::new();

    // FlatMap 测试
    {
        let flatmap = Arc::new(FlatMap::new());

        // 预热
        for (k, v) in test_data.iter().take(WARMUP_OPERATIONS) {
            flatmap.insert(*k, *v);
        }

        let start = Instant::now();
        let total_operations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let flatmap = Arc::clone(&flatmap);
                let test_data = Arc::clone(&test_data);
                let total_operations = Arc::clone(&total_operations);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let mut operations = 0;

                    while start.elapsed() < TEST_DURATION {
                        for _ in 0..100 {
                            let idx = rng.gen_range(0..test_data.len());
                            let (k, v) = test_data[idx];
                            flatmap.insert(k, v);
                            operations += 1;
                        }
                    }

                    total_operations.fetch_add(operations, std::sync::atomic::Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = total_operations.load(std::sync::atomic::Ordering::Relaxed);
        results.push(PerformanceResult::new(
            "FlatMap".to_string(),
            total_ops,
            duration,
        ));
    }

    // HashMap + Mutex 测试
    {
        let hashmap = Arc::new(Mutex::new(HashMap::new()));

        // 预热
        {
            let mut map = hashmap.lock().unwrap();
            for (k, v) in test_data.iter().take(WARMUP_OPERATIONS) {
                map.insert(*k, *v);
            }
        }

        let start = Instant::now();
        let total_operations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let hashmap = Arc::clone(&hashmap);
                let test_data = Arc::clone(&test_data);
                let total_operations = Arc::clone(&total_operations);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let mut operations = 0;

                    while start.elapsed() < TEST_DURATION {
                        for _ in 0..100 {
                            let idx = rng.gen_range(0..test_data.len());
                            let (k, v) = test_data[idx];
                            if let Ok(mut map) = hashmap.lock() {
                                map.insert(k, v);
                                operations += 1;
                            }
                        }
                    }

                    total_operations.fetch_add(operations, std::sync::atomic::Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = total_operations.load(std::sync::atomic::Ordering::Relaxed);
        results.push(PerformanceResult::new(
            "HashMap+Mutex".to_string(),
            total_ops,
            duration,
        ));
    }

    // DashMap 测试
    {
        let dashmap = Arc::new(DashMap::new());

        // 预热
        for (k, v) in test_data.iter().take(WARMUP_OPERATIONS) {
            dashmap.insert(*k, *v);
        }

        let start = Instant::now();
        let total_operations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let dashmap = Arc::clone(&dashmap);
                let test_data = Arc::clone(&test_data);
                let total_operations = Arc::clone(&total_operations);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let mut operations = 0;

                    while start.elapsed() < TEST_DURATION {
                        for _ in 0..100 {
                            let idx = rng.gen_range(0..test_data.len());
                            let (k, v) = test_data[idx];
                            dashmap.insert(k, v);
                            operations += 1;
                        }
                    }

                    total_operations.fetch_add(operations, std::sync::atomic::Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = total_operations.load(std::sync::atomic::Ordering::Relaxed);
        results.push(PerformanceResult::new(
            "DashMap".to_string(),
            total_ops,
            duration,
        ));
    }

    results
}

// 多线程读取测试
fn test_multi_thread_read() -> Vec<PerformanceResult> {
    let num_threads = num_cpus::get();
    println!("=== 多线程读取吞吐量测试 ({} 线程) ===", num_threads);
    let test_data = Arc::new(generate_test_data(100_000));
    let mut results = Vec::new();

    // FlatMap 测试
    {
        let flatmap = Arc::new(FlatMap::new());

        // 预填充数据
        for (k, v) in test_data.iter() {
            flatmap.insert(*k, *v);
        }

        let start = Instant::now();
        let total_operations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let flatmap = Arc::clone(&flatmap);
                let test_data = Arc::clone(&test_data);
                let total_operations = Arc::clone(&total_operations);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let mut operations = 0;

                    while start.elapsed() < TEST_DURATION {
                        for _ in 0..100 {
                            let idx = rng.gen_range(0..test_data.len());
                            let (k, _) = test_data[idx];
                            let _ = flatmap.get(&k);
                            operations += 1;
                        }
                    }

                    total_operations.fetch_add(operations, std::sync::atomic::Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = total_operations.load(std::sync::atomic::Ordering::Relaxed);
        results.push(PerformanceResult::new(
            "FlatMap".to_string(),
            total_ops,
            duration,
        ));
    }

    // HashMap + Mutex 测试
    {
        let hashmap = Arc::new(Mutex::new(HashMap::new()));

        // 预填充数据
        {
            let mut map = hashmap.lock().unwrap();
            for (k, v) in test_data.iter() {
                map.insert(*k, *v);
            }
        }

        let start = Instant::now();
        let total_operations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let hashmap = Arc::clone(&hashmap);
                let test_data = Arc::clone(&test_data);
                let total_operations = Arc::clone(&total_operations);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let mut operations = 0;

                    while start.elapsed() < TEST_DURATION {
                        for _ in 0..100 {
                            let idx = rng.gen_range(0..test_data.len());
                            let (k, _) = test_data[idx];
                            if let Ok(map) = hashmap.lock() {
                                let _ = map.get(&k);
                                operations += 1;
                            }
                        }
                    }

                    total_operations.fetch_add(operations, std::sync::atomic::Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = total_operations.load(std::sync::atomic::Ordering::Relaxed);
        results.push(PerformanceResult::new(
            "HashMap+Mutex".to_string(),
            total_ops,
            duration,
        ));
    }

    // DashMap 测试
    {
        let dashmap = Arc::new(DashMap::new());

        // 预填充数据
        for (k, v) in test_data.iter() {
            dashmap.insert(*k, *v);
        }

        let start = Instant::now();
        let total_operations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let dashmap = Arc::clone(&dashmap);
                let test_data = Arc::clone(&test_data);
                let total_operations = Arc::clone(&total_operations);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let mut operations = 0;

                    while start.elapsed() < TEST_DURATION {
                        for _ in 0..100 {
                            let idx = rng.gen_range(0..test_data.len());
                            let (k, _) = test_data[idx];
                            let _ = dashmap.get(&k);
                            operations += 1;
                        }
                    }

                    total_operations.fetch_add(operations, std::sync::atomic::Ordering::Relaxed);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = total_operations.load(std::sync::atomic::Ordering::Relaxed);
        results.push(PerformanceResult::new(
            "DashMap".to_string(),
            total_ops,
            duration,
        ));
    }

    results
}

// 打印测试结果
fn print_results(title: &str, results: &[PerformanceResult]) {
    println!("\n{}", title);
    println!("{:-<80}", "");
    println!(
        "{:<20} {:>15} {:>15} {:>15}",
        "数据结构", "总操作数", "ops/sec", "相对性能"
    );
    println!("{:-<80}", "");

    let max_ops = results
        .iter()
        .map(|r| r.operations_per_second)
        .max()
        .unwrap_or(1);

    for result in results {
        let relative_perf = result.operations_per_second as f64 / max_ops as f64;
        println!(
            "{:<20} {:>15} {:>15} {:>14.2}x",
            result.name, result.total_operations, result.operations_per_second, relative_perf
        );
    }
    println!();
}

fn main() {
    println!("FlatMap 吞吐量性能测试");
    println!("测试时长: {} 秒", TEST_DURATION.as_secs());
    println!("CPU 核心数: {}", num_cpus::get());
    println!();

    // 运行所有测试
    let single_insert_results = test_single_thread_insert();
    print_results("单线程插入吞吐量测试结果", &single_insert_results);

    let single_read_results = test_single_thread_read();
    print_results("单线程读取吞吐量测试结果", &single_read_results);

    let multi_insert_results = test_multi_thread_insert();
    print_results("多线程插入吞吐量测试结果", &multi_insert_results);

    let multi_read_results = test_multi_thread_read();
    print_results("多线程读取吞吐量测试结果", &multi_read_results);

    // 总结
    println!("=== 性能总结 ===");
    println!(
        "单线程插入最佳: {}",
        single_insert_results
            .iter()
            .max_by_key(|r| r.operations_per_second)
            .unwrap()
            .name
    );
    println!(
        "单线程读取最佳: {}",
        single_read_results
            .iter()
            .max_by_key(|r| r.operations_per_second)
            .unwrap()
            .name
    );
    println!(
        "多线程插入最佳: {}",
        multi_insert_results
            .iter()
            .max_by_key(|r| r.operations_per_second)
            .unwrap()
            .name
    );
    println!(
        "多线程读取最佳: {}",
        multi_read_results
            .iter()
            .max_by_key(|r| r.operations_per_second)
            .unwrap()
            .name
    );
}
