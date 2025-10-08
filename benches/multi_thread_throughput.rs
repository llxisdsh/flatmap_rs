use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use flatmap_rs::{FlatMap, SharedFlatMap};
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

// 生成测试数据
fn generate_test_data(size: usize) -> Vec<(u64, u64)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size).map(|_| (rng.gen(), rng.gen())).collect()
}

fn benchmark_multi_thread_insert(c: &mut Criterion) {
    let test_data = Arc::new(generate_test_data(100000));
    let num_threads = num_cpus::get();
    //let flatmap = Arc::new(FlatMap::new());
    //let dashmap = Arc::new(DashMap::new());
    c.bench_function("flatmap_multi_insert", |b| {
        b.iter(|| {
            //let flatmap = Arc::new(FlatMap::new());
            let flatmap = Arc::new(SharedFlatMap::new());
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let flatmap = Arc::clone(&flatmap);
                    let test_data = Arc::clone(&test_data);

                    thread::spawn(move || {
                        let chunk_size = test_data.len() / num_threads;
                        let start = thread_id * chunk_size;
                        let end = if thread_id == num_threads - 1 {
                            test_data.len()
                        } else {
                            start + chunk_size
                        };
                        //println!("Thread {}: start = {}, end = {}", thread_id, start, end);

                        for i in start..end {
                            let (k, v) = test_data[i];
                            black_box(flatmap.insert(k, v));
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    // c.bench_function("hashmap_mutex_multi_insert", |b| {
    //     b.iter(|| {
    //         let hashmap = Arc::new(Mutex::new(HashMap::new()));
    //         let handles: Vec<_> = (0..num_threads)
    //             .map(|thread_id| {
    //                 let hashmap = Arc::clone(&hashmap);
    //                 let test_data = Arc::clone(&test_data);

    //                 thread::spawn(move || {
    //                     let chunk_size = test_data.len() / num_threads;
    //                     let start = thread_id * chunk_size;
    //                     let end = if thread_id == num_threads - 1 {
    //                         test_data.len()
    //                     } else {
    //                         start + chunk_size
    //                     };

    //                     for i in start..end {
    //                         let (k, v) = test_data[i];
    //                         if let Ok(mut map) = hashmap.lock() {
    //                             black_box(map.insert(k, v));
    //                         }
    //                     }
    //                 })
    //             })
    //             .collect();

    //         for handle in handles {
    //             handle.join().unwrap();
    //         }
    //     })
    // });

    c.bench_function("dashmap_multi_insert", |b| {
        b.iter(|| {
            let dashmap = Arc::new(DashMap::new());
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let dashmap = Arc::clone(&dashmap);
                    let test_data = Arc::clone(&test_data);

                    thread::spawn(move || {
                        let chunk_size = test_data.len() / num_threads;
                        let start = thread_id * chunk_size;
                        let end = if thread_id == num_threads - 1 {
                            test_data.len()
                        } else {
                            start + chunk_size
                        };

                        for i in start..end {
                            let (k, v) = test_data[i];
                            black_box(dashmap.insert(k, v));
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
}

fn benchmark_multi_thread_read(c: &mut Criterion) {
    let test_data = Arc::new(generate_test_data(100000));
    let num_threads = num_cpus::get();

    // 预填充 FlatMap
    let flatmap = Arc::new(FlatMap::new());
    for (k, v) in test_data.iter() {
        flatmap.insert(*k, *v);
    }

    // 预填充 HashMap
    let hashmap = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut map = hashmap.lock().unwrap();
        for (k, v) in test_data.iter() {
            map.insert(*k, *v);
        }
    }

    // 预填充 DashMap
    let dashmap = Arc::new(DashMap::new());
    for (k, v) in test_data.iter() {
        dashmap.insert(*k, *v);
    }

    c.bench_function("flatmap_multi_read", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let flatmap = Arc::clone(&flatmap);
                    let test_data = Arc::clone(&test_data);

                    thread::spawn(move || {
                        let chunk_size = test_data.len() / num_threads;
                        let start = thread_id * chunk_size;
                        let end = if thread_id == num_threads - 1 {
                            test_data.len()
                        } else {
                            start + chunk_size
                        };

                        for i in start..end {
                            let (k, _) = test_data[i];
                            black_box(flatmap.get(&k));
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    // c.bench_function("hashmap_mutex_multi_read", |b| {
    //     b.iter(|| {
    //         let handles: Vec<_> = (0..num_threads)
    //             .map(|thread_id| {
    //                 let hashmap = Arc::clone(&hashmap);
    //                 let test_data = Arc::clone(&test_data);

    //                 thread::spawn(move || {
    //                     let chunk_size = test_data.len() / num_threads;
    //                     let start = thread_id * chunk_size;
    //                     let end = if thread_id == num_threads - 1 {
    //                         test_data.len()
    //                     } else {
    //                         start + chunk_size
    //                     };

    //                     for i in start..end {
    //                         let (k, _) = test_data[i];
    //                         if let Ok(map) = hashmap.lock() {
    //                             black_box(map.get(&k));
    //                         }
    //                     }
    //                 })
    //             })
    //             .collect();

    //         for handle in handles {
    //             handle.join().unwrap();
    //         }
    //     })
    // });

    c.bench_function("dashmap_multi_read", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let dashmap = Arc::clone(&dashmap);
                    let test_data = Arc::clone(&test_data);

                    thread::spawn(move || {
                        let chunk_size = test_data.len() / num_threads;
                        let start = thread_id * chunk_size;
                        let end = if thread_id == num_threads - 1 {
                            test_data.len()
                        } else {
                            start + chunk_size
                        };

                        for i in start..end {
                            let (k, _) = test_data[i];
                            black_box(dashmap.get(&k));
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
}

criterion_group!(
    benches,
    benchmark_multi_thread_insert,
    benchmark_multi_thread_read
);
criterion_main!(benches);
