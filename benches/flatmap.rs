use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use flatmap_rs::FlatMap;
use std::collections::HashMap;

fn bench_insert_get_remove_flatmap(c: &mut Criterion) {
    c.bench_function("flatmap_insert_get_remove", |b| {
        b.iter(|| {
            let m = FlatMap::<u64, u64>::with_capacity(8192);
            for i in 0..50_000 {
                m.insert(i, i);
            }
            for i in 0..50_000 {
                let _ = m.get(&i);
            }
            for i in 0..50_000 {
                let _ = m.remove(i);
            }
            black_box(m.len())
        })
    });
}

fn bench_insert_get_remove_hashmap(c: &mut Criterion) {
    c.bench_function("hashmap_insert_get_remove", |b| {
        b.iter(|| {
            let mut m = HashMap::<u64, u64>::with_capacity(8192);
            for i in 0..50_000 {
                m.insert(i, i);
            }
            for i in 0..50_000 {
                let _ = m.get(&i);
            }
            for i in 0..50_000 {
                let _ = m.remove(&i);
            }
            black_box(m.len())
        })
    });
}

fn bench_insert_get_remove_dashmap(c: &mut Criterion) {
    c.bench_function("dashmap_insert_get_remove", |b| {
        b.iter(|| {
            let m = DashMap::<u64, u64>::with_capacity(8192);
            for i in 0..50_000 {
                m.insert(i, i);
            }
            for i in 0..50_000 {
                let _ = m.get(&i);
            }
            for i in 0..50_000 {
                let _ = m.remove(&i);
            }
            black_box(m.len())
        })
    });
}

fn bench_read_heavy_flatmap(c: &mut Criterion) {
    c.bench_function("flatmap_read_heavy", |b| {
        let m = FlatMap::<u64, u64>::with_capacity(8192);
        for i in 0..10_000 {
            m.insert(i, i);
        }

        b.iter(|| {
            for i in 0..50_000 {
                let _ = black_box(m.get(&(i % 10_000)));
            }
        })
    });
}

fn bench_read_heavy_hashmap(c: &mut Criterion) {
    c.bench_function("hashmap_read_heavy", |b| {
        let mut m = HashMap::<u64, u64>::with_capacity(8192);
        for i in 0..10_000 {
            m.insert(i, i);
        }

        b.iter(|| {
            for i in 0..50_000 {
                let _ = black_box(m.get(&(i % 10_000)));
            }
        })
    });
}

fn bench_read_heavy_dashmap(c: &mut Criterion) {
    c.bench_function("dashmap_read_heavy", |b| {
        let m = DashMap::<u64, u64>::with_capacity(8192);
        for i in 0..10_000 {
            m.insert(i, i);
        }

        b.iter(|| {
            for i in 0..50_000 {
                let _ = black_box(m.get(&(i % 10_000)));
            }
        })
    });
}

fn bench_concurrent_mixed_flatmap(c: &mut Criterion) {
    c.bench_function("flatmap_concurrent_mixed", |b| {
        b.iter(|| {
            let m = std::sync::Arc::new(FlatMap::<u64, u64>::with_capacity(8192));
            for i in 0..5_000 {
                m.insert(i, i);
            }

            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let m = m.clone();
                    std::thread::spawn(move || {
                        let base = thread_id * 10_000;
                        for i in 0..5_000 {
                            let key = base + i;
                            // 70% read, 30% write
                            if i % 10 < 7 {
                                let _ = m.get(&(key % 5_000));
                            } else {
                                m.insert(key, key);
                            }
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
            black_box(m.len())
        })
    });
}

fn bench_concurrent_mixed_dashmap(c: &mut Criterion) {
    c.bench_function("dashmap_concurrent_mixed", |b| {
        b.iter(|| {
            let m = std::sync::Arc::new(DashMap::<u64, u64>::with_capacity(8192));
            for i in 0..5_000 {
                m.insert(i, i);
            }

            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let m = m.clone();
                    std::thread::spawn(move || {
                        let base = thread_id * 10_000;
                        for i in 0..5_000 {
                            let key = base + i;
                            // 70% read, 30% write
                            if i % 10 < 7 {
                                let _ = m.get(&(key % 5_000));
                            } else {
                                m.insert(key, key);
                            }
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
            black_box(m.len())
        })
    });
}

fn bench_concurrent_write_heavy_flatmap(c: &mut Criterion) {
    c.bench_function("flatmap_concurrent_write_heavy", |b| {
        b.iter(|| {
            let m = std::sync::Arc::new(FlatMap::<u64, u64>::with_capacity(8192));

            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let m = m.clone();
                    std::thread::spawn(move || {
                        let start = thread_id * 5_000;
                        let end = start + 5_000;
                        for i in start..end {
                            m.insert(i, i);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
            black_box(m.len())
        })
    });
}

fn bench_concurrent_write_heavy_dashmap(c: &mut Criterion) {
    c.bench_function("dashmap_concurrent_write_heavy", |b| {
        b.iter(|| {
            let m = std::sync::Arc::new(DashMap::<u64, u64>::with_capacity(8192));

            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let m = m.clone();
                    std::thread::spawn(move || {
                        let start = thread_id * 5_000;
                        let end = start + 5_000;
                        for i in start..end {
                            m.insert(i, i);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
            black_box(m.len())
        })
    });
}

criterion_group!(
    benches,
    bench_insert_get_remove_flatmap,
    bench_insert_get_remove_hashmap,
    bench_insert_get_remove_dashmap,
    bench_read_heavy_flatmap,
    bench_read_heavy_hashmap,
    bench_read_heavy_dashmap,
    bench_concurrent_mixed_flatmap,
    bench_concurrent_mixed_dashmap,
    bench_concurrent_write_heavy_flatmap,
    bench_concurrent_write_heavy_dashmap
);
criterion_main!(benches);
