use criterion::{criterion_group, criterion_main, Criterion, black_box};
use flatmap_rs::FlatMap;
use std::collections::HashMap;
use dashmap::DashMap;

fn bench_insert_get_remove_flatmap(c: &mut Criterion) {
    c.bench_function("flatmap_insert_get_remove", |b| {
        b.iter(|| {
            let m = FlatMap::<u64, u64>::with_capacity(8192);
            for i in 0..50_000 { m.insert(i, i); }
            for i in 0..50_000 { let _ = m.get(&i); }
            for i in 0..50_000 { let _ = m.remove(i); }
            black_box(m.len())
        })
    });
}

fn bench_insert_get_remove_hashmap(c: &mut Criterion) {
    c.bench_function("hashmap_insert_get_remove", |b| {
        b.iter(|| {
            let mut m = HashMap::<u64, u64>::with_capacity(8192);
            for i in 0..50_000 { m.insert(i, i); }
            for i in 0..50_000 { let _ = m.get(&i); }
            for i in 0..50_000 { let _ = m.remove(&i); }
            black_box(m.len())
        })
    });
}

fn bench_insert_get_remove_dashmap(c: &mut Criterion) {
    c.bench_function("dashmap_insert_get_remove", |b| {
        b.iter(|| {
            let m = DashMap::<u64, u64>::with_capacity(8192);
            for i in 0..50_000 { m.insert(i, i); }
            for i in 0..50_000 { let _ = m.get(&i); }
            for i in 0..50_000 { let _ = m.remove(&i); }
            black_box(m.len())
        })
    });
}

// 并发性能测试
fn bench_concurrent_operations_flatmap(c: &mut Criterion) {
    c.bench_function("flatmap_concurrent_operations", |b| {
        b.iter(|| {
            let m = std::sync::Arc::new(FlatMap::<u64, u64>::with_capacity(8192));
            let handles: Vec<_> = (0..4).map(|thread_id| {
                let m = m.clone();
                std::thread::spawn(move || {
                    let start = thread_id * 10_000;
                    let end = start + 10_000;
                    for i in start..end { m.insert(i, i); }
                    for i in start..end { let _ = m.get(&i); }
                    for i in start..end { let _ = m.remove(i); }
                })
            }).collect();
            
            for handle in handles {
                handle.join().unwrap();
            }
            black_box(m.len())
        })
    });
}

fn bench_concurrent_operations_dashmap(c: &mut Criterion) {
    c.bench_function("dashmap_concurrent_operations", |b| {
        b.iter(|| {
            let m = std::sync::Arc::new(DashMap::<u64, u64>::with_capacity(8192));
            let handles: Vec<_> = (0..4).map(|thread_id| {
                let m = m.clone();
                std::thread::spawn(move || {
                    let start = thread_id * 10_000;
                    let end = start + 10_000;
                    for i in start..end { m.insert(i, i); }
                    for i in start..end { let _ = m.get(&i); }
                    for i in start..end { let _ = m.remove(&i); }
                })
            }).collect();
            
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
    bench_concurrent_operations_flatmap,
    bench_concurrent_operations_dashmap
);
criterion_main!(benches);