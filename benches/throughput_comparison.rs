use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use flatmap_rs::FlatMap;
use rand::prelude::*;
use std::collections::HashMap;

// 生成测试数据
fn generate_test_data(size: usize) -> Vec<(u64, u64)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size).map(|_| (rng.gen(), rng.gen())).collect()
}

fn benchmark_insert(c: &mut Criterion) {
    let test_data = generate_test_data(10000);

    c.bench_function("flatmap_insert", |b| {
        b.iter(|| {
            let flatmap = FlatMap::new();
            for (k, v) in &test_data {
                black_box(flatmap.insert(*k, *v));
            }
        })
    });

    c.bench_function("hashmap_insert", |b| {
        b.iter(|| {
            let mut hashmap = HashMap::new();
            for (k, v) in &test_data {
                black_box(hashmap.insert(*k, *v));
            }
        })
    });

    c.bench_function("dashmap_insert", |b| {
        b.iter(|| {
            let dashmap = DashMap::new();
            for (k, v) in &test_data {
                black_box(dashmap.insert(*k, *v));
            }
        })
    });
}

fn benchmark_read(c: &mut Criterion) {
    let test_data = generate_test_data(10000);

    // 预填充 FlatMap
    let flatmap = FlatMap::new();
    for (k, v) in &test_data {
        flatmap.insert(*k, *v);
    }

    // 预填充 HashMap
    let mut hashmap = HashMap::new();
    for (k, v) in &test_data {
        hashmap.insert(*k, *v);
    }

    // 预填充 DashMap
    let dashmap = DashMap::new();
    for (k, v) in &test_data {
        dashmap.insert(*k, *v);
    }

    c.bench_function("flatmap_read", |b| {
        b.iter(|| {
            for (k, _) in &test_data {
                black_box(flatmap.get(k));
            }
        })
    });

    c.bench_function("hashmap_read", |b| {
        b.iter(|| {
            for (k, _) in &test_data {
                black_box(hashmap.get(k));
            }
        })
    });

    c.bench_function("dashmap_read", |b| {
        b.iter(|| {
            for (k, _) in &test_data {
                black_box(dashmap.get(k));
            }
        })
    });
}

criterion_group!(benches, benchmark_insert, benchmark_read);
criterion_main!(benches);
