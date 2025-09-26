use criterion::{criterion_group, criterion_main, Criterion, black_box};
use flatmapof::FlatMap;
use std::collections::HashMap;

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

criterion_group!(benches, bench_insert_get_remove_flatmap, bench_insert_get_remove_hashmap);
criterion_main!(benches);