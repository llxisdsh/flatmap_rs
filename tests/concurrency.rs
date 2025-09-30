use std::sync::{Arc, Barrier};
use std::thread;

use flatmap_rs::FlatMap;

#[test]
fn concurrent_mixed_ops_string_keys() {
    let m: Arc<FlatMap<String, usize>> = Arc::new(FlatMap::with_capacity(1024));
    let n_threads = 6;
    let iters = 3_000;
    let barrier = Arc::new(Barrier::new(n_threads));

    let mut handles = Vec::new();
    for t in 0..n_threads {
        let b = barrier.clone();
        let map = m.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..iters {
                let k = format!("k:{}:{}", t, i % 1024);
                if i % 4 == 0 {
                    map.insert(k.clone(), i);
                } else if i % 4 == 1 {
                    let _ = map.get(&k);
                } else if i % 4 == 2 {
                    let _ = map.get_or_insert_with(k.clone(), || i);
                } else {
                    let _ = map.remove(k.clone());
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert!(m.len() <= n_threads * 1024);
}

#[test]
fn concurrent_integer_keys_contention() {
    let m: Arc<FlatMap<u64, u64>> = Arc::new(FlatMap::with_capacity(2048));
    let n_threads = 6;
    let iters = 1_500;
    let barrier = Arc::new(Barrier::new(n_threads));

    let hot_keys: Vec<u64> = (0..64).collect();

    let mut handles = Vec::new();
    for t in 0..n_threads {
        let b = barrier.clone();
        let map = m.clone();
        let hk = hot_keys.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..iters {
                let k = hk[(i + t) as usize % hk.len()];
                match (i + t) % 3 {
                    0 => {
                        map.insert(k, i as u64);
                    }
                    1 => {
                        let _ = map.get(&k);
                    }
                    _ => {
                        let _ = map.remove(k);
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    for k in &hot_keys {
        let _ = m.get(k);
    }
}

#[test]
fn concurrent_load_or_insert_once_under_race() {
    use flatmap_rs::FlatMap;
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;
    use std::thread;

    let m: Arc<FlatMap<i32, i32>> = Arc::new(FlatMap::new());
    let called = Arc::new(AtomicI32::new(0));

    let workers = std::cmp::max(
        2,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2),
    );
    let mut handles = Vec::new();
    for _ in 0..workers {
        let map = m.clone();
        let c = called.clone();
        handles.push(thread::spawn(move || {
            let (_v, _loaded) = map.get_or_insert_with(999, || {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(1));
                777
            });
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(called.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(m.get(&999), Some(777));
}

#[test]
fn double_buffer_consistency_under_updates() {
    use flatmap_rs::FlatMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let m: Arc<FlatMap<i32, i32>> = Arc::new(FlatMap::new());
    const NUM_KEYS: i32 = 100;
    const NUM_UPDATES: i32 = 50;

    for i in 1..=NUM_KEYS {
        let _ = m.insert(i, i);
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();

    // Readers to validate no torn reads (values non-negative)
    for _ in 0..4 {
        let map = m.clone();
        let s = stop.clone();
        handles.push(thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                for i in 1..=NUM_KEYS {
                    if let Some(v) = map.get(&i) {
                        assert!(v >= 0);
                    } else {
                        // transiently may miss if deleted, but our writer never deletes here
                        panic!("key {} missing unexpectedly", i);
                    }
                }
            }
        }));
    }

    // Writer
    {
        let map = m.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..NUM_UPDATES {
                for i in 1..=NUM_KEYS {
                    let _ = map.insert(i, map.get(&i).unwrap() + 1000);
                }
                std::thread::yield_now();
            }
        }));
    }

    thread::sleep(Duration::from_millis(100));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn seqlock_consistency_stress_aba() {
    #[derive(Clone, Copy)]
    struct Pair {
        x: u64,
        y: u64,
    }
    use flatmap_rs::FlatMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let m: Arc<FlatMap<i32, Pair>> = Arc::new(FlatMap::new());
    m.insert(0, Pair { x: 0, y: !0u64 });

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();

    // Writers flipping value rapidly
    let writer_n = 4;
    for _ in 0..writer_n {
        let map = m.clone();
        let s = stop.clone();
        handles.push(thread::spawn(move || {
            let mut seq: u32 = 0;
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                seq = seq.wrapping_add(1);
                let val = Pair {
                    x: seq as u64,
                    y: !(seq as u64),
                };
                let _ = map.insert(0, val);
            }
        }));
    }

    // Readers continuously validate invariant
    let reader_n = 4;
    for _ in 0..reader_n {
        let map = m.clone();
        let s = stop.clone();
        handles.push(thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(v) = map.get(&0) {
                    assert_eq!(v.y, !v.x, "torn read detected: x={}, y={}", v.x, v.y);
                }
            }
        }));
    }

    thread::sleep(Duration::from_millis(150));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn load_delete_race_semantics() {
    use flatmap_rs::FlatMap;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    let m: Arc<FlatMap<String, u32>> = Arc::new(FlatMap::new());
    let key = "k".to_string();
    let inserted_val: u32 = 1;

    let anomalies = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    // writer toggling store/delete
    {
        let map = m.clone();
        let s = stop.clone();
        let key_w = key.clone();
        handles.push(thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(500);
            while Instant::now() < deadline {
                map.insert(key_w.clone(), inserted_val);
                std::thread::yield_now();
                let _ = map.remove(key_w.clone());
                std::thread::yield_now();
            }
            s.store(true, std::sync::atomic::Ordering::Relaxed);
        }));
    }

    // readers
    let reader_n = std::cmp::max(
        2,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2),
    );
    for _ in 0..reader_n {
        let map = m.clone();
        let s = stop.clone();
        let an = anomalies.clone();
        let key_r = key.clone();
        handles.push(thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(v) = map.get(&key_r) {
                    if v == 0 {
                        an.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(
        anomalies.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "observed ok==true && value==0 under load/delete race"
    );
}

#[test]
fn key_torn_read_stress() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct BigKey {
        a: u64,
        b: u64,
    }
    use flatmap_rs::FlatMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let m: Arc<FlatMap<BigKey, i32>> = Arc::new(FlatMap::new());

    const N: usize = 512;
    let mut keys = Vec::with_capacity(N);
    for i in 0..N {
        let ai = (i as u64) * 2147483647 + 123456789;
        let k = BigKey { a: ai, b: !ai };
        keys.push(k);
        let _ = m.insert(k, i as i32);
    }
    let keys = std::sync::Arc::new(keys);

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // Range readers validate invariant
    for _ in 0..4 {
        let map = m.clone();
        let s = stop.clone();
        handles.push(thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                for k in map.keys() {
                    assert_eq!(k.b, !k.a, "torn key detected: a={}, b={}", k.a, k.b);
                }
            }
        }));
    }

    // Load readers hammer key comparisons
    for r in 0..4 {
        let map = m.clone();
        let s = stop.clone();
        let keys_c = keys.clone();
        handles.push(thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                for i in r..N {
                    let _ = map.get(&keys_c[i]);
                }
            }
        }));
    }

    // Writers: delete and re-insert to trigger meta/key memory changes
    for w in 0..2 {
        let map = m.clone();
        let s = stop.clone();
        let keys_c = keys.clone();
        handles.push(thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                for i in w..N {
                    let k = keys_c[i];
                    let _ = map.remove(k);
                    let _ = map.insert(k, i as i32);
                }
                std::thread::yield_now();
            }
        }));
    }

    thread::sleep(Duration::from_millis(800));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}
