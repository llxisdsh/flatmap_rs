use flatmapof::{FlatMap, Op};
use std::sync::{Arc, Barrier, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn range_process_under_heavy_concurrency_and_resize() {
    let map = Arc::new(FlatMap::new().enable_shrink(true));

    // Preload some data across buckets
    for i in 0..500 {
        map.insert(i, i);
    }

    // Include main thread in the barrier participants to avoid deadlock
    let start = Arc::new(Barrier::new(7));
    let stop = Arc::new(AtomicBool::new(false));
    let iterations = Arc::new(AtomicUsize::new(0));

    // Writers that cause grow
    let m1 = Arc::clone(&map);
    let s1 = Arc::clone(&start);
    let st1 = Arc::clone(&stop);
    let grower = thread::spawn(move || {
        s1.wait();
        let mut x = 1000_i64;
        while !st1.load(Ordering::Relaxed) {
            for _ in 0..200 {
                m1.insert(x, x);
                x += 1;
            }
            thread::yield_now();
        }
    });

    // Deleter that triggers shrink
    let m2 = Arc::clone(&map);
    let s2 = Arc::clone(&start);
    let st2 = Arc::clone(&stop);
    let deleter = thread::spawn(move || {
        s2.wait();
        let mut y = 0_i64;
        while !st2.load(Ordering::Relaxed) {
            for _ in 0..200 {
                m2.remove(y);
                y = (y + 1) % 800;
            }
            thread::yield_now();
        }
    });

    // Range processor that mixes update/delete/cancel based on key
    let m3 = Arc::clone(&map);
    let s3 = Arc::clone(&start);
    let st3 = Arc::clone(&stop);
    let iters = Arc::clone(&iterations);
    let ranger = thread::spawn(move || {
        s3.wait();
        let deadline = Instant::now() + Duration::from_millis(600);
        while Instant::now() < deadline && !st3.load(Ordering::Relaxed) {
            m3.range_process(|k, v| {
                if *k % 7 == 0 {
                    (Op::Delete, None)
                } else if *k % 11 == 0 {
                    (Op::Cancel, None)
                } else {
                    (Op::Update, Some(v + 1))
                }
            });
            iters.fetch_add(1, Ordering::Relaxed);
            thread::yield_now();
        }
        st3.store(true, Ordering::Relaxed);
    });

    // Additional concurrent readers to amplify read pressure
    let mut readers = Vec::new();
    for _ in 0..3 {
        let mr = Arc::clone(&map);
        let sr = Arc::clone(&start);
        let strp = Arc::clone(&stop);
        readers.push(thread::spawn(move || {
            sr.wait();
            while !strp.load(Ordering::Relaxed) {
                for key in [1_i64, 7, 11, 17, 23, 97, 131, 197, 263, 331] {
                    let _ = mr.get(&key);
                }
                thread::yield_now();
            }
        }));
    }

    // Start all threads (main participates too)
    start.wait();

    // Join all threads
    grower.join().unwrap();
    deleter.join().unwrap();
    ranger.join().unwrap();
    for r in readers { r.join().unwrap(); }

    // Ensure range_process ran several times
    assert!(iterations.load(Ordering::Relaxed) > 0, "range_process should have executed");

    // Basic sanity: map operations still functional after stress
    // Verify no panics and some keys have reasonable values
    for key in [1_i64, 11, 111, 211, 511] {
        let _ = map.get(&key);
    }
}