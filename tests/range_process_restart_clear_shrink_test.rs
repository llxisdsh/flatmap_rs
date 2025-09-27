use flatmapof::{FlatMap, Op};
use std::sync::{Arc, Barrier, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn range_process_restart_on_clear_and_shrink() {
    // Enable shrink to test Clear/Shrink interactions
    let map = Arc::new(FlatMap::new().enable_shrink(true));

    // Preload
    for i in 0..256 { map.insert(i, i); }

    let start = Arc::new(Barrier::new(3));
    let stop = Arc::new(AtomicBool::new(false));

    // Thread 1: periodically clear the map to force Clear resize
    let m1 = Arc::clone(&map);
    let s1 = Arc::clone(&start);
    let st1 = Arc::clone(&stop);
    let clearer = thread::spawn(move || {
        s1.wait();
        let deadline = Instant::now() + Duration::from_millis(300);
        while Instant::now() < deadline && !st1.load(Ordering::Relaxed) {
            m1.clear();
            // Refill a bit after clear to keep activity
            for i in 0..64 { m1.insert(i, i * 2); }
            thread::sleep(Duration::from_millis(10));
        }
        st1.store(true, Ordering::Relaxed);
    });

    // Thread 2: range_process should restart if table swaps
    let m2 = Arc::clone(&map);
    let s2 = Arc::clone(&start);
    let st2 = Arc::clone(&stop);
    let ranger = thread::spawn(move || {
        s2.wait();
        while !st2.load(Ordering::Relaxed) {
            m2.range_process(|_k, v| {
                // No-op or small update; goal is to cross table swaps without panics/deadlocks
                (Op::Update, Some(v + 1))
            });
            thread::yield_now();
        }
    });

    start.wait();
    clearer.join().unwrap();
    ranger.join().unwrap();

    // Post-condition: map remains usable
    for i in 0..64 {
        let _ = map.get(&i);
    }
}