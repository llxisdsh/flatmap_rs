use flatmap_rs::FlatMap;
use std::sync::Arc;
use std::thread;

#[test]
fn test_parallel_resize_grow() {
    let map = Arc::new(FlatMap::new());

    // Insert enough items to trigger resize
    for i in 0..1000 {
        map.insert(i, i * 2);
    }

    // Verify all items are still there
    for i in 0..1000 {
        assert_eq!(map.get(&i), Some(i * 2));
    }

    assert_eq!(map.len(), 1000);
}

#[test]
fn test_parallel_resize_shrink() {
    let map = Arc::new(FlatMap::new().set_shrink(true));

    // Insert many items to grow the table
    for i in 0..2000 {
        map.insert(i, i * 2);
    }

    // Remove most items to trigger shrink
    for i in 0..1800 {
        map.remove(i);
    }

    // Verify remaining items are still there
    for i in 1800..2000 {
        assert_eq!(map.get(&i), Some(i * 2));
    }

    assert_eq!(map.len(), 200);
}

// #[test]
// fn test_clear_operation() {
//     let map = Arc::new(FlatMap::new());

//     // Insert items
//     for i in 0..100 {
//         map.insert(i, i * 2);
//     }

//     assert_eq!(map.len(), 100);

//     // Clear the map
//     map.clear();

//     // Verify map is empty
//     assert_eq!(map.len(), 0);
//     assert!(map.is_empty());

//     // Verify all items are gone
//     for i in 0..100 {
//         assert_eq!(map.get(&i), None);
//     }
// }

#[test]
fn test_concurrent_resize() {
    let map = Arc::new(FlatMap::new());
    let mut handles = vec![];

    // Spawn multiple threads that insert items concurrently
    for thread_id in 0..4 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for i in 0..500 {
                let key = thread_id * 1000 + i;
                map_clone.insert(key, key * 2);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all items are present
    assert_eq!(map.len(), 2000);
    for thread_id in 0..4 {
        for i in 0..500 {
            let key = thread_id * 1000 + i;
            assert_eq!(map.get(&key), Some(key * 2));
        }
    }
}

#[test]
fn test_resize_during_operations() {
    let map = Arc::new(FlatMap::new().set_shrink(true));
    let mut handles = vec![];

    // Thread 1: Insert items
    let map1 = Arc::clone(&map);
    let handle1 = thread::spawn(move || {
        for i in 0..1000 {
            map1.insert(i, i);
        }
    });
    handles.push(handle1);

    // Thread 2: Remove items
    let map2 = Arc::clone(&map);
    let handle2 = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        for i in 0..500 {
            map2.remove(i);
        }
    });
    handles.push(handle2);

    // Thread 3: Read items
    let map3 = Arc::clone(&map);
    let handle3 = thread::spawn(move || {
        for _ in 0..100 {
            for i in 0..100 {
                let _ = map3.get(&i);
            }
            thread::sleep(std::time::Duration::from_millis(1));
        }
    });
    handles.push(handle3);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify remaining items
    for i in 500..1000 {
        assert_eq!(map.get(&i), Some(i));
    }
}
