use flatmapof::{FlatMap, Op};

fn main() {
    let map = FlatMap::new();
    
    // Insert some initial values
    map.insert(1, 10);
    map.insert(2, 20);
    map.insert(3, 30);
    
    println!("Initial map:");
    map.for_each(|k, v| {
        println!("  {} -> {}", k, v);
        true // continue iteration
    });
    
    // Process key 2: increment its value if it exists, otherwise insert 10
    let (old_val, new_val) = map.process(2, |old| {
        match old {
            Some(v) => (Op::Update, Some(v + 5)),
            None => (Op::Update, Some(10)),
        }
    });
    println!("Process key 2: old={:?}, new={:?}", old_val, new_val);
    
    // Process key 4: insert it with value 100
    let (old_val, new_val) = map.process(4, |_| (Op::Update, Some(100)));
    println!("Process key 4: old={:?}, new={:?}", old_val, new_val);
    
    // Test RangeProcess - increment all values by 1
    let mut count = 0;
    map.range_process(|k, v| {
        count += 1;
        (Op::Update, Some(*v + 1))
    });
    println!("\nRangeProcess processed {} entries", count);
    
    println!("\nFinal map:");
    map.for_each(|k, v| {
        println!("  {} -> {}", k, v);
        true // continue iteration
    });
}