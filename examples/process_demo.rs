use flatmap_rs::FlatMap;

fn main() {
    let map = FlatMap::new();

    // Insert some initial values
    map.insert(1, 10);
    map.insert(2, 20);
    map.insert(3, 30);

    println!("Initial map:");
    for (k, v) in map.iter() {
        println!("  {} -> {}", k, v);
    }

    // Alter key 2: increment its value if it exists, otherwise insert 10
    let old_val = map.alter(2, |old| match old {
        Some(v) => Some(v + 5),
        None => Some(10),
    });
    let new_val = map.get(&2);
    println!("Alter key 2: old={:?}, new={:?}", old_val, new_val);

    // Alter key 4: insert it with value 100
    let old_val = map.alter(4, |_| Some(100));
    let new_val = map.get(&4);
    println!("Alter key 4: old={:?}, new={:?}", old_val, new_val);

    // Test retain - increment all values by 1
    let mut count = 0;
    map.retain(|_, v| {
        count += 1;
        *v += 1;
        true
    });
    println!("\nRetain processed {} entries", count);

    println!("\nFinal map:");
    for (k, v) in map.iter() {
        println!("  {} -> {}", k, v);
    }
}
