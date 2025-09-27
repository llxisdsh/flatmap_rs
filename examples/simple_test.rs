use flatmap_rs::FlatMap;

fn main() {
    println!("Creating FlatMap...");
    let map = FlatMap::new();

    println!("Inserting values...");
    map.insert(1, 10);
    map.insert(2, 20);

    println!("Testing for_each...");
    map.range(|k, v| {
        println!("  {} -> {}", k, v);
        true
    });

    println!("Done!");
}
