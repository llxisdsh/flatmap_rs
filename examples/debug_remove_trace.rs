use flatmap_rs::FlatMap;

fn main() {
    println!("=== 跟踪删除操作 ===");
    let m: FlatMap<i32, String> = FlatMap::new();

    // 插入key 28和32
    println!("插入key 28和32:");
    m.insert(28, "value_28".to_string());
    m.insert(32, "value_32".to_string());

    println!("验证插入:");
    println!("  get(28) = {:?}", m.get(&28));
    println!("  get(32) = {:?}", m.get(&32));

    println!("删除key 32:");
    let result = m.remove(32);
    println!("  remove(32) -> {:?}", result);

    println!("验证删除后:");
    println!("  get(28) = {:?} (应该是Some)", m.get(&28));
    println!("  get(32) = {:?} (应该是None)", m.get(&32));

    // 再测试一个更简单的场景
    println!("\n=== 简单场景测试 ===");
    let m2: FlatMap<i32, String> = FlatMap::new();

    m2.insert(1, "value_1".to_string());
    m2.insert(2, "value_2".to_string());

    println!("插入后:");
    println!("  get(1) = {:?}", m2.get(&1));
    println!("  get(2) = {:?}", m2.get(&2));

    println!("删除key 2:");
    let result2 = m2.remove(2);
    println!("  remove(2) -> {:?}", result2);

    println!("验证删除后:");
    println!("  get(1) = {:?} (应该是Some)", m2.get(&1));
    println!("  get(2) = {:?} (应该是None)", m2.get(&2));

    println!("=== 跟踪完成 ===");
}
