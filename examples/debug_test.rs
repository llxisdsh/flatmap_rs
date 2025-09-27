use flatmap_rs::FlatMap;

fn main() {
    println!("=== 模拟失败的测试场景 ===");
    let m = FlatMap::new();

    // 完全按照测试的步骤执行
    println!("Step 1: insert new 'a' -> '1'");
    let old = m.insert("a".to_string(), "1".to_string());
    println!("  Insert result: {:?}", old);
    println!("  Length: {}", m.len());
    let val = m.get(&"a".to_string());
    println!("  Get 'a': {:?}", val);

    println!("Step 2: insert another 'b' -> '2'");
    let old = m.insert("b".to_string(), "2".to_string());
    println!("  Insert result: {:?}", old);
    println!("  Length: {}", m.len());
    let val = m.get(&"b".to_string());
    println!("  Get 'b': {:?}", val);

    println!("Step 3: update existing 'a' -> '10'");
    let old = m.insert("a".to_string(), "10".to_string());
    println!("  Insert result: {:?}", old);
    let val = m.get(&"a".to_string());
    println!("  Get 'a': {:?}", val);

    println!("Step 4: remove existing 'b'");
    let old = m.remove("b".to_string());
    println!("  Remove result: {:?}", old);
    println!("  Length: {}", m.len());
    let val = m.get(&"b".to_string());
    println!("  Get 'b' after remove: {:?}", val);

    println!("=== 测试完成 ===");
}
