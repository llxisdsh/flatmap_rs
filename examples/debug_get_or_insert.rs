use flatmap_rs::FlatMap;

fn main() {
    println!("=== 调试 get_or_insert_with ===");
    let m: FlatMap<u64, String> = FlatMap::new();

    println!("第一次调用 get_or_insert_with(42, 'hello')");
    let (v, existed) = m.get_or_insert_with(42, || "hello".to_string());
    println!("  返回值: ({:?}, {})", v, existed);
    println!("  长度: {}", m.len());
    
    println!("第二次调用 get_or_insert_with(42, 'world')");
    let (v2, existed2) = m.get_or_insert_with(42, || "world".to_string());
    println!("  返回值: ({:?}, {})", v2, existed2);
    println!("  长度: {}", m.len());
    
    println!("=== 调试完成 ===");
}