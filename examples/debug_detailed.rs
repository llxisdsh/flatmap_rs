use flatmapof::FlatMap;

fn main() {
    println!("=== 详细调试删除问题 ===");
    let m: FlatMap<i32, String> = FlatMap::new();

    // 只测试几个特定的key
    let test_keys = [1, 2, 11, 12, 27, 28, 31, 32];
    
    println!("插入测试keys:");
    for &key in &test_keys {
        let result = m.insert(key, format!("value_{}", key));
        println!("  insert({}) -> {:?}", key, result);
    }
    
    println!("验证插入后的状态:");
    for &key in &test_keys {
        let val = m.get(&key);
        println!("  get({}) = {:?}", key, val);
    }
    
    println!("删除偶数keys:");
    for &key in &test_keys {
        if key % 2 == 0 {
            let result = m.remove(key);
            println!("  remove({}) -> {:?}", key, result);
        }
    }
    
    println!("验证删除后的状态:");
    for &key in &test_keys {
        let val = m.get(&key);
        if key % 2 == 0 {
            println!("  get({}) = {:?} (应该是None)", key, val);
        } else {
            println!("  get({}) = {:?} (应该是Some(value_{}))", key, val, key);
        }
    }
    
    println!("=== 调试完成 ===");
}