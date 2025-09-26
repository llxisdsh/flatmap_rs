use flatmapof::FlatMap;

fn main() {
    println!("=== 调试多key删除场景 ===");
    let m: FlatMap<i32, String> = FlatMap::new();

    // 插入100个key进行测试
    println!("插入keys 0-99:");
    for i in 0..100 {
        let _ = m.insert(i, format!("value_{}", i));
    }
    println!("  插入完成");
    
    println!("验证所有keys:");
    for i in 0..100 {
        let expected = format!("value_{}", i);
        let val = m.get(&i);
        if val != Some(expected.clone()) {
            println!("  ERROR: get({}) = {:?}, expected Some({})", i, val, expected);
        }
    }
    println!("  验证完成");
    
    println!("删除偶数keys:");
    for i in (0..100).step_by(2) {
        let old = m.remove(i);
        if old.is_none() {
            println!("  ERROR: 删除 {} 返回 None", i);
        }
    }
    println!("  删除完成");
    
    println!("验证删除后的状态:");
    let mut error_count = 0;
    for i in 0..100 {
        let val = m.get(&i);
        if i % 2 == 0 {
            if val.is_some() {
                println!("  ERROR: get({}) = {:?} (应该是None)", i, val);
                error_count += 1;
            }
        } else {
            let expected = format!("value_{}", i);
            if val != Some(expected.clone()) {
                println!("  ERROR: get({}) = {:?} (应该是Some({}))", i, val, expected);
                error_count += 1;
            }
        }
    }
    if error_count == 0 {
        println!("  验证通过，没有错误");
    } else {
        println!("  发现 {} 个错误", error_count);
    }
    
    println!("=== 调试完成 ===");
}