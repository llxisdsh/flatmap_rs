const META_MASK: u64 = 0x0080_8080_8080_8080;
const SLOT_MASK: u8 = 0x80;

fn broadcast(b: u8) -> u64 {
    0x0101_0101_0101_0101u64 * (b as u64)
}

fn mark_zero_bytes(w: u64) -> u64 {
    (w.wrapping_sub(0x0101_0101_0101_0101)) & (!w) & META_MASK
}

fn first_marked_byte_index(w: u64) -> usize {
    (w.trailing_zeros() >> 3) as usize
}

fn main() {
    println!("=== 测试mark_zero_bytes函数 ===");
    
    // 测试一个简单的meta值
    let h2 = 0x80u8; // SLOT_MASK
    let h2w = broadcast(h2);
    println!("h2 = 0x{:02x}", h2);
    println!("h2w = 0x{:016x}", h2w);
    
    // 假设slot 0有这个值，其他slot为空
    // meta的每个字节对应一个slot，slot 0在最低字节
    let meta = 0x0000_0000_0000_0080u64; // slot 0有SLOT_MASK位
    println!("meta = 0x{:016x}", meta);
    
    let xor_result = meta ^ h2w;
    println!("meta ^ h2w = 0x{:016x}", xor_result);
    
    let marked = mark_zero_bytes(xor_result);
    println!("marked = 0x{:016x}", marked);
    
    if marked != 0 {
        let slot = first_marked_byte_index(marked);
        println!("找到匹配的slot: {}", slot);
    } else {
        println!("没有找到匹配的slot");
    }
    
    println!("\n=== 测试不匹配的情况 ===");
    let different_h2 = 0x81u8;
    let different_h2w = broadcast(different_h2);
    let xor_result2 = meta ^ different_h2w;
    let marked2 = mark_zero_bytes(xor_result2);
    println!("不同h2的marked = 0x{:016x}", marked2);
    
    println!("=== 测试完成 ===");
}