@echo off
echo 运行吞吐量测试...
echo.

echo ========================================
echo 运行详细吞吐量测试示例
echo ========================================
cargo run --release --example throughput_test

echo.
echo ========================================
echo 运行单线程 Criterion 基准测试
echo ========================================
cargo bench --bench throughput_comparison

echo.
echo ========================================
echo 运行多线程 Criterion 基准测试
echo ========================================
cargo bench --bench multi_thread_throughput

echo.
echo ========================================
echo 运行所有基准测试
echo ========================================
cargo bench

echo.
echo 测试完成！
pause