add_library(ngn-exec STATIC
  expression.cpp
  kernel.cpp
  aggregation_executor.cpp
  operator.cpp
)

target_include_directories(ngn-exec PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(ngn-exec PUBLIC simde::simde)

################################################################################

add_executable(ngn-exec-test
  ut/aggregation_test.cpp
  ut/batch_test.cpp
  ut/expression_test.cpp
  ut/kernel_test.cpp
)

target_link_libraries(ngn-exec-test PUBLIC ngn-exec GTest::gtest_main)

################################################################################

add_executable(ngn-exec-bench
  bench/kernel_bench.cpp
)

target_link_libraries(ngn-exec-bench PUBLIC ngn-exec benchmark::benchmark_main)
