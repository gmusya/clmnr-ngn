add_library(ngn-exec STATIC
  expression.cpp
  kernel.cpp
)

target_include_directories(ngn-exec PUBLIC ${CMAKE_SOURCE_DIR})

################################################################################

add_executable(ngn-exec-test
  ut/batch_test.cpp
  ut/expression_test.cpp
  ut/kernel_test.cpp
)

target_link_libraries(ngn-exec-test PUBLIC ngn-exec gtest_main)
