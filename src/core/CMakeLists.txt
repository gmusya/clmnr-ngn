add_library(ngn-csv INTERFACE)

target_include_directories(ngn-csv INTERFACE ${CMAKE_SOURCE_DIR})

################################################################################

add_executable(ngn-csv-test
  ut/columnar_test.cpp
  ut/csv_test.cpp
)

target_link_libraries(ngn-csv-test PUBLIC ngn-csv gtest_main)
