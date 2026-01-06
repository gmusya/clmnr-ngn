add_executable(csv_to_columnar csv_to_columnar.cpp)

target_link_libraries(csv_to_columnar PUBLIC ngn-csv absl::flags absl::flags_parse)
target_include_directories(csv_to_columnar PUBLIC ${CMAKE_SOURCE_DIR})

################################################################################

add_executable(columnar_to_csv columnar_to_csv.cpp)

target_link_libraries(columnar_to_csv PUBLIC ngn-csv absl::flags absl::flags_parse)
target_include_directories(columnar_to_csv PUBLIC ${CMAKE_SOURCE_DIR})

################################################################################

add_executable(gen gen.cpp)

target_link_libraries(gen PUBLIC ngn-csv absl::flags absl::flags_parse absl::log)
target_include_directories(gen PUBLIC ${CMAKE_SOURCE_DIR})
