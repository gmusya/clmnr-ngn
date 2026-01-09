add_executable(ngn-clickbench-run
  run.cpp
)

target_link_libraries(ngn-clickbench-run PUBLIC
  ngn-exec
  ngn-csv
  absl::flags
  absl::flags_parse
  absl::log
  absl::log_initialize
)

target_include_directories(ngn-clickbench-run PUBLIC ${CMAKE_SOURCE_DIR})
