add_library(ngn-exec STATIC
  expression.cpp
  kernel.cpp
)

target_include_directories(ngn-exec PUBLIC ${CMAKE_SOURCE_DIR})
