cmake_minimum_required(VERSION 3.25)

project(columnar-engine)

include(cmake/compile.cmake)
include(cmake/dependencies.cmake)

add_subdirectory(src)
add_subdirectory(exe)
