cmake_minimum_required(VERSION 3.9)
project(seastar-starter VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# Optimize for the machine we're building on
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(seastar)
add_subdirectory(abseil-cpp)

add_executable(main main.cc)
target_link_libraries(main Seastar::seastar absl::btree)
set_property(TARGET main PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)

