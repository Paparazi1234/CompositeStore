cmake_minimum_required(VERSION 3.10)

project(multi-vesions LANGUAGES C CXX)

option(BUILD_SHARED "Build shared libraries" ON)
option(BUILD_TESTS "build with tests" ON)
option(BUILD_EXAMPLES "build with examples" ON)

# if need to build test, then build gtest first
if(BUILD_TESTS)
  add_subdirectory(third-party/gtest)
endif()

# build third-party
add_subdirectory(third-party/rocksdb)

add_subdirectory(utilities)

if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
