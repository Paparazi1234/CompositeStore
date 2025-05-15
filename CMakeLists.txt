cmake_minimum_required(VERSION 3.10)

set(CMAKE_C_COMPILER "C:\\mingw64\\bin\\gcc")
set(CMAKE_CXX_COMPILER "C:\\mingw64\\bin\\g++")

project(multi-vesions LANGUAGES C CXX)

option(BUILD_SHARED "Build shared libraries" ON)
option(BUILD_TESTS "build with tests" ON)
option(BUILD_EXAMPLES "build with examples" ON)

# if need to build test, then build gtest first
if(BUILD_TESTS)
  add_subdirectory(third-party/gtest)
endif()

add_subdirectory(utilities/sequence_based)

if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
