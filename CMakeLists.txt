cmake_minimum_required(VERSION 3.10)

project(multi-vesions LANGUAGES CXX C ASM)

set(CMAKE_MESSAGE_LOG_LEVEL "DEBUG")

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/build/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/build/static)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/build/bin)

include_directories(${PROJECT_SOURCE_DIR})

option(BUILD_SHARED "Build shared libraries" ON)
option(BUILD_TESTS "build with tests" ON)
option(BUILD_EXAMPLES "build with examples" ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
include(third_party_defines)
include(pluggable_defines)

# if need to build test, then build gtest first
if(BUILD_TESTS)
  add_subdirectory(third-party/gtest)
endif()

# build third-party
add_subdirectory(third-party/rocksdb)

add_subdirectory(multi_version)

add_subdirectory(store)

if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
