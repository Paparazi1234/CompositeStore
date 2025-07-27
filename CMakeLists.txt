cmake_minimum_required(VERSION 3.10)

project(CompositeStore LANGUAGES CXX C ASM)

set(CMAKE_MESSAGE_LOG_LEVEL "DEBUG")

if(NOT CMAKE_BUILD_TYPE)
  if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
    set(default_build_type "Debug")
  else()
    set(default_build_type "RelWithDebInfo")
  endif()
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING
    "Default BUILD_TYPE is ${default_build_type}" FORCE)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wextra -Wall -pthread -std=c++17")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-strict-aliasing -Wno-invalid-offsetof")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" ON)
if(FAIL_ON_WARNINGS)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
  include(CheckCXXCompilerFlag)
  CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
  if(HAVE_OMIT_LEAF_FRAME_POINTER)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
  endif()
endif()

include(cmake/sanitize_check.cmake)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/build/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/build/static)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/build/bin)

include(CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES("
#include <pthread.h>
#include <time.h>
int main() {
  pthread_mutex_t mu;
  pthread_mutex_init(&mu, nullptr);
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  pthread_mutex_timedlock(&mu, &ts);
  return 0;
}
" HAVE_PTHREAD_MUTEX_TIMEDLOCK)
if(HAVE_PTHREAD_MUTEX_TIMEDLOCK)
  message(STATUS "pthread_mutex_timedlock available")
  add_definitions(-DPTHREAD_MUTEX_TIMEDLOCK_AVAILABLE)
endif()

include_directories(${PROJECT_SOURCE_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
include(cmake/third_party_defines.cmake)
include(cmake/common_libs_defines.cmake)
include(cmake/pluggables_defines.cmake)

add_subdirectory(port)

add_subdirectory(util)

# if need to build test, then build gtest first
option(BUILD_TESTS "build with tests" ON)
if(BUILD_TESTS)
  add_subdirectory(third-party/gtest)
endif()

# build third-party
add_subdirectory(third-party/rocksdb)

if(BUILD_TESTS)
  add_subdirectory(test_util)
endif()

add_subdirectory(multi_versions)

add_subdirectory(txn_lock_manager)

add_subdirectory(staging_write)

add_subdirectory(mvcc_write_buffer)

add_subdirectory(transaction_store)

add_subdirectory(store)

add_subdirectory(transaction)

option(BUILD_EXAMPLES "build with examples" ON)
if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

option(BUILD_BENCH "build with bench" ON)
if(BUILD_BENCH)
  add_subdirectory(bench)
endif()
