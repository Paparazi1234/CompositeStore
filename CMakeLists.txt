cmake_minimum_required(VERSION 3.10)

 set(CMAKE_C_COMPILER "C:\\mingw64\\bin\\gcc")
 set(CMAKE_CXX_COMPILER "C:\\mingw64\\bin\\g++")

project(multi-vesions LANGUAGES C CXX)

set(MULTI_VERSIONS_STATIC_LIB multi_versions)
set(MULTI_VERSIONS_SHARED_LIB multi_versions-shared)
set(SOURCES
          utilities/seq_based_multi_versions_impl.cc)

add_library(${MULTI_VERSIONS_STATIC_LIB} STATIC ${SOURCES})
target_link_libraries(${MULTI_VERSIONS_STATIC_LIB} PRIVATE lpthread)

option(BUILD_SHARED "Build shared libraries" ON)
if(BUILD_SHARED)
  add_library(${MULTI_VERSIONS_SHARED_LIB} SHARED ${SOURCES})
  target_link_libraries(${MULTI_VERSIONS_SHARED_LIB} PRIVATE lpthread)
endif()

if(BUILD_SHARED)
  set(MULTI_VERSIONS_LIB ${MULTI_VERSIONS_STATIC_LIB})
else()
  set(MULTI_VERSIONS_LIB ${MULTI_VERSIONS_SHARED_LIB})
endif()

option(BUILD_TESTS "build with tests" ON)
if(BUILD_TESTS)
  add_subdirectory(third-party/gtest)

  set(TEST_SOURCES
        utilities/seq_based_multi_versions_impl_test.cc)
  
  foreach(sourcefile ${TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} gtest ${MULTI_VERSIONS_LIB})
  endforeach(sourcefile ${TEST_SOURCES})
endif()

option(BUILD_EXAMPLES "build with examples" ON)
if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
