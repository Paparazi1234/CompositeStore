set(SEQ_BASED_MULTI_VERSIONS_SOURCES
        infinite_commit_table.cc
        seq_based_multi_versions.cc
        seq_based_snapshot.cc)

add_library(${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB} STATIC ${SEQ_BASED_MULTI_VERSIONS_SOURCES})
target_link_libraries(${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB} ${UTIL_STATIC_LIB})

if(BUILD_SHARED)
  add_library(${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB} SHARED ${SEQ_BASED_MULTI_VERSIONS_SOURCES})
  target_link_libraries(${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB} ${UTIL_SHARED_LIB})
endif()

if(BUILD_TESTS)
  set(SEQ_BASED_MULTI_VERSIONS_TEST_SOURCES
          infinite_commit_table_test.cc
          seq_based_multi_versions_test.cc)

  foreach(sourcefile ${SEQ_BASED_MULTI_VERSIONS_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    if(BUILD_SHARED)
      target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${TEST_UTIL_SHARED_LIB} ${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB})
    else()
      target_link_libraries(${exename} ${GTEST_STATIC_LIB} ${TEST_UTIL_STATIC_LIB} ${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB})
    endif()
  endforeach(sourcefile ${SEQ_BASED_MULTI_VERSIONS_TEST_SOURCES})
endif()
