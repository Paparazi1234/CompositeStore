set(MULTI_VERSIONS_SOURCES
        sequence_based/write_committed/write_committed_multi_versions.cc
        sequence_based/write_committed/write_committed_snapshot.cc
        sequence_based/write_prepared/infinite_commit_table.cc
        sequence_based/write_prepared/write_prepared_multi_versions.cc
        sequence_based/write_prepared/write_prepared_snapshot.cc
        sequence_based/seq_based_multi_versions.cc
        sequence_based/seq_based_snapshot.cc
        multi_versions_impl.cc)

add_library(${MULTI_VERSIONS_SHARED_LIB} SHARED ${MULTI_VERSIONS_SOURCES})
target_link_libraries(${MULTI_VERSIONS_SHARED_LIB} ${UTIL_SHARED_LIB})

if(BUILD_TESTS)
  set(MULTI_VERSIONS_TEST_SOURCES
          sequence_based/write_prepared/infinite_commit_table_test.cc
          sequence_based/seq_based_multi_versions_test.cc)

  foreach(sourcefile ${MULTI_VERSIONS_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${TEST_UTILS_SHARED_LIB} ${MULTI_VERSIONS_SHARED_LIB})
  endforeach(sourcefile ${MULTI_VERSIONS_TEST_SOURCES})
endif()
