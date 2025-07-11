set(MVCC_WRITE_BUFFER_SOURCES
        skiplist_backed/skiplist_backed_mvcc_wb.cc
        skiplist_backed/skiplist_rep.cc
        mvcc_write_buffer_impl.cc)

add_library(${MVCC_WRITE_BUFFER_SHARED_LIB} SHARED ${MVCC_WRITE_BUFFER_SOURCES})
target_link_libraries(${MVCC_WRITE_BUFFER_SHARED_LIB} ${UTIL_SHARED_LIB} ${ROCKSDB_SHARED_LIB})

if(BUILD_TESTS)
  set(MVCC_WRITE_BUFFER_TEST_SOURCES
          skiplist_backed/skiplist_backed_mvcc_wb_test.cc)

  foreach(sourcefile ${MVCC_WRITE_BUFFER_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${MVCC_WRITE_BUFFER_SHARED_LIB})
  endforeach(sourcefile ${MVCC_WRITE_BUFFER_TEST_SOURCES})
endif()
