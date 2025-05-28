set(SKIPLIST_BACKED_STORE_SOURCES
        memory_allocator.cc
        MVCC_based_transaction.cc
        skiplist_rep.cc
        skiplist_backed_in_memory_store.cc
        skiplist_backed_in_memory_txn_store.cc
        write_batch.cc)

add_library(${SKIPLIST_BACKED_STORE_STATIC_LIB} STATIC ${SKIPLIST_BACKED_STORE_SOURCES})
target_link_libraries(${SKIPLIST_BACKED_STORE_STATIC_LIB} PUBLIC ${MULTI_VERSIONS_STATIC_LIB} ${TXN_LOCK_MANAGER_STATIC_LIB} ${ROCKSDB_STATIC_LIB})

if(BUILD_SHARED)
  add_library(${SKIPLIST_BACKED_STORE_SHARED_LIB} SHARED ${SKIPLIST_BACKED_STORE_SOURCES})
  target_link_libraries(${SKIPLIST_BACKED_STORE_SHARED_LIB} PUBLIC ${MULTI_VERSIONS_SHARED_LIB} ${TXN_LOCK_MANAGER_SHARED_LIB} ${ROCKSDB_SHARED_LIB})
endif()

if(BUILD_TESTS)
  set(SKIPLIST_BACKED_STORE_TEST_SOURCES
          MVCC_based_transaction_test.cc
          skiplist_backed_in_memory_store_test.cc
          write_batch_test.cc)

  foreach(sourcefile ${SKIPLIST_BACKED_STORE_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    if(BUILD_SHARED)
      target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${SKIPLIST_BACKED_STORE_SHARED_LIB})
    else()
      target_link_libraries(${exename} ${GTEST_STATIC_LIB} ${SKIPLIST_BACKED_STORE_STATIC_LIB})
    endif()
  endforeach(sourcefile ${SKIPLIST_BACKED_STORE_TEST_SOURCES})
endif()
