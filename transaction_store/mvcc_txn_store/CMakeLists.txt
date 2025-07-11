set(MVCC_TXN_STORE_SOURCES
        mvcc_txn_store.cc
        write_batch.cc
        optimistic_txn_store/optimistic_txn_store.cc
        pessimistic_txn_store/pessimistic_txn_store.cc
        pessimistic_txn_store/write_committed_txn_store.cc
        pessimistic_txn_store/write_prepared_txn_store.cc)

add_library(${MVCC_TXN_STORE_SHARED_LIB} SHARED ${MVCC_TXN_STORE_SOURCES})
target_link_libraries(${MVCC_TXN_STORE_SHARED_LIB} ${UTIL_SHARED_LIB} ${ROCKSDB_SHARED_LIB} ${MULTI_VERSIONS_SHARED_LIB}) # Todo: ${ROCKSDB_SHARED_LIB} ${MULTI_VERSIONS_SHARED_LIB}应该可以不依赖

if(BUILD_TESTS)
  set(MVCC_TXN_STORE_TEST_SOURCES
          mvcc_txn_store_test.cc
          write_batch_test.cc
          pessimistic_txn_store/pessimistic_txn_store_test.cc)

  foreach(sourcefile ${MVCC_TXN_STORE_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${TEST_UTIL_SHARED_LIB} ${MVCC_TXN_STORE_SHARED_LIB} ${TXN_LOCK_MANAGER_SHARED_LIB} ${STAGING_WRITE_SHARED_LIB} ${MVCC_WRITE_BUFFER_SHARED_LIB} ${TRANSACTION_SHARED_LIB})
  endforeach(sourcefile ${MVCC_TXN_STORE_TEST_SOURCES})
endif()
