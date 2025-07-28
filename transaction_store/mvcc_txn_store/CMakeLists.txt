set(MVCC_TXN_STORE_SOURCES
        mvcc_txn_store.cc   
        optimistic_txn_store/optimistic_txn_store.cc
        pessimistic_txn_store/pessimistic_txn_store.cc
        pessimistic_txn_store/write_committed_txn_store.cc
        pessimistic_txn_store/write_prepared_txn_store.cc)

add_library(${MVCC_TXN_STORE_SHARED_LIB} SHARED ${MVCC_TXN_STORE_SOURCES})
target_link_libraries(${MVCC_TXN_STORE_SHARED_LIB} ${UTIL_SHARED_LIB})

if(BUILD_TESTS)
  set(MVCC_TXN_STORE_TEST_SOURCES
          pessimistic_txn_store/pessimistic_txn_store_test.cc
          mvcc_txn_store_test.cc)

  foreach(sourcefile ${MVCC_TXN_STORE_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${TEST_UTIL_SHARED_LIB} ${TXN_STORE_SHARED_LIB})
  endforeach(sourcefile ${MVCC_TXN_STORE_TEST_SOURCES})
endif()
