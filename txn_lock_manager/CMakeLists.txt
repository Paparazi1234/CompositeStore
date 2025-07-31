set(TXN_LOCK_MANAGER_SOURCES
        ordinary_txn_lock_manager.cc
        ordinary_txn_lock_tracker.cc
        txn_lock_manager_impl.cc)

add_library(${TXN_LOCK_MANAGER_SHARED_LIB} SHARED ${TXN_LOCK_MANAGER_SOURCES})
target_link_libraries(${TXN_LOCK_MANAGER_SHARED_LIB} ${UTIL_SHARED_LIB})

if(BUILD_TESTS)
  set(TXN_LOCK_MANAGER_TEST_SOURCES
          txn_lock_manager_test.cc)

  foreach(sourcefile ${TXN_LOCK_MANAGER_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${TEST_UTILS_SHARED_LIB} ${TXN_LOCK_MANAGER_SHARED_LIB})
  endforeach(sourcefile ${TXN_LOCK_MANAGER_TEST_SOURCES})
endif()
