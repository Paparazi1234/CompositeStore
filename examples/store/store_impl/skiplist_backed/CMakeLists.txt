set(SKIPLIST_BACKED_STORE_STATIC_LIB skiplist_backed_store)
set(SKIPLIST_BACKED_STORE_SHARED_LIB skiplist_backed_store-shared)
set(SKIPLIST_BACKED_STORE_SOURCES
        skliplist_backed_in_memory_store.cc
        MVCC_based_transaction.cc
        skliplist_backed_in_memory_txn_store.cc)

add_library(${SKIPLIST_BACKED_STORE_STATIC_LIB} STATIC ${SKIPLIST_BACKED_STORE_SOURCES})
target_link_libraries(${SKIPLIST_BACKED_STORE_STATIC_LIB} PRIVATE ${SEQ_BASED_MULTI_VERSIONS_LIB})

if(BUILD_SHARED)
  add_library(${SKIPLIST_BACKED_STORE_SHARED_LIB} SHARED ${SKIPLIST_BACKED_STORE_SOURCES})
  target_link_libraries(${SKIPLIST_BACKED_STORE_SHARED_LIB} PRIVATE ${SEQ_BASED_MULTI_VERSIONS_LIB})
endif()

if(BUILD_SHARED)
  set(SKIPLIST_BACKED_STORE_LIB ${SKIPLIST_BACKED_STORE_SHARED_LIB})
else()
  set(SKIPLIST_BACKED_STORE_LIB ${SKIPLIST_BACKED_STORE_STATIC_LIB})
endif()

if(BUILD_TESTS)
  set(SKIPLIST_BACKED_STORE_TEST_SOURCES
          MVCC_based_transaction_test.cc)

  foreach(sourcefile ${SKIPLIST_BACKED_STORE_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} gtest ${SKIPLIST_BACKED_STORE_LIB})
  endforeach(sourcefile ${SKIPLIST_BACKED_STORE_TEST_SOURCES})
endif()
