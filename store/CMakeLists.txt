set(STORE_SOURCES
        mvcc_store/mvcc_store.cc
        store_impl.cc)

if(BUILD_SHARED)
  add_library(${STORE_SHARED_LIB} SHARED ${STORE_SOURCES})
  target_link_libraries(${STORE_SHARED_LIB} ${TRANSACTION_STORE_SHARED_LIB})
endif()
