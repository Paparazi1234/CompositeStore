add_subdirectory(mvcc_transaction)

set(TRANSACTION_SOURCES
        transaction_impl.cc)

if(BUILD_SHARED)
  add_library(${TRANSACTION_SHARED_LIB} SHARED ${TRANSACTION_SOURCES})
  target_link_libraries(${TRANSACTION_SHARED_LIB} ${MVCC_TRANSACTION_SHARED_LIB})
endif()
