set(TXN_LOCK_MANAGER_SOURCES
        empty_txn_lock_manager.cc
        txn_lock_manager_impl.cc)

add_library(${TXN_LOCK_MANAGER_STATIC_LIB} STATIC ${TXN_LOCK_MANAGER_SOURCES})

if(BUILD_SHARED)
  add_library(${TXN_LOCK_MANAGER_SHARED_LIB} SHARED ${TXN_LOCK_MANAGER_SOURCES})
endif()
