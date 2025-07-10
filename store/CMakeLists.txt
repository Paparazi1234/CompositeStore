set(STORE_SOURCES
        mvcc_store/mvcc_store.cc
        store_impl.cc)

add_library(${STORE_SHARED_LIB} SHARED ${STORE_SOURCES})
target_link_libraries(${STORE_SHARED_LIB} ${TXN_STORE_SHARED_LIB})
