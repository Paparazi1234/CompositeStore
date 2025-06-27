add_subdirectory(skiplist_backed)

set(STORE_SOURCES
        store_impl.cc
        transaction_store_impl.cc)

add_library(${STORE_STATIC_LIB} STATIC ${STORE_SOURCES})
target_link_libraries(${STORE_STATIC_LIB} ${STORE_STATIC_LIB_LINKING_IMPLS})

if(BUILD_SHARED)
  add_library(${STORE_SHARED_LIB} SHARED ${STORE_SOURCES})
  target_link_libraries(${STORE_SHARED_LIB} ${STORE_SHARED_LIB_LINKING_IMPLS})
endif()
