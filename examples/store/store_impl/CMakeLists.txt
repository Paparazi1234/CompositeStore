add_subdirectory(skiplist_backed)

set(STORE_STATIC_LIB store)
set(STORE_SHARED_LIB store-shared)
set(STORE_SOURCES
        store_impl.cc
        transaction_store_impl.cc)

add_library(${STORE_STATIC_LIB} STATIC ${STORE_SOURCES})
target_link_libraries(${STORE_STATIC_LIB} PRIVATE ${SKIPLIST_BACKED_STORE_STATIC_LIB})

if(BUILD_SHARED)
  add_library(${STORE_SHARED_LIB} SHARED ${STORE_SOURCES})
  target_link_libraries(${STORE_SHARED_LIB} PRIVATE ${SKIPLIST_BACKED_STORE_SHARED_LIB})
endif()

