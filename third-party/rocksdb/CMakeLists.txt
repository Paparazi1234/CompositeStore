set(ROCKSDB_SOURCES
        coding.cc
        slice.cc)

add_library(${ROCKSDB_STATIC_LIB} STATIC ${ROCKSDB_SOURCES})
  target_link_libraries(${ROCKSDB_STATIC_LIB} ${UTIL_STATIC_LIB})

if(BUILD_SHARED)
  add_library(${ROCKSDB_SHARED_LIB} SHARED ${ROCKSDB_SOURCES})
  target_link_libraries(${ROCKSDB_SHARED_LIB} ${UTIL_SHARED_LIB})
endif()
