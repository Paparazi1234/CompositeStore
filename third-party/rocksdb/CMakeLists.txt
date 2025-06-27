set(ROCKSDB_SOURCES
        coding.cc
        random.cc
        slice.cc)

add_library(${ROCKSDB_STATIC_LIB} STATIC ${ROCKSDB_SOURCES})

if(BUILD_SHARED)
  add_library(${ROCKSDB_SHARED_LIB} SHARED ${ROCKSDB_SOURCES})
endif()
