set(ROCKSDB_SOURCES
        coding.cc
        slice.cc)

add_library(${ROCKSDB_SHARED_LIB} SHARED ${ROCKSDB_SOURCES})
target_link_libraries(${ROCKSDB_SHARED_LIB} ${UTILS_SHARED_LIB})
