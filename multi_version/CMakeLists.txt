add_subdirectory(sequence_based)

set(MULTI_VERSIONS_SOURCES
        multi_versions_impl.cc)

add_library(${MULTI_VERSIONS_SHARED_LIB} SHARED ${MULTI_VERSIONS_SOURCES})
target_link_libraries(${MULTI_VERSIONS_SHARED_LIB} ${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB})
