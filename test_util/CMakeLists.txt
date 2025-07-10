set(TEST_UTIL_SOURCES
        test_util.cc)

add_library(${TEST_UTIL_SHARED_LIB} SHARED ${TEST_UTIL_SOURCES})
target_link_libraries(${TEST_UTIL_SHARED_LIB} ${UTIL_SHARED_LIB})
