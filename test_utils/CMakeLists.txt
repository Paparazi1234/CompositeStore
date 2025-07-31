set(TEST_UTIL_SOURCES
        test_utils.cc
        txn_test_utils.cc)

add_library(${TEST_UTILS_SHARED_LIB} SHARED ${TEST_UTIL_SOURCES})
target_link_libraries(${TEST_UTILS_SHARED_LIB} ${UTILS_SHARED_LIB} ${GTEST_SHARED_LIB})
