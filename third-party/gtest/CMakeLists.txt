set(GTEST_SOURCES gtest-all.cc)

add_library(${GTEST_STATIC_LIB} STATIC ${GTEST_SOURCES})
target_link_libraries(${GTEST_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT})

if(BUILD_SHARED)
  add_library(${GTEST_SHARED_LIB} SHARED ${GTEST_SOURCES})
  target_link_libraries(${GTEST_SHARED_LIB} ${CMAKE_THREAD_LIBS_INIT})
endif()
