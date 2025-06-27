set(UTIL_SOURCES
        mutex_lock.cc)

add_library(${UTIL_STATIC_LIB} STATIC ${UTIL_SOURCES})
target_link_libraries(${UTIL_STATIC_LIB} ${PORT_STATIC_LIB})

if(BUILD_SHARED)
  add_library(${UTIL_SHARED_LIB} SHARED ${UTIL_SOURCES})
  target_link_libraries(${UTIL_SHARED_LIB} ${PORT_SHARED_LIB})
endif()
