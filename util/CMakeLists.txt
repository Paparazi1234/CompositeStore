set(UTIL_SOURCES
        random.cc
        system_clock.cc)

add_library(${UTIL_SHARED_LIB} SHARED ${UTIL_SOURCES})
target_link_libraries(${UTIL_SHARED_LIB} ${PORT_SHARED_LIB})
