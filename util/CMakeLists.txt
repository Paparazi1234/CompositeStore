set(UTIL_SOURCES
        bench_util.cc
        memory_allocator.cc
        random.cc
        status.cc
        system_clock.cc)

add_library(${UTIL_SHARED_LIB} SHARED ${UTIL_SOURCES})
target_link_libraries(${UTIL_SHARED_LIB} ${PORT_SHARED_LIB})
