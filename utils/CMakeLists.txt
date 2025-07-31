set(UTIL_SOURCES
        bench_utils.cc
        memory_allocator.cc
        random.cc
        status.cc
        system_clock.cc)

add_library(${UTILS_SHARED_LIB} SHARED ${UTIL_SOURCES})
target_link_libraries(${UTILS_SHARED_LIB} ${PORT_SHARED_LIB})
