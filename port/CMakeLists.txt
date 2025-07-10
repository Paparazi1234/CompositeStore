set(PORT_SOURCES
        port_posix.cc)

add_library(${PORT_SHARED_LIB} SHARED ${PORT_SOURCES})
