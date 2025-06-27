set(PORT_SOURCES
        port_posix.cc)

add_library(${PORT_STATIC_LIB} STATIC ${PORT_SOURCES})

if(BUILD_SHARED)
  add_library(${PORT_SHARED_LIB} SHARED ${PORT_SOURCES})
endif()
