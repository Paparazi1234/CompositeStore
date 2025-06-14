set(EXAMPLES_SOURCES
        my_test.cc
        simple_example.cc
        transaction_example.cc)

foreach(sourcefile ${EXAMPLES_SOURCES})
  if(BUILD_SHARED)
    set(STORE_LIB ${STORE_SHARED_LIB})
  else()
    set(STORE_LIB ${STORE_STATIC_LIB})
  endif()

  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename} ${sourcefile})
  target_link_libraries(${exename} ${STORE_LIB})
endforeach(sourcefile ${EXAMPLES_SOURCES})
