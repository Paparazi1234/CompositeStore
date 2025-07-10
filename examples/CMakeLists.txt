set(EXAMPLES_SOURCES
        my_test.cc
        simple_example.cc
        transaction_example.cc)

foreach(sourcefile ${EXAMPLES_SOURCES})
  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename} ${sourcefile})
  target_link_libraries(${exename} ${STORE_SHARED_LIB} ${TRANSACTION_STORE_SHARED_LIB})
endforeach(sourcefile ${EXAMPLES_SOURCES})
