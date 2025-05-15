add_subdirectory(store/store_impl/skiplist_backed)

set(EXAMPLES_SOURCES
        simpel_examples.cc)

foreach(sourcefile ${EXAMPLES_SOURCES})
  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename} ${sourcefile})
  target_link_libraries(${exename} ${SKIPLIST_BACKED_STORE_LIB})
endforeach(sourcefile ${EXAMPLES_SOURCES})
