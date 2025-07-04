set(BENCH_SOURCES
        txn_store_bench.cc)

foreach(sourcefile ${BENCH_SOURCES})
  if(BUILD_SHARED)
    set(STORE_LIB ${STORE_SHARED_LIB})
  else()
    set(STORE_LIB ${STORE_STATIC_LIB})
  endif()

  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename} ${sourcefile})
  target_link_libraries(${exename} ${STORE_LIB})
endforeach(sourcefile ${BENCH_SOURCES})
