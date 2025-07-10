set(BENCH_SOURCES
        txn_store_bench.cc)

foreach(sourcefile ${BENCH_SOURCES})
  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename} ${sourcefile})
  target_link_libraries(${exename} ${TXN_STORE_SHARED_LIB})
endforeach(sourcefile ${BENCH_SOURCES})
