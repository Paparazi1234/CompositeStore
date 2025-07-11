set(STAGING_WRITE_SOURCES
        ordered_map_backed/ordered_map_backed_sw.cc
        staging_write_impl.cc)

add_library(${STAGING_WRITE_SHARED_LIB} SHARED ${STAGING_WRITE_SOURCES})

if(BUILD_TESTS)
  set(STAGING_WRITE_TEST_SOURCES
          ordered_map_backed/ordered_map_backed_sw_test.cc)

  foreach(sourcefile ${STAGING_WRITE_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${GTEST_SHARED_LIB} ${STAGING_WRITE_SHARED_LIB})
  endforeach(sourcefile ${STAGING_WRITE_TEST_SOURCES})
endif()
