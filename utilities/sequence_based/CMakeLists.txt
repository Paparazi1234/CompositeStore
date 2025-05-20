set(SEQ_BASED_MULTI_VERSIONS_STATIC_LIB seq_based_multi_versions)
set(SEQ_BASED_MULTI_VERSIONS_SHARED_LIB seq_based_multi_versions-shared)
set(SEQ_BASED_MULTI_VERSIONS_SOURCES
        seq_based_multi_versions.cc
        write_prepared_seq_based_multi_versions.cc
        seq_based_snapshot.cc)

# static lib
add_library(${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB} STATIC ${SEQ_BASED_MULTI_VERSIONS_SOURCES})
# target_link_libraries(${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB} PRIVATE lpthread)

# shared lib
if(BUILD_SHARED)
  add_library(${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB} SHARED ${SEQ_BASED_MULTI_VERSIONS_SOURCES})
  # target_link_libraries(${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB} PRIVATE lpthread)
endif()

if(BUILD_TESTS)
  set(SEQ_BASED_MULTI_VERSIONS_TEST_SOURCES
          seq_based_multi_versions_test.cc)

  if(BUILD_SHARED)
    set(SEQ_BASED_MULTI_VERSIONS_LIB ${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB})
  else()
    set(SEQ_BASED_MULTI_VERSIONS_LIB ${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB})
  endif()

  foreach(sourcefile ${SEQ_BASED_MULTI_VERSIONS_TEST_SOURCES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} gtest ${SEQ_BASED_MULTI_VERSIONS_LIB})
  endforeach(sourcefile ${SEQ_BASED_MULTI_VERSIONS_TEST_SOURCES})
endif()
