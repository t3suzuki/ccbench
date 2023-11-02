cmake_minimum_required(VERSION 3.10)

project(ccbench_tpcc_silo
        VERSION 0.0.1
        DESCRIPTION "tpcc_silo of ccbench"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/../cmake")

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

find_package(Doxygen)
find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(Boost
        COMPONENTS filesystem)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)

#enable_testing()
#add_subdirectory(test)

file(GLOB SILO_SOURCES
        "../common/util.cc"
        "epoch.cpp"
        "garbage_collection.cpp"
        "interface/interface_delete.cpp"
        "interface/interface_helper.cpp"
        "interface/interface_scan.cpp"
        "interface/interface_search.cpp"
        "interface/interface_termination.cpp"
        "interface/interface_update_insert.cpp"
        "log.cpp"
        "scheme.cpp"
        "session_info.cpp"
        "session_info_table.cpp"
        "tid.cpp"
        "index/masstree_beta/masstree_beta_wrapper.cpp"
	"silo.cpp"
	"result.cpp"
	"util.cpp"
	"tpcc/tpcc_query.cpp"
	"tpcc/neworder.cpp"
	"tpcc/payment.cpp"
	"../common/util.cc"
	"../common/result.cc"
        )

#set_compile_options(silo.exe)

set(SILO_INCS PRIVATE ${PROJECT_SOURCE_DIR}
  PRIVATE ${PROJECT_SOURCE_DIR}/../third_party
  PRIVATE ${PROJECT_SOURCE_DIR}/include
  PRIVATE ${PROJECT_SOURCE_DIR}/index/masstree_beta/include
  PRIVATE ${PROJECT_SOURCE_DIR}/interface
  PRIVATE ${PROJECT_SOURCE_DIR}/tpcc
  )

set(SILO_LIBS numa
        Boost::filesystem
        gflags::gflags
        ${PROJECT_SOURCE_DIR}/../third_party/mimalloc/out/release/libmimalloc.a
        ${PROJECT_SOURCE_DIR}/../third_party/masstree/libkohler_masstree_json.a
        ${PROJECT_SOURCE_DIR}/../third_party/memkind/.libs/libmemkind.a
	dl
	daxctl
        Threads::Threads
	)

      
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_definitions(-DCCBENCH_LINUX)
endif ()

cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)
add_definitions(-DKVS_EPOCH_TIME=40)
add_definitions(-DKVS_NUMBER_OF_LOGICAL_CORES=65536)
add_definitions(-DKVS_MAX_PARALLEL_THREADS=16384)
add_definitions(-DKVS_MAX_KEY_LENGTH=1000)
add_definitions(-DKVS_LOG_GC_THRESHOLD=1)
add_definitions(-DPROJECT_ROOT=${PROJECT_SOURCE_DIR})

add_definitions(-DMASSTREE_PREFETCH_COUNT=4)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fcoroutines-ts -stdlib=libc++")
#set_target_properties(silo.exe PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DTR_US=1 -DN_CORO=64")

add_executable(silo_original ${SILO_SOURCES})
target_include_directories(silo_original ${SILO_INCS})
target_link_libraries(silo_original ${SILO_LIBS})


if (DEFINED CORO)
  foreach(N_CORO 4)
    add_executable(corobase_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(corobase_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DN_CORO=${N_CORO}")
    target_include_directories(corobase_${N_CORO}coro ${SILO_INCS})
    target_link_libraries(corobase_${N_CORO}coro ${SILO_LIBS})
    
    add_executable(ptx_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DN_CORO=${N_CORO}")
    target_include_directories(ptx_${N_CORO}coro ${SILO_INCS})
    target_link_libraries(ptx_${N_CORO}coro ${SILO_LIBS})
  endforeach()
endif()

if (DEFINED DAX)
  add_executable(silo_dax ${SILO_SOURCES})
  #set_target_properties(silo_dax PROPERTIES COMPILE_FLAGS "-DDAX=1 -DDAX_MIGRATE=1")
  set_target_properties(silo_dax PROPERTIES COMPILE_FLAGS "-DDAX=1 -DUSE_MEMKIND=1")
  target_include_directories(silo_dax ${SILO_INCS})
  target_link_libraries(silo_dax ${SILO_LIBS})

  foreach(N_CORO 4 16)
    # add_executable(corobase_dax_${N_CORO}coro ${SILO_SOURCES})
    # set_target_properties(corobase_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DUSE_MEMKIND=1 -DCOROBASE=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")
    # target_include_directories(corobase_dax_${N_CORO}coro ${SILO_INCS})
    # target_link_libraries(corobase_dax_${N_CORO}coro ${SILO_LIBS})

    add_executable(ptx_dax_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DUSE_MEMKIND=1 -DPTX=1 -DN_CORO=${N_CORO}")
    target_include_directories(ptx_dax_${N_CORO}coro ${SILO_INCS})
    target_link_libraries(ptx_dax_${N_CORO}coro ${SILO_LIBS})
  endforeach()
endif()
  

if (DEFINED EMU)
  if (DEFINED MY_TIME_CORE)
    set(MY_TIME "-DMY_TIME_CORE=${MY_TIME_CORE}")
  else()
    message( FATAL_ERROR "Specify -DMY_TIME_CORE=N to use emulated slow memory." )
  endif()

  foreach(TR_US 4)
    foreach(N_CORO 1 4 16 64 256)
      # add_executable(corobase_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
      # set_target_properties(corobase_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
      # target_include_directories(corobase_${TR_US}us_${N_CORO}coro ${SILO_INCS})
      # target_link_libraries(corobase_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
      
      # add_executable(ptx_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
      # set_target_properties(ptx_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
      # target_include_directories(ptx_${TR_US}us_${N_CORO}coro ${SILO_INCS})
      # target_link_libraries(ptx_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
      
      add_executable(ptx2_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
      set_target_properties(ptx2_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DMYRW=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
      target_include_directories(ptx2_${TR_US}us_${N_CORO}coro ${SILO_INCS})
      target_link_libraries(ptx2_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
    endforeach()
  endforeach()
endif()

