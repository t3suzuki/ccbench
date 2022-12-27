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
        Threads::Threads
	)

      
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_definitions(-DCCBENCH_LINUX)
endif ()

cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)
add_definitions(-DKVS_EPOCH_TIME=40)
add_definitions(-DKVS_NUMBER_OF_LOGICAL_CORES=16384)
add_definitions(-DKVS_MAX_PARALLEL_THREADS=5000)
add_definitions(-DKVS_MAX_KEY_LENGTH=1000)
add_definitions(-DKVS_LOG_GC_THRESHOLD=1)
add_definitions(-DPROJECT_ROOT=${PROJECT_SOURCE_DIR})

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fcoroutines-ts -stdlib=libc++")
#set_target_properties(silo.exe PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DTR_US=1 -DN_CORO=64")

add_executable(silo_original ${SILO_SOURCES})
target_include_directories(silo_original ${SILO_INCS})
target_link_libraries(silo_original ${SILO_LIBS})

# set(MY_TIME "")

foreach(TR_US 0 1 4 16 64)
  foreach(N_CORO 1 2 4 8 16 32 64 128 256 512)  
    add_executable(corobase_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(corobase_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
    target_include_directories(corobase_${TR_US}us_${N_CORO}coro ${SILO_INCS})
    target_link_libraries(corobase_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
    add_executable(pilo_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(pilo_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPILO=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
    target_include_directories(pilo_${TR_US}us_${N_CORO}coro ${SILO_INCS})
    target_link_libraries(pilo_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
  endforeach()
endforeach()

