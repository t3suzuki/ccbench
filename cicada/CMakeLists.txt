cmake_minimum_required(VERSION 3.10)

project(ccbench_cicada
        VERSION 0.0.1
        DESCRIPTION "cicada of ccbench"
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

file(GLOB CICADA_SOURCES
        "../common/result.cc"
        "../common/util.cc"
        "cicada.cc"
        "result.cc"
        "transaction.cc"
        "util.cc"
        )

add_executable(cicada_original ${CICADA_SOURCES})
set_target_properties(cicada_original PROPERTIES COMPILE_FLAGS "-DMASSTREE_PREFETCH_COUNT=4")
#set_target_properties(cicada_original PROPERTIES COMPILE_FLAGS "-DDEFAULT_NEW=1")

set(CICADA_LIBS
  numa
        Boost::filesystem
        gflags::gflags
        ${PROJECT_SOURCE_DIR}/../third_party/mimalloc/out/release/libmimalloc.a
        ${PROJECT_SOURCE_DIR}/../third_party/masstree/libkohler_masstree_json.a
        ${PROJECT_SOURCE_DIR}/../third_party/memkind/.libs/libmemkind.a
	dl
	daxctl
        Threads::Threads
        )

target_link_libraries(cicada_original ${CICADA_LIBS})
      
target_include_directories(cicada_original
        PRIVATE ${PROJECT_SOURCE_DIR}/../include
        PRIVATE ${PROJECT_SOURCE_DIR}/../third_party/spdlog/include
        )

if (DEFINED ADD_ANALYSIS)
    add_definitions(-DADD_ANALYSIS=${ADD_ANALYSIS})
else ()
    add_definitions(-DADD_ANALYSIS=0)
endif ()

if (DEFINED BACK_OFF)
    add_definitions(-DBACK_OFF=${BACK_OFF})
else ()
    add_definitions(-DBACK_OFF=0)
endif ()

if (DEFINED INLINE_VERSION_PROMOTION)
    add_definitions(-DINLINE_VERSION_PROMOTION=${INLINE_VERSION_PROMOTION})
else ()
    add_definitions(-DINLINE_VERSION_PROMOTION=1)
endif ()

if (DEFINED INLINE_VERSION_OPT)
    add_definitions(-DINLINE_VERSION_OPT=${INLINE_VERSION_OPT})
else ()
    add_definitions(-DINLINE_VERSION_OPT=1)
endif ()

if (DEFINED KEY_SIZE)
    add_definitions(-DKEY_SIZE=${KEY_SIZE})
else ()
    add_definitions(-DKEY_SIZE=8)
endif ()

if (DEFINED MASSTREE_USE)
    add_definitions(-DMASSTREE_USE=${MASSTREE_USE})
else ()
    add_definitions(-DMASSTREE_USE=1)
endif ()

if (DEFINED PARTITION_TABLE)
    add_definitions(-DPARTITION_TABLE=${PARTITION_TABLE})
else ()
    add_definitions(-DPARTITION_TABLE=0)
endif ()

if (DEFINED REUSE_VERSION)
    add_definitions(-DREUSE_VERSION=${REUSE_VERSION})
else ()
    add_definitions(-DREUSE_VERSION=1)
endif ()

if (DEFINED SINGLE_EXEC)
    add_definitions(-DSINGLE_EXEC=${CSINGLE_EXEC})
else ()
    add_definitions(-DSINGLE_EXEC=0)
endif ()

if (DEFINED VAL_SIZE)
    add_definitions(-DVAL_SIZE=${VAL_SIZE})
else ()
    add_definitions(-DVAL_SIZE=4)
endif ()

if (DEFINED WRITE_LATEST_ONLY)
    add_definitions(-DWRITE_LATEST_ONLY=${WRITE_LATEST_ONLY})
else ()
    add_definitions(-DWRITE_LATEST_ONLY=0)
endif ()

if (DEFINED WORKER1_INSERT_DELAY_RPHASE)
    add_definitions(-DWORKER1_INSERT_DELAY_RPHASE=${WORKER1_INSERT_DELAY_RPHASE})
else ()
    add_definitions(-DWORKER1_INSERT_DELAY_RPHASE=0)
endif ()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fcoroutines-ts -stdlib=libc++")


if (DEFINED CORO)
  foreach(N_CORO 1 2 4 8 16 32 64)
    #add_executable(corobase_${N_CORO}coro ${CICADA_SOURCES})
    #set_target_properties(corobase_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DN_CORO=${N_CORO}")
    #target_link_libraries(corobase_${N_CORO}coro ${CICADA_LIBS})
    
    add_executable(ptx_${N_CORO}coro ${CICADA_SOURCES})

    target_include_directories(ptx_${N_CORO}coro
      PRIVATE ${PROJECT_SOURCE_DIR}/../include
      PRIVATE ${PROJECT_SOURCE_DIR}/../third_party/spdlog/include
      )
    set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DMASSTREE_PREFETCH_COUNT=4 -DSKIP_INDEX=1 -DN_CORO=${N_CORO}")
    #set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDEFAULT_NEW=1 -DPTX=1 -DSKIP_INDEX=1 -DN_CORO=${N_CORO}")
    #set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DN_CORO=${N_CORO}")
    target_link_libraries(ptx_${N_CORO}coro ${CICADA_LIBS})
  endforeach()
endif()

if (DEFINED DAX)
  add_executable(cicada_dax ${CICADA_SOURCES})
  target_include_directories(cicada_dax
    PRIVATE ${PROJECT_SOURCE_DIR}/../include
    PRIVATE ${PROJECT_SOURCE_DIR}/../third_party/spdlog/include
    )
  set_target_properties(cicada_dax PROPERTIES COMPILE_FLAGS "-DDAX=1 -DDAX_MIGRATE=1 -DUSE_MEMKIND=1 -DMASSTREE_PREFETCH_COUNT=4")
  target_link_libraries(cicada_dax ${CICADA_LIBS})

  foreach(N_CORO 1 2 4 8 16 32 64 128)
    add_executable(ptx_dax_${N_CORO}coro ${CICADA_SOURCES})
    target_include_directories(ptx_dax_${N_CORO}coro
      PRIVATE ${PROJECT_SOURCE_DIR}/../include
      PRIVATE ${PROJECT_SOURCE_DIR}/../third_party/spdlog/include
      )
    #set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDEFAULT_NEW=1 -DDAX=1 -DPTX=1 -DSKIP_INDEX=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")
    set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DUSE_MEMKIND=1 -DMASSTREE_PREFETCH_COUNT=3 -DPTX=1 -DSKIP_INDEX=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")
    target_link_libraries(ptx_dax_${N_CORO}coro ${CICADA_LIBS})
  endforeach()
endif()
