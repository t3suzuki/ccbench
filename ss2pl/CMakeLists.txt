cmake_minimum_required(VERSION 3.10)

project(ccbench_ss2pl
        VERSION 0.0.1
        DESCRIPTION "ss2pl of ccbench"
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

file(GLOB SS2PL_SOURCES
        "../common/result.cc"
        "../common/util.cc"
        "ss2pl.cc"
        "result.cc"
        "transaction.cc"
        "util.cc"
        )

add_executable(ss2pl_original ${SS2PL_SOURCES})

set(SS2PL_LIBS
  numa
        Boost::filesystem
        gflags::gflags
        ${PROJECT_SOURCE_DIR}/../third_party/mimalloc/out/release/libmimalloc.a
        ${PROJECT_SOURCE_DIR}/../third_party/masstree/libkohler_masstree_json.a
        Threads::Threads
	)
      
target_link_libraries(ss2pl_original ${SS2PL_LIBS})

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

add_definitions(-DDLR1)

if (DEFINED KEY_SIZE)
    add_definitions(-DKEY_SIZE=${KEY_SIZE})
else ()
    add_definitions(-DKEY_SIZE=8)
endif ()

if (DEFINED KEY_SORT)
    add_definitions(-DKEY_SORT=${KEY_SORT})
else ()
    add_definitions(-DKEY_SORT=0)
endif ()

if (DEFINED MASSTREE_USE)
    add_definitions(-DMASSTREE_USE=${MASSTREE_USE})
else ()
    add_definitions(-DMASSTREE_USE=1)
endif ()

if (DEFINED VAL_SIZE)
    add_definitions(-DVAL_SIZE=${VAL_SIZE})
else ()
    add_definitions(-DVAL_SIZE=4)
endif ()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fcoroutines-ts -stdlib=libc++")


if (DEFINED CORO)
  foreach(N_CORO 1 2 4 8)
    #add_executable(corobase_${N_CORO}coro ${SS2PL_SOURCES})
    #set_target_properties(corobase_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DN_CORO=${N_CORO}")
    #target_link_libraries(corobase_${N_CORO}coro ${SS2PL_LIBS})
    
    add_executable(ptx_${N_CORO}coro ${SS2PL_SOURCES})
    set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DSKIP_INDEX=1 -DN_CORO=${N_CORO}")
    target_link_libraries(ptx_${N_CORO}coro ${SS2PL_LIBS})
  endforeach()
endif()


if (DEFINED DAX)
  add_executable(ss2pl_dax ${SS2PL_SOURCES})
  set_target_properties(ss2pl_dax PROPERTIES COMPILE_FLAGS "-DDAX=1 -DDAX_MIGRATE=1")
  target_link_libraries(ss2pl_dax ${SS2PL_LIBS})

  foreach(N_CORO 1 2 4 8 16 32 64 128)
    add_executable(ptx_dax_${N_CORO}coro ${SS2PL_SOURCES})
    set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DPTX=1 -DSKIP_INDEX=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")
    target_link_libraries(ptx_dax_${N_CORO}coro ${SS2PL_LIBS})
  endforeach()
endif()
