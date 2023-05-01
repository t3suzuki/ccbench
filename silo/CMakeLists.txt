cmake_minimum_required(VERSION 3.10)

project(ccbench_silo
        VERSION 0.0.1
        DESCRIPTION "silo of ccbench"
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

file(GLOB SILO_SOURCES
        "../common/result.cc"
        "../common/util.cc"
        "result.cc"
        "silo.cc"
        "transaction.cc"
        "util.cc"
        )

file(GLOB REPLAY_SOURCES
        "replayTest.cc"
        )

set(SILO_LIBS
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

if (DEFINED NO_WAIT_LOCKING_IN_VALIDATION)
    add_definitions(-DNO_WAIT_LOCKING_IN_VALIDATION=${NO_WAIT_LOCKING_IN_VALIDATION})
else ()
    add_definitions(-DNO_WAIT_LOCKING_IN_VALIDATION=1)
endif ()

if (DEFINED NO_WAIT_OF_TICTOC)
    add_definitions(-DNO_WAIT_OF_TICTOC=${NO_WAIT_OF_TICTOC})
else ()
    add_definitions(-DNO_WAIT_OF_TICTOC=0)
endif ()

if (DEFINED PARTITION_TABLE)
    add_definitions(-DPARTITION_TABLE=${PARTITION_TABLE})
else ()
    add_definitions(-DPARTITION_TABLE=0)
endif ()

if (DEFINED PROCEDURE_SORT)
    add_definitions(-DPROCEDURE_SORT=${PROCEDURE_SORT})
else ()
    add_definitions(-DPROCEDURE_SORT=0)
endif ()

if (DEFINED SLEEP_READ_PHASE)
    add_definitions(-DSLEEP_READ_PHASE=${SLEEP_READ_PHASE})
else ()
    add_definitions(-DSLEEP_READ_PHASE=0)
endif ()

if (DEFINED VAL_SIZE)
    add_definitions(-DVAL_SIZE=${VAL_SIZE})
else ()
    add_definitions(-DVAL_SIZE=4)
endif ()

if (DEFINED WAL)
    add_definitions(-DWAL=${WAL})
else ()
    add_definitions(-DWAL=0)
endif ()

add_definitions(-DTIDWORD_IN_TUPLE=1)
add_definitions(-DALIGN64_TIDWORD=1)
#add_definitions(-DSHUFFLED_ZIPF=1)

set(MASSTREE_DRAM_DEFAULT 4)
set(MASSTREE_DAX_DEFAULT 3)

add_executable(silo_original ${SILO_SOURCES})
set_target_properties(silo_original PROPERTIES COMPILE_FLAGS "-DMASSTREE_PREFETCH_COUNT=${MASSTREE_DRAM_DEFAULT}")
target_link_libraries(silo_original ${SILO_LIBS})

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fcoroutines-ts -stdlib=libc++")
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fno-omit-frame-pointer -fcoroutines-ts -stdlib=libc++")


if (DEFINED CORO)
  foreach(N_CORO 1 2 4 8 16 32 64 128 256 512)
    add_executable(corobase_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(corobase_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DMASSTREE_PREFETCH_COUNT=${MASSTREE_DRAM_DEFAULT} -DN_CORO=${N_CORO}")
    target_link_libraries(corobase_${N_CORO}coro ${SILO_LIBS})
    
    add_executable(ptx_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DMASSTREE_PREFETCH_COUNT=${MASSTREE_DRAM_DEFAULT} -DSKIP_INDEX=1 -DN_CORO=${N_CORO}")
    #set_target_properties(ptx_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DMASSTREE_PREFETCH_COUNT=${MASSTREE_DRAM_DEFAULT} -DSKIP_INDEX=1 -DN_CORO=${N_CORO} -DMEASURE_TIME")
    target_link_libraries(ptx_${N_CORO}coro ${SILO_LIBS})
  endforeach()
endif()


if (DEFINED DAX)
  add_executable(silo_dax ${SILO_SOURCES})
  set_target_properties(silo_dax PROPERTIES COMPILE_FLAGS "-DDAX=1 -DMASSTREE_PREFETCH_COUNT=4 -DUSE_MEMKIND=1 -DDAX_MIGRATE=1")
  #set_target_properties(silo_dax PROPERTIES COMPILE_FLAGS "-DDAX=1 -DMASSTREE_PREFETCH_COUNT=4 -DUSE_MEMKIND=1")
  target_link_libraries(silo_dax ${SILO_LIBS})

  foreach(N_CORO 1 2 4 8 16 32 64 128 256 512 1024 2048 4096)
    add_executable(corobase_dax_${N_CORO}coro ${SILO_SOURCES})
    set_target_properties(corobase_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DMASSTREE_PREFETCH_COUNT=${MASSTREE_DAX_DEFAULT} -DUSE_MEMKIND=1 -DCOROBASE=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")
    target_link_libraries(corobase_dax_${N_CORO}coro ${SILO_LIBS})

    add_executable(ptx_dax_${N_CORO}coro ${SILO_SOURCES})
    #set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DMASSTREE_PREFETCH_COUNT=${MASSTREE_DAX_DEFAULT} -DUSE_MEMKIND=1 -DPTX=1 -DSKIP_INDEX=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO} -DMEASURE_TIME")
    set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DMASSTREE_PREFETCH_COUNT=${MASSTREE_DAX_DEFAULT} -DUSE_MEMKIND=1 -DPTX=1 -DSKIP_INDEX=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")
    #set_target_properties(ptx_dax_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DDAX=1 -DPTX=1 -DDAX_MIGRATE=1 -DN_CORO=${N_CORO}")    
    target_link_libraries(ptx_dax_${N_CORO}coro ${SILO_LIBS})
  endforeach()
endif()

if (DEFINED EMU)
  if (DEFINED MY_TIME_CORE)
    set(MY_TIME "-DMY_TIME_CORE=${MY_TIME_CORE}")
  else()
    message( FATAL_ERROR "Specify -DMY_TIME_CORE=N to use emulated slow memory." )
  endif()

  foreach(TR_US 0 1 2 4 8 16 32 64)
    foreach(N_CORO 1 2 4 8 16 32 64 128 256 512 1024 2048 4096)
      add_executable(corobase_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
      set_target_properties(corobase_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DCOROBASE=1 -DMASSTREE_PREFETCH_COUNT=4 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
      target_link_libraries(corobase_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
      
      #add_executable(ptxold_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
      #set_target_properties(ptxold_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
      #target_link_libraries(ptxold_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
      
      add_executable(ptx_${TR_US}us_${N_CORO}coro ${SILO_SOURCES})
      set_target_properties(ptx_${TR_US}us_${N_CORO}coro PROPERTIES COMPILE_FLAGS "-DPTX=1 -DMASSTREE_PREFETCH_COUNT=4 -DSKIP_INDEX=1 -DTR_US=${TR_US} -DN_CORO=${N_CORO} ${MY_TIME}")
      target_link_libraries(ptx_${TR_US}us_${N_CORO}coro ${SILO_LIBS})
    endforeach()
  endforeach()
endif()
    
