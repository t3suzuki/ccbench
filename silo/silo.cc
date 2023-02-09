#include <ctype.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <algorithm>
#include <cctype>

#include "boost/filesystem.hpp"

#define GLOBAL_VALUE_DEFINE

#include "include/atomic_tool.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/fileio.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"

#include "../include/coro.h"

using namespace std;

PROMISE(void) corobase_work(size_t thid, int i_coro, FastZipf &zipf,
			    Xoroshiro128Plus &rnd, Result &myres, const bool &quit,
			    uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop,
			    int &n_done, bool &done_coro)
{
  while (!loadAcquire(quit)) {
    TxnExecutor trans(thid, (Result *) &myres);
#if PARTITION_TABLE
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true,
                  thid, myres);
#else
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false,
                  thid, myres);
#endif

#if PROCEDURE_SORT
    sort(trans.pro_set_.begin(), trans.pro_set_.end());
#endif

RETRY:
    if (thid == 0) {
      leaderWork(epoch_timer_start, epoch_timer_stop);
#if BACK_OFF
      leaderBackoffWork(backoff, SiloResult);
#endif
      // printf("Thread #%d: on CPU %d\n", thid, sched_getcpu());
    }
    
    if (loadAcquire(quit)) break;
    
    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
      if ((*itr).ope_ == Ope::READ) {
        AWAIT trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
        AWAIT trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        AWAIT trans.read((*itr).key_);
        AWAIT trans.write((*itr).key_);
      } else {
        ERR;
      }
    }

    if (trans.validationPhase()) {
      trans.writePhase();
      /**
       * local_commit_counts is used at ../include/backoff.hh to calcurate about
       * backoff.
       */
      storeRelease(myres.local_commit_counts_,
                   loadAcquire(myres.local_commit_counts_) + 1);
    } else {
      trans.abort();
      ++myres.local_abort_counts_;
      goto RETRY;
    }
  }
  n_done++;
  done_coro = true;
  RETURN;
}


PILO_PROMISE(void) pilo_work(size_t thid, int i_coro, FastZipf &zipf,
			     Xoroshiro128Plus &rnd, Result &myres, const bool &quit,
			     uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop,
			     int &n_done, bool &done_coro)
{
  while (!loadAcquire(quit)) {
    TxnExecutor trans(thid, (Result *) &myres);
#if PARTITION_TABLE
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true,
                  thid, myres);
#else
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false,
                  thid, myres);
#endif

#if PROCEDURE_SORT
    sort(trans.pro_set_.begin(), trans.pro_set_.end());
#endif

RETRY:
    if (thid == 0) {
      leaderWork(epoch_timer_start, epoch_timer_stop);
#if BACK_OFF
      leaderBackoffWork(backoff, SiloResult);
#endif
      // printf("Thread #%d: on CPU %d\n", thid, sched_getcpu());
    }
    
    if (loadAcquire(quit)) break;
    
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
#if MYRW
      //itr->tuple = PILO_AWAIT trans.prefetch_tree((*itr).key_);
      {
	Tuple *tuple;
	PILO_AWAIT MT.get_value_pilo_flat((*itr).key_, tuple);
	itr->tuple = tuple;
      }
#else
      PILO_AWAIT trans.prefetch_tree((*itr).key_);
#endif
    }
    
    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
#if MYRW
      if ((*itr).ope_ == Ope::READ) {
        PILO_AWAIT trans.myread((*itr));
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.mywrite((*itr));
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        PILO_AWAIT trans.myread((*itr));
        trans.mywrite((*itr));
      } else {
        ERR;
      }
#else
      if ((*itr).ope_ == Ope::READ) {
        trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.read((*itr).key_);
        trans.write((*itr).key_);
      } else {
        ERR;
      }
#endif
    }

    if (trans.validationPhase()) {
      trans.writePhase();
      /**
       * local_commit_counts is used at ../include/backoff.hh to calcurate about
       * backoff.
       */
      storeRelease(myres.local_commit_counts_,
                   loadAcquire(myres.local_commit_counts_) + 1);
    } else {
      trans.abort();
      ++myres.local_abort_counts_;
      goto RETRY;
    }
  }
  n_done++;
  done_coro = true;
  PILO_RETURN;
}


void original_work(size_t thid, TxnExecutor &trans, FastZipf &zipf,
		   Xoroshiro128Plus &rnd, Result &myres, const bool &quit,
		   uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop)
{
  while (!loadAcquire(quit)) {
#if PARTITION_TABLE
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true,
                  thid, myres);
#else
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false,
                  thid, myres);
#endif

#if PROCEDURE_SORT
    sort(trans.pro_set_.begin(), trans.pro_set_.end());
#endif

RETRY:
    if (thid == 0) {
      leaderWork(epoch_timer_start, epoch_timer_stop);
#if BACK_OFF
      leaderBackoffWork(backoff, SiloResult);
#endif
      // printf("Thread #%d: on CPU %d\n", thid, sched_getcpu());
    }
    
    if (loadAcquire(quit)) break;

    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
      if ((*itr).ope_ == Ope::READ) {
        trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.read((*itr).key_);
        trans.write((*itr).key_);
      } else {
        ERR;
      }
    }

    if (trans.validationPhase()) {
      trans.writePhase();
      /**
       * local_commit_counts is used at ../include/backoff.hh to calcurate about
       * backoff.
       */
      storeRelease(myres.local_commit_counts_,
                   loadAcquire(myres.local_commit_counts_) + 1);
    } else {
      trans.abort();
      ++myres.local_abort_counts_;
      goto RETRY;
    }
  }
}


#define _GNU_SOURCE
#include <sched.h>

void setThreadAffinity(int core)
{
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(core, &cpu_set);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set);
}



void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Result &myres = std::ref(SiloResult[thid]);
  Xoroshiro128Plus rnd;
  rnd.init();
  TxnExecutor trans(thid, (Result *) &myres);
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);
  uint64_t epoch_timer_start, epoch_timer_stop;
#if BACK_OFF
  Backoff backoff(FLAGS_clocks_per_us);
#endif

#if WAL
  /*
  const boost::filesystem::path log_dir_path("/tmp/ccbench");
  if (boost::filesystem::exists(log_dir_path)) {
  } else {
    boost::system::error_code error;
    const bool result = boost::filesystem::create_directory(log_dir_path,
  error); if (!result || error) { ERR;
    }
  }
  std::string logpath("/tmp/ccbench");
  */
  std::string logpath;
  genLogFile(logpath, thid);
  trans.logfile_.open(logpath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  trans.logfile_.ftruncate(10 ^ 9);
#endif

  //#ifdef Linux
#if 1
  setThreadAffinity(thid);
  // printf("Thread #%d: on CPU %d\n", res.thid_, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif

#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  if (thid == 0) epoch_timer_start = rdtscp();

#if defined(COROBASE) || defined(PILO)
  int n_done = 0;
  bool done[N_CORO];
#if COROBASE
  PROMISE(void) coro[N_CORO];
#else
  PILO_PROMISE(void) coro[N_CORO];
#endif
  for (int i_coro=0; i_coro<N_CORO; i_coro++) {
    done[i_coro] = false;
#if COROBASE
    coro[i_coro] = corobase_work(thid, i_coro, zipf, rnd, myres,
				 quit, epoch_timer_start, epoch_timer_stop, n_done, done[i_coro]);
#else
    coro[i_coro] = pilo_work(thid, i_coro, zipf, rnd, myres,
			     quit, epoch_timer_start, epoch_timer_stop, n_done, done[i_coro]);
#endif
    coro[i_coro].start();
  }

  do {
    for (int i_coro=0; i_coro<N_CORO; i_coro++) {
      if (!done[i_coro])
        coro[i_coro].resume();
    }
  } while (n_done != N_CORO);
#else
  original_work(thid, trans, zipf, rnd, myres,
		quit, epoch_timer_start, epoch_timer_stop);
#endif
  return;
}

thread_local tcalloc coroutine_allocator;

#if MY_TIME_CORE
unsigned long long current_my_time;

void my_time_func(const bool &all_done)
{
  setThreadAffinity(MY_TIME_CORE);

  unsigned long long local_time = __rdtsc();
  for (;;) {
    while (local_time - current_my_time < TSC_US/4) {
      local_time = __rdtsc();
      _mm_pause();
    }
    current_my_time = local_time;
    if (all_done)
      break;
  }
}
#endif

void
run_perf(const bool &start, const bool &quit)
{
  while (!loadAcquire(start)) _mm_pause();

#if 1
  int pid = getpid();
  int cpid = fork();
  if(cpid == 0) {
    char buf[50];
    printf("perf...\n");
    //sprintf(buf, "perf stat -ddd -p %d   > stat%d.log 2>&1", pid, pid);
    //sprintf(buf, "perf record -C 0-15 -g");
    sprintf(buf, "perf record -C 0-15 -g");
    execl("/bin/sh", "sh", "-c", buf, NULL);
  } else {
    setpgid(cpid, 0);
    while (!loadAcquire(quit)) _mm_pause();
    kill(-cpid, SIGINT);
  }
#endif
}

bool dax_enabled = false;
static size_t dax_used = 0;

#if DAX
static char *dax_base;
void
dax_init()
{
  char path[] = "/dev/dax1.0";
  int fd = open(path, O_RDWR);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
#define MMAP_SIZE ((size_t)1024*1024*1024*128)
  dax_base = (char *)mmap(NULL, MMAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, fd, 0);
  dax_enabled  = true;
}

#endif

void *
dax_malloc(size_t sz)
{
#if DAX
  size_t offset = __sync_fetch_and_add(&dax_used, sz);
  //printf("%s %d %ld\n", __func__, __LINE__, sz);
  return dax_base + offset;
#else
  exit(__LINE__);
#endif
}


int main(int argc, char *argv[]) try {

#if DAX
  dax_init();
#endif

#if MY_TIME_CORE
  bool all_done = false;
  std::thread time_thread(my_time_func, std::ref(all_done));
  printf("Run my_time_func @ core %d...\n", MY_TIME_CORE);
#endif
  
#if COROBASE
  printf("use CoroBase. N_CORO=%d\n", N_CORO);
#elif PILO
  printf("use PILO N_CORO=%d\n", N_CORO);
#else
  printf("use original.\n");
#endif
  //gflags::SetUsageMessage("Silo benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  makeDB();
#if DAX
  MT.dfs_conv();
#endif
  

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;

  initResult();
  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
  
  //std::thread perf_th(run_perf, std::ref(start), std::ref(quit));
  
  waitForReady(readys);
  //system("ipmctl show -dimm -performance");
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  //system("ipmctl show -dimm -performance");
  for (auto &th : thv) th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    SiloResult[0].addLocalAllResult(SiloResult[i]);
  }
  ShowOptParameters();
  SiloResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime,
                                 FLAGS_thread_num);

  sleep(1);
  
#if MY_TIME_CORE
  all_done = true;
  time_thread.join();
#endif

  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  printf("Max RSS: %f MB\n", ru.ru_maxrss / 1024.0);
  printf("DAX USED: %f MB\n", dax_used / 1024.0 / 1024.0);
  
  return 0;
} catch (bad_alloc) {
  ERR;
}
