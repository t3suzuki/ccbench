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
#include "../include/dax.h"

using namespace std;

#if MEASURE_TIME
typedef struct {
  alignas(64)
  uint64_t tsc0;
  uint64_t tsc1;
  uint64_t tsc2;
} measure_t;

measure_t measure_times[64];
#endif

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

PTX_PROMISE(void) ptx_work(size_t thid, int i_coro, FastZipf &zipf,
			   Xoroshiro128Plus &rnd, Result &myres, const bool &quit,
			   uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop,
			   int &n_done, bool &done_coro)
{
  while (!loadAcquire(quit)) {
#if N_CORO
    TxnExecutor trans(thid * N_CORO + i_coro, (Result *) &myres);
#else
    TxnExecutor trans(thid, (Result *) &myres);
#endif
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

#if MEASURE_TIME
    uint64_t measure_start0 = rdtscp();
#endif
    
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
#if SKIP_INDEX
      //itr->tuple = PTX_AWAIT trans.prefetch_tree((*itr).key_);
      {
	Tuple *tuple;
	PTX_AWAIT MT.get_value_ptx_flat((*itr).key_, tuple);
	itr->tuple = tuple;
      }
#else
      PTX_AWAIT trans.prefetch_tree((*itr).key_);
#endif
    }
    
#if MEASURE_TIME
    uint64_t measure_start1 = rdtscp();
#endif
    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
#if SKIP_INDEX
      if ((*itr).ope_ == Ope::READ) {
        trans.read_skip_index((*itr));
	//PTX_AWAIT trans.read_skip_index2((*itr));
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write_skip_index((*itr));
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.read_skip_index((*itr));
	//PTX_AWAIT trans.read_skip_index2((*itr));
        trans.write_skip_index((*itr));
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

#if MEASURE_TIME
    uint64_t measure_start2 = rdtscp();
#endif
    if (trans.validationPhase()) {
      trans.writePhase();
      /**
       * local_commit_counts is used at ../include/backoff.hh to calcurate about
       * backoff.
       */
      storeRelease(myres.local_commit_counts_,
                   loadAcquire(myres.local_commit_counts_) + 1);
#if MEASURE_TIME
      uint64_t measure_end = rdtscp();
      measure_times[thid].tsc0 += measure_end - measure_start0;
      measure_times[thid].tsc1 += measure_end - measure_start1;
      measure_times[thid].tsc2 += measure_end - measure_start2;
#endif
    } else {
      trans.abort();
      ++myres.local_abort_counts_;
#if MEASURE_TIME
      uint64_t measure_end = rdtscp();
      measure_times[thid].tsc0 += measure_end - measure_start0;
      measure_times[thid].tsc1 += measure_end - measure_start1;
      measure_times[thid].tsc2 += measure_end - measure_start2;
#endif
      goto RETRY;
    }
  }
  n_done++;
  done_coro = true;
  PTX_RETURN;
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

#if defined(COROBASE) || defined(PTX)
  int n_done = 0;
  bool done[N_CORO];
#if COROBASE
  PROMISE(void) coro[N_CORO];
#else
  PTX_PROMISE(void) coro[N_CORO];
#endif
  for (int i_coro=0; i_coro<N_CORO; i_coro++) {
    done[i_coro] = false;
#if COROBASE
    coro[i_coro] = corobase_work(thid, i_coro, zipf, rnd, myres,
				 quit, epoch_timer_start, epoch_timer_stop, n_done, done[i_coro]);
#else
    coro[i_coro] = ptx_work(thid, i_coro, zipf, rnd, myres,
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
    sprintf(buf, "perf stat -ddd -p %d   > stat%d.log 2>&1", pid, pid);
    //sprintf(buf, "perf record -C 0-15 -g");
    //sprintf(buf, "perf record -C 0-19 -g");
    execl("/bin/sh", "sh", "-c", buf, NULL);
  } else {
    setpgid(cpid, 0);
    while (!loadAcquire(quit)) _mm_pause();
    kill(-cpid, SIGINT);
  }
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
#elif PTX
  printf("use PTX N_CORO=%d\n", N_CORO);
#else
  printf("use original.\n");
#endif
  //gflags::SetUsageMessage("Silo benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  makeDB();
#if DAX
#if DAX_MIGRATE
  printf("Migrate SCM->DRAM...\n");
  MT.dfs_conv();
#endif
#endif

#if SKIP_INDEX
  printf("Skipping index enabled.\n");
#endif

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;

  initResult();
  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
  
  std::thread perf_th(run_perf, std::ref(start), std::ref(quit));
  
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

#if MEASURE_TIME
  uint64_t sum0 = 0;
  uint64_t sum1 = 0;
  uint64_t sum2 = 0;
  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    sum0 += measure_times[i].tsc0;
    sum1 += measure_times[i].tsc1;
    sum2 += measure_times[i].tsc2;
  }
  printf("total measure time0 (TX start to end latency) = %ld\n", sum0);
  printf("total measure time1 (main-transaciton including validation time) = %ld\n", sum1);
  printf("total measure time2 (validation time) = %ld\n", sum2);
#endif

  
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  printf("Max RSS: %f MB\n", ru.ru_maxrss / 1024.0);
  printf("DAX USED: %f MB\n", dax_used / 1024.0 / 1024.0);
  
  return 0;
} catch (bad_alloc) {
  ERR;
}
