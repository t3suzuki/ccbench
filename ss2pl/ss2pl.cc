
#include <ctype.h>  //isdigit,
#include <pthread.h>
#include <string.h>       //strlen,
#include <sys/syscall.h>  //syscall(SYS_gettid),
#include <sys/types.h>    //syscall(SYS_gettid),
#include <unistd.h>       //syscall(SYS_gettid),
#include <x86intrin.h>

#include <iostream>
#include <string>  //string
#include <thread>

#define GLOBAL_VALUE_DEFINE

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/fence.hh"
#include "../include/int64byte.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/procedure.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "../include/dax.h"

PTX_PROMISE(void) ptx_work(size_t thid, FastZipf &zipf,
			   Xoroshiro128Plus &rnd, Result &myres, const bool &quit, Backoff &backoff,
			   int &n_done, bool &done_coro
 )
{
  while (!loadAcquire(quit)) {
    TxExecutor trans(thid, (Result *) &myres);
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                  FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false, thid, myres);
RETRY:
    if (loadAcquire(quit)) break;
    if (thid == 0) leaderBackoffWork(backoff, SS2PLResult);

    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
#if SKIP_INDEX
      Tuple *tuple;
      PTX_AWAIT MT.get_value_ptx_flat((*itr).key_, tuple);
      itr->tuple = tuple;
#else
      Tuple *tuple;
      PTX_AWAIT MT.get_value_ptx_flat((*itr).key_, tuple);
#endif
    }
    
    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
#if SKIP_INDEX
      if ((*itr).ope_ == Ope::READ) {
        trans.read_skip_index((*itr));
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write_skip_index((*itr));
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.readWrite_skip_index((*itr));
      } else {
        ERR;
      }
#else
      if ((*itr).ope_ == Ope::READ) {
        trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.readWrite((*itr).key_);
      } else {
        ERR;
      }
#endif
      if (trans.status_ == TransactionStatus::aborted) {
        trans.abort();
        goto RETRY;
      }
    }

    trans.commit();
    /**
     * local_commit_counts is used at ../include/backoff.hh to calcurate about
     * backoff.
     */
    storeRelease(myres.local_commit_counts_,
                 loadAcquire(myres.local_commit_counts_) + 1);
  }
  n_done++;
  done_coro = true;
  PTX_RETURN;
}

void original_work(size_t thid, FastZipf &zipf,
		   Xoroshiro128Plus &rnd, Result &myres, const bool &quit, Backoff &backoff)
{
  TxExecutor trans(thid, (Result *) &myres);
  while (!loadAcquire(quit)) {
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                  FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false, thid, myres);
RETRY:
    if (loadAcquire(quit)) break;
    if (thid == 0) leaderBackoffWork(backoff, SS2PLResult);

    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
      if ((*itr).ope_ == Ope::READ) {
        trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.readWrite((*itr).key_);
      } else {
        ERR;
      }

      if (trans.status_ == TransactionStatus::aborted) {
        trans.abort();
        goto RETRY;
      }
    }

    trans.commit();
    /**
     * local_commit_counts is used at ../include/backoff.hh to calcurate about
     * backoff.
     */
    storeRelease(myres.local_commit_counts_,
                 loadAcquire(myres.local_commit_counts_) + 1);
  }
}

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Result &myres = std::ref(SS2PLResult[thid]);
  Xoroshiro128Plus rnd;
  rnd.init();
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);
  Backoff backoff(FLAGS_clocks_per_us);

#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

#if 1
  setThreadAffinity(thid);
  // printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %ld\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif  // Linux

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

#if defined(PTX)
  int n_done = 0;
  bool done[N_CORO];
  PTX_PROMISE(void) coro[N_CORO];
  for (int i_coro=0; i_coro<N_CORO; i_coro++) {
    done[i_coro] = false;
    coro[i_coro] = ptx_work(thid, zipf, rnd, myres,
			    quit, backoff, n_done, done[i_coro]);
    coro[i_coro].start();
  }
  do {
    for (int i_coro=0; i_coro<N_CORO; i_coro++) {
      if (!done[i_coro])
        coro[i_coro].resume();
    }
  } while (n_done != N_CORO);
#else
  original_work(thid, zipf, rnd, myres, quit, backoff);
#endif
  return;
}

thread_local tcalloc coroutine_allocator;
int main(int argc, char *argv[]) try {
#if DAX
  dax_init();
#endif

  //gflags::SetUsageMessage("2PL benchmark.");
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
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv) th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    SS2PLResult[0].addLocalAllResult(SS2PLResult[i]);
  }
  ShowOptParameters();
  SS2PLResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, FLAGS_thread_num);

  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  printf("Max RSS: %f MB\n", ru.ru_maxrss / 1024.0);
  printf("DAX USED: %f MB\n", dax_used / 1024.0 / 1024.0);

  return 0;
} catch (bad_alloc) {
  ERR;
}
