
#include <ctype.h>  //isdigit, 
#include <pthread.h>
#include <string.h> //strlen,
#include <sys/syscall.h>  //syscall(SYS_gettid),  
#include <sys/types.h>  //syscall(SYS_gettid),
#include <time.h>
#include <unistd.h> //syscall(SYS_gettid), 

#include <iostream>
#include <string> //string

#define GLOBAL_VALUE_DEFINE
#include "../include/cpu.hpp"
#include "../include/debug.hpp"
#include "../include/int64byte.hpp"
#include "../include/random.hpp"
#include "../include/tsc.hpp"
#include "../include/zipf.hpp"

#include "include/common.hpp"
#include "include/garbageCollection.hpp"
#include "include/result.hpp"
#include "include/transaction.hpp"

using namespace std;

extern void chkArg(const int argc, const char *argv[]);
extern bool chkClkSpan(const uint64_t start, const uint64_t stop, const uint64_t threshold);
extern void makeDB();
extern void makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd);
extern void makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd, FastZipf &zipf);
extern void naiveGarbageCollection(SIResult &res);
extern void waitForReadyOfAllThread(std::atomic<unsigned int> &running, const unsigned int thnum);

static void *
manager_worker(void *arg)
{
  SIResult &res = *(SIResult *)(arg);
  GarbageCollection gcobject;
 
#ifdef Linux 
  setThreadAffinity(res.thid);
#endif // Linux

  gcobject.decideFirstRange();
  waitForReadyOfAllThread(Running, THREAD_NUM);
  // end, initial work
  
  
  res.bgn = rdtscp();
  for (;;) {
    usleep(1);
    res.end = rdtscp();
    if (chkClkSpan(res.bgn, res.end, EXTIME * 1000 * 1000 * CLOCKS_PER_US)) {
      Finish.store(true, std::memory_order_release);
      return nullptr;
    }

    if (gcobject.chkSecondRange()) {
      gcobject.decideGcThreshold();
      gcobject.mvSecondRangeToFirstRange();
    }
  }

  return nullptr;
}


static void *
worker(void *arg)
{
  SIResult &res = *(SIResult *)(arg);
  TxExecutor trans(res.thid, MAX_OPE);
  Procedure pro[MAX_OPE];
  Xoroshiro128Plus rnd;
  rnd.init();
  FastZipf zipf(&rnd, ZIPF_SKEW, TUPLE_NUM);
  
#ifdef Linux
  setThreadAffinity(res.thid);
  //printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
  //printf("sysconf(_SC_NPROCESSORS_CONF) %ld\n", sysconf(_SC_NPROCESSORS_CONF));
#endif // Linux

  waitForReadyOfAllThread(Running, THREAD_NUM);
  
  //start work (transaction)
  try {
    //printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
    for(;;) {
      if (YCSB) makeProcedure(pro, rnd, zipf);
      else makeProcedure(pro, rnd);

      asm volatile ("" ::: "memory");
RETRY:

      if (Finish.load(std::memory_order_acquire)) {
        return nullptr;
      }

      //-----
      //transaction begin
      trans.tbegin();
      for (unsigned int i = 0; i < MAX_OPE; ++i) {
        if (pro[i].ope == Ope::READ) {
          trans.tread(pro[i].key);
        } else {
          if (RMW) {
            trans.tread(pro[i].key);
            trans.twrite(pro[i].key);
          } else
            trans.twrite(pro[i].key);
        }

        if (trans.status == TransactionStatus::aborted) {
          trans.abort();
          ++res.localAbortCounts;
          goto RETRY;
        }
      }

      trans.commit();
      ++res.localCommitCounts;

      // maintenance phase
      // garbage collection
      uint32_t loadThreshold = trans.gcobject.getGcThreshold();
      if (trans.preGcThreshold != loadThreshold) {
        trans.gcobject.gcVersion(res);
        trans.preGcThreshold = loadThreshold;
#ifdef CCTR_ON
        trans.gcobject.gcTMTElements(res);
#endif // CCTR_ON
        ++res.localGCCounts;
      }
    }
  } catch (bad_alloc) {
    ERR;
  }

  return nullptr;
}

int
main(const int argc, const char *argv[])
{
  chkArg(argc, argv);
  makeDB();

  SIResult rsob[THREAD_NUM];
  SIResult &rsroot = rsob[0];
  pthread_t thread[THREAD_NUM];
  for (unsigned int i = 0; i < THREAD_NUM; ++i) {
    int ret;
    rsob[i].thid = i;
    if (i == 0)
      ret = pthread_create(&thread[i], NULL, manager_worker, (void *)(&rsob[i]));
    else
      ret = pthread_create(&thread[i], NULL, worker, (void *)(&rsob[i]));
    if (ret) ERR;
  }

  for (unsigned int i = 0; i < THREAD_NUM; ++i) {
    pthread_join(thread[i], nullptr);
    rsroot.add_localAllSIResult(rsob[i]);
  }

  rsroot.display_AllSIResult(CLOCKS_PER_US);

  return 0;
}
