#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <thread>
#include <signal.h>

#define GLOBAL_VALUE_DEFINE

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"
#include "../include/dax.h"

using namespace std;

PTX_PROMISE(void) ptx_work(size_t thid, int i_coro, FastZipf &zipf,
			   Xoroshiro128Plus &rnd, Result &myres, const bool &quit, Backoff &backoff,
			   int &n_done, bool &done_coro
 )
{
  TxExecutor trans(thid, (Result*) &CicadaResult[thid]);
    while (!loadAcquire(quit)) {
        /* シングル実行で絶対に競合を起こさないワークロードにおいて，
         * 自トランザクションで read した後に write するのは複雑になる．
         * write した後に read であれば，write set から read
         * するので挙動がシンプルになる．
         * スレッドごとにアクセスブロックを作る形でパーティションを作って
         * スレッド間の競合を無くした後に sort して同一キーに対しては
         * write - read とする．
         * */
#if SINGLE_EXEC
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true, thid, myres);
        sort(trans.pro_set_.begin(), trans.pro_set_.end());
#else
#if PARTITION_TABLE
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true, thid, myres);
#else
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false, thid, myres);
#endif
#endif

RETRY:
        if (thid == 0) {
            leaderWork(std::ref(backoff));
#if BACK_OFF
            leaderBackoffWork(backoff, CicadaResult);
#endif
        }
        if (loadAcquire(quit)) break;

	for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
	     ++itr) {
	  Tuple *tuple;
	  PTX_AWAIT MT.get_value_ptx_flat((*itr).key_, tuple);
#if SKIP_INDEX
	  itr->tuple = tuple;
#endif
	  if (tuple->latest_)
	    ::prefetch(tuple->latest_);
	}
	
        trans.tbegin();
        for (auto &&itr : trans.pro_set_) {
            if ((itr).ope_ == Ope::READ) {
#if SKIP_INDEX
	      trans.tread_skip_index((Tuple *)(itr).tuple, (itr).key_);
#else
	      trans.tread((itr).key_);
#endif
            } else if ((itr).ope_ == Ope::WRITE) {
#if SKIP_INDEX
	      trans.twrite_skip_index((Tuple *)(itr).tuple, (itr).key_);
#else
	      trans.twrite((itr).key_);
#endif
            } else if ((itr).ope_ == Ope::READ_MODIFY_WRITE) {
#if SKIP_INDEX
	      trans.tread_skip_index((Tuple *)(itr).tuple, (itr).key_);
	      trans.twrite_skip_index((Tuple *)(itr).tuple, (itr).key_);
#else
                trans.tread((itr).key_);
                trans.twrite((itr).key_);
#endif
            } else {
                ERR;
            }

            if (trans.status_ == TransactionStatus::abort) {
                trans.earlyAbort();
#if SINGLE_EXEC
#else
                //trans.mainte();
		PTX_AWAIT trans.ptx_mainte();
#endif
                goto RETRY;
            }
        }

        /**
         * Tanabe Optimization for analysis
         */
#if WORKER1_INSERT_DELAY_RPHASE
        if (unlikely(thid == 1) && WORKER1_INSERT_DELAY_RPHASE_US != 0) {
          clock_delay(WORKER1_INSERT_DELAY_RPHASE_US * FLAGS_clocks_per_us);
        }
#endif

        /**
         * Excerpt from original paper 3.1 Multi-Clocks Timestamp Allocation
         * A read-only transaction uses (thread.rts) instead,
         * and does not track or validate the read set;
         */
        if ((*trans.pro_set_.begin()).ronly_) {
            /**
             * local_commit_counts is used at ../include/backoff.hh to calcurate about
             * backoff.
             */
            storeRelease(myres.local_commit_counts_,
                         loadAcquire(myres.local_commit_counts_) + 1);
        } else {
            /**
             * Validation phase
             */
            if (!trans.validation()) {
                trans.abort();
#if SINGLE_EXEC
#else
                /**
                 * Maintenance phase
                 */
                //trans.mainte();
		PTX_AWAIT trans.ptx_mainte();
#endif
                goto RETRY;
            }

            /**
             * Write phase
             */
            trans.writePhase();
            /**
             * local_commit_counts is used at ../include/backoff.hh to calcurate about
             * backoff.
             */
            storeRelease(myres.local_commit_counts_,
                         loadAcquire(myres.local_commit_counts_) + 1);

            /**
             * Maintenance phase
             */
#if SINGLE_EXEC
#else
            //trans.mainte();
	    PTX_AWAIT trans.ptx_mainte();
#endif
        }
    }
  n_done++;
  done_coro = true;
    PTX_RETURN;
}

void original_work(size_t thid, FastZipf &zipf,
		   Xoroshiro128Plus &rnd, Result &myres, const bool &quit, Backoff &backoff)
{
    TxExecutor trans(thid, (Result*) &CicadaResult[thid]);
    while (!loadAcquire(quit)) {
        /* シングル実行で絶対に競合を起こさないワークロードにおいて，
         * 自トランザクションで read した後に write するのは複雑になる．
         * write した後に read であれば，write set から read
         * するので挙動がシンプルになる．
         * スレッドごとにアクセスブロックを作る形でパーティションを作って
         * スレッド間の競合を無くした後に sort して同一キーに対しては
         * write - read とする．
         * */
#if SINGLE_EXEC
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true, thid, myres);
        sort(trans.pro_set_.begin(), trans.pro_set_.end());
#else
#if PARTITION_TABLE
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true, thid, myres);
#else
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false, thid, myres);
#endif
#endif

RETRY:
        if (thid == 0) {
            leaderWork(std::ref(backoff));
#if BACK_OFF
            leaderBackoffWork(backoff, CicadaResult);
#endif
        }
        if (loadAcquire(quit)) break;

        trans.tbegin();
        for (auto &&itr : trans.pro_set_) {
            if ((itr).ope_ == Ope::READ) {
                trans.tread((itr).key_);
            } else if ((itr).ope_ == Ope::WRITE) {
                trans.twrite((itr).key_);
            } else if ((itr).ope_ == Ope::READ_MODIFY_WRITE) {
                trans.tread((itr).key_);
                trans.twrite((itr).key_);
            } else {
                ERR;
            }

            if (trans.status_ == TransactionStatus::abort) {
                trans.earlyAbort();
#if SINGLE_EXEC
#else
                trans.mainte();
#endif
                goto RETRY;
            }
        }

        /**
         * Tanabe Optimization for analysis
         */
#if WORKER1_INSERT_DELAY_RPHASE
        if (unlikely(thid == 1) && WORKER1_INSERT_DELAY_RPHASE_US != 0) {
          clock_delay(WORKER1_INSERT_DELAY_RPHASE_US * FLAGS_clocks_per_us);
        }
#endif

        /**
         * Excerpt from original paper 3.1 Multi-Clocks Timestamp Allocation
         * A read-only transaction uses (thread.rts) instead,
         * and does not track or validate the read set;
         */
        if ((*trans.pro_set_.begin()).ronly_) {
            /**
             * local_commit_counts is used at ../include/backoff.hh to calcurate about
             * backoff.
             */
            storeRelease(myres.local_commit_counts_,
                         loadAcquire(myres.local_commit_counts_) + 1);
        } else {
            /**
             * Validation phase
             */
	  //val++;
            if (!trans.validation()) {
                trans.abort();
#if SINGLE_EXEC
#else
                /**
                 * Maintenance phase
                 */
                trans.mainte();
#endif
                goto RETRY;
            }

            /**
             * Write phase
             */
            trans.writePhase();
            /**
             * local_commit_counts is used at ../include/backoff.hh to calcurate about
             * backoff.
             */
            storeRelease(myres.local_commit_counts_,
                         loadAcquire(myres.local_commit_counts_) + 1);

            /**
             * Maintenance phase
             */
#if SINGLE_EXEC
#else
            trans.mainte();
#endif
        }
    }
    
}

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
    Xoroshiro128Plus rnd;
    rnd.init();
    Result &myres = std::ref(CicadaResult[thid]);
    FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);
    Backoff backoff(FLAGS_clocks_per_us);

#if 1
    setThreadAffinity(thid);
    // printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
    // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
    // sysconf(_SC_NPROCESSORS_CONF));
#endif  // Linux

#ifdef Darwin
    int nowcpu;
    GETCPU(nowcpu);
    // printf("Thread %d on CPU %d\n", *myid, nowcpu);
#endif  // Darwin

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();
#if PTX
    int n_done = 0;
    bool done[N_CORO];
    PTX_PROMISE(void) coro[N_CORO];
    for (int i_coro=0; i_coro<N_CORO; i_coro++) {
      done[i_coro] = false;
      coro[i_coro] = ptx_work(thid, i_coro, zipf, rnd, myres,
			      quit, backoff, n_done, done[i_coro]);
      coro[i_coro].start();
    }
    do {
      for (int i_coro=0; i_coro<N_CORO; i_coro++) {
	//resu++;
	if (!done[i_coro])
	  coro[i_coro].resume();
      }
    } while (n_done != N_CORO);
#else
    original_work(thid, zipf, rnd, myres, quit, backoff);
#endif
    //printf("%ld %ld\n", val, resu);
}


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


thread_local tcalloc coroutine_allocator;
int main(int argc, char* argv[]) try {
#if DAX
  dax_init();
#endif
  
  //gflags::SetUsageMessage("Cicada benchmark.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    chkArg();
    uint64_t initial_wts;
    makeDB(&initial_wts);

#if DAX
#if DAX_MIGRATE
  printf("Migrate SCM->DRAM...\n");
  MT.dfs_conv();
#endif
#endif

#if SKIP_INDEX
  printf("Skipping index enabled.\n");
#endif

#if DEFAULT_NEW
  printf("Using default new constructor for Version.\n");
#else
  printf("Using replacement new constructor for Version.\n");
#endif

    MinWts.store(initial_wts + 2, memory_order_release);

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
#if DAX
    system("ipmctl show -dimm -performance");
#endif
    storeRelease(start, true);
    for (size_t i = 0; i < FLAGS_extime; ++i) {
        sleepMs(1000);
    }
    storeRelease(quit, true);
#if DAX
    system("ipmctl show -dimm -performance");
#endif
    for (auto &th : thv) th.join();

    for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
        CicadaResult[0].addLocalAllResult(CicadaResult[i]);
    }
    ShowOptParameters();
    CicadaResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, FLAGS_thread_num);
    deleteDB();

    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    printf("Max RSS: %f MB\n", ru.ru_maxrss / 1024.0);
    printf("DAX USED: %f MB\n", dax_used / 1024.0 / 1024.0);
    printf("Masstree prefetch count %d\n", MASSTREE_PREFETCH_COUNT);
  
    return 0;
} catch (std::bad_alloc&) {
    ERR;
}
