#include <cstdlib>
#include <cstring>
#include <algorithm>

#include "cpu.h"

#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/result.hh"
#include "include/util.hh"

#include "../include/random.hh"
#include "../include/util.hh"

#include "tpcc_initializer.hpp"
#include "tpcc_query.hpp"
#include "tpcc_txn.hpp"
#include "clock.h"
#include "../include/coro.h"

using namespace std;


#if COROBASE
PROMISE(void) corobase_work(const bool &quit, const uint16_t w_id, std::uint64_t &lcl_cmt_cnt, std::uint64_t &lcl_abt_cnt, Token &token, TPCC::HistoryKeyGenerator &hkg, int i_coro, int &n_done, bool &done_coro)
{
  TPCC::Query query;
  TPCC::query::Option query_opt;
  
  while (!loadAcquire(quit)) {

    query.generate(w_id, query_opt);

    // TODO : add backoff work.

    if (loadAcquire(quit)) break;

    bool validation = true;

    switch (query.type) {
      case TPCC::Q_NEW_ORDER :
        validation = AWAIT TPCC::run_new_order(&query.new_order, token);
        break;
      case TPCC::Q_PAYMENT :
        validation = TPCC::run_payment(&query.payment, &hkg, token);
        break;
      case TPCC::Q_ORDER_STATUS:
        //validation = TPCC::run_order_status(query.order_status);
        break;
      case TPCC::Q_DELIVERY:
        //validation = TPCC::run_delivery(query.delivery);
        break;
      case TPCC::Q_STOCK_LEVEL:
        //validation = TPCC::run_stock_level(query.stock_level);
        break;
      case TPCC::Q_NONE:
        break;
      [[maybe_unused]] defalut:
        std::abort();
    }

    if (validation) {
      ++lcl_cmt_cnt;
    } else {
      ++lcl_abt_cnt;
    }
  }
  n_done++;
  done_coro = true;
  RETURN;
}
#elif PILO
PILO_PROMISE(void) pilo_work(const bool &quit, const uint16_t w_id, std::uint64_t &lcl_cmt_cnt, std::uint64_t &lcl_abt_cnt, Token &token, TPCC::HistoryKeyGenerator &hkg, int i_coro, int &n_done, bool &done_coro)
{
  TPCC::Query query;
  TPCC::query::Option query_opt;
  
  while (!loadAcquire(quit)) {

    query.generate(w_id, query_opt);

    // TODO : add backoff work.

    if (loadAcquire(quit)) break;

    bool validation = true;

    switch (query.type) {
      case TPCC::Q_NEW_ORDER :
        PILO_AWAIT TPCC::run_new_order_pilo(&query.new_order);
        validation = TPCC::run_new_order(&query.new_order, token);
        break;
      case TPCC::Q_PAYMENT :
        validation = TPCC::run_payment(&query.payment, &hkg, token);
        break;
      case TPCC::Q_ORDER_STATUS:
        //validation = TPCC::run_order_status(query.order_status);
        break;
      case TPCC::Q_DELIVERY:
        //validation = TPCC::run_delivery(query.delivery);
        break;
      case TPCC::Q_STOCK_LEVEL:
        //validation = TPCC::run_stock_level(query.stock_level);
        break;
      case TPCC::Q_NONE:
        break;
      [[maybe_unused]] defalut:
        std::abort();
    }

    if (validation) {
      ++lcl_cmt_cnt;
    } else {
      ++lcl_abt_cnt;
    }
  }
  n_done++;
  done_coro = true;
  PILO_RETURN;
}
#else
void original_work(const bool &quit, const uint16_t w_id, std::uint64_t &lcl_cmt_cnt, std::uint64_t &lcl_abt_cnt, TPCC::HistoryKeyGenerator &hkg)
{
  Token token{};
  enter(token);

  TPCC::Query query;
  TPCC::query::Option query_opt;
  
  while (!loadAcquire(quit)) {

    query.generate(w_id, query_opt);

    // TODO : add backoff work.

    if (loadAcquire(quit)) break;

    bool validation = true;

    switch (query.type) {
      case TPCC::Q_NEW_ORDER :
        validation = TPCC::run_new_order(&query.new_order, token);
	break;
      case TPCC::Q_PAYMENT :
        validation = TPCC::run_payment(&query.payment, &hkg, token);
        break;
      case TPCC::Q_ORDER_STATUS:
        //validation = TPCC::run_order_status(query.order_status);
        break;
      case TPCC::Q_DELIVERY:
        //validation = TPCC::run_delivery(query.delivery);
        break;
      case TPCC::Q_STOCK_LEVEL:
        //validation = TPCC::run_stock_level(query.stock_level);
        break;
      case TPCC::Q_NONE:
        break;
      [[maybe_unused]] defalut:
        std::abort();
    }

    if (validation) {
      ++lcl_cmt_cnt;
    } else {
      ++lcl_abt_cnt;
    }
  }
  leave(token);
}
#endif


void worker(size_t thid, char &ready, const bool &start, const bool &quit) try {

#ifdef CCBENCH_LINUX
  ccbench::setThreadAffinity(thid);
#endif

  std::uint64_t lcl_cmt_cnt{0};
  std::uint64_t lcl_abt_cnt{0};

  const uint16_t w_id = (thid % FLAGS_num_wh) + 1; // home warehouse.
#if 1
  // Load per warehouse if necessary.
  // thid in [0, num_th - 1].
  // w_id in [1, FLAGS_num_wh].
  // The worker thread of thid in [0, FLAGS_num_wh - 1]
  // should load data for the warehouse with w_id = thid + 1.

  size_t id = thid + 1;
  while (id <= FLAGS_num_wh) {
    //::printf("load for warehouse %u ...\n", id);
    TPCC::Initializer::load_per_warehouse(id);
    //::printf("load for warehouse %u done.\n", id);
    id += FLAGS_thread_num;
  }
#endif

  TPCC::HistoryKeyGenerator hkg{};
  hkg.init(thid, true);

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

#if defined(COROBASE) || defined(PILO)
  int n_done = 0;
  bool done[N_CORO];
  Token token[N_CORO];

#if COROBASE
  PROMISE(void) coro[N_CORO];
#else
  PILO_PROMISE(void) coro[N_CORO];
#endif
  for (int i_coro=0; i_coro<N_CORO; i_coro++) {
    done[i_coro] = false;
    enter(token[i_coro]);
#if COROBASE
    coro[i_coro] = corobase_work(quit, w_id, lcl_cmt_cnt, lcl_abt_cnt, token[i_coro], hkg,
				 i_coro, n_done, done[i_coro]);
#else
    coro[i_coro] = pilo_work(quit, w_id, lcl_cmt_cnt, lcl_abt_cnt, token[i_coro], hkg,
			     i_coro, n_done, done[i_coro]);
#endif
    coro[i_coro].start();
  }

  do {
    for (int i_coro=0; i_coro<N_CORO; i_coro++) {
      if (!done[i_coro])
        coro[i_coro].resume();
    }
  } while (n_done != N_CORO);
  for (int i_coro=0; i_coro<N_CORO; i_coro++) {
    leave(token[i_coro]);
  }
#else
  original_work(quit, w_id, lcl_cmt_cnt, lcl_abt_cnt, hkg);
#endif
  
  SiloResult[thid].local_commit_counts_ = lcl_cmt_cnt;
  SiloResult[thid].local_abort_counts_ = lcl_abt_cnt;
} catch (std::exception &e) {
  std::cout << "worker thread caught error " << e.what();
  std::abort();
}

thread_local tcalloc coroutine_allocator;

int main(int argc, char *argv[]) try {

#if COROBASE
  printf("use CoroBase. N_CORO=%d, tR=%dus\n", N_CORO, TR_US);
#else
  printf("use original.\n");
#endif

  //gflags::SetUsageMessage("Silo benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  init();
  ccbench::StopWatch stopwatch;
#if 0
  TPCC::Initializer::load();
#else
  TPCC::Initializer::load_item();
  // The remaining load will be processed by each worker threads.
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
  stopwatch.mark();
  ::printf("load latency: %.3f sec.\n", stopwatch.period());
  ::printf("starting workload...\n");
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv) th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    SiloResult[0].addLocalAllResult(SiloResult[i]);
  }
  SiloResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime,
                                 FLAGS_thread_num);

  fin();
  return 0;
} catch (std::bad_alloc &) {
  std::cout << __FILE__ << " : " << __LINE__ << " : bad_alloc error." << std::endl;
  std::abort();
} catch (std::exception &e) {
  std::cout << __FILE__ << " : " << __LINE__ << " : std::exception caught : " << e.what() << std::endl;
}
