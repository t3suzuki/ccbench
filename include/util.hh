#pragma once

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <xmmintrin.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cinttypes>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "debug.hh"
#include "procedure.hh"
#include "random.hh"
#include "result.hh"
#include "tsc.hh"
#include "zipf.hh"

// class
class LibcError : public std::exception {
private:
  std::string str_;

  static std::string generateMessage(int errnum, const std::string &msg) {
    std::string s(msg);
    const size_t BUF_SIZE = 1024;
    char buf[BUF_SIZE];
    ::snprintf(buf, 1024, " %d ", errnum);
    s += buf;
#ifdef Linux
    if (::strerror_r(errnum, buf, BUF_SIZE) != nullptr)
#endif  // Linux
#ifdef Darwin
    if (::strerror_r(errnum, buf, BUF_SIZE) != 0)
#endif  // Darwin
    s += buf;
    return s;
  }

public:
  explicit LibcError(int errnum = errno, const std::string &msg = "libc_error:")
          : str_(generateMessage(errnum, msg)) {}
};

// function
[[maybe_unused]] extern bool chkSpan(struct timeval &start, struct timeval &stop, long threshold);

extern size_t decideParallelBuildNumber(size_t tuple_num);

extern void displayProcedureVector(std::vector <Procedure> &pro);

extern void displayRusageRUMaxrss();

extern bool isReady(const std::vector<char> &readys);

extern void readyAndWaitForReadyOfAllThread(std::atomic <size_t> &running, const size_t thnm);

extern void sleepMs(size_t ms);

extern void waitForReady(const std::vector<char> &readys);

extern void waitForReadyOfAllThread(std::atomic <size_t> &running, const size_t thnm);

//----------
// After this line, intending to force inline function.

[[maybe_unused]] inline static bool chkClkSpan(const uint64_t start,
                                               const uint64_t stop,
                                               const uint64_t threshold) {
  uint64_t diff = 0;
  diff = stop - start;
  if (diff > threshold)
    return true;
  else
    return false;
}

[[maybe_unused]] inline static bool chkClkSpanSec(
        const uint64_t start, const uint64_t stop, const unsigned int clocks_per_us,
        const uint64_t sec) {
  uint64_t diff = 0;
  diff = stop - start;
  diff = diff / clocks_per_us / 1000 / 1000;
  if (diff > sec)
    return true;
  else
    return false;
}

using shuffle_map_t = std::vector<uint64_t>;

static shuffle_map_t g_shuffle_map;

inline static void
init_shuffle_map(uint64_t n)
{
  printf("done\n");
  g_shuffle_map.resize(n);
  for (uint64_t i=0; i<n; i++) {
    g_shuffle_map[i] = i;
  }
  std::random_device rd;
  std::mt19937 rg(rd());
  std::shuffle(g_shuffle_map.begin(), g_shuffle_map.end(), rg);
  printf("done %ld\n", g_shuffle_map.size());
}

inline static void makeProcedure(std::vector <Procedure> &pro, Xoroshiro128Plus &rnd,
                                 FastZipf &zipf, size_t tuple_num, size_t max_ope,
                                 size_t thread_num, size_t rratio, bool rmw, bool ycsb,
                                 bool partition, size_t thread_id, [[maybe_unused]]Result &res) {
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif
  pro.clear();
  bool ronly_flag(true), wonly_flag(true);

  if (max_ope == 9999) {
    // retwis
    ronly_flag = false;
    wonly_flag = false;
    uint32_t r = rnd.next() % 100;
    std::vector<uint64_t> keyIdx;
    int n_rw;
    int n_w;
    if (r < 0) {
      // add_user
      n_rw = 1;
      n_w = 3;
    } else if (r < 20) {
      // follow
      n_rw = 2;
      n_w = 2;
    } else if (r < 50) {
      // post
      n_rw = 3;
      n_w = 5;
    } else {
      // timeline
      n_rw = 1 + (rnd.next() % 9);
      n_w = 0;
      ronly_flag = true;
    }

    for (int i=0; i<std::max(n_rw,n_w); i++) {
      keyIdx.push_back(zipf() % tuple_num);
    }
    std::sort(keyIdx.begin(), keyIdx.end());
    int ik = 0;
    for (int i=0; i<n_rw; i++) {
      if (n_w) {
	pro.emplace_back(Ope::READ_MODIFY_WRITE, keyIdx[ik++]);
      } else {
	pro.emplace_back(Ope::READ, keyIdx[ik++]);
      }
    }
    if (n_rw < n_w) {
      for (int i=0; i<n_w - n_rw; i++) {
	pro.emplace_back(Ope::WRITE, keyIdx[ik++]);
      }
    }
  } else {

  for (size_t i = 0; i < max_ope; ++i) {
    uint64_t tmpkey;
    // decide access destination key.
    if (ycsb) {
      if (partition) {
        size_t block_size = tuple_num / thread_num;
        tmpkey = (block_size * thread_id) + (zipf() % block_size);
      } else {
#if SHUFFLED_ZIPF
        tmpkey = g_shuffle_map[zipf() % tuple_num];
#else
        tmpkey = zipf() % tuple_num;
#endif
      }
    } else {
      if (partition) {
        size_t block_size = tuple_num / thread_num;
        tmpkey = (block_size * thread_id) + (rnd.next() % block_size);
      } else {
        tmpkey = rnd.next() % tuple_num;
      }
    }

    // decide operation type.
    if ((rnd.next() % 100) < rratio) {
      wonly_flag = false;
      pro.emplace_back(Ope::READ, tmpkey);
    } else {
      ronly_flag = false;
      if (rmw) {
        pro.emplace_back(Ope::READ_MODIFY_WRITE, tmpkey);
      } else {
        pro.emplace_back(Ope::WRITE, tmpkey);
      }
    }
  }
  }

  (*pro.begin()).ronly_ = ronly_flag;
  (*pro.begin()).wonly_ = wonly_flag;

#if KEY_SORT
  std::sort(pro.begin(), pro.end());
#endif // KEY_SORT

#if ADD_ANALYSIS
  res.local_make_procedure_latency_ += rdtscp() - start;
#endif
}

[[maybe_unused]] inline static void sleepTics(size_t tics) {
  uint64_t start(rdtscp());
  while (rdtscp() - start < tics) _mm_pause();
}


