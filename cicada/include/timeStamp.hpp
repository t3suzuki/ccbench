#pragma once

#include "../../include/tsc.hpp"
#include "../../include/cache_line_size.hpp"

class TimeStamp {
public:
  alignas(CACHE_LINE_SIZE)
  uint64_t ts_ = 0;
  uint64_t localClock_ = 0;
  uint64_t clockBoost_ = 0;
  uint8_t thid_;

  TimeStamp() {}
  inline uint64_t get_ts() {
    return ts_;
  }
  inline void set_ts(uint64_t &ts) {
    this->ts_ = ts;
  }
  
  inline void set_clockBoost(unsigned int CLOCK_PER_US) {
    // set 0 or some value equivalent to 1 us.
    clockBoost_ = CLOCK_PER_US;
  }

  inline void generateTimeStampFirst(unsigned char tid) {
    localClock_ = rdtscp();
    ts_ = (localClock_ << 8) | tid;
    thid_ = tid;
  }
  
  inline void generateTimeStamp(unsigned char tid) {
    uint64_t tmp = rdtscp();
    uint64_t elapsedTime = tmp - localClock_;
    if (tmp < localClock_) elapsedTime = 0;
    
    //current local clock + elapsed time + clockBoost
    localClock_ += elapsedTime;
    localClock_ += clockBoost_;
    
    ts_ = (localClock_ << 8) | tid;
  }
};
