#pragma once

#include <pthread.h>
#include <string.h>

#include <atomic>
#include <cstdint>

#include "../../include/cache_line_size.hh"

struct Tidword {
  union {
    uint64_t obj_;
    struct {
      bool lock: 1;
      bool latest: 1;
      bool absent: 1;
      uint64_t tid: 29;
      uint64_t epoch: 32;
    };
  };

  Tidword() : obj_(0) {};

  bool operator==(const Tidword &right) const { return obj_ == right.obj_; }

  bool operator!=(const Tidword &right) const { return !operator==(right); }

  bool operator<(const Tidword &right) const { return this->obj_ < right.obj_; }
};


class Tuple {
public:
#if TIDWORD_IN_TUPLE
#if ALIGN64_TIDWORD
  alignas(CACHE_LINE_SIZE)
#endif
  Tidword tidword_;
#endif
  char val_[VAL_SIZE];
  inline Tidword& fetch_tidword();
};

#if TIDWORD_IN_TUPLE
inline Tidword& Tuple::fetch_tidword() {
  return tidword_;
}
#else
class TidwordWrapper {
public:
  Tidword tidword_;
};

extern Tuple *Table;
extern TidwordWrapper *tidwords;
inline Tidword& Tuple::fetch_tidword() {
  uint64_t index = (((uint64_t)this - (uint64_t)Table) / sizeof(Tuple));
  return tidwords[index].tidword_;
}
#endif
