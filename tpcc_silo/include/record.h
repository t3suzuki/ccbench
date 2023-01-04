/**
 * @file record.h
 * @brief header about record
 */

#pragma once

#include "scheme_global.h"
#include "tuple.h"
#include "tid.h"

namespace ccbench {

class Record {  // NOLINT
public:
  Record() {}

#if 0 // QQQQQ
  Record(std::string_view key, std::string_view val, std::align_val_t align) : tuple_(key, val, align) {
    init();
  }
#endif

  explicit Record(Tuple&& tuple) : tuple_(std::move(tuple)) {
    init();
  }

  void init() {
    // init tidw
    tidw_.set_absent(true);
    tidw_.set_lock(true);
  }

  void set_for_load() {
    tidw_.set_absent(false);
    tidw_.set_lock(false);
  }

  Record(const Record &right) = default;

  Record(Record &&right) = default;

  Record &operator=(const Record &right) = default;  // NOLINT
  Record &operator=(Record &&right) = default;       // NOLINT

  tid_word &get_tidw() { return tidw_; }  // NOLINT

  [[nodiscard]] const tid_word &get_tidw() const { return tidw_; }  // NOLINT

  Tuple &get_tuple() { return tuple_; }  // NOLINT

  [[nodiscard]] const Tuple &get_tuple() const { return tuple_; }  // NOLINT

  void set_tidw(tid_word tidw) &{ tidw_.set_obj(tidw.get_obj()); }

private:
  alignas(CACHE_LINE_SIZE)
  Tuple tuple_;
  tid_word tidw_;
};

class MyRW {
private:
  struct tri_t {
    Storage storage;
    std::string key;
    Record *rec;
  };
  std::vector<tri_t> tris;
  int get_id = 0;
public:
  MyRW() {};
  void rd(Storage storage, std::string_view key, Record *rec) {
    tri_t tri{storage, std::string(key), rec};
    tris.push_back(tri);
  };
  Record *get_pfrec(Storage storage, std::string_view key) {
    tri_t tri = tris[get_id];
    get_id++;
    if ((tri.storage == storage) and (tri.key == std::string(key))) {
      return tri.rec;
    } else {
      return nullptr;
    }
  };
};
  
}  // namespace ccbench
