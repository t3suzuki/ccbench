/**
 * @file masstree_wrapper.cc
 * @brief implement about masstree_wrapper
 */

#include <bitset>

#include "cpu.h"
#include "masstree_beta_wrapper.h"
#include "scheme_global.h"

volatile mrcu_epoch_type active_epoch = 1;          // NOLINT
volatile uint64_t globalepoch = 1;                  // NOLINT
[[maybe_unused]] volatile bool recovering = false;  // NOLINT

namespace ccbench {

Status kohler_masstree::insert_record(Storage st, std::string_view key, Record *record) {
  masstree_wrapper<Record>::thread_init(cached_sched_getcpu());
  Status insert_result(get_mtdb(st).insert_value(key, record));
  return insert_result;
}

PROMISE(Status) kohler_masstree::insert_record_coro(Storage st, std::string_view key, Record *record) {
  masstree_wrapper<Record>::thread_init(cached_sched_getcpu());
  auto mt = get_mtdb(st);
  auto res = AWAIT mt.insert_value_coro(key, record);
  RETURN res;
}
  
void *
kohler_masstree::find_record(Storage st, std::string_view key) {
  masstree_wrapper<Record>::thread_init(cached_sched_getcpu());
  return get_mtdb(st).get_value(key.data(), key.size());
}

PROMISE(void *)
kohler_masstree::find_record_coro(Storage st, std::string_view key) {
  masstree_wrapper<Record>::thread_init(cached_sched_getcpu());
  auto mt = get_mtdb(st);
  auto ret = AWAIT mt.get_value_coro(key.data(), key.size());
  RETURN ret;
}

PTX_PROMISE(void *)
kohler_masstree::find_record_ptx(Storage st, std::string_view key) {
  masstree_wrapper<Record>::thread_init(cached_sched_getcpu());
  auto mt = get_mtdb(st);
  auto ret = PTX_AWAIT mt.get_value_ptx(key.data(), key.size());
  PTX_RETURN ret;
}
  
}  // namespace shirakami
