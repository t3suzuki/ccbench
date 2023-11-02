#pragma once

#include <pthread.h>
#include <xmmintrin.h>

#include <atomic>
#include <array>
#include <cstdlib>
#include <random>
#include <thread>
#include <typeinfo>
#include <vector>

#include "record.h"
#include "scheme_global.h"
#include "tpcc_tables.hpp"

/* if you use formatter, following 2 lines may be exchange.
 * but there is a dependency relation, so teh order is restricted config ->
 * compiler. To depend exchanging 2 lines, insert empty line. */
// config
#include "masstree/config.h"
// compiler
#include "masstree/compiler.hh"

// masstree-header
#include "masstree/kvthread.hh"
#include "masstree/masstree.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_print.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/masstree_stats.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/string.hh"

namespace ccbench {

class key_unparse_unsigned {  // NOLINT
public:
  [[maybe_unused]] static int unparse_key(  // NOLINT
          Masstree::key<uint64_t> key, char *buf, int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());  // NOLINT
  }
};

template<typename T>
class SearchRangeScanner {
public:
  using Str = Masstree::Str;

  SearchRangeScanner(const char *const rkey, const std::size_t len_rkey,
                     const bool r_exclusive, std::vector<const T *> *scan_buffer,
                     bool limited_scan)
          : rkey_(rkey),
            len_rkey_(len_rkey),
            r_exclusive_(r_exclusive),
            scan_buffer_(scan_buffer),
            limited_scan_(limited_scan) {
    if (limited_scan) {
      scan_buffer->reserve(kLimit_);
    }
  }

  template<typename SS, typename K>
  [[maybe_unused]] void visit_leaf(const SS &, const K &, threadinfo &) {}

  [[maybe_unused]] bool visit_value(const Str key, T *val,  // NOLINT
                                    threadinfo &) {
    if (limited_scan_) {
      if (scan_buffer_->size() >= kLimit_) {
        return false;
      }
    }

    if (rkey_ == nullptr) {
      scan_buffer_->emplace_back(val);
      return true;
    }

    const int res_memcmp = memcmp(
            rkey_, key.s, std::min(len_rkey_, static_cast<std::size_t>(key.len)));
    if (res_memcmp > 0 ||
        (res_memcmp == 0 &&
         ((!r_exclusive_ && len_rkey_ == static_cast<std::size_t>(key.len)) ||
          len_rkey_ > static_cast<std::size_t>(key.len)))) {
      scan_buffer_->emplace_back(val);
      return true;
    }
    return false;
  }

private:
  const char *const rkey_{};
  const std::size_t len_rkey_{};
  const bool r_exclusive_{};
  std::vector<const T *> *scan_buffer_{};
  const bool limited_scan_{false};
  static constexpr std::size_t kLimit_ = 1000;
};

/* Notice.
 * type of object is T.
 * inserting a pointer of T as value.
 */
template<typename T>
class masstree_wrapper {  // NOLINT
public:
  [[maybe_unused]] static constexpr uint64_t insert_bound =
          UINT64_MAX;  // 0xffffff;
  // static constexpr uint64_t insert_bound = 0xffffff; //0xffffff;
  struct table_params : public Masstree::nodeparams<MASSTREE_FANOUT, MASSTREE_FANOUT> {  // NOLINT
    using value_type = T *;
    using value_print_type = Masstree::value_print<value_type>;
    using threadinfo_type = threadinfo;
    using key_unparse_type = key_unparse_unsigned;
    [[maybe_unused]] static constexpr ssize_t print_max_indent_depth = 12;
  };

  using Str = Masstree::Str;
  using table_type = Masstree::basic_table<table_params>;
  using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
  using cursor_type = Masstree::tcursor<table_params>;
  using leaf_type = Masstree::leaf<table_params>;
  using internode_type = Masstree::internode<table_params>;

  using node_type = typename table_type::node_type;
  using nodeversion_value_type =
  typename unlocked_cursor_type::nodeversion_value_type;

  // tanabe :: todo. it should be wrapped by atomic type if not only one thread
  // use this.
  static __thread typename table_params::threadinfo_type *ti;

  masstree_wrapper() { this->table_init(); }

  ~masstree_wrapper() = default;

  void table_init() {
    // printf("masstree table_init()\n");
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*ti);
    stopping = false;
    printing = 0;
  }

  static void thread_init(int thread_id) {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  }

  /**
   * @brief insert value to masstree
   * @param key This must be a type of const char*.
   * @param len_key
   * @param value
   * @details future work, we try to delete making temporary
   * object std::string buf(key). But now, if we try to do
   * without making temporary object, it fails by masstree.
   * @return Status::WARN_ALREADY_EXISTS The records whose key is the same as @a
   * key exists in masstree, so this function returned immediately.
   * @return Status::OK success.
   */
  Status insert_value(const char *key,      // NOLINT
                      std::size_t len_key,  // NOLINT
                      T *value) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_insert(*ti);
    // always_assert(!found, "keys should all be unique");
    if (found) {
      // release lock of existing nodes meaning the first arg equals 0
      lp.finish(0, *ti);
      // return
      return Status::WARN_ALREADY_EXISTS;
    }
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    return Status::OK;
  }

  inline PROMISE(Status) insert_value_coro(const char *key,      // NOLINT
                      std::size_t len_key,  // NOLINT
                      T *value) {
    cursor_type lp(table_, key, len_key);
    bool found = AWAIT lp.find_insert_coro(*ti);
    // always_assert(!found, "keys should all be unique");
    if (found) {
      // release lock of existing nodes meaning the first arg equals 0
      lp.finish(0, *ti);
      // return
      RETURN Status::WARN_ALREADY_EXISTS;
    }
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    RETURN Status::OK;
  }
  
  Status insert_value(std::string_view key, T *value) {
    return insert_value(key.data(), key.size(), value);
  }

  inline PROMISE(Status) insert_value_coro(std::string_view key, T *value) {
    auto v = AWAIT insert_value_coro(key.data(), key.size(), value);
    RETURN v;
  }
  
  // for bench.
  Status put_value(const char *key, std::size_t len_key,  // NOLINT
                   T *value, T **record) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(*ti);
    if (found) {
      *record = lp.value();
      *value = **record;
      lp.value() = value;
      fence();
      lp.finish(0, *ti);
      return Status::OK;
    }
    fence();
    lp.finish(0, *ti);
    /**
     * Look project_root/third_party/masstree/masstree_get.hh:98 and 100.
     * If the node wasn't found, the lock was acquired and tcursor::state_ is
     * 0. So it needs to release. If state_ == 0, finish function merely
     * release locks of existing nodes.
     */
    return Status::WARN_NOT_FOUND;
  }

  Status remove_value(const char *key,        // NOLINT
                      std::size_t len_key) {  // NOLINT
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(*ti);
    if (found) {
      // try finish_remove. If it fails, following processing unlocks nodes.
      lp.finish(-1, *ti);
      return Status::OK;
    }
    // no nodes
    lp.finish(-1, *ti);
    return Status::WARN_NOT_FOUND;
  }

  T *get_value(const char *key, std::size_t len_key) {  // NOLINT
    unlocked_cursor_type lp(table_, key, len_key);
    bool found = lp.find_unlocked(*ti);
    if (found) {
      return lp.value();
    }
    return nullptr;
  }
  
  inline PROMISE(T *) get_value_coro(const char *key, std::size_t len_key) {  // NOLINT
    unlocked_cursor_type lp(table_, key, len_key);
    bool found = AWAIT lp.find_unlocked_coro(*ti);
    if (found) {
      RETURN lp.value();
    }
    RETURN nullptr;
  }

  inline PTX_PROMISE(T *) get_value_ptx_flat(std::string_view key) {
    unlocked_cursor_type lp(table_, key.data(), key.size());
  
    //bool found = PTX_AWAIT lp.find_unlocked_ptx(*ti);
    bool found;
    {
      int match;
      key_indexed_position kx;
      Masstree::node_base<table_params>* root = const_cast<Masstree::node_base<table_params>*>(lp.root_);
    retry:
      //lp.n_ = PTX_AWAIT root->reach_leaf_ptx(lp.ka_, lp.v_, *ti);
      {
	const Masstree::node_base<table_params> *n[2];
	typename Masstree::node_base<table_params>::nodeversion_type v[2];
	bool sense;
	
	// Get a non-stale root.
	// Detect staleness by checking whether n has ever split.
	// The true root has never split.
	//retry:
	sense = false;
	n[sense] = root;
	while (1) {
#if 0 // t3suzuki
	  const Masstree::internode<table_params> *in = static_cast<const Masstree::internode<table_params>*>(n[sense]);
	  in->prefetch256B();
	  PTX_SUSPEND;
#endif
	  v[sense] = n[sense]->stable_annotated(ti->stable_fence());
	  if (v[sense].is_root())
            break;
	  ti->mark(tc_root_retry);
	  n[sense] = n[sense]->maybe_parent();
	}
	
	// Loop over internal nodes.
	while (!v[sense].isleaf()) {
	  const Masstree::internode<table_params> *in = static_cast<const Masstree::internode<table_params>*>(n[sense]);
#if 0 // t3suzuki
	  in->prefetch();
	  PTX_SUSPEND;
#endif
	  int kp = Masstree::internode<table_params>::bound_type::upper(lp.ka_, *in);
	  n[!sense] = in->child_[kp];
	  if (!n[!sense])
            goto retry;
#if 1 // t3suzuki
	  const Masstree::internode<table_params> *cin = static_cast<const Masstree::internode<table_params>*>(n[!sense]);
	  cin->prefetch256B();
	  PTX_SUSPEND;
#endif
	  v[!sense] = n[!sense]->stable_annotated(ti->stable_fence());
	  
	  if (likely(!in->has_changed(v[sense]))) {
            sense = !sense;
            continue;
	  }
	  
	  typename Masstree::node_base<table_params>::nodeversion_type oldv = v[sense];
	  v[sense] = in->stable_annotated(ti->stable_fence());
	  if (oldv.has_split(v[sense])
	      && in->stable_last_key_compare(lp.ka_, v[sense], *ti) > 0) {
	    ti->mark(tc_root_retry);
	    goto retry;
	  } else
            ti->mark(tc_internode_retry);
	}

	lp.v_ = v[sense];
	//PTX_RETURN const_cast<leaf<table_params> *>(static_cast<const leaf<table_params> *>(n[sense]));
	lp.n_ = const_cast<Masstree::leaf<table_params> *>(static_cast<const Masstree::leaf<table_params> *>(n[sense]));
      }
   
    forward:
      if (lp.v_.deleted())
        goto retry;
   
      lp.n_->prefetchRem();
      PTX_SUSPEND;
      lp.perm_ = lp.n_->permutation();
      kx = Masstree::leaf<table_params>::bound_type::lower(lp.ka_, lp);
      if (kx.p >= 0) {
        lp.lv_ = lp.n_->lv_[kx.p];
        lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
    	PTX_SUSPEND;
        match = lp.n_->ksuf_matches(kx.p, lp.ka_);
      } else
        match = 0;
      if (lp.n_->has_changed(lp.v_)) {
        ti->mark(threadcounter(tc_stable_leaf_insert + lp.n_->simple_has_split(lp.v_)));
        lp.n_ = lp.n_->advance_to_key(lp.ka_, lp.v_, *ti);
        goto forward;
      }
   
      if (match < 0) {
        lp.ka_.shift_by(-match);
        root = lp.lv_.layer();
        goto retry;
      }
      found = (bool)match;
    }
  
    if (found) {
      T *value_ptr = lp.value();
      ::prefetch(value_ptr);
      PTX_SUSPEND;
      PTX_RETURN value_ptr;
    } else {
      PTX_RETURN nullptr;
    }
  }
  
  inline PTX_PROMISE(T *) get_value_ptx(const char *key, std::size_t len_key) {  // NOLINT
    unlocked_cursor_type lp(table_, key, len_key);
    bool found = PTX_AWAIT lp.find_unlocked_ptx(*ti);
    if (found) {
      PTX_RETURN lp.value();
    }
    PTX_RETURN nullptr;
  }

  inline PROMISE(T *) get_value_coro(std::string_view key) {
    auto v = AWAIT get_value_coro(key.data(), key.size());
    RETURN v;
  }

  inline PTX_PROMISE(T *) get_value_ptx(std::string_view key) {
    auto v = PTX_AWAIT get_value_ptx(key.data(), key.size());
    PTX_RETURN v;
  }

  T *get_value(std::string_view key) {
    return get_value(key.data(), key.size());
  }

  node_type* _dfs_conv(node_type* current, int height) {
    int height_th = 15;
    
    if (!current->isleaf()) {
      internode_type* in = (internode_type *)current;
      if (height < height_th) {
	for (int i=0; i<in->nkeys_+1; i++) {
	  if (in->child_[i]) {
	    in->child_[i] = _dfs_conv((node_type *)in->child_[i], height+1);
	  }
	}
      }
      if (height < height_th+1) {
	void *ptr = malloc(sizeof(internode_type));
	memcpy(ptr, in, sizeof(internode_type));
	node_type *n = (node_type*)ptr;
	return n;
      }
    } else {
    }
    return current;
  }
  void dfs_conv() {
    table_.root_ = _dfs_conv((node_type *)table_.root_, 0);
  }

  
  void scan(const char *const lkey, const std::size_t len_lkey,
            const bool l_exclusive, const char *const rkey,
            const std::size_t len_rkey, const bool r_exclusive,
            std::vector<const T *> *res, bool limited_scan) {
    Str mtkey;
    if (lkey == nullptr) {
      mtkey = Str();
    } else {
      mtkey = Str(lkey, len_lkey);
    }

    SearchRangeScanner<T> scanner(rkey, len_rkey, r_exclusive, res,
                                  limited_scan);
    table_.scan(mtkey, !l_exclusive, scanner, *ti);
  }

  [[maybe_unused]] void print_table() {
    // future work.
  }

  [[maybe_unused]] static inline std::atomic<bool> stopping{};      // NOLINT
  [[maybe_unused]] static inline std::atomic<uint32_t> printing{};  // NOLINT

private:
  table_type table_;

  [[maybe_unused]] static inline Str make_key(std::string &buf) {  // NOLINT
    return Str(buf);
  }
};

template<typename T>
__thread typename masstree_wrapper<T>::table_params::threadinfo_type *
        masstree_wrapper<T>::ti = nullptr;

[[maybe_unused]] extern volatile bool recovering;

class kohler_masstree {
public:
  static constexpr std::size_t db_length = 10;

  /**
   * @brief find record from masstree by using args informations.
   * @return the found record pointer.
   */
  static void *find_record(Storage st, std::string_view key);
  static PROMISE(void *) find_record_coro(Storage st, std::string_view key);
  static PTX_PROMISE(void *) find_record_ptx(Storage st, std::string_view key);

  static masstree_wrapper<Record> &get_mtdb(Storage st) {
    return MTDB.at(static_cast<std::uint32_t>(st));
  }
  static void dfs_conv() {
    for (auto it = MTDB.begin(); it != MTDB.end(); it++) {
      it->dfs_conv();
    }
  }

  /**
   * @brief insert record to masstree by using args informations.
   * @pre the record which has the same key as the key of args have never been
   * inserted.
   * @param key
   * @param record It inserts this pointer to masstree database.
   * @return WARN_ALREADY_EXISTS The records whose key is the same as @a key
   * exists in masstree, so this function returned immediately.
   * @return Status::OK It inserted record.
   */
  static Status insert_record(Storage st, std::string_view key, Record *record); // NOLINT
  static PROMISE(Status) insert_record_coro(Storage st, std::string_view key, Record *record); // NOLINT

private:
  static inline std::array<masstree_wrapper<Record>, db_length> MTDB;  // NOLINT
};

}  // namespace ccbench
