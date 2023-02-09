#pragma once

#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include <pthread.h>
#include <stdlib.h>
#include <xmmintrin.h>

// フォーマッターを利用すると，辞書順のために下記2行が入れ替わる．
// しかし，依存関係があるため，config -> compiler の順にしなければ
// 大量のエラーが出てしまう． そのため，改行を防ぐため空行を空けている
#include "../third_party/masstree/config.h"

#include "../third_party/masstree/compiler.hh"

#include "../third_party/masstree/kvthread.hh"
#include "../third_party/masstree/masstree.hh"
#include "../third_party/masstree/masstree_insert.hh"
#include "../third_party/masstree/masstree_print.hh"
#include "../third_party/masstree/masstree_remove.hh"
#include "../third_party/masstree/masstree_scan.hh"
#include "../third_party/masstree/masstree_stats.hh"
#include "../third_party/masstree/masstree_tcursor.hh"
#include "../third_party/masstree/string.hh"

#include "atomic_wrapper.hh"
#include "debug.hh"
#include "random.hh"
#include "util.hh"
#include "coro.h"

class key_unparse_unsigned {
public:
  static int unparse_key(Masstree::key<std::uint64_t> key, char *buf, int buflen) {
    return snprintf(buf, buflen, "%"
                                 PRIu64, key.ikey());
  }
};

/* Notice.
 * type of object is T.
 * inserting a pointer of T as value.
 */
template<typename T>
class MasstreeWrapper {
public:
  static constexpr std::uint64_t insert_bound = UINT64_MAX;  // 0xffffff;
  // static constexpr std::uint64_t insert_bound = 0xffffff; //0xffffff;
  struct table_params : public Masstree::nodeparams<15, 15> {
    typedef T *value_type;
    typedef Masstree::value_print<value_type> value_print_type;
    typedef threadinfo threadinfo_type;
    typedef key_unparse_unsigned key_unparse_type;
    static constexpr ssize_t print_max_indent_depth = 12;
  };

  typedef Masstree::Str Str;
  typedef Masstree::basic_table<table_params> table_type;
  typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
  typedef Masstree::tcursor<table_params> cursor_type;
  typedef Masstree::leaf<table_params> leaf_type;
  typedef Masstree::internode<table_params> internode_type;

  typedef typename table_type::node_type node_type;
  typedef typename unlocked_cursor_type::nodeversion_value_type
          nodeversion_value_type;

  static __thread typename table_params::threadinfo_type *ti;

  MasstreeWrapper() { this->table_init(); }

  void table_init() {
    // printf("masstree table_init()\n");
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*ti);
    key_gen_ = 0;
    stopping = false;
    printing = 0;
  }

  static void thread_init(int thread_id) {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  }

  void table_print() {
    table_.print(stdout);
    fprintf(stdout, "Stats: %s\n",
            Masstree::json_stats(table_, ti)
                    .unparse(lcdf::Json::indent_depth(1000))
                    .c_str());
  }

  void insert_value(std::string_view key, T *value) {
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*ti);
    // always_assert(!found, "keys should all be unique");
    if (found) {
      // release lock of existing nodes meaning the first arg equals 0
      lp.finish(0, *ti);
      // return
      return;
    }
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    return;
  }

  void insert_value(std::uint64_t key, T *value) {
    std::uint64_t key_buf{__builtin_bswap64(key)};
    insert_value({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)}, value); // NOLINT
  }

  inline PROMISE(void) insert_value_coro(std::string_view key, T *value) {
    cursor_type lp(table_, key.data(), key.size());
    bool found = AWAIT lp.find_insert_coro(*ti);
    // always_assert(!found, "keys should all be unique");
    if (found) {
      // release lock of existing nodes meaning the first arg equals 0
      lp.finish(0, *ti);
      RETURN;
    }
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    RETURN;
  }

  inline PROMISE(void) insert_value_coro(std::uint64_t key, T *value) {
    std::uint64_t key_buf{__builtin_bswap64(key)};
    AWAIT insert_value_coro({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)}, value); // NOLINT
    RETURN;
  }
  
  using value_ptr_t = T *;
  inline PROMISE(bool) get_value_coro(std::string_view key, value_ptr_t &value_ptr) {
    unlocked_cursor_type lp(table_, key.data(), key.size());
    bool found = AWAIT lp.find_unlocked_coro(*ti);
    if (found) {
      value_ptr = lp.value();
    } else {
      printf("something wrong?\n");
      std::cout << key << std::endl;
      exit(2);
      value_ptr = nullptr;
    }
    RETURN found;
  }

  inline PROMISE(bool) get_value_coro(std::uint64_t key, value_ptr_t &value_ptr) {
    std::uint64_t key_buf{__builtin_bswap64(key)};
    bool found = AWAIT get_value_coro({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)}, value_ptr);
    RETURN found;
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

  inline PROMISE(bool) get_value_coro_flat(std::uint64_t _key, value_ptr_t &value_ptr) {
    std::uint64_t key_buf{__builtin_bswap64(_key)};
    std::string_view key{reinterpret_cast<char *>(&key_buf), sizeof(key_buf)};
    unlocked_cursor_type lp(table_, key.data(), key.size());
    //bool found = AWAIT get_value_coro({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)}, value_ptr);
    bool found;
    {
      int match;
      key_indexed_position kx;
      Masstree::node_base<table_params>* root = const_cast<Masstree::node_base<table_params>*>(lp.root_);
    retry:
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
	  SUSPEND;
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
	  SUSPEND;
#endif
	  int kp = Masstree::internode<table_params>::bound_type::upper(lp.ka_, *in);
	  n[!sense] = in->child_[kp];
	  if (!n[!sense])
            goto retry;
#if 1 // t3suzuki
	  const Masstree::internode<table_params> *cin = static_cast<const Masstree::internode<table_params>*>(n[!sense]);
	  cin->prefetch256B();
	  SUSPEND;
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
      SUSPEND;
      lp.perm_ = lp.n_->permutation();
      kx = Masstree::leaf<table_params>::bound_type::lower(lp.ka_, lp);
      if (kx.p >= 0) {
        lp.lv_ = lp.n_->lv_[kx.p];
        lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
    	SUSPEND;
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
      value_ptr = lp.value();
      ::prefetch(value_ptr);
      SUSPEND;
    } else {
      printf("something wrong?\n");
      std::cout << key << std::endl;
      exit(2);
      value_ptr = nullptr;
    }
    RETURN found;
  }
  
  inline PTX_PROMISE(bool) get_value_ptx_flat(std::uint64_t _key, value_ptr_t &value_ptr) {
    std::uint64_t key_buf{__builtin_bswap64(_key)};
    //bool found = PTX_AWAIT get_value_ptx({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)}, value_ptr);
    std::string_view key{reinterpret_cast<char *>(&key_buf), sizeof(key_buf)};
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
      value_ptr = lp.value();
      ::prefetch(value_ptr);
      PTX_SUSPEND;
    } else {
      printf("something wrong?\n");
      std::cout << key << std::endl;
      exit(2);
      value_ptr = nullptr;
    }
    PTX_RETURN found;
  }
  
  inline PTX_PROMISE(bool) get_value_ptx(std::string_view key, value_ptr_t &value_ptr) {
    unlocked_cursor_type lp(table_, key.data(), key.size());
    bool found = PTX_AWAIT lp.find_unlocked_ptx(*ti);
    if (found) {
      value_ptr = lp.value();
    } else {
      printf("something wrong?\n");
      std::cout << key << std::endl;
      exit(2);
      value_ptr = nullptr;
    }
    PTX_RETURN found;
  }

  inline PTX_PROMISE(bool) get_value_ptx(std::uint64_t key, value_ptr_t &value_ptr) {
    std::uint64_t key_buf{__builtin_bswap64(key)};
    bool found = PTX_AWAIT get_value_ptx({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)}, value_ptr);
    PTX_RETURN found;
  }
  
  inline T * get_value(std::string_view key) {
    unlocked_cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_unlocked(*ti);
    if (found) {
      return lp.value();
    }
    return nullptr;
  }

  inline T * get_value(std::uint64_t key) {
    std::uint64_t key_buf{__builtin_bswap64(key)};
    return get_value({reinterpret_cast<char *>(&key_buf), sizeof(key_buf)});
  }
  
  static inline std::atomic<bool> stopping{};
  static inline std::atomic<std::uint32_t> printing{};

private:
  table_type table_;
  std::uint64_t key_gen_;

  static inline Str make_key(std::uint64_t int_key, std::uint64_t &key_buf) {
    key_buf = __builtin_bswap64(int_key);
    return Str((const char *) &key_buf, sizeof(key_buf));
  }
};

template<typename T>
__thread typename MasstreeWrapper<T>::table_params::threadinfo_type *
        MasstreeWrapper<T>::ti = nullptr;
#ifdef GLOBAL_VALUE_DEFINE
volatile mrcu_epoch_type active_epoch = 1;
volatile std::uint64_t globalepoch = 1;
volatile bool recovering = false;
#else
extern volatile mrcu_epoch_type active_epoch;
extern volatile std::uint64_t globalepoch;
extern volatile bool recovering;
#endif
