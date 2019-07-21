
#include <string.h>
#include <atomic>
#include <algorithm>
#include <bitset>

#include "../include/atomic_wrapper.hpp"
#include "../include/debug.hpp"
#include "include/common.hpp"
#include "include/transaction.hpp"
#include "include/version.hpp"

using namespace std;

inline
SetElement<Tuple> *
TxExecutor::searchReadSet(uint64_t key) 
{
  for (auto itr = readSet.begin(); itr != readSet.end(); ++itr) {
    if ((*itr).key == key) return &(*itr);
  }

  return nullptr;
}

inline
SetElement<Tuple> *
TxExecutor::searchWriteSet(uint64_t key) 
{
  for (auto itr = writeSet.begin(); itr != writeSet.end(); ++itr) {
    if ((*itr).key == key) return &(*itr);
  }

  return nullptr;
}

void
TxExecutor::tbegin()
{
#ifdef CCTR_ON
  TransactionTable *newElement, *tmt;

  tmt = loadAcquire(TMT[thid_]);
  if (this->status == TransactionStatus::committed) {
    this->txid = cstamp;
    newElement = new TransactionTable(0, cstamp);
  }
  else {
    this->txid = TMT[thid_]->lastcstamp.load(memory_order_acquire);
    newElement = new TransactionTable(0, tmt->lastcstamp.load(std::memory_order_acquire));
  }

  for (unsigned int i = 1; i < THREAD_NUM; ++i) {
    if (thid_ == i) continue;
    do {
      tmt = loadAcquire(TMT[i]);
    } while (tmt == nullptr);
    this->txid = max(this->txid, tmt->lastcstamp.load(memory_order_acquire));
  }
  this->txid += 1;
  newElement->txid = this->txid;
  
  TransactionTable *expected, *desired;
  tmt = loadAcquire(TMT[thid_]);
  expected = tmt;
  gcobject.gcqForTMT.push_back(expected);

  for (;;) {
    desired = newElement;
    if (compareExchange(TMT[thid_], expected, desired)) break;
  }
#endif // CCTR_ON

#ifdef CCTR_TW
  this->txid = ++CCtr;
  TMT[thid_]->txid.store(this->txid, std::memory_order_release);
#endif // CCTR_TW

  status = TransactionStatus::inFlight;
}

char*
TxExecutor::tread(uint64_t key)
{
  //if it already access the key object once.
  // w
  SetElement<Tuple> *inW = searchWriteSet(key);
  if (inW) return writeVal;

  SetElement<Tuple> *inR = searchReadSet(key);
  if (inR) return inR->ver->val;

#if MASSTREE_USE
  Tuple *tuple = MT.get_value(key);
  #if ADD_ANALYSIS
    ++sres_->local_tree_traversal;
  #endif
#else
  Tuple *tuple = get_tuple(Table, key);
#endif

  // if v not in t.writes:
  Version *ver = tuple->latest.load(std::memory_order_acquire);
  if (ver->status.load(memory_order_acquire) != VersionStatus::committed) {
    ver = ver->committed_prev;
  }

  while (txid < ver->cstamp.load(memory_order_acquire)) {
    //printf("txid %d, (verCstamp >> 1) %d\n", txid, verCstamp >> 1);
    //fflush(stdout);
    ver = ver->committed_prev;
    if (ver == nullptr) {
      ERR;
    }
  }

  readSet.emplace_back(key, tuple, ver);

  // for fairness
  // ultimately, it is wasteful in prototype system.
  memcpy(returnVal, ver->val, VAL_SIZE);

  return ver->val;
}

void
TxExecutor::twrite(uint64_t key)
{
  // if it already wrote the key object once.
  SetElement<Tuple> *inW = searchWriteSet(key);
  if (inW) return;

  // if v not in t.writes:
  //
  //first-updater-wins rule
  //Forbid a transaction to update  a record that has a committed head version later than its begin timestamp.
  
  Version *expected, *desired;
  desired = new Version();
  desired->cstamp.store(this->txid, memory_order_relaxed);  // storing before CAS because it will be accessed from read operation, write operation and garbage collection.
  desired->status.store(VersionStatus::inFlight, memory_order_relaxed);

#if MASSTREE_USE
  Tuple *tuple = MT.get_value(key);
  #if ADD_ANALYSIS
    ++sres_->local_tree_traversal;
  #endif
#else
  Tuple *tuple = get_tuple(Table, key);
#endif

  Version *vertmp;
  expected = tuple->latest.load(std::memory_order_acquire);
  for (;;) {
    // w-w conflict with concurrent transactions.
    if (expected->status.load(memory_order_acquire) == VersionStatus::inFlight) {

      uint64_t rivaltid = expected->cstamp.load(memory_order_acquire);
      if (this->txid <= rivaltid) {
      //if (1) { // no-wait で abort させても性能劣化はほぼ起きていない．
      //性能が向上されるケースもある．
        this->status = TransactionStatus::aborted;
        delete desired;
        return;
      }

      expected = tuple->latest.load(std::memory_order_acquire);
      continue;
    }
    
    // if a head version isn't committed version
    vertmp = expected;
    while (vertmp->status.load(memory_order_acquire) != VersionStatus::committed) {
      vertmp = vertmp->committed_prev;
      if (vertmp == nullptr) ERR;
    }

    // vertmp is committed latest version.
    if (txid < vertmp->cstamp.load(memory_order_acquire)) {  
      //  write - write conflict, first-updater-wins rule.
      // Writers must abort if they would overwirte a version created after their snapshot.
      this->status = TransactionStatus::aborted;
      delete desired;
      return;
    }

    desired->prev = expected;
    desired->committed_prev = vertmp;
    if (tuple->latest.compare_exchange_strong(expected, desired, memory_order_acq_rel, memory_order_acquire)) break;
  }

  writeSet.emplace_back(key, tuple, desired);
}

void
TxExecutor::commit()
{
  this->cstamp = ++CCtr;
  status = TransactionStatus::committed;

  for (auto itr = writeSet.begin(); itr != writeSet.end(); ++itr) {
    (*itr).ver->cstamp.store(this->cstamp, memory_order_release);
    memcpy((*itr).ver->val, writeVal, VAL_SIZE);
    (*itr).ver->status.store(VersionStatus::committed, memory_order_release);
    gcobject.gcqForVersion.push_back(GCElement((*itr).key, (*itr).rcdptr, (*itr).ver, cstamp));
  }

  readSet.clear();
  writeSet.clear();

#ifdef CCTR_TW
  TMT[thid_]->lastcstamp.store(this->cstamp, std::memory_order_release);
#endif // CCTR_TW

  ++sres_->local_commit_counts;
  return;
}

void
TxExecutor::abort()
{
  status = TransactionStatus::aborted;

  for (auto itr = writeSet.begin(); itr != writeSet.end(); ++itr) {
    (*itr).ver->status.store(VersionStatus::aborted, memory_order_release);
    gcobject.gcqForVersion.push_back(GCElement((*itr).key, (*itr).rcdptr, (*itr).ver, this->txid));
  }

  readSet.clear();
  writeSet.clear();
  ++sres_->local_abort_counts;
}

void
TxExecutor::dispWS()
{
  cout << "th " << this->thid_ << " : write set : ";
  for (auto itr = writeSet.begin(); itr != writeSet.end(); ++itr) {
    cout << "(" << (*itr).key << ", " << (*itr).ver->val << "), ";
  }
  cout << endl;
}

void
TxExecutor::dispRS()
{
  cout << "th " << this->thid_ << " : read set : ";
  for (auto itr = readSet.begin(); itr != readSet.end(); ++itr) {
    cout << "(" << (*itr).key << ", " << (*itr).ver->val << "), ";
  }
  cout << endl;
}

