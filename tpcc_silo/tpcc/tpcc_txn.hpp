/**
 * @file tpcc_txn.hpp
 */

#pragma once

namespace TPCC {
  PROMISE(bool) run_new_order(query::NewOrder *query, Token &token, MyRW *myrw = nullptr);
  PTX_PROMISE(bool) run_new_order_ptx(query::NewOrder *query, MyRW *myrw = nullptr);
  PTX_PROMISE(bool) run_new_order_ptx2(query::NewOrder *query, MyRW *myrw);

  PROMISE(bool) run_payment(query::Payment *query, HistoryKeyGenerator *hkg, Token &token, MyRW *myrw = nullptr);
  PTX_PROMISE(bool) run_payment_ptx(query::Payment *query, HistoryKeyGenerator *hkg, MyRW *myrw = nullptr);
  PTX_PROMISE(void) run_payment_ptx2(query::Payment *query);
}
