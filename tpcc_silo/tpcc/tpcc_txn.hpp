/**
 * @file tpcc_txn.hpp
 */

#pragma once

namespace TPCC {
  PROMISE(bool) run_new_order(query::NewOrder *query, Token &token, MyRW *myrw = nullptr);
  PILO_PROMISE(bool) run_new_order_pilo(query::NewOrder *query, MyRW *myrw = nullptr);

  PROMISE(bool) run_payment(query::Payment *query, HistoryKeyGenerator *hkg, Token &token, MyRW *myrw = nullptr);
  PILO_PROMISE(bool) run_payment_pilo(query::Payment *query, HistoryKeyGenerator *hkg, MyRW *myrw = nullptr);
}
