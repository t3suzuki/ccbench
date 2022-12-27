/**
 * @file tpcc_txn.hpp
 */

#pragma once

namespace TPCC {
PROMISE(bool) run_new_order(query::NewOrder *query, Token &token);
PILO_PROMISE(bool) run_new_order_pilo(query::NewOrder *query);

bool run_payment(query::Payment *query, HistoryKeyGenerator *hkg, Token &token);
}
