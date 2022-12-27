/**
 * @file tpcc_txn.hpp
 */

#pragma once

namespace TPCC {
PROMISE(bool) run_new_order(query::NewOrder *query, Token &token);
PILO_PROMISE(bool) run_new_order_pilo(query::NewOrder *query);

PROMISE(bool) run_payment(query::Payment *query, HistoryKeyGenerator *hkg, Token &token);
PILO_PROMISE(bool) run_payment_pilo(query::Payment *query, HistoryKeyGenerator *hkg);
}
