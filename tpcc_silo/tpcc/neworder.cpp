#include "interface.h"
#include "masstree_beta_wrapper.h"
#include "tpcc/tpcc_query.hpp"
#include "common.hh"

using namespace ccbench;

namespace TPCC {

namespace {


/**
 * =======================================================================+
 * EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
 * INTO :c_discount, :c_last, :c_credit, :w_tax
 * FROM customer, warehouse
 * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
 * +========================================================================
 */
bool get_warehouse(Token& token, uint16_t w_id, const TPCC::Warehouse*& ware)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status stat = search_key(token, Storage::WAREHOUSE, w_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  return true;
}

PROMISE(bool) get_warehouse_coro(Token& token, uint16_t w_id, const TPCC::Warehouse*& ware)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status stat = AWAIT search_key_coro(token, Storage::WAREHOUSE, w_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  RETURN true;
}
  
bool get_warehouse_pref(Token& token, uint16_t w_id, const TPCC::Warehouse*& ware)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status stat = search_key_pref(Storage::WAREHOUSE, w_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  return true;
}
  
PILO_PROMISE(bool) get_warehouse_pilo(uint16_t w_id, const TPCC::Warehouse*& ware)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status stat = PILO_AWAIT search_key_pilo(Storage::WAREHOUSE, w_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  PILO_RETURN true;
}
  
bool get_customer(
  Token& token, uint32_t c_id, uint8_t d_id, uint16_t w_id,
  const TPCC::Customer*& cust)
{
  SimpleKey<8> c_key;
  TPCC::Customer::CreateKey(w_id, d_id, c_id, c_key.ptr());
  Tuple *tuple;
  Status stat = search_key(token, Storage::CUSTOMER, c_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  cust = &tuple->get_value().cast_to<TPCC::Customer>();
  return true;
}

PILO_PROMISE(bool) get_customer_pilo(
  uint32_t c_id, uint8_t d_id, uint16_t w_id,
  const TPCC::Customer*& cust)
{
  SimpleKey<8> c_key;
  TPCC::Customer::CreateKey(w_id, d_id, c_id, c_key.ptr());
  Tuple *tuple;
  Status stat = PILO_AWAIT search_key_pilo(Storage::CUSTOMER, c_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  cust = &tuple->get_value().cast_to<TPCC::Customer>();
  PILO_RETURN true;
}
  
PROMISE(bool) get_customer_coro(
  Token& token, uint32_t c_id, uint8_t d_id, uint16_t w_id,
  const TPCC::Customer*& cust)
{
  SimpleKey<8> c_key;
  TPCC::Customer::CreateKey(w_id, d_id, c_id, c_key.ptr());
  Tuple *tuple;
  Status stat = AWAIT search_key_coro(token, Storage::CUSTOMER, c_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  cust = &tuple->get_value().cast_to<TPCC::Customer>();
  RETURN true;
}
  

/**
 * ==================================================+
 * EXEC SQL SELECT d_next_o_id, d_tax
 * INTO :d_next_o_id, :d_tax
 * FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
 *
 * EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
 * WHERE d_id = :d_id AND d_w_id = :w_id ;
 * +===================================================
 */
bool get_and_update_district(
    Token& token, uint8_t d_id, uint16_t w_id,
    const TPCC::District*& dist)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Tuple *tuple;
  Status stat = search_key(token, Storage::DISTRICT, d_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  const TPCC::District& old_dist = tuple->get_value().cast_to<TPCC::District>();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));
  new_dist.D_NEXT_O_ID++;
  stat = update(token, Storage::DISTRICT, Tuple(d_key.view(), std::move(d_obj)), &tuple);
  if (stat == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  dist = &tuple->get_value().cast_to<TPCC::District>();
  return true;
}

PILO_PROMISE(bool) get_and_update_district_pilo(
    uint8_t d_id, uint16_t w_id,
    const TPCC::District*& dist)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Tuple *tuple;
  Status stat = PILO_AWAIT search_key_pilo(Storage::DISTRICT, d_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  const TPCC::District& old_dist = tuple->get_value().cast_to<TPCC::District>();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));
  new_dist.D_NEXT_O_ID++;
  dist = &new_dist;
  PILO_RETURN true;
}

  
PROMISE(bool) get_and_update_district_coro(
    Token& token, uint8_t d_id, uint16_t w_id,
    const TPCC::District*& dist)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Tuple *tuple;
  Status stat = AWAIT search_key_coro(token, Storage::DISTRICT, d_key.view(), &tuple);
  if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  const TPCC::District& old_dist = tuple->get_value().cast_to<TPCC::District>();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));
  new_dist.D_NEXT_O_ID++;
  stat = AWAIT update_coro(token, Storage::DISTRICT, Tuple(d_key.view(), std::move(d_obj)), &tuple);
  if (stat == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  dist = &tuple->get_value().cast_to<TPCC::District>();
  RETURN true;
}

/**
 * =========================================+
 * EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
 * VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
 * +=======================================
 */
bool insert_order(
  Token& token, uint32_t o_id, uint8_t d_id, uint16_t w_id, uint32_t c_id,
  uint8_t ol_cnt, bool remote,
  const TPCC::Order*& ord)
{
  HeapObject o_obj;
  o_obj.allocate<TPCC::Order>();
  TPCC::Order& new_ord = o_obj.ref();
  new_ord.O_ID = o_id;
  new_ord.O_D_ID = d_id;
  new_ord.O_W_ID = w_id;
  new_ord.O_C_ID = c_id;
  new_ord.O_ENTRY_D = ccbench::epoch::get_lightweight_timestamp();
  new_ord.O_OL_CNT = ol_cnt;
  new_ord.O_ALL_LOCAL = (remote ? 0 : 1);
  SimpleKey<8> o_key;
  TPCC::Order::CreateKey(new_ord.O_W_ID, new_ord.O_D_ID, new_ord.O_ID, o_key.ptr());
  Tuple *tuple;
  Status sta = insert(token, Storage::ORDER, Tuple(o_key.view(), std::move(o_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  ord = &tuple->get_value().cast_to<TPCC::Order>();
  return true;
}

PILO_PROMISE(bool) insert_order_pilo(
  uint32_t o_id, uint8_t d_id, uint16_t w_id, uint32_t c_id,
  uint8_t ol_cnt, bool remote,
  const TPCC::Order*& ord)
{
  HeapObject o_obj;
  o_obj.allocate<TPCC::Order>();
  TPCC::Order& new_ord = o_obj.ref();
  new_ord.O_ID = o_id;
  new_ord.O_D_ID = d_id;
  new_ord.O_W_ID = w_id;
  new_ord.O_C_ID = c_id;
  new_ord.O_ENTRY_D = ccbench::epoch::get_lightweight_timestamp();
  new_ord.O_OL_CNT = ol_cnt;
  new_ord.O_ALL_LOCAL = (remote ? 0 : 1);
  SimpleKey<8> o_key;
  TPCC::Order::CreateKey(new_ord.O_W_ID, new_ord.O_D_ID, new_ord.O_ID, o_key.ptr());
  Status sta = PILO_AWAIT insert_pilo(Storage::ORDER, Tuple(o_key.view(), std::move(o_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  ord = &new_ord;
  PILO_RETURN true;
}

PROMISE(bool) insert_order_coro(
  Token& token, uint32_t o_id, uint8_t d_id, uint16_t w_id, uint32_t c_id,
  uint8_t ol_cnt, bool remote,
  const TPCC::Order*& ord)
{
  HeapObject o_obj;
  o_obj.allocate<TPCC::Order>();
  TPCC::Order& new_ord = o_obj.ref();
  new_ord.O_ID = o_id;
  new_ord.O_D_ID = d_id;
  new_ord.O_W_ID = w_id;
  new_ord.O_C_ID = c_id;
  new_ord.O_ENTRY_D = ccbench::epoch::get_lightweight_timestamp();
  new_ord.O_OL_CNT = ol_cnt;
  new_ord.O_ALL_LOCAL = (remote ? 0 : 1);
  SimpleKey<8> o_key;
  TPCC::Order::CreateKey(new_ord.O_W_ID, new_ord.O_D_ID, new_ord.O_ID, o_key.ptr());
  Tuple *tuple;
  Status sta = AWAIT insert_coro(token, Storage::ORDER, Tuple(o_key.view(), std::move(o_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  //ord = &tuple->get_value().cast_to<TPCC::Order>();
  RETURN true;
}
  

/**
 * =======================================================+
 * EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
 * VALUES (:o_id, :d_id, :w_id);
 * +=======================================================
 */
bool insert_neworder(Token& token, uint32_t o_id, uint8_t d_id, uint16_t w_id)
{
  HeapObject no_obj;
  no_obj.allocate<TPCC::NewOrder>();
  TPCC::NewOrder& new_no = no_obj.ref();
  new_no.NO_O_ID = o_id;
  new_no.NO_D_ID = d_id;
  new_no.NO_W_ID = w_id;
  SimpleKey<8> no_key;
  TPCC::NewOrder::CreateKey(new_no.NO_W_ID, new_no.NO_D_ID, new_no.NO_O_ID, no_key.ptr());
  Status sta = insert(token, Storage::NEWORDER, Tuple(no_key.view(), std::move(no_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  return true;
}


bool insert_neworder_pref(uint32_t o_id, uint8_t d_id, uint16_t w_id)
{
  HeapObject no_obj;
  no_obj.allocate<TPCC::NewOrder>();
  TPCC::NewOrder& new_no = no_obj.ref();
  new_no.NO_O_ID = o_id;
  new_no.NO_D_ID = d_id;
  new_no.NO_W_ID = w_id;
  SimpleKey<8> no_key;
  TPCC::NewOrder::CreateKey(new_no.NO_W_ID, new_no.NO_D_ID, new_no.NO_O_ID, no_key.ptr());
  Status sta = insert_pref(Storage::NEWORDER, Tuple(no_key.view(), std::move(no_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    return false;
  }
  return true;
}

PILO_PROMISE(bool) insert_neworder_pilo(uint32_t o_id, uint8_t d_id, uint16_t w_id)
{
  HeapObject no_obj;
  no_obj.allocate<TPCC::NewOrder>();
  TPCC::NewOrder& new_no = no_obj.ref();
  new_no.NO_O_ID = o_id;
  new_no.NO_D_ID = d_id;
  new_no.NO_W_ID = w_id;
  SimpleKey<8> no_key;
  TPCC::NewOrder::CreateKey(new_no.NO_W_ID, new_no.NO_D_ID, new_no.NO_O_ID, no_key.ptr());
  Status sta = PILO_AWAIT insert_pilo(Storage::NEWORDER, Tuple(no_key.view(), std::move(no_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  PILO_RETURN true;
}

PROMISE(bool) insert_neworder_coro(Token& token, uint32_t o_id, uint8_t d_id, uint16_t w_id)
{
  HeapObject no_obj;
  no_obj.allocate<TPCC::NewOrder>();
  TPCC::NewOrder& new_no = no_obj.ref();
  new_no.NO_O_ID = o_id;
  new_no.NO_D_ID = d_id;
  new_no.NO_W_ID = w_id;
  SimpleKey<8> no_key;
  TPCC::NewOrder::CreateKey(new_no.NO_W_ID, new_no.NO_D_ID, new_no.NO_O_ID, no_key.ptr());
  Status sta = AWAIT insert_coro(token, Storage::NEWORDER, Tuple(no_key.view(), std::move(no_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  RETURN true;
}


/**
 * ===========================================+
 * EXEC SQL SELECT i_price, i_name , i_data
 * INTO :i_price, :i_name, :i_data
 * FROM item
 * WHERE i_id = :ol_i_id;
 * +===========================================
 */
bool get_item(Token& token, uint32_t ol_i_id, const TPCC::Item*& item)
{
  SimpleKey<8> i_key;
  TPCC::Item::CreateKey(ol_i_id, i_key.ptr());
  Tuple *tuple;
  Status sta = search_key(token, Storage::ITEM, i_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  item = &tuple->get_value().cast_to<TPCC::Item>();
  return true;
}

bool get_item_pref(uint32_t ol_i_id, const TPCC::Item*& item)
{
  SimpleKey<8> i_key;
  TPCC::Item::CreateKey(ol_i_id, i_key.ptr());
  Tuple *tuple;
  Status sta = search_key_pref(Storage::ITEM, i_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    return false;
  }
  item = &tuple->get_value().cast_to<TPCC::Item>();
  return true;
}

PILO_PROMISE(bool) get_item_pilo(uint32_t ol_i_id, const TPCC::Item*& item)
{
  SimpleKey<8> i_key;
  TPCC::Item::CreateKey(ol_i_id, i_key.ptr());
  Tuple *tuple;
  Status sta = PILO_AWAIT search_key_pilo(Storage::ITEM, i_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  item = &tuple->get_value().cast_to<TPCC::Item>();
  PILO_RETURN true;
}

PROMISE(bool) get_item_coro(Token& token, uint32_t ol_i_id, const TPCC::Item*& item)
{
  SimpleKey<8> i_key;
  TPCC::Item::CreateKey(ol_i_id, i_key.ptr());
  Tuple *tuple;
  Status sta = AWAIT search_key_coro(token, Storage::ITEM, i_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  item = &tuple->get_value().cast_to<TPCC::Item>();
  RETURN true;
}


/** ===================================================================+
 * EXEC SQL SELECT s_quantity, s_data,
 * s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
 * s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
 * INTO :s_quantity, :s_data,
 * :s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
 * :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
 * FROM stock
 * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
 *
 * EXEC SQL UPDATE stock SET s_quantity = :s_quantity
 * WHERE s_i_id = :ol_i_id
 * AND s_w_id = :ol_supply_w_id;
 * +===============================================
 */
bool get_and_update_stock(
  Token& token, uint16_t ol_supply_w_id, uint32_t ol_i_id, uint8_t ol_quantity, bool remote,
  const TPCC::Stock*& sto)
{
    SimpleKey<8> s_key;
    TPCC::Stock::CreateKey(ol_supply_w_id, ol_i_id, s_key.ptr());
    Tuple *tuple;
    Status stat = search_key(token, Storage::STOCK, s_key.view(), &tuple);
    if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
      abort(token);
      return false;
    }
    const TPCC::Stock& old_sto = tuple->get_value().cast_to<TPCC::Stock>();

    HeapObject s_obj;
    s_obj.allocate<TPCC::Stock>();
    TPCC::Stock& new_sto = s_obj.ref();
    memcpy(&new_sto, &old_sto, sizeof(new_sto));

    new_sto.S_YTD = old_sto.S_YTD + ol_quantity;
    new_sto.S_ORDER_CNT = old_sto.S_ORDER_CNT + 1;
    if (remote) {
      new_sto.S_REMOTE_CNT = old_sto.S_REMOTE_CNT + 1;
    }

    int32_t s_quantity = old_sto.S_QUANTITY;
    int32_t quantity = s_quantity - ol_quantity;
    if (s_quantity <= ol_quantity + 10) quantity += 91;
    new_sto.S_QUANTITY = quantity;

    stat = update(token, Storage::STOCK, Tuple(s_key.view(), std::move(s_obj)), &tuple);
    if (stat == Status::WARN_NOT_FOUND) {
      abort(token);
      return false;
    }
    sto = &tuple->get_value().cast_to<TPCC::Stock>();
    return true;
}

bool get_and_update_stock_pref(
  uint16_t ol_supply_w_id, uint32_t ol_i_id, uint8_t ol_quantity, bool remote,
  const TPCC::Stock*& sto)
{
    SimpleKey<8> s_key;
    TPCC::Stock::CreateKey(ol_supply_w_id, ol_i_id, s_key.ptr());
    Tuple *tuple;
    Status stat = search_key_pref(Storage::STOCK, s_key.view(), &tuple);
    if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
      return false;
    }
    const TPCC::Stock& old_sto = tuple->get_value().cast_to<TPCC::Stock>();

    HeapObject s_obj;
    s_obj.allocate<TPCC::Stock>();
    TPCC::Stock& new_sto = s_obj.ref();
    memcpy(&new_sto, &old_sto, sizeof(new_sto));

    new_sto.S_YTD = old_sto.S_YTD + ol_quantity;
    new_sto.S_ORDER_CNT = old_sto.S_ORDER_CNT + 1;
    if (remote) {
      new_sto.S_REMOTE_CNT = old_sto.S_REMOTE_CNT + 1;
    }

    int32_t s_quantity = old_sto.S_QUANTITY;
    int32_t quantity = s_quantity - ol_quantity;
    if (s_quantity <= ol_quantity + 10) quantity += 91;
    new_sto.S_QUANTITY = quantity;
    sto = &new_sto;
    return true;
}

PILO_PROMISE(bool) get_and_update_stock_pilo(
  uint16_t ol_supply_w_id, uint32_t ol_i_id, uint8_t ol_quantity, bool remote,
  const TPCC::Stock*& sto)
{
    SimpleKey<8> s_key;
    TPCC::Stock::CreateKey(ol_supply_w_id, ol_i_id, s_key.ptr());
    Tuple *tuple;
    Status stat = PILO_AWAIT search_key_pilo(Storage::STOCK, s_key.view(), &tuple);
    if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
      PILO_RETURN false;
    }
    const TPCC::Stock& old_sto = tuple->get_value().cast_to<TPCC::Stock>();

    HeapObject s_obj;
    s_obj.allocate<TPCC::Stock>();
    TPCC::Stock& new_sto = s_obj.ref();
    memcpy(&new_sto, &old_sto, sizeof(new_sto));

    new_sto.S_YTD = old_sto.S_YTD + ol_quantity;
    new_sto.S_ORDER_CNT = old_sto.S_ORDER_CNT + 1;
    if (remote) {
      new_sto.S_REMOTE_CNT = old_sto.S_REMOTE_CNT + 1;
    }

    int32_t s_quantity = old_sto.S_QUANTITY;
    int32_t quantity = s_quantity - ol_quantity;
    if (s_quantity <= ol_quantity + 10) quantity += 91;
    new_sto.S_QUANTITY = quantity;
    sto = &new_sto;
    PILO_RETURN true;
}


PROMISE(bool) get_and_update_stock_coro(
  Token& token, uint16_t ol_supply_w_id, uint32_t ol_i_id, uint8_t ol_quantity, bool remote,
  const TPCC::Stock*& sto)
{
    SimpleKey<8> s_key;
    TPCC::Stock::CreateKey(ol_supply_w_id, ol_i_id, s_key.ptr());
    Tuple *tuple;
    Status stat = AWAIT search_key_coro(token, Storage::STOCK, s_key.view(), &tuple);
    if (stat == Status::WARN_CONCURRENT_DELETE || stat == Status::WARN_NOT_FOUND) {
      abort(token);
      RETURN false;
    }
    const TPCC::Stock& old_sto = tuple->get_value().cast_to<TPCC::Stock>();

    HeapObject s_obj;
    s_obj.allocate<TPCC::Stock>();
    TPCC::Stock& new_sto = s_obj.ref();
    memcpy(&new_sto, &old_sto, sizeof(new_sto));

    new_sto.S_YTD = old_sto.S_YTD + ol_quantity;
    new_sto.S_ORDER_CNT = old_sto.S_ORDER_CNT + 1;
    if (remote) {
      new_sto.S_REMOTE_CNT = old_sto.S_REMOTE_CNT + 1;
    }

    int32_t s_quantity = old_sto.S_QUANTITY;
    int32_t quantity = s_quantity - ol_quantity;
    if (s_quantity <= ol_quantity + 10) quantity += 91;
    new_sto.S_QUANTITY = quantity;

    stat = AWAIT update_coro(token, Storage::STOCK, Tuple(s_key.view(), std::move(s_obj)), &tuple);
    if (stat == Status::WARN_NOT_FOUND) {
      abort(token);
      RETURN false;
    }
    sto = &tuple->get_value().cast_to<TPCC::Stock>();
    RETURN true;
}


/**
 * ====================================================+
 * EXEC SQL INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_num, ol_i_id, ol_supply_w_id,
 * ol_quantity, ol_amount, ol_dist_info)
 * VALUES(:o_id, :d_id, :w_id, :ol_number,
 * :ol_i_id, :ol_supply_w_id,
 * :ol_quantity, :ol_amount, :ol_dist_info);
 * +====================================================
 */
bool insert_orderline(
  Token& token, uint32_t o_id, uint8_t d_id, uint16_t w_id,
  uint8_t ol_num, uint32_t ol_i_id, uint16_t ol_supply_w_id,
  uint8_t ol_quantity, double ol_amount, const TPCC::Stock* sto)
{
  HeapObject ol_obj;
  ol_obj.allocate<TPCC::OrderLine>();
  TPCC::OrderLine& new_ol = ol_obj.ref();
  new_ol.OL_O_ID = o_id;
  new_ol.OL_D_ID = d_id;
  new_ol.OL_W_ID = w_id;
  new_ol.OL_NUMBER = ol_num;
  new_ol.OL_I_ID = ol_i_id;
  new_ol.OL_SUPPLY_W_ID = ol_supply_w_id;
  new_ol.OL_QUANTITY = ol_quantity;
  new_ol.OL_AMOUNT = ol_amount;
  auto pick_sdist = [&]() -> const char* {
    switch (d_id) {
    case 1: return sto->S_DIST_01;
    case 2: return sto->S_DIST_02;
    case 3: return sto->S_DIST_03;
    case 4: return sto->S_DIST_04;
    case 5: return sto->S_DIST_05;
    case 6: return sto->S_DIST_06;
    case 7: return sto->S_DIST_07;
    case 8: return sto->S_DIST_08;
    case 9: return sto->S_DIST_09;
    case 10: return sto->S_DIST_10;
    default: return nullptr; // BUG
    }
  };
  copy_cstr(new_ol.OL_DIST_INFO, pick_sdist(), sizeof(new_ol.OL_DIST_INFO));

  SimpleKey<8> ol_key;
  TPCC::OrderLine::CreateKey(new_ol.OL_W_ID, new_ol.OL_D_ID, new_ol.OL_O_ID,
                             new_ol.OL_NUMBER, ol_key.ptr());
  Status sta = insert(token, Storage::ORDERLINE, Tuple(ol_key.view(), std::move(ol_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  return true;
}

bool insert_orderline_pref(
  uint32_t o_id, uint8_t d_id, uint16_t w_id,
  uint8_t ol_num, uint32_t ol_i_id, uint16_t ol_supply_w_id,
  uint8_t ol_quantity, double ol_amount, const TPCC::Stock* sto)
{
  HeapObject ol_obj;
  ol_obj.allocate<TPCC::OrderLine>();
  TPCC::OrderLine& new_ol = ol_obj.ref();
  new_ol.OL_O_ID = o_id;
  new_ol.OL_D_ID = d_id;
  new_ol.OL_W_ID = w_id;
  new_ol.OL_NUMBER = ol_num;
  new_ol.OL_I_ID = ol_i_id;
  new_ol.OL_SUPPLY_W_ID = ol_supply_w_id;
  new_ol.OL_QUANTITY = ol_quantity;
  new_ol.OL_AMOUNT = ol_amount;
  auto pick_sdist = [&]() -> const char* {
    switch (d_id) {
    case 1: return sto->S_DIST_01;
    case 2: return sto->S_DIST_02;
    case 3: return sto->S_DIST_03;
    case 4: return sto->S_DIST_04;
    case 5: return sto->S_DIST_05;
    case 6: return sto->S_DIST_06;
    case 7: return sto->S_DIST_07;
    case 8: return sto->S_DIST_08;
    case 9: return sto->S_DIST_09;
    case 10: return sto->S_DIST_10;
    default: return nullptr; // BUG
    }
  };
  copy_cstr(new_ol.OL_DIST_INFO, pick_sdist(), sizeof(new_ol.OL_DIST_INFO));

  SimpleKey<8> ol_key;
  TPCC::OrderLine::CreateKey(new_ol.OL_W_ID, new_ol.OL_D_ID, new_ol.OL_O_ID,
                             new_ol.OL_NUMBER, ol_key.ptr());
  Status sta = insert_pref(Storage::ORDERLINE, Tuple(ol_key.view(), std::move(ol_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    return false;
  }
  return true;
}

PILO_PROMISE(bool) insert_orderline_pilo(
  uint32_t o_id, uint8_t d_id, uint16_t w_id,
  uint8_t ol_num, uint32_t ol_i_id, uint16_t ol_supply_w_id,
  uint8_t ol_quantity, double ol_amount, const TPCC::Stock* sto)
{
  HeapObject ol_obj;
  ol_obj.allocate<TPCC::OrderLine>();
  TPCC::OrderLine& new_ol = ol_obj.ref();
  new_ol.OL_O_ID = o_id;
  new_ol.OL_D_ID = d_id;
  new_ol.OL_W_ID = w_id;
  new_ol.OL_NUMBER = ol_num;
  new_ol.OL_I_ID = ol_i_id;
  new_ol.OL_SUPPLY_W_ID = ol_supply_w_id;
  new_ol.OL_QUANTITY = ol_quantity;
  new_ol.OL_AMOUNT = ol_amount;
  auto pick_sdist = [&]() -> const char* {
    switch (d_id) {
    case 1: return sto->S_DIST_01;
    case 2: return sto->S_DIST_02;
    case 3: return sto->S_DIST_03;
    case 4: return sto->S_DIST_04;
    case 5: return sto->S_DIST_05;
    case 6: return sto->S_DIST_06;
    case 7: return sto->S_DIST_07;
    case 8: return sto->S_DIST_08;
    case 9: return sto->S_DIST_09;
    case 10: return sto->S_DIST_10;
    default: return nullptr; // BUG
    }
  };
  copy_cstr(new_ol.OL_DIST_INFO, pick_sdist(), sizeof(new_ol.OL_DIST_INFO));

  SimpleKey<8> ol_key;
  TPCC::OrderLine::CreateKey(new_ol.OL_W_ID, new_ol.OL_D_ID, new_ol.OL_O_ID,
                             new_ol.OL_NUMBER, ol_key.ptr());
  Status sta = PILO_AWAIT insert_pilo(Storage::ORDERLINE, Tuple(ol_key.view(), std::move(ol_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    PILO_RETURN false;
  }
  PILO_RETURN true;
}

PROMISE(bool) insert_orderline_coro(
  Token& token, uint32_t o_id, uint8_t d_id, uint16_t w_id,
  uint8_t ol_num, uint32_t ol_i_id, uint16_t ol_supply_w_id,
  uint8_t ol_quantity, double ol_amount, const TPCC::Stock* sto)
{
  HeapObject ol_obj;
  ol_obj.allocate<TPCC::OrderLine>();
  TPCC::OrderLine& new_ol = ol_obj.ref();
  new_ol.OL_O_ID = o_id;
  new_ol.OL_D_ID = d_id;
  new_ol.OL_W_ID = w_id;
  new_ol.OL_NUMBER = ol_num;
  new_ol.OL_I_ID = ol_i_id;
  new_ol.OL_SUPPLY_W_ID = ol_supply_w_id;
  new_ol.OL_QUANTITY = ol_quantity;
  new_ol.OL_AMOUNT = ol_amount;
  auto pick_sdist = [&]() -> const char* {
    switch (d_id) {
    case 1: return sto->S_DIST_01;
    case 2: return sto->S_DIST_02;
    case 3: return sto->S_DIST_03;
    case 4: return sto->S_DIST_04;
    case 5: return sto->S_DIST_05;
    case 6: return sto->S_DIST_06;
    case 7: return sto->S_DIST_07;
    case 8: return sto->S_DIST_08;
    case 9: return sto->S_DIST_09;
    case 10: return sto->S_DIST_10;
    default: return nullptr; // BUG
    }
  };
  copy_cstr(new_ol.OL_DIST_INFO, pick_sdist(), sizeof(new_ol.OL_DIST_INFO));

  SimpleKey<8> ol_key;
  TPCC::OrderLine::CreateKey(new_ol.OL_W_ID, new_ol.OL_D_ID, new_ol.OL_O_ID,
                             new_ol.OL_NUMBER, ol_key.ptr());
  Status sta = AWAIT insert_coro(token, Storage::ORDERLINE, Tuple(ol_key.view(), std::move(ol_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  RETURN true;
}


} // unnamed namespace


PROMISE(bool) run_new_order(TPCC::query::NewOrder *query, Token &token)
{
  bool remote = query->remote;
  uint16_t w_id = query->w_id;
  uint8_t d_id = query->d_id;
  uint32_t c_id = query->c_id;
  uint8_t ol_cnt = query->ol_cnt;

  const TPCC::Warehouse *ware;
#if 0
  if (!get_warehouse(token, w_id, ware)) RETURN false;
#else
  auto ret_warehouse = AWAIT get_warehouse_coro(token, w_id, ware);
  if (!ret_warehouse) RETURN false;
#endif

  const TPCC::Customer *cust;
#if 0
  if (!get_customer(token, c_id, d_id, w_id, cust)) RETURN false;
#else
  auto ret_customer = AWAIT get_customer_coro(token, c_id, d_id, w_id, cust);
  if (!ret_customer)
    RETURN false;
#endif

  const TPCC::District *dist;
#if 0
  if (!get_and_update_district(token, d_id, w_id, dist)) RETURN false;
#else
  auto ret_district = AWAIT get_and_update_district_coro(token, d_id, w_id, dist);
  if (!ret_district)
    RETURN false;
#endif

  uint32_t o_id = dist->D_NEXT_O_ID;
  [[maybe_unused]] const TPCC::Order *ord;

  if (FLAGS_insert_exe) {
#if 0
    if (!insert_order(token, o_id, d_id, w_id, c_id, ol_cnt, remote, ord)) RETURN false;
    if (!insert_neworder(token, o_id, d_id, w_id)) RETURN false;
#else
    auto ret_order = AWAIT insert_order_coro(token, o_id, d_id, w_id, c_id, ol_cnt, remote, ord);
    if (!ret_order)
      RETURN false;
    auto ret_neworder = AWAIT insert_neworder_coro(token, o_id, d_id, w_id);
    if (!ret_neworder)
      RETURN false;
#endif
  }

  for (std::uint32_t ol_num = 0; ol_num < ol_cnt; ++ol_num) {
    uint32_t ol_i_id = query->items[ol_num].ol_i_id;
    uint16_t ol_supply_w_id = query->items[ol_num].ol_supply_w_id;
    uint8_t ol_quantity = query->items[ol_num].ol_quantity;

    const TPCC::Item *item;
#if 0
    if (!get_item(token, ol_i_id, item)) RETURN false;
#else
    auto ret_item = AWAIT get_item_coro(token, ol_i_id, item);
    if (!ret_item)
      RETURN false;
#endif

    const TPCC::Stock *sto;
#if 0
    if (!get_and_update_stock(token, ol_supply_w_id, ol_i_id, ol_quantity, remote, sto)) RETURN false;
#else
    auto ret_stock = AWAIT get_and_update_stock_coro(token, ol_supply_w_id, ol_i_id, ol_quantity, remote, sto);
    if (!ret_stock)
      RETURN false;
#endif

    if (FLAGS_insert_exe) {
      double i_price = item->I_PRICE;
      double w_tax = ware->W_TAX;
      double d_tax = dist->D_TAX;
      double c_discount = cust->C_DISCOUNT;
      double ol_amount = ol_quantity * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
#if 0
      if (!insert_orderline(
            token, o_id, d_id, w_id, ol_num, ol_i_id,
            ol_supply_w_id, ol_quantity, ol_amount, sto)) RETURN false;
#else
      auto ret_orderline = AWAIT insert_orderline_coro(
						       token, o_id, d_id, w_id, ol_num, ol_i_id,
						       ol_supply_w_id, ol_quantity, ol_amount, sto);
      if (!ret_orderline)
	RETURN false;
#endif
    }
  } // end of ol loop
  if (commit(token) == Status::OK) {
    RETURN true;
  }
  abort(token);
  RETURN false;
}



PILO_PROMISE(bool) run_new_order_pilo(TPCC::query::NewOrder *query)
{
  bool remote = query->remote;
  uint16_t w_id = query->w_id;
  uint8_t d_id = query->d_id;
  uint32_t c_id = query->c_id;
  uint8_t ol_cnt = query->ol_cnt;

  const TPCC::Warehouse *ware_pref;
  auto ret_warehouse = PILO_AWAIT get_warehouse_pilo(w_id, ware_pref);
  if (!ret_warehouse) PILO_RETURN false;
  const TPCC::Customer *cust_pref;
  auto ret_customer = PILO_AWAIT get_customer_pilo(c_id, d_id, w_id, cust_pref);
  if (!ret_customer) PILO_RETURN false;
  const TPCC::District *dist_pref;
  auto ret_district = PILO_AWAIT get_and_update_district_pilo(d_id, w_id, dist_pref);
  if (!ret_district) PILO_RETURN false;
  uint32_t o_id_pref = dist_pref->D_NEXT_O_ID;
  [[maybe_unused]] const TPCC::Order *ord_pref;
  if (FLAGS_insert_exe) {
    auto ret_order = PILO_AWAIT insert_order_pilo(o_id_pref, d_id, w_id, c_id, ol_cnt, remote, ord_pref);
    if (!ret_order) PILO_RETURN false;
    auto ret_neworder = PILO_AWAIT insert_neworder_pilo(o_id_pref, d_id, w_id);
    if (!ret_neworder) PILO_RETURN false;
  }
  for (std::uint32_t ol_num = 0; ol_num < ol_cnt; ++ol_num) {
    uint32_t ol_i_id = query->items[ol_num].ol_i_id;
    uint16_t ol_supply_w_id = query->items[ol_num].ol_supply_w_id;
    uint8_t ol_quantity = query->items[ol_num].ol_quantity;

    const TPCC::Item *item;
    auto ret_item = PILO_AWAIT get_item_pilo(ol_i_id, item);
    if (!ret_item) PILO_RETURN false;

    const TPCC::Stock *sto;
    auto ret_stock = PILO_AWAIT get_and_update_stock_pilo(ol_supply_w_id, ol_i_id, ol_quantity, remote, sto);
    if (!ret_stock) PILO_RETURN false;

    if (FLAGS_insert_exe) {
      double i_price = item->I_PRICE;
      double w_tax = ware_pref->W_TAX;
      double d_tax = dist_pref->D_TAX;
      double c_discount = cust_pref->C_DISCOUNT;
      double ol_amount = ol_quantity * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
      auto ret_orderline = PILO_AWAIT insert_orderline_pilo(
							    o_id_pref, d_id, w_id, ol_num, ol_i_id,
							    ol_supply_w_id, ol_quantity, ol_amount, sto);
      if (!ret_orderline) PILO_RETURN false;
    }
  } // end of ol loop

  PILO_RETURN true;
}

  
} // namespace TPCC
