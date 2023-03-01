/**
 * @file payment.cpp
 */

#include "interface.h"
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#include "tpcc/tpcc_query.hpp"


using namespace ccbench;

namespace TPCC {


namespace {


/**
 * ====================================================+
 * EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
 *   WHERE w_id=:w_id;
 * EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
 * INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
 * FROM warehouse
 * WHERE w_id=:w_id;
 * +===================================================================
 */
bool get_and_update_warehouse(
  Token& token, uint16_t w_id, double h_amount,
  const TPCC::Warehouse*& ware)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status sta = search_key(token, Storage::WAREHOUSE, w_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  TPCC::Warehouse& old_ware = tuple->get_value().cast_to<TPCC::Warehouse>();

  HeapObject w_obj;
  w_obj.allocate<TPCC::Warehouse>();
  TPCC::Warehouse& new_ware = w_obj.ref();
  memcpy(&new_ware, &old_ware, sizeof(new_ware));

  new_ware.W_YTD = old_ware.W_YTD + h_amount;

  sta = update(token, Storage::WAREHOUSE, Tuple(w_key.view(), std::move(w_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  return true;
}

bool get_and_update_warehouse_myrw(
  Token& token, uint16_t w_id, double h_amount,
  const TPCC::Warehouse*& ware, MyRW *myrw)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status sta = search_key_myrw(token, Storage::WAREHOUSE, w_key.view(), &tuple, myrw);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  TPCC::Warehouse& old_ware = tuple->get_value().cast_to<TPCC::Warehouse>();

  HeapObject w_obj;
  w_obj.allocate<TPCC::Warehouse>();
  TPCC::Warehouse& new_ware = w_obj.ref();
  memcpy(&new_ware, &old_ware, sizeof(new_ware));

  new_ware.W_YTD = old_ware.W_YTD + h_amount;

  sta = update(token, Storage::WAREHOUSE, Tuple(w_key.view(), std::move(w_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  return true;
}
  
PROMISE(bool) get_and_update_warehouse_coro(
  Token& token, uint16_t w_id, double h_amount,
  const TPCC::Warehouse*& ware)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Tuple *tuple;
  Status sta = AWAIT search_key_coro(token, Storage::WAREHOUSE, w_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  TPCC::Warehouse& old_ware = tuple->get_value().cast_to<TPCC::Warehouse>();

  HeapObject w_obj;
  w_obj.allocate<TPCC::Warehouse>();
  TPCC::Warehouse& new_ware = w_obj.ref();
  memcpy(&new_ware, &old_ware, sizeof(new_ware));

  new_ware.W_YTD = old_ware.W_YTD + h_amount;

  sta = AWAIT update_coro(token, Storage::WAREHOUSE, Tuple(w_key.view(), std::move(w_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  ware = &tuple->get_value().cast_to<TPCC::Warehouse>();
  RETURN true;
}

PTX_PROMISE(bool) get_and_update_warehouse_ptx(
						 pobjs_t &pobjs,
  uint16_t w_id, double h_amount,
						 const TPCC::Warehouse*& ware, MyRW *myrw = nullptr)
{
  SimpleKey<8> w_key;
  TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
  Record *rec = nullptr;
  Status sta = PTX_AWAIT search_key_ptx(Storage::WAREHOUSE, w_key.view(), &rec);
#if MYRW
  myrw->rd(Storage::WAREHOUSE, w_key.view(), rec);
#endif
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    PTX_RETURN false;
  }
  TPCC::Warehouse& old_ware = rec->get_tuple().get_value().cast_to<TPCC::Warehouse>();

  HeapObject w_obj;
  w_obj.allocate<TPCC::Warehouse>();
  TPCC::Warehouse& new_ware = w_obj.ref();
  memcpy(&new_ware, &old_ware, sizeof(new_ware));

  new_ware.W_YTD = old_ware.W_YTD + h_amount;
  ware = &new_ware;
  pobjs.emplace_back(std::move(w_obj));
  PTX_RETURN true;
}
  

/** =====================================================+
 * EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
 * WHERE d_w_id=:w_id AND d_id=:d_id;
 * EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
 * INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
 * FROM district
 * WHERE d_w_id=:w_id AND d_id=:d_id;
 * +====================================================================
 */
bool get_and_update_district(
  Token& token, uint8_t d_id, uint16_t w_id, double h_amount,
  const TPCC::District*& dist)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Tuple *tuple;
  Status sta = search_key(token, Storage::DISTRICT, d_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  TPCC::District& old_dist = tuple->get_value().cast_to<TPCC::District>();

  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));

  new_dist.D_YTD += h_amount;

  sta = update(token, Storage::DISTRICT, Tuple(d_key.view(), std::move(d_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  dist = &tuple->get_value().cast_to<TPCC::District>();
  return true;
}

bool get_and_update_district_myrw(
  Token& token, uint8_t d_id, uint16_t w_id, double h_amount,
  const TPCC::District*& dist, MyRW *myrw)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Tuple *tuple;
  Status sta = search_key_myrw(token, Storage::DISTRICT, d_key.view(), &tuple, myrw);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  TPCC::District& old_dist = tuple->get_value().cast_to<TPCC::District>();

  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));

  new_dist.D_YTD += h_amount;

  sta = update(token, Storage::DISTRICT, Tuple(d_key.view(), std::move(d_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  dist = &tuple->get_value().cast_to<TPCC::District>();
  return true;
}


PROMISE(bool) get_and_update_district_coro(
  Token& token, uint8_t d_id, uint16_t w_id, double h_amount,
  const TPCC::District*& dist)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Tuple *tuple;
  Status sta = AWAIT search_key_coro(token, Storage::DISTRICT, d_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  TPCC::District& old_dist = tuple->get_value().cast_to<TPCC::District>();

  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));

  new_dist.D_YTD += h_amount;

  sta = AWAIT update_coro(token, Storage::DISTRICT, Tuple(d_key.view(), std::move(d_obj)), &tuple);
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  dist = &tuple->get_value().cast_to<TPCC::District>();
  RETURN true;
}

PTX_PROMISE(bool) get_and_update_district_ptx(
						pobjs_t &pobjs,
  uint8_t d_id, uint16_t w_id, double h_amount,
						const TPCC::District*& dist, MyRW *myrw = nullptr)
{
  SimpleKey<8> d_key;
  TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
  Record *rec = nullptr;
  Status sta = PTX_AWAIT search_key_ptx(Storage::DISTRICT, d_key.view(), &rec);
#if MYRW
  myrw->rd(Storage::DISTRICT, d_key.view(), rec);
#endif
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    PTX_RETURN false;
  }
  TPCC::District& old_dist = rec->get_tuple().get_value().cast_to<TPCC::District>();

  HeapObject d_obj;
  d_obj.allocate<TPCC::District>();
  TPCC::District& new_dist = d_obj.ref();
  memcpy(&new_dist, &old_dist, sizeof(new_dist));

  new_dist.D_YTD += h_amount;

  dist = &new_dist;
  pobjs.emplace_back(std::move(d_obj));
  PTX_RETURN true;
}
  

/**
 * ==========================================================
 * EXEC SQL SELECT count(c_id) INTO :namecnt
 * FROM customer
 * WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
 * EXEC SQL DECLARE c_byname CURSOR FOR
 * SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
 * c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
 * FROM customer
 * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
 * ORDER BY c_first;
 * EXEC SQL OPEN c_byname;
 * if (namecnt%2) namecnt++; // Locate midpoint customer;
 * for (n=0; n<namecnt/2; n++) {
 * EXEC SQL FETCH c_byname
 * INTO :c_first, :c_middle, :c_id,
 * :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
 * :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
 * }
 * EXEC SQL CLOSE c_byname;
 * ==========================================================
 */
bool get_customer_key_by_last_name(
  uint16_t c_w_id, uint8_t c_d_id, const char* c_last, SimpleKey<8>& c_key)
{
  char c_last_key_buf[Customer::CLastKey::required_size()];
  std::string_view c_last_key = Customer::CreateSecondaryKey(c_w_id, c_d_id, c_last, &c_last_key_buf[0]);
  void *ret_ptr = kohler_masstree::find_record(Storage::SECONDARY, c_last_key);
  assert(ret_ptr != nullptr);
  std::vector<SimpleKey<8>> *vec_ptr;
  std::string_view value_view = reinterpret_cast<Record *>(ret_ptr)->get_tuple().get_val();
  assert(value_view.size() == sizeof(uintptr_t));
  ::memcpy(&vec_ptr, value_view.data(), sizeof(uintptr_t));
  size_t nr_same_name = vec_ptr->size();
  assert(nr_same_name > 0);
  size_t idx = (nr_same_name + 1) / 2 - 1; // midpoint.
  c_key = (*vec_ptr)[idx];
  return true;
}

bool get_customer_key_by_last_name_myrw(
					uint16_t c_w_id, uint8_t c_d_id, const char* c_last, SimpleKey<8>& c_key, MyRW *myrw)
{
  char c_last_key_buf[Customer::CLastKey::required_size()];
  std::string_view c_last_key = Customer::CreateSecondaryKey(c_w_id, c_d_id, c_last, &c_last_key_buf[0]);
  Record *pfrec = myrw->get_pfrec(Storage::SECONDARY, c_last_key);
  void *ret_ptr;
  if (pfrec) {
    ret_ptr = pfrec;
  } else {
    ret_ptr = kohler_masstree::find_record(Storage::SECONDARY, c_last_key);
  }
  assert(ret_ptr != nullptr);
  std::vector<SimpleKey<8>> *vec_ptr;
  std::string_view value_view = reinterpret_cast<Record *>(ret_ptr)->get_tuple().get_val();
  assert(value_view.size() == sizeof(uintptr_t));
  ::memcpy(&vec_ptr, value_view.data(), sizeof(uintptr_t));
  size_t nr_same_name = vec_ptr->size();
  assert(nr_same_name > 0);
  size_t idx = (nr_same_name + 1) / 2 - 1; // midpoint.
  c_key = (*vec_ptr)[idx];
  return true;
}

PROMISE(bool) get_customer_key_by_last_name_coro(
  uint16_t c_w_id, uint8_t c_d_id, const char* c_last, SimpleKey<8>& c_key)
{
  char c_last_key_buf[Customer::CLastKey::required_size()];
  std::string_view c_last_key = Customer::CreateSecondaryKey(c_w_id, c_d_id, c_last, &c_last_key_buf[0]);
  void *ret_ptr = AWAIT kohler_masstree::find_record_coro(Storage::SECONDARY, c_last_key);
  assert(ret_ptr != nullptr);
  std::vector<SimpleKey<8>> *vec_ptr;
  std::string_view value_view = reinterpret_cast<Record *>(ret_ptr)->get_tuple().get_val();
  assert(value_view.size() == sizeof(uintptr_t));
  ::memcpy(&vec_ptr, value_view.data(), sizeof(uintptr_t));
  size_t nr_same_name = vec_ptr->size();
  assert(nr_same_name > 0);
  size_t idx = (nr_same_name + 1) / 2 - 1; // midpoint.
  c_key = (*vec_ptr)[idx];
  RETURN true;
}

PTX_PROMISE(bool) get_customer_key_by_last_name_ptx(
						      uint16_t c_w_id, uint8_t c_d_id, const char* c_last, SimpleKey<8>& c_key, MyRW *myrw)
{
  char c_last_key_buf[Customer::CLastKey::required_size()];
  std::string_view c_last_key = Customer::CreateSecondaryKey(c_w_id, c_d_id, c_last, &c_last_key_buf[0]);
  void *ret_ptr = PTX_AWAIT kohler_masstree::find_record_ptx(Storage::SECONDARY, c_last_key);
  assert(ret_ptr != nullptr);
#if MYRW
  myrw->rd(Storage::SECONDARY, c_last_key, reinterpret_cast<Record *>(ret_ptr));
#endif
  std::vector<SimpleKey<8>> *vec_ptr;
  std::string_view value_view = reinterpret_cast<Record *>(ret_ptr)->get_tuple().get_val();
  assert(value_view.size() == sizeof(uintptr_t));
  ::memcpy(&vec_ptr, value_view.data(), sizeof(uintptr_t));
  size_t nr_same_name = vec_ptr->size();
  assert(nr_same_name > 0);
  size_t idx = (nr_same_name + 1) / 2 - 1; // midpoint.
  c_key = (*vec_ptr)[idx];
  PTX_RETURN true;
}



/** ==========================================================
 * EXEC SQL SELECT c_first, c_middle, c_last,
 * c_street_1, c_street_2, c_city, c_state, c_zip,
 * c_phone, c_credit, c_credit_lim,
 * c_discount, c_balance, c_since
 * INTO :c_first, :c_middle, :c_last,
 * :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
 * :c_phone, :c_credit, :c_credit_lim,
 * :c_discount, :c_balance, :c_since
 * FROM customer
 * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
 * ==========================================================
 *
 * ==========================================================
 * EXEC SQL SELECT c_data INTO :c_data
 * FROM customer
 * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
 * sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f %12c %24c",
 * c_id,c_d_id,c_w_id,d_id,w_id,h_amount
 * h_date, h_data);
 * strncat(c_new_data,c_data,500-strlen(c_new_data));
 * ==========================================================
 *
 * ==========================================================
 * EXEC SQL UPDATE customer
 * SET c_balance = :c_balance, c_data = :c_new_data
 * WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND
 * c_id = :c_id;
 * ==========================================================
 */
bool get_and_update_customer(
  Token& token, const SimpleKey<8>& c_key,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount)
{
  Tuple *tuple;
  Status sta = search_key(token, Storage::CUSTOMER, c_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  const TPCC::Customer& old_cust = tuple->get_value().cast_to<TPCC::Customer>();

  HeapObject c_obj;
  c_obj.allocate<TPCC::Customer>();
  TPCC::Customer& new_cust = c_obj.ref();
  ::memcpy(&new_cust, &old_cust, sizeof(new_cust));

  new_cust.C_BALANCE += h_amount;
  new_cust.C_YTD_PAYMENT += h_amount;
  new_cust.C_PAYMENT_CNT += 1;

  if (new_cust.C_CREDIT[0] == 'B' && new_cust.C_CREDIT[1] == 'C') {
    size_t len = snprintf(
      &new_cust.C_DATA[0], 501,
      "| %4" PRIu32 " %2" PRIu8 " %4" PRIu16 " %2" PRIu16 " %4" PRIu16 " $%7.2f",
      c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
    assert(len <= 500);
    len += copy_cstr(&new_cust.C_DATA[len], &old_cust.C_DATA[0], 501 - len);
  }

  sta = update(token, Storage::CUSTOMER, Tuple(c_key.view(), std::move(c_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  return true;
}

bool get_and_update_customer_myrw(
  Token& token, const SimpleKey<8>& c_key,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount, MyRW *myrw)
{
  Tuple *tuple;
  Status sta = search_key_myrw(token, Storage::CUSTOMER, c_key.view(), &tuple, myrw);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  const TPCC::Customer& old_cust = tuple->get_value().cast_to<TPCC::Customer>();

  HeapObject c_obj;
  c_obj.allocate<TPCC::Customer>();
  TPCC::Customer& new_cust = c_obj.ref();
  ::memcpy(&new_cust, &old_cust, sizeof(new_cust));

  new_cust.C_BALANCE += h_amount;
  new_cust.C_YTD_PAYMENT += h_amount;
  new_cust.C_PAYMENT_CNT += 1;

  if (new_cust.C_CREDIT[0] == 'B' && new_cust.C_CREDIT[1] == 'C') {
    size_t len = snprintf(
      &new_cust.C_DATA[0], 501,
      "| %4" PRIu32 " %2" PRIu8 " %4" PRIu16 " %2" PRIu16 " %4" PRIu16 " $%7.2f",
      c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
    assert(len <= 500);
    len += copy_cstr(&new_cust.C_DATA[len], &old_cust.C_DATA[0], 501 - len);
  }

  sta = update(token, Storage::CUSTOMER, Tuple(c_key.view(), std::move(c_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  return true;
}


PROMISE(bool) get_and_update_customer_coro(
  Token& token, const SimpleKey<8>& c_key,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount)
{
  Tuple *tuple;
  Status sta = AWAIT search_key_coro(token, Storage::CUSTOMER, c_key.view(), &tuple);
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  const TPCC::Customer& old_cust = tuple->get_value().cast_to<TPCC::Customer>();

  HeapObject c_obj;
  c_obj.allocate<TPCC::Customer>();
  TPCC::Customer& new_cust = c_obj.ref();
  ::memcpy(&new_cust, &old_cust, sizeof(new_cust));

  new_cust.C_BALANCE += h_amount;
  new_cust.C_YTD_PAYMENT += h_amount;
  new_cust.C_PAYMENT_CNT += 1;

  if (new_cust.C_CREDIT[0] == 'B' && new_cust.C_CREDIT[1] == 'C') {
    size_t len = snprintf(
      &new_cust.C_DATA[0], 501,
      "| %4" PRIu32 " %2" PRIu8 " %4" PRIu16 " %2" PRIu16 " %4" PRIu16 " $%7.2f",
      c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
    assert(len <= 500);
    len += copy_cstr(&new_cust.C_DATA[len], &old_cust.C_DATA[0], 501 - len);
  }

  sta = AWAIT update_coro(token, Storage::CUSTOMER, Tuple(c_key.view(), std::move(c_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  RETURN true;
}

PTX_PROMISE(bool) get_and_update_customer_ptx(
  const SimpleKey<8>& c_key,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount, MyRW *myrw = nullptr)
{
  Record *rec = nullptr;
  Status sta = PTX_AWAIT search_key_ptx(Storage::CUSTOMER, c_key.view(), &rec);
#if MYRW
  myrw->rd(Storage::CUSTOMER, c_key.view(), rec);
#endif
  if (sta == Status::WARN_CONCURRENT_DELETE || sta == Status::WARN_NOT_FOUND) {
    PTX_RETURN false;
  }
  PTX_RETURN true;
}


/**
 * ================================================================================
 * EXEC SQL INSERT INTO
 * history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
 * VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
 * ================================================================================
 */
bool insert_history(
  Token& token,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount, const char* w_name, const char* d_name,
  HistoryKeyGenerator *hkg)
{
  HeapObject h_obj;
  h_obj.allocate<TPCC::History>();
  TPCC::History& new_hist = h_obj.ref();
  new_hist.H_C_ID = c_id;
  new_hist.H_C_D_ID = c_d_id;
  new_hist.H_C_W_ID = c_w_id;
  new_hist.H_D_ID = d_id;
  new_hist.H_W_ID = w_id;
  new_hist.H_DATE = ccbench::epoch::get_lightweight_timestamp();
  new_hist.H_AMOUNT = h_amount;
  ::snprintf(new_hist.H_DATA, sizeof(new_hist.H_DATA),
             "%-10.10s    %.10s", w_name, d_name);

  SimpleKey<8> h_key = hkg->get_as_simple_key();
  Status sta = insert(token, Storage::HISTORY, Tuple(h_key.view(), std::move(h_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    return false;
  }
  return true;
}


PROMISE(bool) insert_history_coro(
  Token& token,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount, const char* w_name, const char* d_name,
  HistoryKeyGenerator *hkg)
{
  HeapObject h_obj;
  h_obj.allocate<TPCC::History>();
  TPCC::History& new_hist = h_obj.ref();
  new_hist.H_C_ID = c_id;
  new_hist.H_C_D_ID = c_d_id;
  new_hist.H_C_W_ID = c_w_id;
  new_hist.H_D_ID = d_id;
  new_hist.H_W_ID = w_id;
  new_hist.H_DATE = ccbench::epoch::get_lightweight_timestamp();
  new_hist.H_AMOUNT = h_amount;
  ::snprintf(new_hist.H_DATA, sizeof(new_hist.H_DATA),
             "%-10.10s    %.10s", w_name, d_name);

  SimpleKey<8> h_key = hkg->get_as_simple_key();
  Status sta = AWAIT insert_coro(token, Storage::HISTORY, Tuple(h_key.view(), std::move(h_obj)));
  if (sta == Status::WARN_NOT_FOUND) {
    abort(token);
    RETURN false;
  }
  RETURN true;
}


PTX_PROMISE(bool) insert_history_ptx(
						pobjs_t &pobjs,
  uint32_t c_id, uint8_t c_d_id, uint16_t c_w_id, uint8_t d_id, uint16_t w_id,
  double h_amount, const char* w_name, const char* d_name,
  HistoryKeyGenerator *hkg)
{
  HeapObject h_obj;
  h_obj.allocate<TPCC::History>();
  TPCC::History& new_hist = h_obj.ref();
  new_hist.H_C_ID = c_id;
  new_hist.H_C_D_ID = c_d_id;
  new_hist.H_C_W_ID = c_w_id;
  new_hist.H_D_ID = d_id;
  new_hist.H_W_ID = w_id;
  new_hist.H_DATE = ccbench::epoch::get_lightweight_timestamp();
  new_hist.H_AMOUNT = h_amount;
  ::snprintf(new_hist.H_DATA, sizeof(new_hist.H_DATA),
             "%-10.10s    %.10s", w_name, d_name);

  SimpleKey<8> h_key = hkg->get_as_simple_key();
  Status sta = PTX_AWAIT insert_ptx(Storage::HISTORY, Tuple(h_key.view(), std::move(h_obj)));
  pobjs.emplace_back(std::move(h_obj));
  if (sta == Status::WARN_NOT_FOUND) {
    PTX_RETURN false;
  }
  PTX_RETURN true;
}

} // unnamed namespace


PROMISE(bool) run_payment(query::Payment *query, HistoryKeyGenerator *hkg, Token &token, MyRW *myrw)
{
  uint16_t w_id = query->w_id;
  uint16_t c_w_id = query->c_w_id;
  uint8_t d_id = query->d_id;
  uint32_t c_id = query->c_id;
  uint8_t c_d_id = query->c_d_id;
  double h_amount = query->h_amount;

  const TPCC::Warehouse *ware;
#if MYRW
  auto ret_warehouse = get_and_update_warehouse_myrw(token, w_id, h_amount, ware, myrw);
#else
  auto ret_warehouse = AWAIT get_and_update_warehouse_coro(token, w_id, h_amount, ware);
#endif
  if (!ret_warehouse) RETURN false;
  const TPCC::District *dist;
#if MYRW
  auto ret_district = get_and_update_district_myrw(token, d_id, w_id, h_amount, dist, myrw);
#else
  auto ret_district = AWAIT get_and_update_district_coro(token, d_id, w_id, h_amount, dist);
#endif
  if (!ret_district) RETURN false;

  SimpleKey<8> c_key;
  if (query->by_last_name) {
#if MYRW
    auto ret_custkey = AWAIT get_customer_key_by_last_name_myrw(c_w_id, c_d_id, query->c_last, c_key, myrw);
#else
    auto ret_custkey = AWAIT get_customer_key_by_last_name_coro(c_w_id, c_d_id, query->c_last, c_key);
#endif
    if (!ret_custkey) RETURN false;
  } else {
    // search customers by c_id
    TPCC::Customer::CreateKey(c_w_id, c_d_id, c_id, c_key.ptr());
  }
#if MYRW
  auto ret_customer = get_and_update_customer_myrw(
						   token, c_key, c_id, c_d_id, c_w_id, d_id, w_id, h_amount, myrw);
#else
  auto ret_customer = AWAIT get_and_update_customer_coro(
					      token, c_key, c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
#endif
  if (!ret_customer) RETURN false;

  if (FLAGS_insert_exe) {
    auto ret_history = AWAIT insert_history_coro(
          token, c_id, c_d_id, c_w_id, d_id, w_id, h_amount,
          &ware->W_NAME[0], &dist->D_NAME[0], hkg);
    if (!ret_history) RETURN false;
  }

  if (commit(token) == Status::OK) {
    RETURN true;
  }
  abort(token);
  RETURN false;
}



PTX_PROMISE(void) run_payment_ptx2(query::Payment *query)
{
  uint16_t w_id = query->w_id;
  uint16_t c_w_id = query->c_w_id;
  uint8_t d_id = query->d_id;
  uint32_t c_id = query->c_id;
  uint8_t c_d_id = query->c_d_id;
  double h_amount = query->h_amount;

  Record *w_ptr;
  if (1) {
    SimpleKey<8> w_key;
    TPCC::Warehouse::CreateKey(w_id, w_key.ptr());
    PTX_AWAIT kohler_masstree::get_mtdb(Storage::WAREHOUSE).get_value_ptx_flat(w_key.view());
  }

  if (1) {
    SimpleKey<8> d_key;
    TPCC::District::CreateKey(w_id, d_id, d_key.ptr());
    PTX_AWAIT kohler_masstree::get_mtdb(Storage::DISTRICT).get_value_ptx_flat(d_key.view());
  }

  
  PTX_RETURN;
}


PTX_PROMISE(bool) run_payment_ptx(query::Payment *query, HistoryKeyGenerator *hkg, MyRW *myrw = nullptr)
{
  uint16_t w_id = query->w_id;
  uint16_t c_w_id = query->c_w_id;
  uint8_t d_id = query->d_id;
  uint32_t c_id = query->c_id;
  uint8_t c_d_id = query->c_d_id;
  double h_amount = query->h_amount;

  pobjs_t pobjs;

  const TPCC::Warehouse *ware;
  auto ret_warehouse = PTX_AWAIT get_and_update_warehouse_ptx(pobjs, w_id, h_amount, ware, myrw);
  if (!ret_warehouse) PTX_RETURN ret_false_ptx(pobjs);
  const TPCC::District *dist;
  auto ret_district = PTX_AWAIT get_and_update_district_ptx(pobjs, d_id, w_id, h_amount, dist, myrw);
  if (!ret_district) PTX_RETURN ret_false_ptx(pobjs);

  SimpleKey<8> c_key;
  if (query->by_last_name) {
    auto ret_custkey = PTX_AWAIT get_customer_key_by_last_name_ptx(c_w_id, c_d_id, query->c_last, c_key, myrw);
    if (!ret_custkey) PTX_RETURN ret_false_ptx(pobjs);
  } else {
    // search customers by c_id
    TPCC::Customer::CreateKey(c_w_id, c_d_id, c_id, c_key.ptr());
  }
  auto ret_customer = PTX_AWAIT get_and_update_customer_ptx(
							      c_key, c_id, c_d_id, c_w_id, d_id, w_id, h_amount, myrw);
  if (!ret_customer) PTX_RETURN ret_false_ptx(pobjs);

  if (FLAGS_insert_exe) {
    auto ret_history = PTX_AWAIT insert_history_ptx(pobjs,
          c_id, c_d_id, c_w_id, d_id, w_id, h_amount,
          &ware->W_NAME[0], &dist->D_NAME[0], hkg);
    if (!ret_history) PTX_RETURN ret_false_ptx(pobjs);
  }

  free_pobjs(pobjs);
  PTX_RETURN true;
}


} // namespace TPCC
