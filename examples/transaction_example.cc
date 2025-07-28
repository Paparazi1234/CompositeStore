#include <assert.h>

#include "composite_store/transaction_store.h"

using COMPOSITE_STORE_NAMESPACE::Status;
using COMPOSITE_STORE_NAMESPACE::TransactionStore;
using COMPOSITE_STORE_NAMESPACE::StoreOptions;
using COMPOSITE_STORE_NAMESPACE::TransactionStoreOptions;
using COMPOSITE_STORE_NAMESPACE::StoreTraits;
using COMPOSITE_STORE_NAMESPACE::TransactionOptions;
using COMPOSITE_STORE_NAMESPACE::WriteOptions;
using COMPOSITE_STORE_NAMESPACE::ReadOptions;
using COMPOSITE_STORE_NAMESPACE::Transaction;

int main() {
  TransactionStore* txn_store_ptr;
  StoreOptions store_options;
  TransactionStoreOptions txn_store_options;
  StoreTraits store_traits;

  // open txn store
  Status s = TransactionStore::Open(
    store_options, txn_store_options, store_traits, &txn_store_ptr);
  assert(s.IsOK());

  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  // begin transaction
  Transaction* txn = txn_store_ptr->BeginTransaction(write_options);
  assert(txn != nullptr);

  s = txn->Put("foo", "bar");
  assert(s.IsOK());

  // read txn's own writes
  s = txn->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar");

  // non-transactional read
  s = txn_store_ptr->Get(read_options, "foo", &value);
  assert(s.IsNotFound());

  // commit(2PC)
  s = txn->Prepare();
  assert(s.IsOK());
  s = txn->Commit();
  assert(s.IsOK());

  // transactional read
  s = txn->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar");

  // non-transactional read
  s = txn_store_ptr->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar");

  // release transaction and close txn store
  delete txn;
  delete txn_store_ptr;
  return 0;
}