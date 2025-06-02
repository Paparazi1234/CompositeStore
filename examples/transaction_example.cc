#include <assert.h>

#include "include/transaction_store.h"

using MULTI_VERSIONS_NAMESPACE::Status;
using MULTI_VERSIONS_NAMESPACE::TransactionStore;
using MULTI_VERSIONS_NAMESPACE::StoreOptions;
using MULTI_VERSIONS_NAMESPACE::TransactionStoreOptions;
using MULTI_VERSIONS_NAMESPACE::StoreTraits;
using MULTI_VERSIONS_NAMESPACE::TransactionOptions;
using MULTI_VERSIONS_NAMESPACE::WriteOptions;
using MULTI_VERSIONS_NAMESPACE::ReadOptions;
using MULTI_VERSIONS_NAMESPACE::Transaction;

int main() {
  TransactionStore* txn_store_ptr;
  StoreOptions store_options;
  TransactionStoreOptions txn_store_options;
  StoreTraits store_traits;

  // open txn store
  Status s = TransactionStore::Open(
    store_options, txn_store_options, store_traits, &txn_store_ptr);
  assert(s.IsOK());

  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  // begin transaction
  Transaction* txn =
      txn_store_ptr->BeginTransaction(txn_options, write_options);
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