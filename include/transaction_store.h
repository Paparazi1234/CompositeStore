#pragma once

#include "store.h"
#include "transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TransactionStore : public Store {
 public:
  static Status Open(const StoreOptions& store_options,
                     const TransactionStoreOptions& txn_store_options,
                     const StoreTraits& store_traits,
                     TransactionStore** txn_store_ptr);

  // No copying allowed
  TransactionStore(const TransactionStore&) = delete;
  TransactionStore& operator=(const TransactionStore&) = delete;

  TransactionStore() {}
  virtual ~TransactionStore() {}

  virtual Transaction* BeginTransaction(
      const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions(),
      Transaction* reused = nullptr) = 0;

  virtual const Snapshot* TakeSnapshot() = 0;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
