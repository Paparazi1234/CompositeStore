#pragma once

#include "store.h"
#include "transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TransactionStore : public Store {
 public:
  static Status Open(TransactionStore** store_ptr);

  // No copying allowed
  TransactionStore(const TransactionStore&) = delete;
  TransactionStore& operator=(const TransactionStore&) = delete;

  TransactionStore() {}
  virtual ~TransactionStore() {}

  virtual Transaction* BeginTransaction(
      const TransactionOptions& txn_options,
      const WriteOptions& write_options) = 0;

  virtual const Snapshot* TakeSnapshot() = 0;

  virtual Status TryLock(const std::string& key) = 0;
  virtual void UnLock(const std::string& key) = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
