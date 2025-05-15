#pragma once

#include "store.h"
#include "transaction.h"
#include "snapshot.h"

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
};

}   // namespace MULTI_VERSIONS_NAMESPACE
