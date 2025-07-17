#pragma once

#include "transaction/mvcc_transaction/mvcc_transaction.h"

namespace COMPOSITE_STORE_NAMESPACE {

class OptimisticTransaction : public MVCCTransaction {
 public:
  // No copying allowed
  OptimisticTransaction(const OptimisticTransaction&) = delete;
  OptimisticTransaction& operator=(const OptimisticTransaction&) = delete;

  OptimisticTransaction(TransactionStore* txn_store,
                        const WriteOptions& write_options,
                        const TransactionOptions& txn_options)
      : MVCCTransaction(txn_store, write_options, txn_options) {}
  virtual ~OptimisticTransaction() {}

  virtual Status Prepare() override;
  virtual Status Commit() override;
  virtual Status Rollback() override;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
