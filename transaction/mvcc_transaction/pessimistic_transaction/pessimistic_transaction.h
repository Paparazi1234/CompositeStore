#pragma once

#include "transaction/mvcc_transaction/mvcc_transaction.h"

namespace COMPOSITE_STORE_NAMESPACE {

class PessimisticTransaction : public MVCCTransaction {
 public:
  // No copying allowed
  PessimisticTransaction(const PessimisticTransaction&) = delete;
  PessimisticTransaction& operator=(const PessimisticTransaction&) = delete;

  PessimisticTransaction(TransactionStore* txn_store,
                         const WriteOptions& write_options,
                         const TransactionOptions& txn_options)
      : MVCCTransaction(txn_store, write_options, txn_options) {}
  virtual ~PessimisticTransaction() {}

  virtual Status Prepare() override;
  virtual Status Commit() override;
  virtual Status Rollback() override;

 protected:
  virtual Status PrepareImpl() = 0;
  virtual Status CommitWithPrepareImpl() = 0;
  virtual Status CommitWithoutPrepareImpl() = 0;
  virtual Status RollbackImpl() = 0;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
