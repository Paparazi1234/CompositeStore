#pragma once

#include "pessimistic_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteCommittedTransaction : public PessimisticTransaction {
 public:
  // No copying allowed
  WriteCommittedTransaction(const WriteCommittedTransaction&) = delete;
  WriteCommittedTransaction& operator=(
      const WriteCommittedTransaction&) = delete;

  WriteCommittedTransaction(TransactionStore* txn_store,
                            const WriteOptions& write_options,
                            const TransactionOptions& txn_options)
      : PessimisticTransaction(txn_store, write_options, txn_options) {}
  virtual ~WriteCommittedTransaction() {}

 private:
  virtual Status PrepareImpl() override;
  virtual Status CommitWithPrepareImpl() override;
  virtual Status CommitWithoutPrepareImpl() override;
  virtual Status RollbackImpl() override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
