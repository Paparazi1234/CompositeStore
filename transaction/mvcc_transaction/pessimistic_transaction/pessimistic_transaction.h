#pragma once

#include "transaction/mvcc_transaction/mvcc_transaction.h"
#include "transaction_store/mvcc_txn_store/pessimistic_txn_store/pessimistic_txn_store.h"
#include "util/cast_util.h"

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

  virtual Status TryLock(const std::string& key, bool exclusive,
                         int64_t timeout_time_ms) override;

  virtual void Clear() override {
    // first do current class's own cleanup
    PessimisticTxnStore* txn_store_impl =
        static_cast_with_check<PessimisticTxnStore>(GetTxnStore());
    TxnLockManager* txn_lock_manager = txn_store_impl->GetTxnLockManager();
    txn_lock_manager->UnLock(TxnId(), *GetTxnLockTracker());

    // then invoke direct parent's Clear()
    MVCCTransaction::Clear();
  }
};

}   // namespace COMPOSITE_STORE_NAMESPACE
