#pragma once

#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class PessimisticTxnStore : public MVCCTxnStore {
 public:
  // No copying allowed
  PessimisticTxnStore(const PessimisticTxnStore&) = delete;
  PessimisticTxnStore& operator=(const PessimisticTxnStore&) = delete;

  PessimisticTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const MultiVersionsManagerFactory& multi_versions_mgr_factory,
      const TxnLockManagerFactory& txn_lock_mgr_factory,
      TransactionFactory* txn_factory,
      WriteQueue& prepare_queue,
      WriteQueue& commit_queue);

  WriteQueue& GetPrepareQueue() {
    return prepare_queue_;
  }

  WriteQueue& GetCommitQueue() {
    return commit_queue_;
  }

  bool IsEnableTwoWriteQueues() const {
    return enable_two_write_queues_;
  }
 protected:
  virtual WriteQueue& CalcuPrepareQueue(bool enable_two_write_queues) = 0;
  virtual WriteQueue& CalcuCommitQueue(bool enable_two_write_queues) = 0;

  bool enable_two_write_queues_;
  WriteQueue& prepare_queue_;
  WriteQueue& commit_queue_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
