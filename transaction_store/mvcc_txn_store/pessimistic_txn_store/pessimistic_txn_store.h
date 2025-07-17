#pragma once

#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

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
      StagingWriteFactory* staging_write_factory,
      const MVCCWriteBufferFactory& mvcc_write_buffer_factory,
      WriteQueue& prepare_queue,
      WriteQueue& commit_queue);

  virtual WriteQueue& GetPrepareQueue() {
    return prepare_queue_;
  }

  virtual WriteQueue& GetCommitQueue() {
    return commit_queue_;
  }

  virtual bool EnableTwoWriteQueues() const {
    return enable_two_write_queues_;
  }
 protected:
  virtual WriteQueue& CalcuPrepareQueue(bool enable_two_write_queues) = 0;
  virtual WriteQueue& CalcuCommitQueue(bool enable_two_write_queues) = 0;

  bool enable_two_write_queues_;
  WriteQueue& prepare_queue_;
  WriteQueue& commit_queue_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
