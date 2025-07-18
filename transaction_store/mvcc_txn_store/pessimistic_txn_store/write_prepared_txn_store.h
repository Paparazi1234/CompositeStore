#pragma once

#include "pessimistic_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

class WritePreparedTxnStore : public PessimisticTxnStore {
 public:
  // No copying allowed
  WritePreparedTxnStore(const WritePreparedTxnStore&) = delete;
  WritePreparedTxnStore& operator=(const WritePreparedTxnStore&) = delete;

  WritePreparedTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const MultiVersionsManagerFactory& multi_versions_mgr_factory,
      const TxnLockManagerFactory& txn_lock_mgr_factory,
      TransactionFactory* txn_factory,
      StagingWriteFactory* staging_write_factory,
      const MVCCWriteBufferFactory& mvcc_write_buffer_factory);
  ~WritePreparedTxnStore() {}

 protected:
  uint64_t CalcuNumVersionsIncForStagingWrite(
			const StagingWrite* staging_write) const override {
    // 1 we employ seq per batch in WritePrepared policy
    // 2 when we commit an empty txn write, it will also consume a version
		return 1;
	}

  WriteQueue& CalcuPrepareQueue(bool /*enable_two_write_queues*/) override {
    // for WritePrepared txn, we use the first write queue to Prepare() no mater
    // enable two_write_queues or not, because we do write buffer insertion of
    // WritePrepared txn during Prepare()
    return first_write_queue_;
  }

  WriteQueue& CalcuCommitQueue(bool enable_two_write_queues) override {
    if (enable_two_write_queues) {
      // for WritePrepared txn, we use the second write queue to commit when
      // enable two_write_queues, because we don't perform write buffer
      // insertion of WritePrepared txn during Commit()
      return second_write_queue_;
    } else {
      // for WritePrepared txn, we also use the first write queue to commit when
      // not enable two_write_queues
      return first_write_queue_;
    }
  }
};

}   // namespace COMPOSITE_STORE_NAMESPACE
