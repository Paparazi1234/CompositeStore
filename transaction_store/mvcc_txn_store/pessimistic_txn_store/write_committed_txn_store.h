#pragma once

#include "pessimistic_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteCommittedTxnStore : public PessimisticTxnStore {
 public:
  // No copying allowed
  WriteCommittedTxnStore(const WriteCommittedTxnStore&) = delete;
  WriteCommittedTxnStore& operator=(const WriteCommittedTxnStore&) = delete;

  WriteCommittedTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const MultiVersionsManagerFactory& multi_versions_mgr_factory,
      const TxnLockManagerFactory& txn_lock_mgr_factory,
      TransactionFactory* txn_factory,
      StagingWriteFactory* staging_write_factory,
      const MVCCWriteBufferFactory& mvcc_write_buffer_factory);
  ~WriteCommittedTxnStore() {}

 protected:
  virtual uint64_t CalculateNumVersionsForWriteBatch(
			const WriteBatch* write_batch) const override {
    uint64_t count = write_batch->Count();
    // we employ version per key in WriteCommitted policy
    if (count == 0) {
      // the commitment of an empty txn write will consume a version in
      // WriteCommitted policy
      return 1;
    }
    return count; // otherwise each key will consume a version
	}

  WriteQueue& CalcuPrepareQueue(bool enable_two_write_queues) override {
    if (enable_two_write_queues) {
      // for WriteCommitted txn, we use the second write queue to Prepare() when
      // enable two_write_queues, because we don't perform write buffer
      // insertion of WriteCommitted txn during Prepare() 
      return second_write_queue_;
    } else {
      // for WriteCommitted txn, we alse use the first write queue to Prepare()
      // when not enable two_write_queues
      return first_write_queue_;
    }
  }

  WriteQueue& CalcuCommitQueue(bool /*enable_two_write_queues*/) override {
    // for WriteCommitted txn, we use the first write queue to Commit() no mater
    // enable two_write_queues or not, because we do write buffer insertion of
    // WriteCommitted txn during Commit() 
    return first_write_queue_;
  }
};

}   // namespace MULTI_VERSIONS_NAMESPACE
