#pragma once

#include <atomic>

#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCTransaction : public Transaction {
 public:
  // No copying allowed
  MVCCTransaction(const MVCCTransaction&) = delete;
  MVCCTransaction& operator=(const MVCCTransaction&) = delete;

  MVCCTransaction(TransactionStore* txn_store,
                  const WriteOptions& write_options,
                  const TransactionOptions& txn_options);
  virtual ~MVCCTransaction() {}

  virtual Status Put(const std::string& key, const std::string& value) override; 
  virtual Status Delete(const std::string& key) override;

  virtual Status Get(const ReadOptions& read_options, const std::string& key,
                     std::string* value) override;

  virtual void SetSnapshot() override;

  void Reinitialize(TransactionStore* txn_store,
                    const WriteOptions& write_options,
                    const TransactionOptions& txn_options);

  MVCCTxnStore* GetTxnStore() const {
    return txn_store_;
  }

 protected:
  class ReleaseTxnLockHandler;
  enum TransactionState : unsigned char {
    STAGE_WRITING = 0x0,
    STAGE_PREPARING = 0x1,
    STAGE_PREPARED = 0x2,
    STAGE_COMMITTING = 0x3,
    STAGE_COMMITTED = 0x4,
    STAGE_ROLLBACKING = 0x5,
    STAGE_ROLLBACKED = 0x6
  };

  Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

  void ClearTxnLocks();
  virtual void Clear() {
    // clear txn locks before clearing write_batch_, because clearing txn locks
    // depends on the write_batch_
    ClearTxnLocks();
    write_batch_.Clear();
  }

  bool IsInWriteStage() {
    return txn_state_ == STAGE_WRITING;
  }

  MVCCTxnStore* txn_store_;
  std::atomic<TransactionState> txn_state_;

  WriteOptions write_options_;
  WriteBatch write_batch_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
