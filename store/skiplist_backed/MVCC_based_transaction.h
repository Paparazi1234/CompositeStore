#pragma once

#include <atomic>

#include "skiplist_backed_in_memory_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCBasedTransaction : public Transaction {
 public:
  // No copying allowed
  MVCCBasedTransaction(const MVCCBasedTransaction&) = delete;
  MVCCBasedTransaction& operator=(const MVCCBasedTransaction&) = delete;

  MVCCBasedTransaction(TransactionStore* transaction_store,
                       const TransactionOptions& txn_options,
                       const WriteOptions& write_options);
  ~MVCCBasedTransaction() {}

  virtual Status Put(const std::string& key, const std::string& value) override; 
  virtual Status Delete(const std::string& key) override;

  virtual Status Get(const ReadOptions& read_options, const std::string& key,
                     std::string* value) override;

  virtual Status Prepare() override;
  virtual Status Commit() override;
  virtual Status Rollback() override;

  virtual void SetSnapshot() override;

  void Reinitialize(TransactionStore* transaction_store,
      const TransactionOptions& txn_options, const WriteOptions& write_options);

 protected:
  friend class ReleaseTxnLockHandler;
  enum TransactionState : unsigned char {
    STAGE_WRITING = 0x0,
    STAGE_PREPARING = 0x1,
    STAGE_PREPARED = 0x2,
    STAGE_COMMITTING = 0x3,
    STAGE_COMMITTED = 0x4,
    STAGE_ROLLBACKING = 0x5,
    STAGE_ROLLBACKED = 0x6
  };

  virtual Status PrepareImpl() = 0;
  virtual Status CommitWithPrepareImpl() = 0;
  virtual Status CommitWithoutPrepareImpl() = 0;
  virtual Status RollbackImpl() = 0;

  Status TryLock(const std::string& key); 
  void UnLock(const std::string& key);

  void ClearTxnLocks();
  void Clear() {
    // clear txn locks before clearing write_batch_, because clearing txn locks
    // depends on the write_batch_
    ClearTxnLocks();
    write_batch_.Clear();
  }

  SkipListBackedInMemoryStore* base_store_;
  SkipListBackedInMemoryTxnStore* transaction_store_;

  WriteOptions write_options_;

  std::atomic<TransactionState> txn_state_;
  WriteBatch write_batch_;
};

class WriteCommittedTransaction : public MVCCBasedTransaction {
 public:
  // No copying allowed
  WriteCommittedTransaction(const WriteCommittedTransaction&) = delete;
  WriteCommittedTransaction& operator=(
      const WriteCommittedTransaction&) = delete;

  WriteCommittedTransaction(TransactionStore* transaction_store,
      const TransactionOptions& txn_options, const WriteOptions& write_options)
        : MVCCBasedTransaction(transaction_store, txn_options, write_options) {}
  ~WriteCommittedTransaction() {}

 protected:
  virtual Status PrepareImpl() override;
  virtual Status CommitWithPrepareImpl() override;
  virtual Status CommitWithoutPrepareImpl() override;
  virtual Status RollbackImpl() override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
