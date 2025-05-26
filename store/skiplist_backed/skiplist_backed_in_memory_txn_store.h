#pragma once

#include "include/transaction_store.h"
#include "include/txn_lock_manager.h"
#include "skiplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryTxnStore : public TransactionStore {
 public:
  // No copying allowed
  SkipListBackedInMemoryTxnStore(
      const SkipListBackedInMemoryTxnStore&) = delete;
  SkipListBackedInMemoryTxnStore& operator=(
      const SkipListBackedInMemoryTxnStore&) = delete;

  SkipListBackedInMemoryTxnStore(
      const TransactionStoreOptions& txn_store_options,
      SkipListBackedInMemoryStore* base_store,
      const TxnLockManagerFactory& txn_lock_mgr_factory);
  ~SkipListBackedInMemoryTxnStore() {
    delete base_store_;
  }

  // non-transactional write
  virtual Status Put(const WriteOptions& write_options,
                     const std::string& key, const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                        const std::string& key) override;

  // non-transactional read
  virtual Status Get(const ReadOptions& read_options,
                  const std::string& key, std::string* value) override;

  Transaction* BeginTransaction(const TransactionOptions& txn_options,
      const WriteOptions& write_options, Transaction* old_txn) override = 0;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

 protected:
  friend class MVCCBasedTransaction;

  Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

  SkipListBackedInMemoryStore* GetBaseStore() {
    return base_store_;
  }

  void ReinitializeTransaction(Transaction* txn,
      const TransactionOptions& txn_options, const WriteOptions& write_options);

  Transaction* BeginInternalTransaction(const WriteOptions& write_options);

  SkipListBackedInMemoryStore* base_store_;
  std::unique_ptr<TxnLockManager> txn_lock_manager_;
};

class WriteCommittedTransactionStore : public SkipListBackedInMemoryTxnStore {
 public:
  // No copying allowed
  WriteCommittedTransactionStore(
      const WriteCommittedTransactionStore&) = delete;
  WriteCommittedTransactionStore& operator=(
      const WriteCommittedTransactionStore&) = delete;

  WriteCommittedTransactionStore(
      const TransactionStoreOptions& txn_store_options,
      SkipListBackedInMemoryStore* base_store,
      const TxnLockManagerFactory& txn_lock_mgr_factory)
      : SkipListBackedInMemoryTxnStore(txn_store_options,
                                       base_store,
                                       txn_lock_mgr_factory) {}
  ~WriteCommittedTransactionStore() {}

  Transaction* BeginTransaction(const TransactionOptions& txn_options,
      const WriteOptions& write_options, Transaction* old_txn) override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
