#pragma once

#include "include/multi_versions.h"
#include "include/txn_lock_manager.h"
#include "skiplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryTxnStore : public SkipListBackedInMemoryStore {
 public:
  // No copying allowed
  SkipListBackedInMemoryTxnStore(
      const SkipListBackedInMemoryTxnStore&) = delete;
  SkipListBackedInMemoryTxnStore& operator=(
      const SkipListBackedInMemoryTxnStore&) = delete;

  SkipListBackedInMemoryTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const MultiVersionsManagerFactory& multi_versions_mgr_factory,
      const TxnLockManagerFactory& txn_lock_mgr_factory);
  ~SkipListBackedInMemoryTxnStore() {}

  // non-transactional write
  virtual Status Put(const WriteOptions& write_options,
                     const std::string& key, const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                        const std::string& key) override;

  // non-transactional read
  using SkipListBackedInMemoryStore::Get;
  virtual Status Get(const ReadOptions& read_options,
                  const std::string& key, std::string* value) override;

  Transaction* BeginTransaction(const WriteOptions& write_options,
      const TransactionOptions& txn_options, Transaction* old_txn) override = 0;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

 protected:
  friend class MVCCBasedTxn;

  Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

  void ReinitializeTransaction(Transaction* txn,
      const TransactionOptions& txn_options, const WriteOptions& write_options);

  Transaction* BeginInternalTransaction(const WriteOptions& write_options);

  std::unique_ptr<TxnLockManager> txn_lock_manager_;
};

class WriteCommittedTxnStore : public SkipListBackedInMemoryTxnStore {
 public:
  // No copying allowed
  WriteCommittedTxnStore(const WriteCommittedTxnStore&) = delete;
  WriteCommittedTxnStore& operator=(const WriteCommittedTxnStore&) = delete;

  WriteCommittedTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const TxnLockManagerFactory& txn_lock_mgr_factory)
      : SkipListBackedInMemoryTxnStore(store_options, txn_store_options,
          WriteCommittedMultiVersionsManagerFactory(), txn_lock_mgr_factory) {}
  ~WriteCommittedTxnStore() {}

  Transaction* BeginTransaction(const WriteOptions& write_options,
      const TransactionOptions& txn_options, Transaction* old_txn) override;
};

class WritePreparedTxnStore : public SkipListBackedInMemoryTxnStore {
 public:
  // No copying allowed
  WritePreparedTxnStore(const WritePreparedTxnStore&) = delete;
  WritePreparedTxnStore& operator=(const WritePreparedTxnStore&) = delete;

  WritePreparedTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const TxnLockManagerFactory& txn_lock_mgr_factory)
      : SkipListBackedInMemoryTxnStore(store_options, txn_store_options,
          WritePreparedMultiVersionsManagerFactory(), txn_lock_mgr_factory) {}
  ~WritePreparedTxnStore() {}

  Transaction* BeginTransaction(const WriteOptions& write_options,
      const TransactionOptions& txn_options, Transaction* old_txn) override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
