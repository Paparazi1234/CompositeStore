#pragma once


#include "skiplist_backed_in_memory_store.h"
#include "multi_version/sequence_based/seq_based_snapshot.h"
#include "include/multi_versions.h"
#include "include/txn_lock_manager.h"

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
      WriteLock& prepare_queue,
      WriteLock& commit_queue,
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

  WriteLock& GetPrepareQueue() {
    return prepare_queue_;
  }

  WriteLock& GetCommitQueue() {
    return commit_queue_;
  }

  bool IsEnableTwoWriteQueues() const {
    return enable_two_write_queues_;
  }
 protected:
  friend class MVCCBasedTxn;
  virtual WriteLock& CalcuPrepareQueue(bool enable_two_write_queues) = 0;
  virtual WriteLock& CalcuCommitQueue(bool enable_two_write_queues) = 0;

  Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

  void ReinitializeTransaction(Transaction* txn,
                               const WriteOptions& write_options,
                               const TransactionOptions& txn_options);

  Transaction* BeginInternalTransaction(const WriteOptions& write_options);

  bool enable_two_write_queues_ = false;
  WriteLock& prepare_queue_;
  WriteLock& commit_queue_;

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
      : SkipListBackedInMemoryTxnStore(
          store_options, txn_store_options,
          WriteCommittedMultiVersionsManagerFactory(
              store_options.enable_two_write_queues),
          CalcuPrepareQueue(store_options.enable_two_write_queues),
          CalcuCommitQueue(store_options.enable_two_write_queues),
          txn_lock_mgr_factory) {
    assert(std::addressof(commit_queue_) == &write_lock_);
    if (store_options.enable_two_write_queues) {
      assert(std::addressof(prepare_queue_) == &second_write_lock_);
    } else {
      assert(std::addressof(prepare_queue_) == &write_lock_);
    }
    if (!store_options.enable_two_write_queues) {
      assert(std::addressof(prepare_queue_) == std::addressof(commit_queue_));
    }
  }
  ~WriteCommittedTxnStore() {}

  Transaction* BeginTransaction(const WriteOptions& write_options,
      const TransactionOptions& txn_options, Transaction* old_txn) override;

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

  WriteLock& CalcuPrepareQueue(bool enable_two_write_queues) override {
    if (enable_two_write_queues) {
      // for WriteCommitted txn, we use the second write queue to prepare when
      // enable two_write_queues
      return second_write_lock_;
    } else {
      // for WriteCommitted txn, we use the first write queue to prepare when
      // not enable two_write_queues
      return write_lock_;
    }
  }

  WriteLock& CalcuCommitQueue(bool /*enable_two_write_queues*/) override {
    // for WriteCommitted txn, we use the first write queue to commit no mater
    // enable two_write_queues or not
    return write_lock_;
  }
};

class WritePreparedTxnStore : public SkipListBackedInMemoryTxnStore {
 public:
  // No copying allowed
  WritePreparedTxnStore(const WritePreparedTxnStore&) = delete;
  WritePreparedTxnStore& operator=(const WritePreparedTxnStore&) = delete;

  WritePreparedTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const CommitTableOptions& commit_table_options,
      const TxnLockManagerFactory& txn_lock_mgr_factory)
      : SkipListBackedInMemoryTxnStore(
          store_options, txn_store_options,
          WritePreparedMultiVersionsManagerFactory(
              commit_table_options,
              store_options.enable_two_write_queues),
          CalcuPrepareQueue(store_options.enable_two_write_queues),
          CalcuCommitQueue(store_options.enable_two_write_queues),
          txn_lock_mgr_factory) {
    assert(std::addressof(prepare_queue_) == &write_lock_);
    if (store_options.enable_two_write_queues) {
      assert(std::addressof(commit_queue_) == &second_write_lock_);
    } else {
      assert(std::addressof(commit_queue_) == &write_lock_);
    }
    if (!store_options.enable_two_write_queues) {
      assert(std::addressof(prepare_queue_) == std::addressof(commit_queue_));
    }

    PostInitializeMultiVersionManager();
  }
  ~WritePreparedTxnStore() {}

  Transaction* BeginTransaction(const WriteOptions& write_options,
      const TransactionOptions& txn_options, Transaction* old_txn) override;

  class WPAdvanceMaxCommittedByOneCallback :
      public AdvanceMaxCommittedByOneCallback {
   public:
    WPAdvanceMaxCommittedByOneCallback(WritePreparedTxnStore* txn_store)
        : txn_store_(txn_store) {}
    ~WPAdvanceMaxCommittedByOneCallback() {}

    void AdvanceLatestVisibleByOne() override {
      WriteOptions write_options;
      TransactionOptions txn_options;
      Transaction* txn = txn_store_->BeginTransaction(write_options,
                                                      txn_options, nullptr);
      // commit(without prepare) an empty write batch will consume a seq
      // commit(with prepare) an empty write batch will consume two seq
      Status s = txn->Commit();
      assert(s.IsOK());
      delete txn;
    }
   private:
    WritePreparedTxnStore* txn_store_;         
  };

 protected:
  void PostInitializeMultiVersionManager() {
    WritePreparedMultiVersionsManager* multi_version_manager_impl =
        reinterpret_cast<WritePreparedMultiVersionsManager*>(
            GetMultiVersionsManager());
    WritePreparedSnapshotManager* snapshot_manager_impl =
        reinterpret_cast<WritePreparedSnapshotManager*>(
            GetSnapshotManager());
    // set AdvanceMaxCommittedByOneCallback to multi versions manager
    multi_version_manager_impl->SetAdvanceMaxCommittedByOneCallback(
        new WPAdvanceMaxCommittedByOneCallback(this));
    // set SnapshotsRetrieveCallback to multi versions manager
    multi_version_manager_impl->SetSnapshotsRetrieveCallback(
        new WritePreparedSnapshotManager::WPGetSnapshotsCallback(
            snapshot_manager_impl));
    // set SnapshotCreationCallback to snapshot manager
    snapshot_manager_impl->SetSnapshotCreationCallback(
        multi_version_manager_impl->GetSnapshotCreationCallback());
  }

  virtual uint64_t CalculateNumVersionsForWriteBatch(
			const WriteBatch* write_batch) const override {
    // 1 we employ seq per batch in WritePrepared policy
    // 2 when we commit an empty txn write, it will also consume a version
		return 1;
	}

  WriteLock& CalcuPrepareQueue(bool /*enable_two_write_queues*/) override {
    // for WritePrepared txn, we use the first write queue to prepare no mater
    // enable two_write_queues or not
    return write_lock_;
  }

  WriteLock& CalcuCommitQueue(bool enable_two_write_queues) override {
    if (enable_two_write_queues) {
      // for WritePrepared txn, we use the second write queue to commit when
      // enable two_write_queues
      return second_write_lock_;
    } else {
      // for WritePrepared txn, we use the first write queue to commit when
      // not enable two_write_queues
      return write_lock_;
    }
  }
};

}   // namespace MULTI_VERSIONS_NAMESPACE
