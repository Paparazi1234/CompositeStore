#include "skiplist_backed_in_memory_txn_store.h"

#include "MVCC_based_txn.h"

namespace MULTI_VERSIONS_NAMESPACE {

SkipListBackedInMemoryTxnStore::SkipListBackedInMemoryTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MultiVersionsManagerFactory& multi_versions_mgr_factory,
    WriteLock& prepare_queue,
    WriteLock& commit_queue,
    const TxnLockManagerFactory& txn_lock_mgr_factory)
    : SkipListBackedInMemoryStore(store_options, multi_versions_mgr_factory),
      enable_two_write_queues_(store_options.enable_two_write_queues),
      prepare_queue_(prepare_queue),
      commit_queue_(commit_queue),
      txn_lock_manager_(txn_lock_mgr_factory.CreateTxnLockManager()) {
  (void)txn_store_options;
}

Status SkipListBackedInMemoryTxnStore::Put(const WriteOptions& write_options,
                                           const std::string& key,
                                           const std::string& value) {
  Transaction* txn = BeginInternalTransaction(write_options);
  Status s = txn->Put(key, value);
  if (s.IsOK()) {
    s = txn->Commit();
  }
  delete txn;
  return s;                                      
}

Status SkipListBackedInMemoryTxnStore::Delete(
    const WriteOptions& write_options, const std::string& key) {
  Transaction* txn = BeginInternalTransaction(write_options);
  Status s = txn->Delete(key);
  if (s.IsOK()) {
    s = txn->Commit();
  }
  delete txn;
  return s;    
}

Status SkipListBackedInMemoryTxnStore::Get(const ReadOptions& read_options,
                                           const std::string& key,
                                           std::string* value) {
  return SkipListBackedInMemoryStore::Get(read_options, key, value);
}

const Snapshot* SkipListBackedInMemoryTxnStore::TakeSnapshot() {
  SnapshotManager* snapshot_manager = GetSnapshotManager();
  return snapshot_manager->TakeSnapshot();
}

void SkipListBackedInMemoryTxnStore::ReleaseSnapshot(const Snapshot* snapshot) {
  SnapshotManager* snapshot_manager = GetSnapshotManager();
  snapshot_manager->ReleaseSnapshot(snapshot);
}

Status SkipListBackedInMemoryTxnStore::TryLock(const std::string& key) {
  return txn_lock_manager_->TryLock(key);
}

void SkipListBackedInMemoryTxnStore::UnLock(const std::string& key) {
  txn_lock_manager_->UnLock(key);
}

void SkipListBackedInMemoryTxnStore::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  MVCCBasedTxn* mvcc_txn = reinterpret_cast<MVCCBasedTxn*>(txn);
  mvcc_txn->Reinitialize(this, write_options, txn_options);
}

Transaction* SkipListBackedInMemoryTxnStore::BeginInternalTransaction(
    const WriteOptions& write_options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(write_options, txn_options, nullptr);
  return txn;
}

Transaction* WriteCommittedTxnStore::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WriteCommittedTxn(this, write_options, txn_options);
  }
}

Transaction* WritePreparedTxnStore::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn) {
    WritePreparedTxn* txn_impl = reinterpret_cast<WritePreparedTxn*>(old_txn);
    txn_impl->ResetUnCommittedSeqs();
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WritePreparedTxn(this, write_options, txn_options);
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE
