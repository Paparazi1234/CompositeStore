#include "skiplist_backed_in_memory_txn_store.h"

#include "MVCC_based_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

SkipListBackedInMemoryTxnStore::SkipListBackedInMemoryTxnStore(
    const TransactionStoreOptions& txn_store_options,
    SkipListBackedInMemoryStore* base_store,
    const TxnLockManagerFactory& txn_lock_mgr_factory)
    : base_store_(base_store),
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
  return base_store_->Get(read_options, key, value);
}

const Snapshot* SkipListBackedInMemoryTxnStore::TakeSnapshot() {
  SnapshotManager* snapshot_manager = base_store_->GetSnapshotManager();
  return snapshot_manager->TakeSnapshot();
}

void SkipListBackedInMemoryTxnStore::ReleaseSnapshot(const Snapshot* snapshot) {
  SnapshotManager* snapshot_manager = base_store_->GetSnapshotManager();
  snapshot_manager->ReleaseSnapshot(snapshot);
}

Status SkipListBackedInMemoryTxnStore::TryLock(const std::string& key) {
  return txn_lock_manager_->TryLock(key);
}

void SkipListBackedInMemoryTxnStore::UnLock(const std::string& key) {
  txn_lock_manager_->UnLock(key);
}

void SkipListBackedInMemoryTxnStore::ReinitializeTransaction(Transaction* txn,
    const TransactionOptions& txn_options, const WriteOptions& write_options) {
  MVCCBasedTransaction* mvcc_txn = reinterpret_cast<MVCCBasedTransaction*>(txn);
  mvcc_txn->Reinitialize(this, txn_options, write_options);
}

Transaction* SkipListBackedInMemoryTxnStore::BeginInternalTransaction(
    const WriteOptions& write_options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(txn_options, write_options, nullptr);
  return txn;
}

Transaction* WriteCommittedTransactionStore::BeginTransaction(
    const TransactionOptions& txn_options, const WriteOptions& write_options,
    Transaction* old_txn) {
  if (old_txn) {
    ReinitializeTransaction(old_txn, txn_options, write_options);
    return old_txn;
  } else {
    return new WriteCommittedTransaction(this, txn_options, write_options);
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE
