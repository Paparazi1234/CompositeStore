#include "mvcc_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCTxnStore::MVCCTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MultiVersionsManagerFactory& multi_versions_mgr_factory,
    const TxnLockManagerFactory& txn_lock_mgr_factory,
    TransactionFactory* txn_factory)
      : multi_versions_manager_(
            multi_versions_mgr_factory.CreateMultiVersionsManager()),
        snapshot_manager_(multi_versions_mgr_factory.CreateSnapshotManager(
            multi_versions_manager_.get())),
        txn_lock_manager_(txn_lock_mgr_factory.CreateTxnLockManager()),
        txn_factory_(txn_factory),
        skiplist_backed_rep_(multi_versions_manager_.get()),
        first_write_queue_(multi_versions_manager_.get()),
        second_write_queue_(multi_versions_manager_.get()) {
  (void)store_options;
  (void)txn_store_options;
}

Status MVCCTxnStore::Put(const WriteOptions& write_options,
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

Status MVCCTxnStore::Delete(const WriteOptions& write_options,
                            const std::string& key) {
  Transaction* txn = BeginInternalTransaction(write_options);
  Status s = txn->Delete(key);
  if (s.IsOK()) {
    s = txn->Commit();
  }
  delete txn;
  return s;    
}

Status MVCCTxnStore::Get(const ReadOptions& read_options,
                         const std::string& key, 
                         std::string* value) {
  return GetInternal(read_options, key, value);
}

Transaction* MVCCTxnStore::BeginTransaction(
    const WriteOptions& write_options,
    const TransactionOptions& txn_options,
    Transaction* reused) {
  return txn_factory_->CreateTransaction(write_options,
                                         txn_options, this, reused);
}

const Snapshot* MVCCTxnStore::TakeSnapshot() {
  return snapshot_manager_->TakeSnapshot();
}

void MVCCTxnStore::ReleaseSnapshot(const Snapshot* snapshot) {
  snapshot_manager_->ReleaseSnapshot(snapshot);
}

Status MVCCTxnStore::TryLock(const std::string& key) {
  return txn_lock_manager_->TryLock(key);
}

void MVCCTxnStore::UnLock(const std::string& key) {
  txn_lock_manager_->UnLock(key);
}

Status MVCCTxnStore::WriteInternal(
    const WriteOptions& write_options,
    WriteBatch* write_batch,
    MaintainVersionsCallbacks& maintain_versions_callbacks,
    WriteQueue& write_queue) {
  ManagedWriteQueue managed_write_lock = ManagedWriteQueue(&write_queue);
  Status s;
  // before persist WAL
  if (maintain_versions_callbacks.NeedMaintainBeforePersistWAL()) {
    s = maintain_versions_callbacks.BeforePersistWALCallback(this);
    if (!s.IsOK()) {
      return s;
    }
  }
  Version* version_for_insert = write_queue.VersionForInsert();
  uint64_t version_inc = CalculateNumVersionsForWriteBatch(write_batch);
  Version* allocated_started =
      multi_versions_manager_->AllocateVersion(version_inc, version_for_insert);
  // before insert write buffer
  if (maintain_versions_callbacks.NeedMaintainBeforeInsertWriteBuffer()) {
    s = maintain_versions_callbacks.BeforeInsertWriteBufferCallback(
        allocated_started, version_inc);
    if (!s.IsOK()) {
      return s;
    }
  }
  SkipListInsertHandler handler(&skiplist_backed_rep_, allocated_started,
                                version_inc);
  // insert write buffer
  s = write_batch->Iterate(&handler);
  // after insert write buffer
  if (s.IsOK() &&
      maintain_versions_callbacks.NeedMaintainAfterInsertWriteBuffer()) {
    s = maintain_versions_callbacks.AfterInsertWriteBufferCallback(
        allocated_started);
  }
  return s;
}

Status MVCCTxnStore::GetInternal(const ReadOptions& read_options,
                                 const std::string& key,
                                 std::string* value) {
  assert(value);
  std::unique_ptr<const Snapshot> snapshot_tmp = nullptr;
  const Snapshot* read_snapshot = nullptr;
  if (read_options.snapshot) {
    read_snapshot = read_options.snapshot;
  } else {
    // if client did not specify a read snapshot, then we will read under the
    // latest read view of the underlying store
    snapshot_tmp.reset(snapshot_manager_->LatestReadView());
    read_snapshot = snapshot_tmp.get();
  }
  assert(read_snapshot != nullptr);
  return skiplist_backed_rep_.Get(key, *read_snapshot, value);
}

Transaction* MVCCTxnStore::BeginInternalTransaction(
    const WriteOptions& write_options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(write_options, txn_options, nullptr);
  return txn;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
