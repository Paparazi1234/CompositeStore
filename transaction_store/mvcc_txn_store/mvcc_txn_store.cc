#include "mvcc_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

MVCCTxnStore::MVCCTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MVCCTxnStoreCreationParam& creation_param)
      : multi_versions_manager_(
            creation_param.mvm_factory->CreateMultiVersionsManager()),
        snapshot_manager_(
            creation_param.mvm_factory->CreateSnapshotManager(
                multi_versions_manager_.get())),
        txn_lock_manager_(
            creation_param.lock_manager_factory->CreateTxnLockManager()),
        txn_factory_(creation_param.transaction_factory),
        staging_write_factory_(creation_param.staging_write_factory),
        mvcc_write_buffer_(
            creation_param.write_buffer_factory->CreateMVCCWriteBuffer(
                multi_versions_manager_.get())),
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

namespace {
class InsertMVCCWriteBufferHandler : public StagingWrite::Handler {
 public:
  InsertMVCCWriteBufferHandler(MVCCWriteBuffer* mvcc_write_buffer,
                               Version* started_version,
                               uint64_t version_inc)
      : mvcc_write_buffer_(mvcc_write_buffer),
        started_version_(started_version),
        version_inc_(version_inc) {
    assert(started_version_);
    assert(version_inc_ > 0);
  }
  ~InsertMVCCWriteBufferHandler() {}

  Status Put(const std::string& key, const std::string& value) override {
    MaybeAdvanceVersion();
    Status s = mvcc_write_buffer_->Insert(key, value, ValueType::kTypeValue,
                                          *started_version_);
    is_first_iterated_entry_ = false;
    return s;
  }

  Status Delete(const std::string& key) override {
    MaybeAdvanceVersion();
    Status s = mvcc_write_buffer_->Insert(key, "", ValueType::kTypeDeletion,
                                          *started_version_);
    is_first_iterated_entry_ = false;
    return s;
  }

 private:
  void MaybeAdvanceVersion() {
    if (!is_first_iterated_entry_ && version_inc_ > 1) {
      started_version_->IncreaseByOne();
    }
  }
  MVCCWriteBuffer* mvcc_write_buffer_;
  Version* started_version_;
  uint64_t version_inc_;
  bool is_first_iterated_entry_ = true;
};
}   // anonymous namespace

Status MVCCTxnStore::CommitStagingWrite(
    const WriteOptions& write_options,
    StagingWrite* staging_write,
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
  uint64_t version_inc = CalcuNumVersionsIncForStagingWrite(staging_write);
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
  InsertMVCCWriteBufferHandler handler(mvcc_write_buffer_.get(),
                                       allocated_started,
                                       version_inc);
  // insert write buffer
  s = staging_write->Iterate(&handler);
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
  return mvcc_write_buffer_->Get(key, value, *read_snapshot);
}

Transaction* MVCCTxnStore::BeginInternalTransaction(
    const WriteOptions& write_options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(write_options, txn_options, nullptr);
  return txn;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
