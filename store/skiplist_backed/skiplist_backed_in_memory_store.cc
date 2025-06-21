#include "skiplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

SkipListBackedInMemoryStore::SkipListBackedInMemoryStore(
    const StoreOptions& store_options,
    const MultiVersionsManagerFactory& multi_versions_mgr_factory)
		: multi_versions_manager_(
          multi_versions_mgr_factory.CreateMultiVersionsManager()),
			snapshot_manager_(multi_versions_mgr_factory.CreateSnapshotManager(
          multi_versions_manager_.get())),
			skiplist_backed_rep_(multi_versions_manager_.get()) {
  (void)store_options;
}

namespace {
class EmptyAfterInsertWBCB :
		public MaintainVersionsCallbacks::AfterInsertWriteBufferCallback {
 public:
  EmptyAfterInsertWBCB(TransactionStore* store) {
    store_impl_ = reinterpret_cast<SkipListBackedInMemoryStore*>(store);
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~EmptyAfterInsertWBCB() {}

  virtual Status DoCallback(const Version* version) override {
    const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    multi_versions_manager_->EndCommitVersions(dummy_version, *version);
    return Status::OK();
  }

 private:
  SkipListBackedInMemoryStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status SkipListBackedInMemoryStore::Put(const WriteOptions& write_options,
                                        const std::string& key,
                                        const std::string& value) {
  EmptyAfterInsertWBCB callback(this);
  MaintainVersionsCallbacks callbacks;
  callbacks.after_insert_write_buffer_ = &callback;
  WriteBatch write_batch;
  write_batch.Put(key, value);
  return WriteInternal(write_options, &write_batch, callbacks, write_lock_);
}

Status SkipListBackedInMemoryStore::Delete(const WriteOptions& write_options,
                                           const std::string& key) {
  EmptyAfterInsertWBCB callback(this);
  MaintainVersionsCallbacks callbacks;
  callbacks.after_insert_write_buffer_ = &callback;
  WriteBatch write_batch;
  write_batch.Delete(key);
  return WriteInternal(write_options, &write_batch, callbacks, write_lock_);
}

Status SkipListBackedInMemoryStore::Get(const ReadOptions& read_options,
                                        const std::string& key, 
                                        std::string* value) {
  return GetInternal(read_options, key, value);
}

Status SkipListBackedInMemoryStore::WriteInternal(
    const WriteOptions& write_options, WriteBatch* write_batch,
    const MaintainVersionsCallbacks& maintain_versions_callbacks,
    WriteLock& write_queue) {
  ManagedWriteLock managed_write_lock = ManagedWriteLock(&write_queue);
  Status s;
  if (maintain_versions_callbacks.before_persist_wal_) {
    s = maintain_versions_callbacks.before_persist_wal_->DoCallback(this);
    if (!s.IsOK()) {
      return s;
    }
  }
  Version* version_for_insert = VersionForInsert();
  uint64_t version_inc = CalculateNumVersionsForWriteBatch(write_batch);
  Version* allocated_started =
      multi_versions_manager_->AllocateVersion(version_inc, version_for_insert);
  if (maintain_versions_callbacks.before_insert_write_buffer_) {
    s = maintain_versions_callbacks.before_insert_write_buffer_->DoCallback(
        allocated_started, version_inc);
    if (!s.IsOK()) {
      return s;
    }
  }
  SkipListInsertHandler handler(&skiplist_backed_rep_, allocated_started,
                                version_inc);
  s = write_batch->Iterate(&handler);
  if (s.IsOK() && maintain_versions_callbacks.after_insert_write_buffer_) {
    s = maintain_versions_callbacks.after_insert_write_buffer_->DoCallback(
        allocated_started);
  }
  return s;
}

Status SkipListBackedInMemoryStore::GetInternal(const ReadOptions& read_options,
             				                            const std::string& key,
                                                std::string* value) {
  assert(value);
  std::unique_ptr<const Snapshot> read_snapshot_tmp;
  const Snapshot* read_snapshot;
  if (read_options.snapshot) {
    read_snapshot = read_options.snapshot;
  } else {
    read_snapshot_tmp.reset(snapshot_manager_->LatestReadView());
    read_snapshot = read_snapshot_tmp.get();
  }
  return skiplist_backed_rep_.Get(key, *read_snapshot, value);
}

// invalidate transaction relative interfaces
Transaction* SkipListBackedInMemoryStore::BeginTransaction(
    const WriteOptions& /*write_options*/,
    const TransactionOptions& /*txn_options*/,
    Transaction* /*old_txn*/) {
  assert(false);
  return nullptr;
}

const Snapshot* SkipListBackedInMemoryStore::TakeSnapshot() {
  assert(false);
  return nullptr;
}

void SkipListBackedInMemoryStore::ReleaseSnapshot(
    const Snapshot* /*snapshot*/) {
  assert(false);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
