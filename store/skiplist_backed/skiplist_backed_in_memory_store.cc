#include "skiplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

SkipListBackedInMemoryStore::SkipListBackedInMemoryStore(
    const StoreOptions& store_options,
    const MultiVersionsManagerFactory& multi_versions_mgr_factory)
		: multi_versions_manager_(
          multi_versions_mgr_factory.CreateMultiVersionsManager()),
			snapshot_manager_(multi_versions_mgr_factory.CreateSnapshotManager(
          multi_versions_manager_.get())),
			skiplist_backed_rep_(multi_versions_manager_.get()),
      first_write_queue_(multi_versions_manager_.get()),
      second_write_queue_(multi_versions_manager_.get()) {
  (void)store_options;
}

namespace {
class EmptyMaintainVersionsCallbacks : public MaintainVersionsCallbacks {
  public:
  EmptyMaintainVersionsCallbacks(TransactionStore* store) {
    store_impl_ = reinterpret_cast<SkipListBackedInMemoryStore*>(store);
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
	~EmptyMaintainVersionsCallbacks() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return false; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

	Status AfterInsertWriteBufferCallback(const Version* version)  override {
    const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    const Version& started_uncommitted = dummy_version;
    const Version& committed = *version;
    uint32_t num_uncommitteds = 0;
    multi_versions_manager_->EndCommitVersions(started_uncommitted,
                                               committed,
                                               num_uncommitteds);
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
  WriteBatch write_batch;
  write_batch.Put(key, value);
  EmptyMaintainVersionsCallbacks empty_maintain_versions_cb(this);
  return WriteInternal(write_options,
                       &write_batch,
                       empty_maintain_versions_cb,
                       first_write_queue_);
}

Status SkipListBackedInMemoryStore::Delete(const WriteOptions& write_options,
                                           const std::string& key) {
  WriteBatch write_batch;
  write_batch.Delete(key);
  EmptyMaintainVersionsCallbacks empty_maintain_versions_cb(this);
  return WriteInternal(write_options,
                       &write_batch,
                       empty_maintain_versions_cb,
                       first_write_queue_);
}

Status SkipListBackedInMemoryStore::Get(const ReadOptions& read_options,
                                        const std::string& key, 
                                        std::string* value) {
  return GetInternal(read_options, key, value);
}

Status SkipListBackedInMemoryStore::WriteInternal(
    const WriteOptions& write_options, WriteBatch* write_batch,
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

Status SkipListBackedInMemoryStore::GetInternal(const ReadOptions& read_options,
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
