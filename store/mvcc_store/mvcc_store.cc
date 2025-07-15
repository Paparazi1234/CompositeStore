#include "mvcc_store.h"

#include "multi_version/sequence_based/seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

namespace {

// just a wrapper of WriteCommittedMultiVersionsManager and
// enable_two_write_queues == false
class EmptyMultiVersionsManager : public WriteCommittedMultiVersionsManager {
 public:
  // No copying allowed
  EmptyMultiVersionsManager(const EmptyMultiVersionsManager&) = delete;
  EmptyMultiVersionsManager& operator=(
      const EmptyMultiVersionsManager&) = delete;

  EmptyMultiVersionsManager() {}
  ~EmptyMultiVersionsManager() {}
};

// just a wrapper of WriteCommittedSnapshotManager
class EmptySnapshotManager : public WriteCommittedSnapshotManager {
 public:
  // No copying allowed
  EmptySnapshotManager(const EmptySnapshotManager&) = delete;
  EmptySnapshotManager& operator=(const EmptySnapshotManager&) = delete;
  
  EmptySnapshotManager(SeqBasedMultiVersionsManager* multi_versions_manager)
      : WriteCommittedSnapshotManager(multi_versions_manager) {}
  ~EmptySnapshotManager() {}
};

class EmptyMultiVersionsManagerFactory : public MultiVersionsManagerFactory {
 public:
  ~EmptyMultiVersionsManagerFactory() {}

  MultiVersionsManager* CreateMultiVersionsManager() const override {
    return new EmptyMultiVersionsManager();
  }

  SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override {
    EmptyMultiVersionsManager* mvm_impl =
        static_cast_with_check<EmptyMultiVersionsManager>(
            multi_versions_manager);
    return new EmptySnapshotManager(mvm_impl);
  }
};

class EmptyMaintainVersionsCallbacks : public MaintainVersionsCallbacks {
  public:
  EmptyMaintainVersionsCallbacks(MVCCTxnStore* store)
      : multi_versions_manager_(store->GetMultiVersionsManager()) {}
	~EmptyMaintainVersionsCallbacks() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return false; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

	Status AfterInsertWriteBufferCallback(const Version* version)  override {
    const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    const Version& prepared_uncommitted_started = dummy_version;
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = 0;
    // as for WriteCommitted txn, all we need to do it's to advance max visible
    // version after insert the txn's staging write to write buffer
    multi_versions_manager_->EndCommitVersions(prepared_uncommitted_started,
                                               committed,
                                               num_prepared_uncommitteds);
    return Status::OK();
  }

 private:
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

MVCCStore::MVCCStore(const StoreOptions& store_options)
    : MVCCTxnStore(store_options,
                   TransactionStoreOptions(),
                   EmptyMultiVersionsManagerFactory(),
                   EmptyTxnLockManagerFactory(),
                   nullptr,
                   new OrderedMapBackedStagingWriteFactory(),
                   SkipListBackedMVCCWriteBufferFactory()) {}

Status MVCCStore::Put(const WriteOptions& write_options,
                      const std::string& key,
                      const std::string& value) {
  std::unique_ptr<StagingWrite>
      staging_write(GetStagingWriteFactory()->CreateStagingWrite());
  staging_write->Put(key, value);
  EmptyMaintainVersionsCallbacks empty_maintain_versions_cb(this);
  return CommitStagingWrite(write_options,
                            staging_write.get(),
                            empty_maintain_versions_cb,
                            first_write_queue_);
}

Status MVCCStore::Delete(const WriteOptions& write_options,
                         const std::string& key) {
  std::unique_ptr<StagingWrite>
      staging_write(GetStagingWriteFactory()->CreateStagingWrite());
  staging_write->Delete(key);
  EmptyMaintainVersionsCallbacks empty_maintain_versions_cb(this);
  return CommitStagingWrite(write_options,
                            staging_write.get(),
                            empty_maintain_versions_cb,
                            first_write_queue_);
}

Status MVCCStore::Get(const ReadOptions& read_options,
                      const std::string& key,
                      std::string* value) {
  return GetInternal(read_options, key, value);
}

Transaction* MVCCStore::BeginTransaction(
    const WriteOptions& /*write_options*/,
    const TransactionOptions& /*txn_options*/,
    Transaction* /*reused*/) {
  assert(false);
  return nullptr;
}

const Snapshot* MVCCStore::TakeSnapshot() {
  assert(false);
  return nullptr;
}

void MVCCStore::ReleaseSnapshot(const Snapshot* /*snapshot*/) {
  assert(false);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
