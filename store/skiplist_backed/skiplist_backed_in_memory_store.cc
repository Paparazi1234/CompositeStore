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

Status SkipListBackedInMemoryStore::Put(const WriteOptions& write_options,
                                        const std::string& key,
                                        const std::string& value) {
  WriteBatch write_batch;
  write_batch.Put(key, value);
  return WriteInternal(write_options, &write_batch);
}

Status SkipListBackedInMemoryStore::Delete(const WriteOptions& write_options,
                                           const std::string& key) {
  WriteBatch write_batch;
  write_batch.Delete(key);
  return WriteInternal(write_options, &write_batch);
}

Status SkipListBackedInMemoryStore::WriteInternal(
    const WriteOptions& write_options, WriteBatch* write_batch) {
  ManagedWriteLock managed_write_lock = ManagedWriteLock(&write_lock_);
  Version* version_for_insert = VersionForInsert();
  Version* latest_version =
      multi_versions_manager_->LatestVisibleVersion(version_for_insert);
  SkipListInsertHandler handler(
      &skiplist_backed_rep_, multi_versions_manager_.get(), latest_version);
  Status s = write_batch->Iterate(&handler);
  if (s.IsOK()) {
    multi_versions_manager_->CommitVersion(*latest_version);
  }
  return s;
}

Status SkipListBackedInMemoryStore::Get(const ReadOptions& read_options,
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
    const TransactionOptions& /*txn_options*/,
    const WriteOptions& /*write_options*/,
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
