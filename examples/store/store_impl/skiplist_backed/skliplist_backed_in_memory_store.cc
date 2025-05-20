#include "skliplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

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
  Version* latest_version =
      multi_versions_manager_->LatestVisibleVersion();
  Version* started_version = multi_versions_manager_->ConstructVersion(
      *latest_version, 1, latest_version);
  SkipListInsertHandler handler(
      &skiplist_backed_rep_, multi_versions_manager_.get(), started_version);
  Status s = write_batch->Iterate(&handler);
  delete latest_version;
  if (s.IsOK()) {
    multi_versions_manager_->AdvanceVersionBy(write_batch->Count());
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

}   // namespace MULTI_VERSIONS_NAMESPACE
