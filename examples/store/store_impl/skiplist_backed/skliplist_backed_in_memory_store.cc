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
  const Version& latest_version =
      multi_versions_manager_->LatestVisibleVersion();
  const size_t count = write_batch->Count(); 
  for (int i = 0; i < count; ++i) {
    const WriteBatch::BufferedWrite& one_write = write_batch->GetOneWrite(i);
    Version* version =
        multi_versions_manager_->ConstructVersion(latest_version, i);
    SkipListKey skiplist_key(one_write.Key(),
                             one_write.Value(),
                             version,
                             one_write.Type());
    skiplist_backed_rep_.Insert(skiplist_key);
  }
  multi_versions_manager_->AdvanceVersionBy(count);
  return Status::OK();
}

Status SkipListBackedInMemoryStore::Get(const ReadOptions& read_options,
                                        const std::string& key, 
                                        std::string* value) {
  
}

}   // namespace MULTI_VERSIONS_NAMESPACE
