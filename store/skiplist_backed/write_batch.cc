#include "write_batch.h"

#include <assert.h>

namespace MULTI_VERSIONS_NAMESPACE {

Status WriteBatch::Iterate(Handler* handler) {
  Status s;
  for (auto& buffered_write : buffered_writes_) {
    switch (buffered_write.second.Type()) {
      case kTypeValue :
        s = handler->Put(buffered_write.first, buffered_write.second.Value());
        assert(s.IsOK());
        break;
      case kTypeDeletion :
        s = handler->Delete(buffered_write.first);
        assert(s.IsOK());
        break;
      default:
        s = Status::Corruption();
    }
    if (!s.IsOK()) {
      return s;
    }
  }
  return s;
}

Status SkipListInsertHandler::Put(
    const std::string& key, const std::string& value) {
  Status s = skiplist_backed_rep_->Insert(
      key, value, started_version_, kTypeValue);
  if (s.IsOK()) {
    started_version_ = multi_version_manager_->ConstructVersion(
        *started_version_, 1, started_version_);
  }
  return s;
}

Status SkipListInsertHandler::Delete(const std::string& key) {
  Status s = skiplist_backed_rep_->Insert(
      key, "", started_version_, kTypeDeletion);
  if (s.IsOK()) {
    started_version_ = multi_version_manager_->ConstructVersion(
        *started_version_, 1, started_version_);
  }
  return s;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
