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
  started_version_->IncreaseByOne();
  Status s = skiplist_backed_rep_->Insert(
      key, value, started_version_, kTypeValue);
  return s;
}

Status SkipListInsertHandler::Delete(const std::string& key) {
  started_version_->IncreaseByOne();
  Status s = skiplist_backed_rep_->Insert(
      key, "", started_version_, kTypeDeletion);
  return s;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
