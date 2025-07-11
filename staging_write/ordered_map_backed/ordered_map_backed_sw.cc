#include "ordered_map_backed_sw.h"

namespace MULTI_VERSIONS_NAMESPACE {

Status OrderedMapBackedStagingWrite::Iterate(Handler* handler) {
  if (buffered_writes_.empty()) {
    return Status::OK();
  }
  Status s;
  for (auto& buffered_write : buffered_writes_) {
    switch (buffered_write.second.Type()) {
      case kTypeValue :
        s = handler->Put(buffered_write.first, buffered_write.second.Value());
        break;
      case kTypeDeletion :
        s = handler->Delete(buffered_write.first);
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

}   // namespace MULTI_VERSIONS_NAMESPACE
