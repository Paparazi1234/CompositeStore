#include "skiplist_backed_mvcc_wb.h"

#include <sstream>

namespace MULTI_VERSIONS_NAMESPACE {

Status SkipListBackedMVCCWriteBuffer::Insert(const std::string& key,
                                             const std::string& value,
                                             ValueType value_type,
                                             const Version& version) {
  Status s = skiplist_backed_rep_.Insert(key, value, value_type, version);
  if (s.IsOK()) {
    num_entries_++;
    if (value_type == kTypeDeletion) {
      num_deletes_++;
    }
    RecordRawDataSize(key, value);
  }
  return s;
}

Status SkipListBackedMVCCWriteBuffer::Get(const std::string& key,
                                          std::string* value,
                                          const Snapshot& read_snapshot) {
  return skiplist_backed_rep_.Get(key, value, read_snapshot);
}

void SkipListBackedMVCCWriteBuffer::Dump(std::stringstream* oss,
                                         const size_t dump_count) {
  uint64_t actual_dump = skiplist_backed_rep_.Dump(oss, dump_count);
  *oss<<"  Total count in store: "<<num_entries_
      <<", dump count: "<<actual_dump<<"\n";
}

}   // namespace MULTI_VERSIONS_NAMESPACE
