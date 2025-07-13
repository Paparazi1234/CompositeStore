#include "skiplist_rep.h"

#include <sstream>

#include "third-party/rocksdb/coding.h"

namespace MULTI_VERSIONS_NAMESPACE {

int SkipListKeyComparator::operator()(const char* prefix_len_key1,
                                      const char* prefix_len_key2) const {
  ROCKSDB_NAMESPACE::Slice key1 =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_key1);
  ROCKSDB_NAMESPACE::Slice key2 =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_key2);
  return CompareImpl(key1, key2);
}

int SkipListKeyComparator::operator()(
  const char* prefix_len_key, const ROCKSDB_NAMESPACE::Slice& key) const {
  ROCKSDB_NAMESPACE::Slice key1 =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_key);
  return CompareImpl(key1, key);
}

// only compare key and version
int SkipListKeyComparator::CompareImpl(
    const ROCKSDB_NAMESPACE::Slice& key1,
		const ROCKSDB_NAMESPACE::Slice& key2) const {
  // key format:
  // |key len|key bytes|version len|version bytes|type|value len|value bytes|
  int key_cmp = key1.compare(key2);
  if (key_cmp != 0) {
    return key_cmp;
  }
  const char* prefix_len_version1 = key1.data() + key1.size();
  const char* prefix_len_version2 = key2.data() + key2.size();
  std::string version1_str =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_version1).ToString();
  std::string version2_str =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_version2).ToString();
  std::unique_ptr<Version> version1(multi_versions_manager_->CreateVersion());
  std::unique_ptr<Version> version2(multi_versions_manager_->CreateVersion());
  version1->DecodeFrom(version1_str);
  version2->DecodeFrom(version2_str);
  return version1->CompareWith(*version2);
}

Status SkipListBackedRep::Insert(const std::string& key,
                                 const std::string& value,
                                 ValueType value_type,
                                 const Version& version) {
  std::string version_str;
  version.EncodeTo(&version_str);
  uint32_t key_size = static_cast<uint32_t>(key.size());
  uint32_t value_size = static_cast<uint32_t>(value.size());
  uint32_t version_size = static_cast<uint32_t>(version_str.size());
  // key format:
  // |key len|key bytes|version len|version bytes|type|value len|value bytes|
  uint32_t total_size =
      ROCKSDB_NAMESPACE::VarintLength(key_size) + key_size +
      ROCKSDB_NAMESPACE::VarintLength(version_size) + version_size +
      ROCKSDB_NAMESPACE::VarintLength((uint32_t)value_type) +
      ROCKSDB_NAMESPACE::VarintLength(value_size) + value_size;
  char* buf = skiplist_rep_.AllocateKey(total_size);
  char* p = ROCKSDB_NAMESPACE::EncodeVarint32(buf, key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  p = ROCKSDB_NAMESPACE::EncodeVarint32(p, version_size);
  memcpy(p, version_str.data(), version_size);
  p += version_size;
  p = ROCKSDB_NAMESPACE::EncodeVarint32(p, (uint32_t)value_type);
  p = ROCKSDB_NAMESPACE::EncodeVarint32(p, value_size);
  memcpy(p, value.data(), value_size);
  assert((uint32_t)(p + value_size - buf) == total_size);
  bool inserted = skiplist_rep_.Insert(buf);
  return inserted ? Status::OK() : Status::TryAgain(); 
}

Status SkipListBackedRep::Get(const std::string& key,
                              std::string* value,
                              const Snapshot& read_snapshot) {
  value->clear();
  std::unique_ptr<const Version>
      max_version(read_snapshot.MaxVersionInSnapshot());
  std::unique_ptr<Version>
      version_for_get(multi_versions_manager_->CreateVersion());
  SkipListLookupKey lookup_key(key, *max_version);
  ROCKSDB_NAMESPACE::Slice target_key = ROCKSDB_NAMESPACE::Slice(key);
  SkipListRep::Iterator iter(&skiplist_rep_);
  for (iter.Seek(lookup_key.LookupKey()); iter.Valid(); iter.Next()) {
    ROCKSDB_NAMESPACE::Slice key_slice =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(iter.key());
    if (key_slice.compare(target_key) == 0) {
      const char* prefix_len_version = key_slice.data() + key_slice.size();
      ROCKSDB_NAMESPACE::Slice version_slice =
          ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_version);
      std::string version_str = version_slice.ToString();
      Version* version = version_for_get.get();
      version->DecodeFrom(version_str);
      Status s;
      bool found = ValidateVisibility(*version, read_snapshot, &s);
      if (found) {
        const char* p = version_slice.data() + version_slice.size();
        uint32_t type;
        p = ROCKSDB_NAMESPACE::GetVarint32Ptr(p, p + 5, &type);
        if (type == (uint32_t)kTypeDeletion) {
          return Status::NotFound();
        } else if (type == (uint32_t)kTypeValue) {
          ROCKSDB_NAMESPACE::Slice value_slice =
              ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(p);
          value->assign(value_slice.data(), value_slice.size());
          return Status::OK();
        } else {
          return Status::Corruption();
        }
      } else if (!s.IsOK()) {
        assert(s.IsTryAgain());
        return s;
      } else {
        assert(!found && s.IsOK());
        // !found && s.IsOK(): then continue to search skiplist
      }
    } else {
      return Status::NotFound();
    }
  }
  return Status::NotFound();
}

namespace {
void ParseOneKVPair(const char* entry, std::string* key, std::string* value,
    std::string* version, uint32_t* type) {
  // key format:
  // |key len|key bytes|version len|version bytes|type|value len|value bytes|
  const char* p = entry;
  ROCKSDB_NAMESPACE::Slice key_slice =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(p);
  key->assign(key_slice.data(), key_slice.size());
  p = key_slice.data() + key_slice.size();
  ROCKSDB_NAMESPACE::Slice version_slice =
      ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(p);
  version->assign(version_slice.data(), version_slice.size());
  p = version_slice.data() + version_slice.size();
  p = ROCKSDB_NAMESPACE::GetVarint32Ptr(p, p + 5, type);
  if (*type == kTypeValue) {
    ROCKSDB_NAMESPACE::Slice value_slice =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(p);
    value->assign(value_slice.data(), value_slice.size());
  }
}
}  // anonymous namespace

uint64_t SkipListBackedRep::Dump(std::stringstream* oss,
                                 const size_t dump_count) {
  std::string key, value, version, type_str;
  uint32_t type;
  SkipListRep::Iterator iter(&skiplist_rep_);
  size_t count = 0;
  *oss<<"KV pairs in store:\n";
  for (iter.SeekToFirst(); count < dump_count && iter.Valid(); iter.Next()) {
    ParseOneKVPair(iter.key(), &key, &value, &version, &type);
    *oss<<"  {key: "<<key<<"@"<<version<<",\ttype: ";
    if (type == kTypeValue) {
      *oss<<"Put,\tvalue: "<<value<<"}\n";
    } else if (type == kTypeDeletion) {
      *oss<<"Delete,\tvalue: Nil}\n";
    } else {
      *oss<<"unknown,\tvalue: Nil}\n";
    }
    count++;
  }
  return count;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
