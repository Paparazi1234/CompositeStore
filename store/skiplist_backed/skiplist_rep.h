#pragma once

#include <memory>
#include <sstream>

#include "format.h"
#include "memory_allocator.h"
#include "include/multi_versions.h"
#include "third-party/rocksdb/inlineskiplist.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListLookupKey {
 public:
	SkipListLookupKey(const std::string& key, const Version& max_version) {
		std::string version_str;
		max_version.EncodeTo(&version_str);
		uint32_t key_size = (uint32_t)key.size();
		uint32_t version_size = (uint32_t)version_str.size();
		uint32_t total_size =
				ROCKSDB_NAMESPACE::VarintLength(key_size) + key_size +
				ROCKSDB_NAMESPACE::VarintLength(version_size) + version_size;
		if (total_size < (uint32_t)sizeof(space_)) {
			key_buff_ = space_;
		} else {
			key_buff_ = new char[total_size];
		}
		char* p = key_buff_;
		p = ROCKSDB_NAMESPACE::EncodeVarint32(p, key_size);
		memcpy(p, key.data(), key_size);
		p += key_size;
		p = ROCKSDB_NAMESPACE::EncodeVarint32(p, version_size);
		memcpy(p, version_str.data(), version_size);
		assert((uint32_t)(p + version_size - key_buff_) == total_size);
	}

	~SkipListLookupKey() {
		if (key_buff_ != space_) {
			delete[] key_buff_;
		}
	}

	const char* LookupKey() const {
		return key_buff_;
	}

 private:
	char* key_buff_;
	char space_[100];
};

// key format:
// |key len|key bytes|version len|version bytes|type|value len|value bytes|
class SkipListKeyComparator : ROCKSDB_NAMESPACE::KeyComparator {
 public:
	using ROCKSDB_NAMESPACE::KeyComparator::DecodedType;
	using ROCKSDB_NAMESPACE::KeyComparator::decode_key;
	SkipListKeyComparator(const MultiVersionsManager* mvm)
			: multi_versions_manager_(mvm),
			  version1_(multi_versions_manager_->CreateVersion()),
				version2_(multi_versions_manager_->CreateVersion()) {}
	~SkipListKeyComparator() {}

	virtual int operator()(const char* prefix_len_key1,
                         const char* prefix_len_key2) const override;

  virtual int operator()(const char* prefix_len_key,
                         const ROCKSDB_NAMESPACE::Slice& key) const override;
 private:
	int CompareImpl(const ROCKSDB_NAMESPACE::Slice& key1,
								  const ROCKSDB_NAMESPACE::Slice& key2) const;
	const MultiVersionsManager* multi_versions_manager_;
	// use to decode version out of underlying stored format
	std::unique_ptr<Version> version1_;
	std::unique_ptr<Version> version2_;
};

class SkipListBackedRep {
 public:
	SkipListBackedRep(const MultiVersionsManager* multi_versions_manager)
			: multi_versions_manager_(multi_versions_manager),
				comparator_(multi_versions_manager),
			  allocator_(),
				version_for_get_(nullptr),
				skiplist_rep_(comparator_, &allocator_),
				num_entries_(0),
				num_deletes_(0) {}
	~SkipListBackedRep() {}

	Status Insert(const std::string& key, const std::string& value,
							  Version* version, ValueType type);
	
	Status Get(const std::string& key, const Snapshot& read_snapshot,
						 std::string* value);

	void Dump(std::stringstream* oss, const size_t dump_count = -1);

 private:
	Version* VersionForGet() {
		if (version_for_get_.get() == nullptr) {
			version_for_get_.reset(multi_versions_manager_->CreateVersion());
		}
		return version_for_get_.get();
	}
	const MultiVersionsManager* multi_versions_manager_;
	SkipListKeyComparator comparator_;
	MemoryAllocator allocator_;
	
	// used to decode version out of underlying store in Get path, lazy initialize
	std::unique_ptr<Version> version_for_get_ = nullptr;
	using SkipListRep =
			ROCKSDB_NAMESPACE::InlineSkipList<const SkipListKeyComparator&>;
	SkipListRep skiplist_rep_;

	uint64_t num_entries_;
	uint64_t num_deletes_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
