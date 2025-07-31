#pragma once

#include <memory>

#include "composite_store/format.h"
#include "composite_store/multi_versions.h"
#include "utils/memory_allocator.h"
#include "third-party/rocksdb/inlineskiplist.h"

namespace COMPOSITE_STORE_NAMESPACE {

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
		if (total_size <= (uint32_t)sizeof(space_)) {
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

class AllocatorForSkipList : public ROCKSDB_NAMESPACE::Allocator {
 public:
	AllocatorForSkipList(MemoryAllocator* memory_allocator)
			: memory_allocator_(memory_allocator) {}
	~AllocatorForSkipList() {}

  char* Allocate(size_t bytes) {
		return memory_allocator_->Allocate(bytes);
	}

  char* AllocateAligned(size_t bytes) {
		return memory_allocator_->AllocateAligned(bytes);
	}

 private:
	MemoryAllocator* memory_allocator_;
};

// key format:
// |key len|key bytes|version len|version bytes|type|value len|value bytes|
class SkipListKeyComparator : ROCKSDB_NAMESPACE::KeyComparator {
 public:
	using ROCKSDB_NAMESPACE::KeyComparator::DecodedType;
	using ROCKSDB_NAMESPACE::KeyComparator::decode_key;
	SkipListKeyComparator(const MultiVersionsManager* mvm)
			: multi_versions_manager_(mvm) {}
	~SkipListKeyComparator() {}

	virtual int operator()(const char* prefix_len_key1,
                         const char* prefix_len_key2) const override;

  virtual int operator()(const char* prefix_len_key,
                         const ROCKSDB_NAMESPACE::Slice& key) const override;
 private:
	int CompareImpl(const ROCKSDB_NAMESPACE::Slice& key1,
								  const ROCKSDB_NAMESPACE::Slice& key2) const;
	const MultiVersionsManager* multi_versions_manager_;
};

class SkipListBackedRep {
 public:
	SkipListBackedRep(const MultiVersionsManager* multi_versions_manager,
										MemoryAllocator* memory_allocator)
			: multi_versions_manager_(multi_versions_manager),
				comparator_(multi_versions_manager),
			  allocator_(memory_allocator),
				skiplist_rep_(comparator_, &allocator_) {}
	~SkipListBackedRep() {}

	Status Insert(const std::string& key,
								const std::string& value,
								ValueType value_type,
							  const Version& version);
	
	Status Get(const std::string& key,
						 std::string* value,
						 const Snapshot& read_snapshot);

	uint64_t Dump(std::stringstream* oss, const size_t dump_count = -1);

 private:
	bool ValidateVisibility(const Version& version,
													const Snapshot& snapshot,
													Status* status) {
		assert(status);
		bool found = true;
		*status = Status::OK();
		bool snap_exist;
		bool visible = multi_versions_manager_->IsVersionVisibleToSnapshot(
				version, snapshot, &snap_exist);
		if (visible && snap_exist) {
			found = true;
			*status = Status::OK();
		} else if (!visible && snap_exist) {
			found = false;
			*status = Status::OK();
		} else if (!visible && !snap_exist) {
			found = false;
			*status = Status::TryAgain();
		} else {	// won't return (visible && !snap_exist)
			assert(false);
		}
		assert((found && status->IsOK()) ||
					 (!found && status->IsOK()) ||
					 (!found && status->IsTryAgain()));
		return found;
	}

	const MultiVersionsManager* multi_versions_manager_;
	SkipListKeyComparator comparator_;
	AllocatorForSkipList allocator_;
	using SkipListRep =
			ROCKSDB_NAMESPACE::InlineSkipList<const SkipListKeyComparator&>;
	SkipListRep skiplist_rep_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
