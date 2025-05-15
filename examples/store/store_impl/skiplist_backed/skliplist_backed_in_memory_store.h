#pragma once

#include <memory>

#include "store.h"
#include "multi_versions.h"
#include "write_batch.h"
#include "../../../third-party/rocksdb/inlineskiplist.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListKey {
 public:
	SkipListKey(const std::string& key, const std::string& value,
						  Version* version, ValueType value_type)
							: key_(key), value_(value), version_(version),
								value_type_(value_type) {}
	~SkipListKey() {}

 private:
	friend class SkipListKeyComparator;
	const std::string key_;
	const std::string value_;
	std::unique_ptr<Version> version_;
	const ValueType value_type_;
};

class SkipListKeyComparator {
 public:
	~SkipListKeyComparator() {}
	int operator()(const SkipListKey& lhs, const SkipListKey& rhs) const {
		if (lhs.key_ != rhs.key_) {
			if (lhs.key_ < rhs.key_) {
				return -1;
			}
			return 1;
		}
		return lhs.version_->CompareWith(*rhs.version_);
	}
};

class SkipListBackedInMemoryStore : public Store {
 public:
 	static Status Open(Store** store_ptr);
  // No copying allowed
  SkipListBackedInMemoryStore(const SkipListBackedInMemoryStore&) = delete;
  SkipListBackedInMemoryStore& operator=(
      const SkipListBackedInMemoryStore&) = delete;

  SkipListBackedInMemoryStore()
			: skiplist_backed_rep_(SkipListKeyComparator(), nullptr) {}
  ~SkipListBackedInMemoryStore() {}

  virtual Status Put(const WriteOptions& write_options,
              		   const std::string& key, const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                				const std::string& key) override;
  virtual Status Get(const ReadOptions& read_options,
             				 const std::string& key, std::string* value) override;
	
	virtual MultiVersionsManager* GetMultiVersionsManager() const override {
		return multi_versions_manager_.get();
	}

 protected:
 	friend class SkipListBackedInMemoryTxnStore;
	Status WriteInternal(
			const WriteOptions& write_options, WriteBatch* write_batch);
  using SkipListBackedRep =
			ROCKSDB_NAMESPACE::InlineSkipList<const SkipListKeyComparator&>;
	SkipListBackedRep skiplist_backed_rep_;

	std::shared_ptr<MultiVersionsManager> multi_versions_manager_;
	std::shared_ptr<SnapshotManager> snapshot_manager_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
