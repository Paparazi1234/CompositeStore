#pragma once

#include <memory>

#include "write_lock.h"
#include "write_batch.h"
#include "skiplist_rep.h"
#include "include/store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryStore : public Store {
 public:
  // No copying allowed
  SkipListBackedInMemoryStore(const SkipListBackedInMemoryStore&) = delete;
  SkipListBackedInMemoryStore& operator=(
      const SkipListBackedInMemoryStore&) = delete;

  SkipListBackedInMemoryStore(const StoreOptions& store_options,
															const MultiVersionsManagerFactory& factory);
  ~SkipListBackedInMemoryStore() {}

  virtual Status Put(const WriteOptions& write_options,
              		   const std::string& key, const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                				const std::string& key) override;
  virtual Status Get(const ReadOptions& read_options,
             				 const std::string& key, std::string* value) override;

	void DumpKVPairs(std::stringstream* oss, const size_t dump_count = -1) {
		skiplist_backed_rep_.Dump(oss, dump_count);
	}

 protected:
 	friend class SkipListBackedInMemoryTxnStore;
	friend class WriteCommittedTransaction;

	MultiVersionsManager* GetMultiVersionsManager() const {
		return multi_versions_manager_.get();
	}

	SnapshotManager* GetSnapshotManager() const {
		return snapshot_manager_.get();
	}

	Status WriteInternal(
			const WriteOptions& write_options, WriteBatch* write_batch);
  
  Version* VersionForInsert() {
    if (version_for_insert_.get() == nullptr) {
      version_for_insert_.reset(multi_versions_manager_->CreateVersion());
    }
    return version_for_insert_.get();
  }

 private:
	std::unique_ptr<MultiVersionsManager> multi_versions_manager_;
	std::unique_ptr<SnapshotManager> snapshot_manager_;

	WriteLock write_lock_;
	// used to assign version for inserting entries in write path, lazy initialize
	std::unique_ptr<Version> version_for_insert_ = nullptr;
	SkipListBackedRep skiplist_backed_rep_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
