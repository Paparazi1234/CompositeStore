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

  SkipListBackedInMemoryStore(MultiVersionsManagerFactory& factory)
			: multi_versions_manager_(factory.CreateMultiVersionsManager()),
				snapshot_manager_(
						factory.CreateSnapshotManager(multi_versions_manager_.get())),
				skiplist_backed_rep_(multi_versions_manager_.get()) {}
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

	std::unique_ptr<MultiVersionsManager> multi_versions_manager_;
	std::unique_ptr<SnapshotManager> snapshot_manager_;

	WriteLock write_lock_;

	SkipListBackedRep skiplist_backed_rep_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
