#pragma once

#include <memory>

#include "write_queue.h"
#include "write_batch.h"
#include "skiplist_rep.h"
#include "include/transaction_store.h"
#include "multi_version/sequence_based/seq_based_multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MaintainVersionsCallbacks {
 public:
	virtual ~MaintainVersionsCallbacks() {}

	virtual Status BeforePersistWALCallback(TransactionStore* store) {
		return Status::OK();
	}
	virtual Status BeforeInsertWriteBufferCallback(const Version* version,
																								 uint32_t count) {
		return Status::OK();
	}
	virtual Status AfterInsertWriteBufferCallback(const Version* version) {
		return Status::OK();
	}

	virtual bool NeedMaintainBeforePersistWAL() const { return false; }
	virtual bool NeedMaintainBeforeInsertWriteBuffer() const { return false; }
	virtual bool NeedMaintainAfterInsertWriteBuffer() const { return false; }
};

class SkipListBackedInMemoryStore : public TransactionStore {
 public:
  // No copying allowed
  SkipListBackedInMemoryStore(const SkipListBackedInMemoryStore&) = delete;
  SkipListBackedInMemoryStore& operator=(
      const SkipListBackedInMemoryStore&) = delete;

  SkipListBackedInMemoryStore(
			const StoreOptions& store_options,
			const MultiVersionsManagerFactory& multi_versions_mgr_factory);
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

	uint64_t RawDataSize() const {
		return skiplist_backed_rep_.RawDataSize();
	}

	virtual Transaction* BeginTransaction(
			const WriteOptions& /*write_options*/,
			const TransactionOptions& /*txn_options*/,
			Transaction* /*old_txn*/) override;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* /*snapshot*/) override;

	MultiVersionsManager* GetMultiVersionsManager() const {
		return multi_versions_manager_.get();
	}

	void RecoverMultiVersionsManagerFrom(const Version& orig) {
		multi_versions_manager_->Initialize(orig);
	}

 protected:
	friend class WriteCommittedTxn;
	friend class WritePreparedTxn;

	SnapshotManager* GetSnapshotManager() const {
		return snapshot_manager_.get();
	}

	Status WriteInternal(
			const WriteOptions& write_options, WriteBatch* write_batch,
			MaintainVersionsCallbacks& maintain_versions_callbacks,
		  WriteQueue& write_queue);

	Status GetInternal(const ReadOptions& read_options,
             				 const std::string& key, std::string* value);

  Version* VersionForInsert() {
    if (version_for_insert_.get() == nullptr) {
      version_for_insert_.reset(multi_versions_manager_->CreateVersion());
    }
    return version_for_insert_.get();
  }

	Snapshot* ReadViewForGet() {
		if (read_view_for_get_.get() == nullptr) {
			read_view_for_get_.reset(snapshot_manager_->CreateSnapshot());
		}
		return read_view_for_get_.get();
	}

	virtual uint64_t CalculateNumVersionsForWriteBatch(
			const WriteBatch* write_batch) const {
		assert(write_batch->Count() > 0);
		// default: version per key
		return write_batch->Count();
	}

	void TEST_Crash() override {
    multi_versions_manager_->TEST_Crash();
  }

	// first write queue: used to deal with write buffer relative operation
	WriteQueue first_write_queue_;
	// second write queue: used to deal with non write buffer relative operation,
	// like WAL persisting, etc
	WriteQueue second_write_queue_;

	// used to allocate memory for stuff that have the same lift time as the store
	MemoryAllocator permanent_stuff_allocator_;
 private:
	std::unique_ptr<MultiVersionsManager> multi_versions_manager_;
	std::unique_ptr<SnapshotManager> snapshot_manager_;

	// used to assign version for inserting entries in write path, lazy initialize
	std::unique_ptr<Version> version_for_insert_ = nullptr;
	// used to obtain the latest read view of the underlying store when clients
	// didn't specify a snapshot in read options of their Get() calls, lazy
	// initialize
	std::unique_ptr<Snapshot> read_view_for_get_ = nullptr;
	SkipListBackedRep skiplist_backed_rep_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
