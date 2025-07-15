#pragma once

#include <memory>

#include "write_queue.h"
#include "include/transaction_store.h"
#include "include/multi_versions.h"
#include "include/txn_lock_manager.h"
#include "include/staging_write.h"
#include "include/mvcc_write_buffer.h"

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

class MVCCTxnStore : public TransactionStore {
 public:
  // No copying allowed
  MVCCTxnStore(const MVCCTxnStore&) = delete;
  MVCCTxnStore& operator=(const MVCCTxnStore&) = delete;

  MVCCTxnStore(const StoreOptions& store_options,
      				 const TransactionStoreOptions& txn_store_options,
      				 const MultiVersionsManagerFactory& multi_versions_mgr_factory,
      				 const TxnLockManagerFactory& txn_lock_mgr_factory,
      				 TransactionFactory* txn_factory,
							 StagingWriteFactory* staging_write_factory,
							 const MVCCWriteBufferFactory& mvcc_write_buffer_factory);
  virtual ~MVCCTxnStore() {}

  virtual Status Put(const WriteOptions& write_options,
              		   const std::string& key,
										 const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                				const std::string& key) override;
  virtual Status Get(const ReadOptions& read_options,
             				 const std::string& key,
										 std::string* value) override;

	virtual Transaction* BeginTransaction(const WriteOptions& write_options,
      																  const TransactionOptions& txn_options,
																				Transaction* reused) override;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

	Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

	void DumpKVPairs(std::stringstream* oss, const size_t dump_count = -1) {
		mvcc_write_buffer_->Dump(oss, dump_count);
	}

	uint64_t RawDataSize() const {
		return mvcc_write_buffer_->RawDataSize();
	}

	void RecoverMultiVersionsManagerFrom(const Version& orig) {
		multi_versions_manager_->Initialize(orig);
	}

	MultiVersionsManager* GetMultiVersionsManager() const {
		return multi_versions_manager_.get();
	}

	SnapshotManager* GetSnapshotManager() const {
		return snapshot_manager_.get();
	}

	StagingWriteFactory* GetStagingWriteFactory() {
		return staging_write_factory_.get();
	}

	Status CommitStagingWrite(
			const WriteOptions& write_options,
			StagingWrite* staging_write,
			MaintainVersionsCallbacks& maintain_versions_callbacks,
			WriteQueue& write_queue);

	Status GetInternal(const ReadOptions& read_options,
             				 const std::string& key,
										 std::string* value);

	virtual uint64_t CalcuNumVersionsIncForStagingWrite(
			const StagingWrite* staging_write) const {
		assert(staging_write->Count() > 0);
		// default: version per key
		return staging_write->Count();
	}

	// Default: use first_write_queue_ as prepare queue
	virtual WriteQueue& GetPrepareQueue() {
		return first_write_queue_;
	}

	// Default: use first_write_queue_ as commit queue
	virtual WriteQueue& GetCommitQueue() {
		return first_write_queue_;
	}

	void TEST_Crash() override {
    multi_versions_manager_->TEST_Crash();
  }

 protected:
	std::unique_ptr<MultiVersionsManager> multi_versions_manager_;
	std::unique_ptr<SnapshotManager> snapshot_manager_;
	std::unique_ptr<TxnLockManager> txn_lock_manager_;
	std::unique_ptr<TransactionFactory> txn_factory_;
	std::unique_ptr<StagingWriteFactory> staging_write_factory_;

	std::unique_ptr<MVCCWriteBuffer> mvcc_write_buffer_;

	// first write queue: can be used to deal with both write buffer relative and
	// non write buffer relative operation, like WAL persisting, write buffer
	// insertion, etc
	WriteQueue first_write_queue_;
	// second write queue: can only be used to deal with non write buffer relative
	// operation, like WAL persisting, etc
	WriteQueue second_write_queue_;

 private:
	Transaction* BeginInternalTransaction(const WriteOptions& write_options);
};

}   // namespace MULTI_VERSIONS_NAMESPACE
