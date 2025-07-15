#pragma once

#include "pessimistic_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WCTxnMaintainVersionsCB : public MaintainVersionsCallbacks {
 public:
  WCTxnMaintainVersionsCB(MVCCTxnStore* store)
      : multi_versions_manager_(store->GetMultiVersionsManager()) {}
  virtual ~WCTxnMaintainVersionsCB() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return false; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

	Status AfterInsertWriteBufferCallback(const Version* version)  override {
    const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    const Version& prepared_uncommitted_started = dummy_version;
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = 0;
    // as for WriteCommitted txn, all we need to do it's to advance max visible
    // version after insert the txn's staging write to write buffer
    multi_versions_manager_->EndCommitVersions(prepared_uncommitted_started,
                                               committed,
                                               num_prepared_uncommitteds);
    return Status::OK();
  }

 private:
  MultiVersionsManager* multi_versions_manager_;
};

class WriteCommittedTransaction : public PessimisticTransaction {
 public:
  // No copying allowed
  WriteCommittedTransaction(const WriteCommittedTransaction&) = delete;
  WriteCommittedTransaction& operator=(
      const WriteCommittedTransaction&) = delete;

  WriteCommittedTransaction(TransactionStore* txn_store,
                            const WriteOptions& write_options,
                            const TransactionOptions& txn_options)
      : PessimisticTransaction(txn_store, write_options, txn_options) {}
  virtual ~WriteCommittedTransaction() {}

 private:
  virtual Status PrepareImpl() override;
  virtual Status CommitWithPrepareImpl() override;
  virtual Status CommitWithoutPrepareImpl() override;
  virtual Status RollbackImpl() override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
