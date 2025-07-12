#include "write_committed_transaction.h"

#include "transaction_store/mvcc_txn_store/pessimistic_txn_store/write_committed_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

namespace {
class WCTxnMaintainVersionsCB : public MaintainVersionsCallbacks {
  public:
  WCTxnMaintainVersionsCB(MVCCTxnStore* store) {
    store_impl_ = reinterpret_cast<WriteCommittedTxnStore*>(store);
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
	~WCTxnMaintainVersionsCB() {}

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
  WriteCommittedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status WriteCommittedTransaction::PrepareImpl() {
  // in-memory only store, so nothing to do when prepare(because prepare mainly
  // deals with WAL)
  return Status::OK();
}

Status WriteCommittedTransaction::CommitWithPrepareImpl() {
  WCTxnMaintainVersionsCB wc_maintain_versions_cb(this->txn_store_);
  WriteCommittedTxnStore* store_impl =
      reinterpret_cast<WriteCommittedTxnStore*>(txn_store_);
  return txn_store_->WriteInternal(write_options_,
                                   staging_write_.get(),
                                   wc_maintain_versions_cb,
                                   store_impl->GetCommitQueue());
}

Status WriteCommittedTransaction::CommitWithoutPrepareImpl() {
  WCTxnMaintainVersionsCB wc_maintain_versions_cb(this->txn_store_);
  WriteCommittedTxnStore* store_impl =
      reinterpret_cast<WriteCommittedTxnStore*>(txn_store_);
  return txn_store_->WriteInternal(write_options_,
                                   staging_write_.get(),
                                   wc_maintain_versions_cb,
                                   store_impl->GetCommitQueue());
}

Status WriteCommittedTransaction::RollbackImpl() {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
