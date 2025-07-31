#include "write_committed_transaction.h"

#include "utils/cast_util.h"

namespace COMPOSITE_STORE_NAMESPACE {

Status WriteCommittedTransaction::PrepareImpl() {
  // in-memory only store, so nothing to do when prepare(because prepare mainly
  // deals with WAL)
  return Status::OK();
}

Status WriteCommittedTransaction::CommitWithPrepareImpl() {
  MVCCTxnStore* txn_store = GetTxnStore();
  WCTxnMaintainVersionsCB wc_maintain_versions_cb(txn_store);
  return txn_store->CommitStagingWrite(write_options_,
                                       GetStagingWrite(),
                                       wc_maintain_versions_cb,
                                       txn_store->GetCommitQueue());
}

Status WriteCommittedTransaction::CommitWithoutPrepareImpl() {
  MVCCTxnStore* txn_store = GetTxnStore();
  WCTxnMaintainVersionsCB wc_maintain_versions_cb(txn_store);
  return txn_store->CommitStagingWrite(write_options_,
                                       GetStagingWrite(),
                                       wc_maintain_versions_cb,
                                       txn_store->GetCommitQueue());
}

Status WriteCommittedTransaction::RollbackImpl() {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
  return Status::OK();
}

}   // namespace COMPOSITE_STORE_NAMESPACE
