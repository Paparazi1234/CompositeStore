#include "mvcc_transaction.h"

#include "util/cast_util.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCTransaction::MVCCTransaction(TransactionStore* txn_store,
                                 const WriteOptions& write_options,
                                 const TransactionOptions& txn_options) {
  Reinitialize(txn_store, write_options, txn_options);
}

Status MVCCTransaction::TryLock(const std::string& key) {
  return txn_store_->TryLock(key);
}

void MVCCTransaction::UnLock(const std::string& key) {
  txn_store_->TryLock(key);
}

Status MVCCTransaction::Put(const std::string& key, const std::string& value) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key);
  if (s.IsOK()) {
    GetStagingWrite()->Put(key, value);
  }
  return s;
}

Status MVCCTransaction::Delete(const std::string& key) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key);
  if (s.IsOK()) {
    GetStagingWrite()->Delete(key);
  }
  return s;
}

// first get from transaction's self staging write, if not found then get from
// store
Status MVCCTransaction::Get(const ReadOptions& read_options,
                            const std::string& key, std::string* value) {
  assert(value);
  value->clear();
  StagingWrite::GetReault result = GetStagingWrite()->Get(key, value);
  if (result == StagingWrite::GetReault::kFound) {
    return Status::OK();
  } else if (result == StagingWrite::GetReault::kDeleted) {
    return Status::NotFound();
  } else {
    assert(result == StagingWrite::GetReault::kNotFound);
    return txn_store_->Get(read_options, key, value);
  }
}

void MVCCTransaction::SetSnapshot() {

}

void MVCCTransaction::Reinitialize(TransactionStore* txn_store,
                                   const WriteOptions& write_options,
                                   const TransactionOptions& txn_options) {
  txn_store_ = static_cast_with_check<MVCCTxnStore>(txn_store);
  txn_state_ = STAGE_WRITING;
  write_options_ = write_options;
  if (staging_write_.get() == nullptr) {
    staging_write_.reset(
        txn_store_->GetStagingWriteFactory()->CreateStagingWrite());
  }
  Clear();
}

class MVCCTransaction::ReleaseTxnLockHandler : public StagingWrite::Handler {
 public:
  ReleaseTxnLockHandler(MVCCTransaction* txn) : txn_(txn) {}
  ~ReleaseTxnLockHandler() {}
  virtual Status Put(const std::string& key,
                     const std::string& value) override {
    txn_->UnLock(key);
    return Status::OK();
  }

  virtual Status Delete(const std::string& key) override {
    txn_->UnLock(key);
    return Status::OK();
  }

 private:
  MVCCTransaction* txn_;
};

void MVCCTransaction::ClearTxnLocks() {
  StagingWrite* staging_write = GetStagingWrite();
  if (staging_write->IsEmpty()) {
    return;
  }
  ReleaseTxnLockHandler handler(this);
  Status s = staging_write->Iterate(&handler);
  assert(s.IsOK());
}

}   // namespace MULTI_VERSIONS_NAMESPACE
