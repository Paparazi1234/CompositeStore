#include "mvcc_transaction.h"

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
    write_batch_.Put(key, value);
  }
  return s;
}

Status MVCCTransaction::Delete(const std::string& key) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Delete(key);
  }
  return s;
}

// first get from transaction's self staging write, if not found then get from
// store
Status MVCCTransaction::Get(const ReadOptions& read_options,
                            const std::string& key, std::string* value) {
  assert(value);
  value->clear();
  WriteBatch::GetReault result = write_batch_.Get(key, value);
  if (result == WriteBatch::GetReault::kFound) {
    return Status::OK();
  } else if (result == WriteBatch::GetReault::kDeleted) {
    return Status::NotFound();
  } else {
    assert(result == WriteBatch::GetReault::kNotFound);
    return txn_store_->Get(read_options, key, value);
  }
}

void MVCCTransaction::SetSnapshot() {

}

void MVCCTransaction::Reinitialize(TransactionStore* txn_store,
                                   const WriteOptions& write_options,
                                   const TransactionOptions& txn_options) {
  txn_store_ = reinterpret_cast<MVCCTxnStore*>(txn_store);
  txn_state_ = STAGE_WRITING;
  write_options_ = write_options;
  Clear();
}

class MVCCTransaction::ReleaseTxnLockHandler : public WriteBatch::Handler {
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
  if (write_batch_.IsEmpty()) {
    return;
  }
  ReleaseTxnLockHandler handler(this);
  Status s = write_batch_.Iterate(&handler);
  assert(s.IsOK());
}

}   // namespace MULTI_VERSIONS_NAMESPACE
