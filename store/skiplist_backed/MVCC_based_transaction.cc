#include "MVCC_based_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCBasedTransaction::MVCCBasedTransaction(
    TransactionStore* transaction_store, const TransactionOptions& txn_options,
    const WriteOptions& write_options)
    : write_options_(write_options),
      txn_state_(STAGE_WRITING) {
  transaction_store_ =
    reinterpret_cast<SkipListBackedInMemoryTxnStore*>(transaction_store);
  base_store_ = transaction_store_->GetBaseStore();
}
                      
Status MVCCBasedTransaction::TryLock(const std::string& key) {
  return transaction_store_->TryLock(key);
}

void MVCCBasedTransaction::UnLock(const std::string& key) {
  transaction_store_->TryLock(key);
}

Status MVCCBasedTransaction::Put(const std::string& key,
                                 const std::string& value) {
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Put(key, value);
  }
  return s;
}

Status MVCCBasedTransaction::Delete(const std::string& key) {
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Delete(key);
  }
  return s;
}

// first get from transaction self buffered writes, if not found then get from
// store
Status MVCCBasedTransaction::Get(const ReadOptions& read_options,
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
    return transaction_store_->Get(read_options, key, value);
  }
}

// only control the transaction excution behavior, let derived class
// implements the details
Status MVCCBasedTransaction::Prepare() {
  Status s;
  if (txn_state_ == STAGE_WRITING) {
    txn_state_.store(STAGE_PREPARING, std::memory_order_relaxed);
    s = PrepareImpl();
    if (s.IsOK()) {
      txn_state_.store(STAGE_PREPARED, std::memory_order_relaxed);
    }
  } else if (txn_state_ == STAGE_PREPARED) {
    s = Status::InvalidArgument();
  } else if (txn_state_ == STAGE_COMMITTED) {
    s = Status::InvalidArgument();
  } else if (txn_state_ == STAGE_ROLLBACKED) {
    s = Status::InvalidArgument();
  } else {
    s = Status::InvalidArgument();
  }
  return s;
}

Status MVCCBasedTransaction::Commit() {
  Status s;
  if (txn_state_ == STAGE_WRITING) {
    txn_state_.store(STAGE_COMMITTING, std::memory_order_relaxed);
    s = CommitWithoutPrepareImpl();
    if (s.IsOK()) {
      txn_state_.store(STAGE_COMMITTED, std::memory_order_relaxed);
    }
  } else if (txn_state_ == STAGE_PREPARED) {
    txn_state_.store(STAGE_COMMITTING, std::memory_order_relaxed);
    s = CommitWithPrepareImpl();
    if (s.IsOK()) {
      txn_state_.store(STAGE_COMMITTED, std::memory_order_relaxed);
    }
  } else if (txn_state_ == STAGE_COMMITTED) {
    s = Status::InvalidArgument();
  } else if (txn_state_ == STAGE_ROLLBACKED) {
    s = Status::InvalidArgument();
  } else {
    s = Status::InvalidArgument();
  }
  return s;
}

Status MVCCBasedTransaction::Rollback() {
  Status s;
  // if rollback during write stage, then txn remains in STAGE_WRITING state and
  // can be used to future writes(equivalent to rollback to savepoint)
  if (txn_state_ == STAGE_WRITING) {
    s = RollbackImpl();
  } else if (txn_state_ == STAGE_PREPARED) {
    txn_state_.store(STAGE_ROLLBACKING, std::memory_order_relaxed);
    s = RollbackImpl();
    if (s.IsOK()) {
      // if rollback after prepare executed, the txn can't be used to do future 
      // writes any more
      txn_state_.store(STAGE_ROLLBACKED, std::memory_order_relaxed);
    }
  } else if (txn_state_ == STAGE_COMMITTED) {
    s = Status::InvalidArgument();
  } else if (txn_state_ == STAGE_ROLLBACKED) {
    s = Status::InvalidArgument();
  } else {
    s = Status::InvalidArgument();
  }
  return s;
}

void MVCCBasedTransaction::SetSnapshot() {

}

void MVCCBasedTransaction::Reinitialize(TransactionStore* transaction_store,
    const TransactionOptions& txn_options, const WriteOptions& write_options) {
  transaction_store_ =
    reinterpret_cast<SkipListBackedInMemoryTxnStore*>(transaction_store);
  base_store_ = transaction_store_->GetBaseStore();
  write_options_ = write_options;
  txn_state_ = STAGE_WRITING;
  Clear();
}

class ReleaseTxnLockHandler : public WriteBatch::Handler {
 public:
  ReleaseTxnLockHandler(MVCCBasedTransaction* txn) : txn_(txn) {}
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
  MVCCBasedTransaction* txn_;
};

void MVCCBasedTransaction::ClearTxnLocks() {
  if (write_batch_.IsEmpty()) {
    return;
  }
  ReleaseTxnLockHandler handler(this);
  Status s = write_batch_.Iterate(&handler);
  assert(s.IsOK());
}

Status WriteCommittedTransaction::PrepareImpl() {
  // in-memory only store, so nothing to do when prepare(because prepare mainly
  // deals with WAL)
  return Status::OK();
}

Status WriteCommittedTransaction::CommitWithPrepareImpl() {
  Status s = base_store_->WriteInternal(write_options_, &write_batch_);
  Clear();
  return s;
}

Status WriteCommittedTransaction::CommitWithoutPrepareImpl() {
  Status s = base_store_->WriteInternal(write_options_, &write_batch_);
  Clear();
  return s;
}

Status WriteCommittedTransaction::RollbackImpl() {
  Clear();
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
