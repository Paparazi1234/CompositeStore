#include "MVCC_based_txn.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCBasedTxn::MVCCBasedTxn(TransactionStore* txn_store,
                          const TransactionOptions& txn_options,
                          const WriteOptions& write_options)
                          : write_options_(write_options),
                            txn_state_(STAGE_WRITING) {
  txn_store_ = reinterpret_cast<SkipListBackedInMemoryTxnStore*>(txn_store);
}

Status MVCCBasedTxn::TryLock(const std::string& key) {
  return txn_store_->TryLock(key);
}

void MVCCBasedTxn::UnLock(const std::string& key) {
  txn_store_->TryLock(key);
}

Status MVCCBasedTxn::Put(const std::string& key, const std::string& value) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Put(key, value);
  }
  return s;
}

Status MVCCBasedTxn::Delete(const std::string& key) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Delete(key);
  }
  return s;
}

// first get from transaction's self buffered writes, if not found then get from
// store
Status MVCCBasedTxn::Get(const ReadOptions& read_options,
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

// only control the transaction excution behavior, let derived class
// implements the details
Status MVCCBasedTxn::Prepare() {
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

Status MVCCBasedTxn::Commit() {
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

Status MVCCBasedTxn::Rollback() {
  Status s;
  // if rollback during write stage, then txn remains in STAGE_WRITING state and
  // can be used to do future writes(equivalent to rollback to savepoint)
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

void MVCCBasedTxn::SetSnapshot() {

}

void MVCCBasedTxn::Reinitialize(TransactionStore* txn_store,
                                const TransactionOptions& txn_options,
                                const WriteOptions& write_options) {
  txn_store_ = reinterpret_cast<SkipListBackedInMemoryTxnStore*>(txn_store);
  write_options_ = write_options;
  txn_state_ = STAGE_WRITING;
  Clear();
}

class ReleaseTxnLockHandler : public WriteBatch::Handler {
 public:
  ReleaseTxnLockHandler(MVCCBasedTxn* txn) : txn_(txn) {}
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
  MVCCBasedTxn* txn_;
};

void MVCCBasedTxn::ClearTxnLocks() {
  if (write_batch_.IsEmpty()) {
    return;
  }
  ReleaseTxnLockHandler handler(this);
  Status s = write_batch_.Iterate(&handler);
  assert(s.IsOK());
}

Status WriteCommittedTxn::PrepareImpl() {
  // in-memory only store, so nothing to do when prepare(because prepare mainly
  // deals with WAL)
  return Status::OK();
}

Status WriteCommittedTxn::CommitWithPrepareImpl() {
  Status s = txn_store_->WriteInternal(write_options_, &write_batch_);
  Clear();
  return s;
}

Status WriteCommittedTxn::CommitWithoutPrepareImpl() {
  Status s = txn_store_->WriteInternal(write_options_, &write_batch_);
  Clear();
  return s;
}

Status WriteCommittedTxn::RollbackImpl() {
  Clear();
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
