#include "mvcc_transaction.h"

#include "util/cast_util.h"

namespace COMPOSITE_STORE_NAMESPACE {

MVCCTransaction::MVCCTransaction(TransactionStore* txn_store,
                                 const WriteOptions& write_options,
                                 const TransactionOptions& txn_options) {
  Initialize(txn_store, write_options, txn_options);
}

Status MVCCTransaction::Put(const std::string& key, const std::string& value) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key, true /* exclusive */, GetLockTimeOutMs());
  if (s.IsOK()) {
    GetStagingWrite()->Put(key, value);
  }
  return s;
}

Status MVCCTransaction::Delete(const std::string& key) {
  if (!IsInWriteStage()) {
    return Status::InvalidArgument();
  }
  Status s = TryLock(key, true /* exclusive */, GetLockTimeOutMs());
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

void MVCCTransaction::Initialize(TransactionStore* txn_store,
                                 const WriteOptions& write_options,
                                 const TransactionOptions& txn_options) {
  txn_store_ = static_cast_with_check<MVCCTxnStore>(txn_store);
  txn_state_ = STAGE_WRITING;

  write_options_ = write_options;
  staging_write_.reset(
      txn_store_->GetStagingWriteFactory()->CreateStagingWrite());
  txn_lock_tracker_.reset(
      txn_store_->GetTxnLockTrackerFactory()->CreateTxnLockTracker());

  txn_id_ = txn_store_->NextTxnId();

  lock_timeout_ms_ = txn_options.lock_timeout_ms;
  txn_started_ts_us_ = txn_store_->GetSystemClock()->NowMicros();
  if (txn_options.txn_duration_ms > 0) {
    txn_expired_ts_us_ =
        txn_started_ts_us_ + txn_options.txn_duration_ms * 1000;
  } else {
    txn_expired_ts_us_ = 0;
  }
}

void MVCCTransaction::Reinitialize(TransactionStore* txn_store,
                                   const WriteOptions& write_options,
                                   const TransactionOptions& txn_options) {
  txn_store_ = static_cast_with_check<MVCCTxnStore>(txn_store);
  txn_state_ = STAGE_WRITING;

  write_options_ = write_options;
  assert(staging_write_);
  staging_write_->Clear();
  assert(txn_lock_tracker_);
  txn_lock_tracker_->Clear();

  // reuse txn id
  assert(txn_id_ != 0);

  lock_timeout_ms_ = txn_options.lock_timeout_ms;
  txn_started_ts_us_ = txn_store_->GetSystemClock()->NowMicros();
  if (txn_options.txn_duration_ms > 0) {
    txn_expired_ts_us_ =
        txn_started_ts_us_ + txn_options.txn_duration_ms * 1000;
  } else {
    txn_expired_ts_us_ = 0;
  }
}

bool MVCCTransaction::IsExpired() const {
  if (txn_expired_ts_us_ > 0) {
    return txn_store_->GetSystemClock()->NowMicros() >= txn_expired_ts_us_;
  }
  return false;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
