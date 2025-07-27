#include "pessimistic_transaction.h"

namespace COMPOSITE_STORE_NAMESPACE {

// only control the transaction excution behavior, let derived class
// implements the details
Status PessimisticTransaction::Prepare() {
  if (IsExpired()) {
    return Status::Expired();
  }

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

Status PessimisticTransaction::Commit() {
  if (IsExpired()) {
    return Status::Expired();
  }

  Status s;
  if (txn_state_ == STAGE_WRITING) {
    txn_state_.store(STAGE_COMMITTING, std::memory_order_relaxed);
    s = CommitWithoutPrepareImpl();
    // when commit without prepare, Clear() not mater commit successfully or not
    Clear();
    if (s.IsOK()) {
      txn_state_.store(STAGE_COMMITTED, std::memory_order_relaxed);
    }
  } else if (txn_state_ == STAGE_PREPARED) {
    txn_state_.store(STAGE_COMMITTING, std::memory_order_relaxed);
    s = CommitWithPrepareImpl();
    if (s.IsOK()) {
      // when commit with prepare, Clear() only when commit successfully
      Clear();
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

Status PessimisticTransaction::Rollback() {
  Status s;
  // if rollback during write stage, then txn remains in STAGE_WRITING state and
  // can be used to do future writes(equivalent to rollback to savepoint)
  if (txn_state_ == STAGE_WRITING) {
    // when rollback during write stage, just Clear()
    Clear();
  } else if (txn_state_ == STAGE_PREPARED) {
    txn_state_.store(STAGE_ROLLBACKING, std::memory_order_relaxed);
    s = RollbackImpl();
    if (s.IsOK()) {
      // when rollback after prepare, Clear() only when rollback successfully
      Clear();
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

Status PessimisticTransaction::TryLock(const std::string& key, bool exclusive,
                                       int64_t timeout_time_ms) {
  Status s;
  PessimisticTxnStore* txn_store_impl =
      static_cast_with_check<PessimisticTxnStore>(GetTxnStore());
  TxnLockManager* lock_manager = txn_store_impl->GetTxnLockManager();
  TxnLockTracker* lock_tracker = GetTxnLockTracker();
  bool tracked_exclusive;
  bool previously_locked = lock_tracker->IsKeyAlreadyTracked(
      key, &tracked_exclusive);
  bool lock_upgrade = previously_locked && exclusive && !tracked_exclusive;
  // not locked yet or already locked previously but need to upgrade lock
  if (!previously_locked || lock_upgrade) {
    s = lock_manager->TryLock(TxnId(), key, exclusive, timeout_time_ms);
    if (s.IsOK()) {
      lock_tracker->TrackKey(key, exclusive);
    }
  } else {
    // already locked previously and no need to upgrade lock
    assert(previously_locked && !lock_upgrade);
  }
  return s;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
