#include "pessimistic_transaction.h"

namespace COMPOSITE_STORE_NAMESPACE {

// only control the transaction excution behavior, let derived class
// implements the details
Status PessimisticTransaction::Prepare() {
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

}   // namespace COMPOSITE_STORE_NAMESPACE
