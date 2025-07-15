#pragma once

#include "pessimistic_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WritePreparedTransaction : public PessimisticTransaction {
 public:
  // No copying allowed
  WritePreparedTransaction(const WritePreparedTransaction&) = delete;
  WritePreparedTransaction& operator=(const WritePreparedTransaction&) = delete;

  WritePreparedTransaction(TransactionStore* txn_store,
                           const WriteOptions& write_options,
                           const TransactionOptions& txn_options)
      : PessimisticTransaction(txn_store, write_options, txn_options) {}
  virtual ~WritePreparedTransaction() {}

 void RecordPreparedUnCommittedSeqs(uint64_t started, uint32_t count) {
    assert(started > 0 && count > 0);
    prepared_uncommitted_started_seq_ = started;
    num_prepared_uncommitted_seq_ = count;
  }

  void GetPreparedUnCommittedSeqs(uint64_t* started, uint32_t* count) const {
    *started = prepared_uncommitted_started_seq_;
    *count = num_prepared_uncommitted_seq_;
  }

  void RecordRollbackedUnCommittedSeqs(uint64_t started, uint32_t count) {
    assert(started > 0 && count > 0);
    rollbacked_uncommitted_started_seq_ = started;
    num_rollbacked_uncommitted_seq_ = count;
  }

  void GetRollackedUnCommittedSeqs(uint64_t* started, uint32_t* count) const {
    *started = rollbacked_uncommitted_started_seq_;
    *count = num_rollbacked_uncommitted_seq_;
  }

  class RollbackStagingWriteBuilder;
 private:
  virtual Status PrepareImpl() override;
  virtual Status CommitWithPrepareImpl() override;
  virtual Status CommitWithoutPrepareImpl() override;
  virtual Status RollbackImpl() override;

  StagingWrite* GetEmptyStagingWrite() {
    if (empty_staging_write_.get() == nullptr) {
      StagingWriteFactory* factory = GetTxnStore()->GetStagingWriteFactory();
      empty_staging_write_.reset(factory->CreateStagingWrite());
    }
    assert(empty_staging_write_.get() != nullptr &&
           empty_staging_write_->IsEmpty());
    return empty_staging_write_.get();
  }

  void ResetUnCommittedSeqs() {
    prepared_uncommitted_started_seq_ = 0;
    num_prepared_uncommitted_seq_ = 0;
    rollbacked_uncommitted_started_seq_ = 0;
    num_rollbacked_uncommitted_seq_ = 0;
  }

  virtual void Clear() override {
    // clear txn locks before clearing staging_write_, because clearing txn locks
    // depends on the staging_write_
    ClearTxnLocks();
    GetStagingWrite()->Clear();
    ResetUnCommittedSeqs();
  }

  // for Commit purpose and lazy initialized
  std::unique_ptr<StagingWrite> empty_staging_write_ = nullptr;

  uint64_t prepared_uncommitted_started_seq_ = 0;
  uint64_t num_prepared_uncommitted_seq_ = 0;
  uint64_t rollbacked_uncommitted_started_seq_ = 0;
  uint64_t num_rollbacked_uncommitted_seq_ = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
