#include "MVCC_based_txn.h"

#include "multi_version/sequence_based/seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCBasedTxn::MVCCBasedTxn(TransactionStore* txn_store,
                           const WriteOptions& write_options,
                           const TransactionOptions& txn_options)
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

Status MVCCBasedTxn::Rollback() {
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

void MVCCBasedTxn::SetSnapshot() {

}

void MVCCBasedTxn::Reinitialize(TransactionStore* txn_store,
                                const WriteOptions& write_options,
                                const TransactionOptions& txn_options) {
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

namespace {
class WCTxnAfterInsertWBCB :
		public MaintainVersionsCallbacks::AfterInsertWriteBufferCallback {
 public:
  WCTxnAfterInsertWBCB(TransactionStore* store) {
    store_impl_ = reinterpret_cast<WriteCommittedTxnStore*>(store);
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WCTxnAfterInsertWBCB() {}

  virtual Status DoCallback(const Version* version) override {
    const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    multi_versions_manager_->EndCommitVersions(dummy_version, *version);
    return Status::OK();
  }

 private:
  WriteCommittedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status WriteCommittedTxn::CommitWithPrepareImpl() {
  WCTxnAfterInsertWBCB after_insert_wb_cb(txn_store_);
  MaintainVersionsCallbacks commit_callbacks;
  commit_callbacks.after_insert_write_buffer_ = &after_insert_wb_cb;
  return txn_store_->WriteInternal(write_options_, &write_batch_,
                                   commit_callbacks,
                                   txn_store_->GetCommitQueue());
}

Status WriteCommittedTxn::CommitWithoutPrepareImpl() {
  WCTxnAfterInsertWBCB after_insert_wb_cb(txn_store_);
  MaintainVersionsCallbacks commit_callbacks;
  commit_callbacks.after_insert_write_buffer_ = &after_insert_wb_cb;
  return txn_store_->WriteInternal(write_options_, &write_batch_,
                                   commit_callbacks,
                                   txn_store_->GetCommitQueue());
}

Status WriteCommittedTxn::RollbackImpl() {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
  return Status::OK();
}

namespace {
class WPTxnPrepareBeforeInsertWBCB :
    public MaintainVersionsCallbacks::BeforeInsertWriteBufferCallback {
 public:
  WPTxnPrepareBeforeInsertWBCB(WritePreparedTxn* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnPrepareBeforeInsertWBCB() {}

  virtual Status DoCallback(const Version* version, uint32_t count) override {
    const SeqBasedVersion* version_impl =
        reinterpret_cast<const SeqBasedVersion*>(version);
    // record prepared versions info
    txn_->SetUnCommittedSeqs(version_impl->Seq(), count);
    multi_versions_manager_->BeginPrepareVersions(*version, count);
    return Status::OK();
  }

 private:
  WritePreparedTxn* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnPrepareAfterInsertWBCB :
    public MaintainVersionsCallbacks::AfterInsertWriteBufferCallback {
 public:
  WPTxnPrepareAfterInsertWBCB(WritePreparedTxn* txn)
      :txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnPrepareAfterInsertWBCB() {}

  virtual Status DoCallback(const Version* version) override {
    // advance max_readable_seq_ after insert to write buffer
    multi_versions_manager_->EndPrepareVersions(*version);
    return Status::OK();
  }

 private:
  WritePreparedTxn* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status WritePreparedTxn::PrepareImpl() {
  WPTxnPrepareBeforeInsertWBCB before_insert_wb_cb(this);
  WPTxnPrepareAfterInsertWBCB after_insert_wb_cb(this);
  MaintainVersionsCallbacks prepare_callbacks;
  prepare_callbacks.before_insert_write_buffer_ = &before_insert_wb_cb;
  prepare_callbacks.after_insert_write_buffer_ = &after_insert_wb_cb;
  return txn_store_->WriteInternal(write_options_, &write_batch_,
                                   prepare_callbacks,
                                   txn_store_->GetPrepareQueue());    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

namespace {
class WPTxnCommitWithPrepareBeforeInsertWBCB :
		public MaintainVersionsCallbacks::BeforeInsertWriteBufferCallback {
 public:
  WPTxnCommitWithPrepareBeforeInsertWBCB(WritePreparedTxn* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnCommitWithPrepareBeforeInsertWBCB() {}

  virtual Status DoCallback(const Version* version,
                            uint32_t /*count*/) override {
    uint64_t started_uncommitted_seq;
    uint32_t num_uncommitted_seq;
    txn_->GetUnCommittedSeqs(&started_uncommitted_seq, &num_uncommitted_seq);
    assert(started_uncommitted_seq > 0 && num_uncommitted_seq > 0);
    SeqBasedVersion started_uncommitted(started_uncommitted_seq);
    const Version& committed = *version;
    multi_versions_manager_->BeginCommitVersions(started_uncommitted,
                                                 committed,
                                                 num_uncommitted_seq);
    return Status::OK();
  }

 private:
  WritePreparedTxn* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnCommitWithPrepareAfterInsertWBCB :
    public MaintainVersionsCallbacks::AfterInsertWriteBufferCallback {
 public:
  WPTxnCommitWithPrepareAfterInsertWBCB(WritePreparedTxn* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnCommitWithPrepareAfterInsertWBCB() {}

  virtual Status DoCallback(const Version* version) override {
    uint64_t started_uncommitted_seq;
    uint32_t num_uncommitted_seq;
    txn_->GetUnCommittedSeqs(&started_uncommitted_seq, &num_uncommitted_seq);
    assert(started_uncommitted_seq > 0 && num_uncommitted_seq > 0);
    SeqBasedVersion started_uncommitted(started_uncommitted_seq);
    const Version& committed = *version;
    multi_versions_manager_->EndCommitVersions(started_uncommitted,
                                               committed,
                                               num_uncommitted_seq);
    return Status::OK();
  }

 private:
  WritePreparedTxn* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnCommitWithoutPrepareBeforeInsertWBCB :
		public MaintainVersionsCallbacks::BeforeInsertWriteBufferCallback {
 public:
  WPTxnCommitWithoutPrepareBeforeInsertWBCB() {}
  ~WPTxnCommitWithoutPrepareBeforeInsertWBCB() {}

  virtual Status DoCallback(const Version* version,
                            uint32_t /*count*/) override {
    // when commit without prepare takes effect, WritePrepared txn behaves like
    // WriteCommitted txn, so there is no need to interact with commit_table_,
    // all we need to do is to AdvanceMaxVisibleVersion() after insert write
    // buffer 
    return Status::OK();
  }
};

class WPTxnCommitWithoutPrepareAfterInsertWBCB :
    public MaintainVersionsCallbacks::AfterInsertWriteBufferCallback {
 public:
  WPTxnCommitWithoutPrepareAfterInsertWBCB(WritePreparedTxn* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnCommitWithoutPrepareAfterInsertWBCB() {}

  virtual Status DoCallback(const Version* version) override {
    const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    uint32_t num_uncommitted_seq = 0;
    const Version& committed = *version;
    // here EndCommitVersions() will only AdvanceMaxVisibleVersion()
    multi_versions_manager_->EndCommitVersions(dummy_version,
                                               committed,
                                               num_uncommitted_seq);
    return Status::OK();
  }

 private:
  WritePreparedTxn* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status WritePreparedTxn::CommitWithPrepareImpl() {
  WPTxnCommitWithPrepareBeforeInsertWBCB before_insert_wb_cb(this);
  WPTxnCommitWithPrepareAfterInsertWBCB after_insert_wb_cb(this);
  MaintainVersionsCallbacks commit_callbacks;
  commit_callbacks.before_insert_write_buffer_ = &before_insert_wb_cb;
  commit_callbacks.after_insert_write_buffer_ = &after_insert_wb_cb;
  // use an empty write batch for commit purpose, it will consume a seq
  WriteBatch empty_write_batch;
  return txn_store_->WriteInternal(write_options_, &empty_write_batch,
                                   commit_callbacks,
                                   txn_store_->GetCommitQueue());   // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

Status WritePreparedTxn::CommitWithoutPrepareImpl() {
  bool enable_two_write_queues = txn_store_->IsEnableTwoWriteQueues();
  // commit without prepare only takes effect when
  // enable_two_write_queues == false, when enable_two_write_queues == true, we
  // will switch commit without prepare to commit with prepare internally
  if (!enable_two_write_queues) {
    WPTxnCommitWithoutPrepareBeforeInsertWBCB before_insert_wb_cb;
    WPTxnCommitWithoutPrepareAfterInsertWBCB after_insert_wb_cb(this);
    MaintainVersionsCallbacks commit_callbacks;
    commit_callbacks.before_insert_write_buffer_ = &before_insert_wb_cb;
    commit_callbacks.after_insert_write_buffer_ = &after_insert_wb_cb;
    return txn_store_->WriteInternal(write_options_, &write_batch_,
                                     commit_callbacks,
                                     txn_store_->GetCommitQueue());
  }

  // enable_two_write_queues == true
  // first prepare: insert write batch into write buffer
  WPTxnPrepareBeforeInsertWBCB prepare_before_insert_wb_cb(this);
  WPTxnPrepareAfterInsertWBCB prepare_after_insert_wb_cb(this);
  MaintainVersionsCallbacks prepare_callbacks;
  prepare_callbacks.before_insert_write_buffer_ = &prepare_before_insert_wb_cb;
  prepare_callbacks.after_insert_write_buffer_ = &prepare_after_insert_wb_cb;
  Status s = txn_store_->WriteInternal(write_options_, &write_batch_,
                                       prepare_callbacks,
                                       txn_store_->GetPrepareQueue());
  if (!s.IsOK()) {    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
    return s;
  }
  // then commit: write an empty write batch
  WPTxnCommitWithPrepareBeforeInsertWBCB commit_before_insert_wb_cb(this);
  WPTxnCommitWithPrepareAfterInsertWBCB commit_after_insert_wb_cb(this);
  MaintainVersionsCallbacks commit_callbacks;
  commit_callbacks.before_insert_write_buffer_ = &commit_before_insert_wb_cb;
  commit_callbacks.after_insert_write_buffer_ = &commit_after_insert_wb_cb;
  // use an empty write batch for commit purpose, it will consume a seq
  WriteBatch empty_write_batch;
  return txn_store_->WriteInternal(write_options_, &empty_write_batch,
                                   commit_callbacks,
                                   txn_store_->GetCommitQueue()); // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

class WritePreparedTxn::RollbackWriteBatchBuilder : public WriteBatch::Handler {
 public:
  RollbackWriteBatchBuilder(WritePreparedTxnStore* txn_store,
                            WriteBatch* rollback_write_batch)
                            : snapshot_limit_max_(kSeqNumberLimitsMax),
                              txn_store_(txn_store),
                              rollback_write_batch_(rollback_write_batch) {
    read_options_.snapshot = &snapshot_limit_max_;
  }
  ~RollbackWriteBatchBuilder() {}

  Status Put(const std::string& key, const std::string& /*value*/) override {
    return GetLatestCommittedOfKey(key);
  }

  Status Delete(const std::string& key) override {
    return GetLatestCommittedOfKey(key);
  }

  Status GetLatestCommittedOfKey(const std::string& key) {
    std::string value;
    Status s = txn_store_->GetInternal(read_options_, key, &value);
    if (s.IsOK()) {
      rollback_write_batch_->Put(key, value);
    } else if (s.IsNotFound()) {
      rollback_write_batch_->Delete(key);
      s = Status::OK();
    } else {
      assert(!s.IsTryAgain());
    }
    return s;
  }

 private:
  ReadOptions read_options_;
  // can see all version during GetInternal(), but only care about latest
  // committed of target key 
  WPSeqBasedSnapshot snapshot_limit_max_;
  WritePreparedTxnStore* txn_store_;
  WriteBatch* rollback_write_batch_;
};

namespace {

}   // anonymous namespace

Status WritePreparedTxn::RollbackImpl() {
  WriteBatch rollback_write_batch;
  RollbackWriteBatchBuilder rollback_builder(
      reinterpret_cast<WritePreparedTxnStore*>(txn_store_),
      &rollback_write_batch);
  Status s = write_batch_.Iterate(&rollback_builder);
  if (!s.IsOK()) {
    return s;
  }
  // insert the rollback_write_batch into write buffer to eliminate this txn's
  // footprint in the write buffer
  bool enable_two_write_queues = txn_store_->IsEnableTwoWriteQueues();
  // commit without prepare only takes effect when
  // enable_two_write_queues == false, when enable_two_write_queues == true, we
  // will switch commit without prepare to commit with prepare internally
  if (!enable_two_write_queues) {
    WPTxnCommitWithoutPrepareBeforeInsertWBCB before_insert_wb_cb;              // 将每种场景的WriteInte燃料相关参数固化为类成员变量
    WPTxnCommitWithoutPrepareAfterInsertWBCB after_insert_wb_cb(this);
    MaintainVersionsCallbacks commit_callbacks;
    commit_callbacks.before_insert_write_buffer_ = &before_insert_wb_cb;
    commit_callbacks.after_insert_write_buffer_ = &after_insert_wb_cb;
    return txn_store_->WriteInternal(write_options_, &rollback_write_batch,
                                     commit_callbacks,
                                     txn_store_->GetCommitQueue());
  }

  // enable_two_write_queues == true
  // first prepare: insert rollback write batch into write buffer
  WPTxnPrepareBeforeInsertWBCB prepare_before_insert_wb_cb(this);
  WPTxnPrepareAfterInsertWBCB prepare_after_insert_wb_cb(this);    // Todo: 将prepare_callbacks和commit_callbacks实例化到类中
  MaintainVersionsCallbacks prepare_callbacks;
  prepare_callbacks.before_insert_write_buffer_ = &prepare_before_insert_wb_cb;
  prepare_callbacks.after_insert_write_buffer_ = &prepare_after_insert_wb_cb;
  s = txn_store_->WriteInternal(write_options_, &rollback_write_batch,
                                prepare_callbacks,
                                txn_store_->GetPrepareQueue());
  if (!s.IsOK()) {    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
    return s;
  }
  // then commit: write an empty write batch
  WPTxnCommitWithPrepareBeforeInsertWBCB commit_before_insert_wb_cb(this);
  WPTxnCommitWithPrepareAfterInsertWBCB commit_after_insert_wb_cb(this);
  MaintainVersionsCallbacks commit_callbacks;
  commit_callbacks.before_insert_write_buffer_ = &commit_before_insert_wb_cb;
  commit_callbacks.after_insert_write_buffer_ = &commit_after_insert_wb_cb;
  // use an empty write batch for commit purpose, it will consume a seq
  WriteBatch empty_write_batch;
  return txn_store_->WriteInternal(write_options_, &empty_write_batch,
                                   commit_callbacks,
                                   txn_store_->GetCommitQueue()); // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

}   // namespace MULTI_VERSIONS_NAMESPACE
