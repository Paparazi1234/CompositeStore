#include "write_prepared_transaction.h"

#include "transaction_store/mvcc_txn_store/pessimistic_txn_store/write_prepared_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

namespace{
// callbacks definition
class WPTxnMaintainVersionsCBForPrepare : public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForPrepare(WritePreparedTransaction* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnMaintainVersionsCBForPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		const SeqBasedVersion* version_impl =
        reinterpret_cast<const SeqBasedVersion*>(version);
    // record the uncommitted versions info of the Prepare stage
    txn_->RecordPreparedUnCommittedSeqs(version_impl->Seq(), count);
    const Version& prepared_uncommitted_started = *version;
    uint32_t num_prepared_uncommitteds = count;
    multi_versions_manager_->BeginPrepareVersions(prepared_uncommitted_started,
                                                  num_prepared_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		// advance max readable version after insert to write buffer
    const Version& end_uncommitted = *version;
    multi_versions_manager_->EndPrepareVersions(end_uncommitted);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForCommitWithPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForCommitWithPrepare(WritePreparedTransaction* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnMaintainVersionsCBForCommitWithPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		assert(count == 1);
    // calculate the final committed version
    SeqBasedVersion committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must have be something inserted during Prepare() even though we
    // Prepare() an empty write batch(empty write batch also consume a version)
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    SeqBasedVersion
        prepared_uncommitted_started(prepared_uncommitted_started_seq);
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    multi_versions_manager_->BeginCommitVersions(prepared_uncommitted_started,
                                                 committed,
                                                 num_prepared_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must have be something inserted during Prepare() even though we
    // Prepare() an empty write batch(empty write batch also consume a version)
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    SeqBasedVersion
        prepared_uncommitted_started(prepared_uncommitted_started_seq);
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    multi_versions_manager_->EndCommitVersions(prepared_uncommitted_started,
                                               committed,
                                               num_prepared_uncommitteds);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForCommitWithoutPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForCommitWithoutPrepare(WritePreparedTransaction* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnMaintainVersionsCBForCommitWithoutPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		assert(count == 1);
    // calculate the final committed version
    SeqBasedVersion committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    // when commit without prepare takes effect: we just insert the committed
    // versions of the txn's write batch to commit_table_
    // note that the prepared_uncommitted_started and committed are the same,
    // since the count is 1 currently
    const Version& prepared_uncommitted_started = *version;
    uint32_t num_prepared_uncommitteds = count;
    multi_versions_manager_->BeginCommitVersions(prepared_uncommitted_started,
                                                 committed,
                                                 num_prepared_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
    // when commit without prepare takes effect, there is no uncommitted version
    // that the txn created before commit, so num_prepared_uncommitteds is 0
    const Version& prepared_uncommitted_started = dummy_version;
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = 0;
    // here EndCommitVersions() will only advance max visible version
    multi_versions_manager_->EndCommitVersions(prepared_uncommitted_started,
                                               committed,
                                               num_prepared_uncommitteds);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForPrepareForRollback :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForPrepareForRollback(WritePreparedTransaction* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnMaintainVersionsCBForPrepareForRollback() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		// currently the rollback write batch consumes one version
    assert(count == 1);
    const SeqBasedVersion* version_impl =
        reinterpret_cast<const SeqBasedVersion*>(version);
    // the rollback write batch goes through an internal prepare stage, so
    // record the uncommitted versions info of the rollback write batch
    txn_->RecordRollbackedUnCommittedSeqs(version_impl->Seq(), count);
    const Version& rollbacked_uncommitted_started = *version;
    uint32_t num_rollbacked_uncommitteds = count;
    multi_versions_manager_->BeginPrepareVersions(
        rollbacked_uncommitted_started,
        num_rollbacked_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		// advance max readable version after insert the rollback write batch to
    // write buffer
    const Version& end_uncommitted = *version;
    multi_versions_manager_->EndPrepareVersions(end_uncommitted);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForRollbackWithPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForRollbackWithPrepare(WritePreparedTransaction* txn)
      : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnMaintainVersionsCBForRollbackWithPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		// the internal empty write batch consumes one version
    assert(count == 1);
    // calculate the final committed version
    SeqBasedVersion committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must be some uncommitted version of Prepare() stage to rollback,
    // even though we Prepare() an empty write batch during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    uint64_t rollbacked_uncommitted_started_seq;
    uint32_t num_rollbacked_uncommitted_seq;
    txn_->GetRollackedUnCommittedSeqs(&rollbacked_uncommitted_started_seq,
                                      &num_rollbacked_uncommitted_seq);
    // the rollback write batch went through prepare stage, so it's
    // rollbacked_uncommitted_started_seq and num_rollbacked_uncommitted_seq are
    // both not 0
    assert(rollbacked_uncommitted_started_seq > 0 &&
           num_rollbacked_uncommitted_seq > 0);
    SeqBasedVersion
        prepared_uncommitted_started(prepared_uncommitted_started_seq);
    SeqBasedVersion
        rollbacked_uncommitted_started(rollbacked_uncommitted_started_seq);
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    uint32_t num_rollbacked_uncommitteds = num_rollbacked_uncommitted_seq;
    multi_versions_manager_->BeginRollbackVersions(
        prepared_uncommitted_started,
        rollbacked_uncommitted_started,
        committed,
        num_prepared_uncommitteds,
        num_rollbacked_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must be some uncommitted version of Prepare() stage to rollback,
    // even though we Prepare() an empty write batch during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    uint64_t rollbacked_uncommitted_started_seq;
    uint32_t num_rollbacked_uncommitted_seq;
    txn_->GetRollackedUnCommittedSeqs(&rollbacked_uncommitted_started_seq,
                                      &num_rollbacked_uncommitted_seq);
    // the rollback write batch went through prepare stage, so it's
    // rollbacked_uncommitted_started_seq and num_rollbacked_uncommitted_seq are
    // both not 0
    assert(rollbacked_uncommitted_started_seq > 0 &&
           num_rollbacked_uncommitted_seq > 0);
    SeqBasedVersion
        prepared_uncommitted_started(prepared_uncommitted_started_seq);
    SeqBasedVersion
        rollbacked_uncommitted_started(rollbacked_uncommitted_started_seq);
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    uint32_t num_rollbacked_uncommitteds = num_rollbacked_uncommitted_seq;
    multi_versions_manager_->EndRollbackVersions(
        prepared_uncommitted_started,
        rollbacked_uncommitted_started,
        committed,
        num_prepared_uncommitteds,
        num_rollbacked_uncommitteds);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForRollbackWithoutPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForRollbackWithoutPrepare(
      WritePreparedTransaction* txn)
        : txn_(txn) {
    store_impl_ = reinterpret_cast<WritePreparedTxnStore*>(txn_->GetTxnStore());
    multi_versions_manager_ = store_impl_->GetMultiVersionsManager();
  }
  ~WPTxnMaintainVersionsCBForRollbackWithoutPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		assert(count == 1);
    // calculate the final committed version
    SeqBasedVersion committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must be some uncommitted version of Prepare() stage to rollback,
    // even though we Prepare() an empty write batch during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    SeqBasedVersion
        prepared_uncommitted_started(prepared_uncommitted_started_seq);
    const Version& rollbacked_uncommitted_started = *version;
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    uint32_t num_rollbacked_uncommitteds = count;
    multi_versions_manager_->BeginRollbackVersions(
        prepared_uncommitted_started,
        rollbacked_uncommitted_started,
        committed,
        num_prepared_uncommitteds,
        num_rollbacked_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();                         
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must be some uncommitted version of Prepare() stage to rollback,
    // even though we Prepare() an empty write batch during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    uint64_t rollbacked_uncommitted_started_seq;
    uint32_t num_rollbacked_uncommitted_seq;
    txn_->GetRollackedUnCommittedSeqs(&rollbacked_uncommitted_started_seq,
                                      &num_rollbacked_uncommitted_seq);
    // the rollback write batch didn't go through prepare stage, so it's
    // rollbacked_uncommitted_started_seq and num_rollbacked_uncommitted_seq are
    // both 0
    assert(rollbacked_uncommitted_started_seq == 0 &&
           num_rollbacked_uncommitted_seq == 0);
    SeqBasedVersion
        prepared_uncommitted_started(prepared_uncommitted_started_seq);
    const Version& rollbacked_uncommitted_started = dummy_version;
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    uint32_t num_rollbacked_uncommitteds = num_rollbacked_uncommitted_seq;
    multi_versions_manager_->EndRollbackVersions(
        prepared_uncommitted_started,
        rollbacked_uncommitted_started,
        committed,
        num_prepared_uncommitteds,
        num_rollbacked_uncommitteds);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  WritePreparedTxnStore* store_impl_;
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status WritePreparedTransaction::PrepareImpl() {
  WritePreparedTxnStore* store_impl =
      reinterpret_cast<WritePreparedTxnStore*>(txn_store_);
  WPTxnMaintainVersionsCBForPrepare wp_maintain_versions_cb_for_prepare(this);
  return txn_store_->WriteInternal(write_options_,
                                   &write_batch_,
                                   wp_maintain_versions_cb_for_prepare,
                                   store_impl->GetPrepareQueue());    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

Status WritePreparedTransaction::CommitWithPrepareImpl() {
  // use an empty write batch for commit purpose, it will consume a version;
  // when we write an empty write batch in this way, it's will consume a 
  // version, but we won't add a committed version to commit_table_ for it,
  // while we commit an empty write batch through txn, it not only consumes a
  // version but we add a committed version to commit_table_ for it;
  // that we don't add a committed version to commit_table_ for the
  // empty_write_batch is fine, since we have advance max visible version in
  // EndCommitVersions()
  WriteBatch empty_write_batch;
  WritePreparedTxnStore* store_impl =
      reinterpret_cast<WritePreparedTxnStore*>(txn_store_);
  WPTxnMaintainVersionsCBForCommitWithPrepare
      wp_maintain_versions_cb_for_commit_with_prepare(this);
  return txn_store_->WriteInternal(
      write_options_,
      &empty_write_batch,
      wp_maintain_versions_cb_for_commit_with_prepare,
      store_impl->GetCommitQueue());   // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

Status WritePreparedTransaction::CommitWithoutPrepareImpl() {
  WritePreparedTxnStore* store_impl =
      reinterpret_cast<WritePreparedTxnStore*>(txn_store_);
  bool enable_two_write_queues = store_impl->IsEnableTwoWriteQueues();
  // commit without prepare only takes effect when
  // enable_two_write_queues == false, when enable_two_write_queues == true, we
  // will switch commit without prepare to commit with prepare internally
  if (!enable_two_write_queues) {
    WPTxnMaintainVersionsCBForCommitWithoutPrepare
        wp_maintain_versions_cb_for_commit_without_prepare(this);
    return txn_store_->WriteInternal(
        write_options_,
        &write_batch_,
        wp_maintain_versions_cb_for_commit_without_prepare,
        store_impl->GetCommitQueue());
  }

  // enable_two_write_queues == true
  // first prepare: insert write batch into write buffer through an internal
  // Prepare() stage
  WPTxnMaintainVersionsCBForPrepare wp_maintain_versions_cb_for_prepare(this);
  Status s = txn_store_->WriteInternal(write_options_,
                                       &write_batch_,
                                       wp_maintain_versions_cb_for_prepare,
                                       store_impl->GetPrepareQueue());
  if (!s.IsOK()) {    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
    return s;
  }
  // then commit
  // use an empty write batch for commit purpose, it will consume a version
  // when we write an empty write batch in this way, it's will consume a 
  // version, but we won't add a committed version to commit_table_ for it,
  // while we commit an empty write batch through txn, it not only consumes a
  // version but we add a committed version to commit_table_ for it;
  // that we don't add a committed version to commit_table_ for the
  // empty_write_batch is fine, since we have advance max visible version in
  // EndCommitVersions()
  WriteBatch empty_write_batch;
  WPTxnMaintainVersionsCBForCommitWithPrepare
      wp_maintain_versions_cb_for_commit_with_prepare(this);
  return txn_store_->WriteInternal(
      write_options_,
      &empty_write_batch,
      wp_maintain_versions_cb_for_commit_with_prepare,
      store_impl->GetCommitQueue()); // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

class WritePreparedTransaction::RollbackWriteBatchBuilder :
    public WriteBatch::Handler {
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

Status WritePreparedTransaction::RollbackImpl() {
  // build the rollback write batch
  WriteBatch rollback_write_batch;
  WritePreparedTxnStore* store_impl =
      reinterpret_cast<WritePreparedTxnStore*>(txn_store_);
  RollbackWriteBatchBuilder rollback_builder(store_impl, &rollback_write_batch);
  Status s = write_batch_.Iterate(&rollback_builder);
  if (!s.IsOK()) {
    return s;
  }
  // insert the rollback_write_batch into write buffer to eliminate this txn's
  // footprint in the write buffer

  bool enable_two_write_queues = store_impl->IsEnableTwoWriteQueues();
  // rollback without prepare only takes effect when
  // enable_two_write_queues == false, when enable_two_write_queues == true, we
  // will switch rollback without prepare to rollback with prepare internally
  if (!enable_two_write_queues) {
    WPTxnMaintainVersionsCBForRollbackWithoutPrepare
        wp_maintain_versions_cb_for_rollback_without_prepare(this);
    return txn_store_->WriteInternal(
        write_options_,
        &rollback_write_batch,
        wp_maintain_versions_cb_for_rollback_without_prepare,
        store_impl->GetCommitQueue());
  }

  // enable_two_write_queues == true
  // first prepare: insert rollback write batch into write buffer
  WPTxnMaintainVersionsCBForPrepareForRollback
      wp_maintain_versions_cb_for_prepare_for_rollback(this);
  s = txn_store_->WriteInternal(
      write_options_,
      &rollback_write_batch,
      wp_maintain_versions_cb_for_prepare_for_rollback,
      store_impl->GetPrepareQueue());
  if (!s.IsOK()) {    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
    return s;
  }
  // then commit
  // use an empty write batch for commit purpose, it will consume a version
  // when we write an empty write batch in this way, it's will consume a 
  // version, but we won't add a committed version to commit_table_ for it,
  // while we commit an empty write batch through txn, it not only consumes a
  // version but we add a committed version to commit_table_ for it;
  // that we don't add a committed version to commit_table_ for the
  // empty_write_batch is fine, since we have advance max visible version in
  // EndCommitVersions()
  WriteBatch empty_write_batch;
  WPTxnMaintainVersionsCBForRollbackWithPrepare
      wp_maintain_versions_cb_for_rollback_with_prepare(this);
  return txn_store_->WriteInternal(
      write_options_,
      &empty_write_batch,
      wp_maintain_versions_cb_for_rollback_with_prepare,
      store_impl->GetCommitQueue()); // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

}   // namespace MULTI_VERSIONS_NAMESPACE
