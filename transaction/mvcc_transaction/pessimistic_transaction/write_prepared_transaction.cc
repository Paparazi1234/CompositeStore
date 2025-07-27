#include "write_prepared_transaction.h"

#include "multi_versions/sequence_based/seq_based_multi_versions.h"

namespace COMPOSITE_STORE_NAMESPACE {

using VersionImpl = SeqBasedVersion;

namespace{
// callbacks definition
class WPTxnMaintainVersionsCBForPrepare : public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForPrepare(WritePreparedTransaction* txn)
      : txn_(txn),
        multi_versions_manager_(
            txn_->GetTxnStore()->GetMultiVersionsManager()) {}
  ~WPTxnMaintainVersionsCBForPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		const VersionImpl* version_impl =
        static_cast_with_check<const VersionImpl>(version);
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
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForCommitWithPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForCommitWithPrepare(WritePreparedTransaction* txn)
      : txn_(txn),
        multi_versions_manager_(
            txn_->GetTxnStore()->GetMultiVersionsManager()) {}
  ~WPTxnMaintainVersionsCBForCommitWithPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		assert(count == 1);
    // calculate the final committed version
    VersionImpl committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must have be something inserted during Prepare() even though we
    // Prepare() an empty staging write(empty staging write also consume a
    // version)
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    VersionImpl prepared_uncommitted_started(prepared_uncommitted_started_seq);
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
    // Prepare() an empty staging write(empty staging write also consume a
    // version)
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    VersionImpl prepared_uncommitted_started(prepared_uncommitted_started_seq);
    const Version& committed = *version;
    uint32_t num_prepared_uncommitteds = num_prepared_uncommitted_seq;
    multi_versions_manager_->EndCommitVersions(prepared_uncommitted_started,
                                               committed,
                                               num_prepared_uncommitteds);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForCommitWithoutPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForCommitWithoutPrepare(WritePreparedTransaction* txn)
      : txn_(txn),
        multi_versions_manager_(
            txn_->GetTxnStore()->GetMultiVersionsManager()) {}
  ~WPTxnMaintainVersionsCBForCommitWithoutPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		assert(count == 1);
    // calculate the final committed version
    VersionImpl committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    // when commit without prepare takes effect: we just insert the committed
    // versions of the txn's staging write to commit_table_
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
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForPrepareForRollback :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForPrepareForRollback(WritePreparedTransaction* txn)
      : txn_(txn),
        multi_versions_manager_(
            txn_->GetTxnStore()->GetMultiVersionsManager()) {}
  ~WPTxnMaintainVersionsCBForPrepareForRollback() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		// currently the rollback staging write consumes one version
    assert(count == 1);
    const VersionImpl* version_impl =
        static_cast_with_check<const VersionImpl>(version);
    // the rollback staging write goes through an internal prepare stage, so
    // record the uncommitted versions info of the rollback staging write
    txn_->RecordRollbackedUnCommittedSeqs(version_impl->Seq(), count);
    const Version& rollbacked_uncommitted_started = *version;
    uint32_t num_rollbacked_uncommitteds = count;
    multi_versions_manager_->BeginPrepareVersions(
        rollbacked_uncommitted_started,
        num_rollbacked_uncommitteds);
    return Status::OK();
	}

	Status AfterInsertWriteBufferCallback(const Version* version) override {
		// advance max readable version after insert the rollback staging write to
    // write buffer
    const Version& end_uncommitted = *version;
    multi_versions_manager_->EndPrepareVersions(end_uncommitted);
    return Status::OK();
	}

 private:
  WritePreparedTransaction* txn_;
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForRollbackWithPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForRollbackWithPrepare(WritePreparedTransaction* txn)
      : txn_(txn),
        multi_versions_manager_(
            txn_->GetTxnStore()->GetMultiVersionsManager()) {}
  ~WPTxnMaintainVersionsCBForRollbackWithPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		// the internal empty staging write consumes one version
    assert(count == 1);
    // calculate the final committed version
    VersionImpl committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must be some uncommitted version of Prepare() stage to rollback,
    // even though we Prepare() an empty staging write during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    uint64_t rollbacked_uncommitted_started_seq;
    uint32_t num_rollbacked_uncommitted_seq;
    txn_->GetRollackedUnCommittedSeqs(&rollbacked_uncommitted_started_seq,
                                      &num_rollbacked_uncommitted_seq);
    // the rollback staging write went through prepare stage, so it's
    // rollbacked_uncommitted_started_seq and num_rollbacked_uncommitted_seq are
    // both not 0
    assert(rollbacked_uncommitted_started_seq > 0 &&
           num_rollbacked_uncommitted_seq > 0);
    VersionImpl prepared_uncommitted_started(prepared_uncommitted_started_seq);
    VersionImpl
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
    // even though we Prepare() an empty staging write during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    uint64_t rollbacked_uncommitted_started_seq;
    uint32_t num_rollbacked_uncommitted_seq;
    txn_->GetRollackedUnCommittedSeqs(&rollbacked_uncommitted_started_seq,
                                      &num_rollbacked_uncommitted_seq);
    // the rollback staging write went through prepare stage, so it's
    // rollbacked_uncommitted_started_seq and num_rollbacked_uncommitted_seq are
    // both not 0
    assert(rollbacked_uncommitted_started_seq > 0 &&
           num_rollbacked_uncommitted_seq > 0);
    VersionImpl prepared_uncommitted_started(prepared_uncommitted_started_seq);
    VersionImpl
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
  MultiVersionsManager* multi_versions_manager_;
};

class WPTxnMaintainVersionsCBForRollbackWithoutPrepare :
    public MaintainVersionsCallbacks {
 public:
  WPTxnMaintainVersionsCBForRollbackWithoutPrepare(
      WritePreparedTransaction* txn)
        : txn_(txn),
        multi_versions_manager_(
            txn_->GetTxnStore()->GetMultiVersionsManager()) {}
  ~WPTxnMaintainVersionsCBForRollbackWithoutPrepare() {}

  bool NeedMaintainBeforePersistWAL() const override { return false; }
	bool NeedMaintainBeforeInsertWriteBuffer() const override { return true; }
	bool NeedMaintainAfterInsertWriteBuffer() const override { return true; }

  Status BeforeInsertWriteBufferCallback(const Version* version,
																				 uint32_t count) override {
		assert(count == 1);
    // calculate the final committed version
    VersionImpl committed;
    committed.DuplicateFrom(*version);
    committed.IncreaseBy(count - 1);
    // currently committed == *version, since count == 1
    assert(committed.CompareWith(*version) == 0);
    uint64_t prepared_uncommitted_started_seq;
    uint32_t num_prepared_uncommitted_seq;
    txn_->GetPreparedUnCommittedSeqs(&prepared_uncommitted_started_seq,
                                     &num_prepared_uncommitted_seq);
    // there must be some uncommitted version of Prepare() stage to rollback,
    // even though we Prepare() an empty staging write during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    VersionImpl prepared_uncommitted_started(prepared_uncommitted_started_seq);
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
    // even though we Prepare() an empty staging write during prepare stage
    assert(prepared_uncommitted_started_seq > 0 &&
           num_prepared_uncommitted_seq > 0);
    uint64_t rollbacked_uncommitted_started_seq;
    uint32_t num_rollbacked_uncommitted_seq;
    txn_->GetRollackedUnCommittedSeqs(&rollbacked_uncommitted_started_seq,
                                      &num_rollbacked_uncommitted_seq);
    // the rollback staging write didn't go through prepare stage, so it's
    // rollbacked_uncommitted_started_seq and num_rollbacked_uncommitted_seq are
    // both 0
    assert(rollbacked_uncommitted_started_seq == 0 &&
           num_rollbacked_uncommitted_seq == 0);
    VersionImpl prepared_uncommitted_started(prepared_uncommitted_started_seq);
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
  MultiVersionsManager* multi_versions_manager_;
};
}   // anonymous namespace

Status WritePreparedTransaction::PrepareImpl() {
  MVCCTxnStore* txn_store = GetTxnStore();
  WPTxnMaintainVersionsCBForPrepare wp_maintain_versions_cb_for_prepare(this);
  return txn_store->CommitStagingWrite(write_options_,
                                       GetStagingWrite(),
                                       wp_maintain_versions_cb_for_prepare,
                                       txn_store->GetPrepareQueue());    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

Status WritePreparedTransaction::CommitWithPrepareImpl() {
  // use an empty staging write for commit purpose, it will consume a version;
  // when we write an empty staging write in this way, it's will consume a 
  // version, but we won't add a committed version to commit_table_ for it,
  // while we commit an empty staging write through txn, it not only consumes a
  // version but we add a committed version to commit_table_ for it;
  // that we don't add a committed version to commit_table_ for the
  // empty_staging_write is fine, since we have advance max visible version in
  // EndCommitVersions()
  StagingWrite* empty_staging_write = GetEmptyStagingWrite();
  WPTxnMaintainVersionsCBForCommitWithPrepare
      wp_maintain_versions_cb_for_commit_with_prepare(this);
  MVCCTxnStore* txn_store = GetTxnStore();
  return txn_store->CommitStagingWrite(
      write_options_,
      empty_staging_write,
      wp_maintain_versions_cb_for_commit_with_prepare,
      txn_store->GetCommitQueue());   // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

Status WritePreparedTransaction::CommitWithoutPrepareImpl() {
  MVCCTxnStore* txn_store = GetTxnStore();
  bool enable_two_write_queues = txn_store->IsTwoWriteQueuesEnabled();
  // commit without prepare only takes effect when
  // enable_two_write_queues == false, when enable_two_write_queues == true, we
  // will switch commit without prepare to commit with prepare internally
  if (!enable_two_write_queues) {
    WPTxnMaintainVersionsCBForCommitWithoutPrepare
        wp_maintain_versions_cb_for_commit_without_prepare(this);
    return txn_store->CommitStagingWrite(
        write_options_,
        GetStagingWrite(),
        wp_maintain_versions_cb_for_commit_without_prepare,
        txn_store->GetCommitQueue());
  }

  // enable_two_write_queues == true
  // first prepare: insert staging write into write buffer through an internal
  // Prepare() stage
  WPTxnMaintainVersionsCBForPrepare wp_maintain_versions_cb_for_prepare(this);
  Status s = txn_store->CommitStagingWrite(write_options_,
                                           GetStagingWrite(),
                                           wp_maintain_versions_cb_for_prepare,
                                           txn_store->GetPrepareQueue());
  if (!s.IsOK()) {    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
    return s;
  }
  // then commit
  // use an empty staging write for commit purpose, it will consume a version
  // when we write an empty staging write in this way, it's will consume a 
  // version, but we won't add a committed version to commit_table_ for it,
  // while we commit an empty staging write through txn, it not only consumes a
  // version but we add a committed version to commit_table_ for it;
  // that we don't add a committed version to commit_table_ for the
  // empty_staging_write is fine, since we have advance max visible version in
  // EndCommitVersions()
  StagingWrite* empty_staging_write = GetEmptyStagingWrite();
  WPTxnMaintainVersionsCBForCommitWithPrepare
      wp_maintain_versions_cb_for_commit_with_prepare(this);
  return txn_store->CommitStagingWrite(
      write_options_,
      empty_staging_write,
      wp_maintain_versions_cb_for_commit_with_prepare,
      txn_store->GetCommitQueue()); // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

namespace {
class RollbackStagingWriteBuilder : public StagingWrite::Handler {
 public:
  RollbackStagingWriteBuilder(MVCCTxnStore* txn_store,
                              StagingWrite* rollback_staging_write)
      : txn_store_(txn_store),
        snapshot_manager_(txn_store_->GetSnapshotManager()),
        rollback_staging_write_(rollback_staging_write),
        snapshot_limit_max_(snapshot_manager_->SnapshotLimitsMax()) {
    read_options_.snapshot = &snapshot_limit_max_;
  }
  ~RollbackStagingWriteBuilder() {}

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
      rollback_staging_write_->Put(key, value);
    } else if (s.IsNotFound()) {
      rollback_staging_write_->Delete(key);
      s = Status::OK();
    } else {
      assert(!s.IsTryAgain());
    }
    return s;
  }

 private:
  ReadOptions read_options_;
  MVCCTxnStore* txn_store_;
  const SnapshotManager* snapshot_manager_;
  StagingWrite* rollback_staging_write_;
  // can see all version during GetInternal(), but only care about latest
  // committed of target key 
  const Snapshot& snapshot_limit_max_;
};
}   // anonymous namespace

Status WritePreparedTransaction::RollbackImpl() {
  // build the rollback staging write
  MVCCTxnStore* txn_store = GetTxnStore();
  const StagingWriteFactory* factory = txn_store->GetStagingWriteFactory();
  std::unique_ptr<StagingWrite>
      rollback_staging_write(factory->CreateStagingWrite());
  RollbackStagingWriteBuilder
      rollback_staging_write_builder(txn_store, rollback_staging_write.get());
  Status s = GetStagingWrite()->Iterate(&rollback_staging_write_builder);
  if (!s.IsOK()) {
    return s;
  }
  // insert the rollback_staging_write into write buffer to eliminate this txn's
  // footprint in the write buffer

  bool enable_two_write_queues = txn_store->IsTwoWriteQueuesEnabled();
  // rollback without prepare only takes effect when
  // enable_two_write_queues == false, when enable_two_write_queues == true, we
  // will switch rollback without prepare to rollback with prepare internally
  if (!enable_two_write_queues) {
    WPTxnMaintainVersionsCBForRollbackWithoutPrepare
        wp_maintain_versions_cb_for_rollback_without_prepare(this);
    return txn_store->CommitStagingWrite(
        write_options_,
        rollback_staging_write.get(),
        wp_maintain_versions_cb_for_rollback_without_prepare,
        txn_store->GetCommitQueue());
  }

  // enable_two_write_queues == true
  // first prepare: insert rollback staging write into write buffer
  WPTxnMaintainVersionsCBForPrepareForRollback
      wp_maintain_versions_cb_for_prepare_for_rollback(this);
  s = txn_store->CommitStagingWrite(
      write_options_,
      rollback_staging_write.get(),
      wp_maintain_versions_cb_for_prepare_for_rollback,
      txn_store->GetPrepareQueue());
  if (!s.IsOK()) {    // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
    return s;
  }
  // then commit
  // use an empty staging write for commit purpose, it will consume a version
  // when we write an empty staging write in this way, it's will consume a 
  // version, but we won't add a committed version to commit_table_ for it,
  // while we commit an empty staging write through txn, it not only consumes a
  // version but we add a committed version to commit_table_ for it;
  // that we don't add a committed version to commit_table_ for the
  // empty_staging_write is fine, since we have advance max visible version in
  // EndCommitVersions()
  StagingWrite* empty_staging_write = GetEmptyStagingWrite();
  WPTxnMaintainVersionsCBForRollbackWithPrepare
      wp_maintain_versions_cb_for_rollback_with_prepare(this);
  return txn_store->CommitStagingWrite(
      write_options_,
      empty_staging_write,
      wp_maintain_versions_cb_for_rollback_with_prepare,
      txn_store->GetCommitQueue()); // Todo: 失败的话可能需要清理prepared Heap里面本次事务插入的seq
}

}   // namespace COMPOSITE_STORE_NAMESPACE
