#pragma once

#include "pessimistic_txn_store.h"
#include "multi_versions/sequence_based/seq_based_multi_versions.h"
#include "test_util/test_util.h"
#include "third-party/gtest/gtest.h"

namespace COMPOSITE_STORE_NAMESPACE {

class PessimisticTxnTestsBase {
 public:
  PessimisticTxnTestsBase(const TxnTestsSetups& setups)
      : write_policy_(setups.write_policy),
        enable_two_write_queues_(setups.enable_two_write_queues),
        with_prepare_(setups.with_prepare),
        started_version_seq_(0) {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    StoreTraits store_traits;
    store_options.enable_two_write_queues = enable_two_write_queues_;
    store_traits.txn_store_impl_type = TxnStoreImplType::kMVCC;
    store_traits.txn_store_write_policy = write_policy_;
    Status s = TransactionStore::Open(store_options, txn_store_options,
                                      store_traits, &txn_store_);
    assert(s.IsOK() && txn_store_ != nullptr);
    txn_store_impl_ = static_cast_with_check<PessimisticTxnStore>(txn_store_);
    mvm_impl_ = static_cast_with_check<SeqBasedMultiVersionsManager>(
        txn_store_impl_->GetMultiVersionsManager());
    assert(mvm_impl_);

    if (setups.encoded_version == "") {
      started_version_seq_ = 0;
    } else {
      started_version_seq_ = std::stoull(setups.encoded_version);
      Version* orig = mvm_impl_->CreateVersion();
      orig->DecodeFrom(setups.encoded_version);
      txn_store_impl_->RecoverMultiVersionsManagerFrom(*orig);
      // RecoverMultiVersionsManagerFrom() use started_version_seq_ + 1 as
      // recovered started seq
      started_version_seq_ += 1;
      delete orig;
    }
  }

  virtual ~PessimisticTxnTestsBase() {
    delete txn_store_;
  }

  void PrintTestSetups() const {
    const char* write_policy =
        write_policy_ == TxnStoreWritePolicy::kWriteCommitted ?
            "kWriteCommitted" : "kWritePrepared";
    const char* two_write_queues = enable_two_write_queues_ ? "TRUE" : "FALSE";
    const char* with_prepare = with_prepare_ ? "TRUE" : "FALSE";
    std::cout<<"Write policy: "<<write_policy
             <<", Two write queues: "<<two_write_queues
             <<", With prepare: "<<with_prepare
             <<", Started version: "<<started_version_seq_<<std::endl;
  }

 protected:
  TxnStoreWritePolicy write_policy_;
  bool enable_two_write_queues_;
  bool with_prepare_;
  uint64_t started_version_seq_;
  TransactionStore* txn_store_;
  PessimisticTxnStore* txn_store_impl_;
  SeqBasedMultiVersionsManager* mvm_impl_;
};

class CommonPessimisticTxnTests : public PessimisticTxnTestsBase {
 public:
  CommonPessimisticTxnTests(const TxnTestsSetups& setups)
      : PessimisticTxnTestsBase(setups) {}
  virtual ~CommonPessimisticTxnTests() {}

  void SimpleTransactionalReadWrite();
  void SimpleNonTransactionalReadWrite();
  void ReadTxnOwnWrites();
  void ReadAfterPrepare();
  void ReadAfterCommit();
  void ReadAfterRollback();
  void CommitWithPrepare();
  void CommitWithoutPrepare();
  void RollbackWithPrepare();
  void RollbackWithoutPrepare();
  void PrepareEmptyStagingWrite();
  void CommitEmptyStagingWrite();
  void RollbackEmptyStagingWrite();
  void InterleavingPrepareCommitBetweenMultiTxns();
  void InterleavingPrepareRollbackBetweenMultiTxns();
  void ReadUnderSnapshot();
  void ReuseTransaction();
  void SingleTxnExcutionFlowTest();

  void LockTimeOut();
  void TxnExpired();
};

class MultiThreadingPessimisticTxnTests : public PessimisticTxnTestsBase {
 public:
  MultiThreadingPessimisticTxnTests(const TxnTestsSetups& setups)
      : PessimisticTxnTestsBase(setups) {}
  virtual ~MultiThreadingPessimisticTxnTests() {}

  void MultiThreadsTxnsExcution();
  void SingleWriterMultiReaders();
  void SingleReaderMultiWriters();
  void MultiWritersMultiReaders();

 private:
  void SetupTxnExecutorCfg(
      TransactionExecutorCfg& cfg,
      uint32_t min_with_prepare, uint32_t max_with_prepare, GenType gen0,
      uint32_t min_to_be_rollbacked, uint32_t max_to_be_rollbacked,
          GenType gen1,
      uint32_t min_delay_ms, uint32_t max_delay_ms, GenType gen2,
      uint32_t min_inc_per_time, uint32_t max_inc_per_time, GenType gen3) {
    cfg.with_prepare_rate = UIntRange(min_with_prepare, max_with_prepare, gen0);
    cfg.to_be_rollbacked_rate =
        UIntRange(min_to_be_rollbacked, max_to_be_rollbacked, gen1);
    cfg.delay_ms_after_prepare = UIntRange(min_delay_ms, max_delay_ms, gen2);
    cfg.inc_per_time = UIntRange(min_inc_per_time, max_inc_per_time, gen3);
  }
};

class InspectPessimisticTxnTests : public PessimisticTxnTestsBase {
 public:
  InspectPessimisticTxnTests(const TxnTestsSetups& setups)
      : PessimisticTxnTestsBase(setups) {}
  virtual ~InspectPessimisticTxnTests() {}

  void VersionIncrement();
  void VersionIncrementForPreparingOfEmptyStagingWrite();
  void VersionIncrementForCommittingOfEmptyStagingWrite();
  void VersionIncrementForRollbackingOfEmptyStagingWrite();
  void WriteBufferInsertTimingBetweenDifferentWritePolicy();

 private:
  struct SeqIncInfos {
    uint64_t max_readable_inc;
    uint64_t max_visible_inc;
    uint64_t so_far_allocated_inc;
  };

  void CheckSeqInfos(const SeqIncInfos& expected) const {
    uint64_t actual_max_readable = mvm_impl_->MaxReadableVersion();
    uint64_t actual_max_visible = mvm_impl_->MaxVisibleVersion();
    uint64_t actual_so_far_allocated =
        mvm_impl_->seq_allocator_.SoFarAllocated();
    uint64_t expected_max_readable =
        expected.max_readable_inc + started_version_seq_;
    uint64_t expected_max_visible =
        expected.max_visible_inc + started_version_seq_;
    uint64_t expected_so_far_allocated =
        expected.so_far_allocated_inc + started_version_seq_;
    ASSERT_EQ(actual_max_readable, expected_max_readable);
    ASSERT_EQ(actual_max_visible, expected_max_visible);
    ASSERT_EQ(actual_so_far_allocated, expected_so_far_allocated);
  }

  void GetExpectedSeqIncInfos(std::vector<SeqIncInfos>** expected,
      std::vector<std::vector<SeqIncInfos>>& expect_infos) const {
    if (with_prepare_) {
      if (enable_two_write_queues_) {
        *expected = &expect_infos[0];
      } else {
        *expected = &expect_infos[1];
      }
    } else {
      if (enable_two_write_queues_) {
        *expected = &expect_infos[2];
      } else {
        *expected = &expect_infos[3];
      }
    }
  }
};

void CommonPessimisticTxnTests::SimpleTransactionalReadWrite() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  // begin transaction
  Transaction* txn = txn_store_->BeginTransaction(write_options);

  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());

  // read txn's own writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // commit with prepare
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // transactional read
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  delete txn;
}

void CommonPessimisticTxnTests::SimpleNonTransactionalReadWrite() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  // read non-existence
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // read existence
  s = txn_store_->Put(write_options, "foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // read after overwritten
  s = txn_store_->Put(write_options, "foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  // read after deletion
  s = txn_store_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // delete non-existence
  s = txn_store_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
}

void CommonPessimisticTxnTests::ReadTxnOwnWrites() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // read txn's own writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // read txn's own write during write stage
  s = txn->Delete("foo");
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  // non-transactional read can't see the txn's own writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsNotFound());

  // cleanup
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  delete txn;
}

void CommonPessimisticTxnTests::ReadAfterPrepare() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // read after prepare
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());

  // transactional read can see the prepared writes equivalent to read txn's
  // own write
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read can't see the prepared writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn_store_->TEST_Crash();
  delete txn;
}

void CommonPessimisticTxnTests::ReadAfterCommit() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // commit with prepare
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // read after commit
  // transactional read can see the committed writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read can see the committed writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  s = txn->Put("foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar3");
  ASSERT_TRUE(s.IsOK());

  // commit without prepare
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // read after commit
  // transactional read can see the committed writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar3");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  // non-transactional read can see the committed writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar3");
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  delete txn;
}

void CommonPessimisticTxnTests::ReadAfterRollback() {
  WriteOptions write_options;
  TransactionOptions txn_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);

  // first write somthing
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  Transaction* txn1 = txn_store_->BeginTransaction(write_options);
  s = txn1->Delete("foo");
  ASSERT_TRUE(s.IsOK());

  // rollback without prepare
  s = txn1->Rollback();
  ASSERT_TRUE(s.IsOK());

  // read after rollback
  s = txn1->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // can reuse txn after rollback without prepare
  s = txn1->Delete("foo");
  ASSERT_TRUE(s.IsOK());
  s = txn1->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());

  // rollback with prepare
  s = txn1->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn1->Rollback();
  ASSERT_TRUE(s.IsOK());

  // read after rollback
  s = txn1->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  s = txn1->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  delete txn1;
}

void CommonPessimisticTxnTests::CommitWithPrepare() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // commit with prepare
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // transactional read can see the committed writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read can see the committed writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  delete txn;
}

void CommonPessimisticTxnTests::CommitWithoutPrepare() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // commit without prepare
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // transactional read can see the committed writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read can see the committed writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  delete txn;
}

void CommonPessimisticTxnTests::RollbackWithPrepare() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // insert something to underlying store initially
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // start a new round of txn write and rollback it after Prepare()
  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar1");
  ASSERT_TRUE(s.IsOK());

  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());

  // rollback after prepare
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // won't read the rollbacked value
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // can't reuse txn directly after rollback(with Prepare)
  s = txn->Put("foo2", "bar1");
  ASSERT_TRUE(s.IsInvalidArgument());

  delete txn;
}

void CommonPessimisticTxnTests::RollbackWithoutPrepare() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // insert something to underlying store initially
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // start a new round of txn write and rollback it during write stage
  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar1");
  ASSERT_TRUE(s.IsOK());

  // rollback during write stage
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // won't read the rollbacked value
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // can reuse txn direct after rollback(without Prepare)
  s = txn->Put("foo2", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo2", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  delete txn;
}

void CommonPessimisticTxnTests::PrepareEmptyStagingWrite() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // prepare an empty staging write
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn_store_->TEST_Crash();
  delete txn;
}

void CommonPessimisticTxnTests::CommitEmptyStagingWrite() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // commit(with prepare) an empty staging write
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // commit(without prepare) an empty staging write
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

void CommonPessimisticTxnTests::RollbackEmptyStagingWrite() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // rollback(without prepare) an empty staging write
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // rollback(with prepare) an empty staging write
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

void CommonPessimisticTxnTests::InterleavingPrepareCommitBetweenMultiTxns() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn0 = txn_store_->BeginTransaction(write_options);
  Transaction* txn1 = txn_store_->BeginTransaction(write_options);
  Transaction* txn2 = txn_store_->BeginTransaction(write_options);

  s = txn0->Put("foo0", "bar0");
  ASSERT_TRUE(s.IsOK());
  s = txn1->Put("foo1", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn2->Put("foo2", "bar2");
  ASSERT_TRUE(s.IsOK());

  // Prepare()
  s = txn0->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn1->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn2->Prepare();
  ASSERT_TRUE(s.IsOK());

  // Interleave Commit()
  s = txn1->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn2->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn0->Commit();
  ASSERT_TRUE(s.IsOK());

  // Verify
  s = txn_store_->Get(read_options, "foo0", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar0");
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn_store_->Get(read_options, "foo2", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  delete txn0;
  delete txn1;
  delete txn2;
}

void CommonPessimisticTxnTests::InterleavingPrepareRollbackBetweenMultiTxns() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn0 = txn_store_->BeginTransaction(write_options);
  Transaction* txn1 = txn_store_->BeginTransaction(write_options);
  Transaction* txn2 = txn_store_->BeginTransaction(write_options);

  s = txn0->Put("foo0", "bar0");
  ASSERT_TRUE(s.IsOK());
  s = txn1->Put("foo1", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn2->Put("foo2", "bar2");
  ASSERT_TRUE(s.IsOK());

  // Prepare()
  s = txn0->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn1->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn2->Prepare();
  ASSERT_TRUE(s.IsOK());

  // Interleave Rollback()
  s = txn1->Rollback();
  ASSERT_TRUE(s.IsOK());
  s = txn2->Rollback();
  ASSERT_TRUE(s.IsOK());
  s = txn0->Rollback();
  ASSERT_TRUE(s.IsOK());

  // Verify
  s = txn_store_->Get(read_options, "foo0", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo2", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn0;
  delete txn1;
  delete txn2;
}

void CommonPessimisticTxnTests::ReadUnderSnapshot() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;
  const Snapshot* snapshot1;
  const Snapshot* snapshot2;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();  // commit without prepare
  ASSERT_TRUE(s.IsOK());
  snapshot1 = txn_store_->TakeSnapshot();   // take a snapshot

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();  // commit without prepare
  ASSERT_TRUE(s.IsOK());
  snapshot2 = txn_store_->TakeSnapshot();   // take another snapshot

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  s = txn->Put("foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  read_options.snapshot = snapshot1;
  s = txn->Get(read_options, "foo", &value);
  // txn's own write preceding even though a snapshot provided
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  s = txn->Commit();  // commit without prepare
  ASSERT_TRUE(s.IsOK());

  // transactional read
  read_options.snapshot = snapshot1;
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  read_options.snapshot = snapshot2;
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  read_options.snapshot = nullptr;    // read latest value
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  // non-transactional read
  read_options.snapshot = snapshot1;
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  read_options.snapshot = snapshot2;
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  read_options.snapshot = nullptr;    // read latest value
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  txn_store_->ReleaseSnapshot(snapshot1);
  txn_store_->ReleaseSnapshot(snapshot2);
  delete txn;
}

void CommonPessimisticTxnTests::ReuseTransaction() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Delete("foo1");
  ASSERT_TRUE(s.IsOK());

  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // reuse transaction
  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar1");
  ASSERT_TRUE(s.IsOK());

  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  delete txn;
}

void CommonPessimisticTxnTests::SingleTxnExcutionFlowTest() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // write stage
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  
  // can't write after prepared
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't prepare after prepared
  s = txn->Prepare();
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't write after committed(2PC here)
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn->Delete("foo");
  ASSERT_TRUE(s.IsInvalidArgument());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // can't prepare after commited
  s = txn->Prepare();
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't commit after committed
  s = txn->Commit();
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't rollback after committed
  s = txn->Rollback();
  ASSERT_TRUE(s.IsInvalidArgument());

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  // write stage
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // rollback during write stage(equivalent to rollback to savepoint, txn will
  // be in initial state after rollback so act as a newly created txn)
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // can write、prepare and commit
  s = txn->Put("foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  // write stage
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // rollback after prepared
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // can't write after rollback with prepare executed
  s = txn->Delete("foo1");
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't prepare after rollback with prepare executed
  s = txn->Prepare();
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't commit after rollback with prepare executed
  s = txn->Commit();
  ASSERT_TRUE(s.IsInvalidArgument());

  // can't rollback after rollback with prepare executed
  s = txn->Rollback();
  ASSERT_TRUE(s.IsInvalidArgument());
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar2");

  delete txn;
}

void CommonPessimisticTxnTests::LockTimeOut() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  Status s;

  // key locked by txn1
  Transaction* txn1 = txn_store_->BeginTransaction(write_options);
  s = txn1->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());

  // txn2 try lock for 2ms and timeout failed
  txn_options.lock_timeout_ms = 2;
  Transaction* txn2 = txn_store_->BeginTransaction(write_options, txn_options);
  s = txn2->Put("foo", "bar1");
  ASSERT_TRUE(s.IsTimedOut());

  // txn2 try lock and failed immediately
  txn_options.lock_timeout_ms = 0;
  txn2 = txn_store_->BeginTransaction(write_options, txn_options, txn2);
  s = txn2->Put("foo", "bar2");
  ASSERT_TRUE(s.IsTimedOut());

  // cleanup
  s = txn1->Commit();
  ASSERT_TRUE(s.IsOK());

  delete txn1;
  delete txn2;
}

void CommonPessimisticTxnTests::TxnExpired() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  Status s;

  txn_options.txn_duration_ms = 2;    // txn will expire after 2ms
  Transaction* txn = txn_store_->BeginTransaction(write_options, txn_options);
  // write stage
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());

  // wait 3ms
  const int SLEEP_DURATION_3MS = 3 * 1000;
  SystemClock::GetSingleton()->SleepForMicroseconds(SLEEP_DURATION_3MS);

  // preapre failed
  s = txn->Prepare();
  ASSERT_TRUE(s.IsExpired());

  // commit failed
  s = txn->Commit();
  ASSERT_TRUE(s.IsExpired());

  // cleanup
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  delete txn;
}

namespace {
void ThreadFuncInsertStoreRdMoWr(
    TransactionStore* txn_store, TransactionExecutorCfg* cfg,
    uint16_t target_key_set, uint32_t num_keys_in_set,
    uint64_t target_increment) {
  TransactionExecutor executor(txn_store, cfg);
  executor.InsertStoreRdMoWr(target_key_set, num_keys_in_set, target_increment);
  uint64_t actual_increment =
      executor.SumAllKeysOfSet(target_key_set, num_keys_in_set);
  ASSERT_EQ(actual_increment, target_increment);
}
}   // anonymous namespace

void MultiThreadingPessimisticTxnTests::MultiThreadsTxnsExcution() {
  const uint32_t num_threads = 4;
  const uint32_t num_keys_in_set = 1000;
  const uint64_t total_increment = 1000;
  std::vector<TransactionExecutorCfg> vec_cfgs(num_threads);
  std::vector<port::Thread> vec_threads;
  vec_threads.reserve(num_threads);
  // cfg0: default cfg
  // cfg1
  SetupTxnExecutorCfg(vec_cfgs[1],
                      0, 0, GenType::kMin,         // with_prepare_rate
                      0, 0, GenType::kMin,         // to_be_rollbacked_rate
                      0, 0, GenType::kMin,         // delay_ms_after_prepare
                      1, 1, GenType::kMin);        // inc_per_time
  // cfg2
  SetupTxnExecutorCfg(vec_cfgs[2],
                      70, 90, GenType::kRandom,    // with_prepare_rate
                      5, 15, GenType::kRandom,     // to_be_rollbacked_rate
                      0, 0, GenType::kMin,         // delay_ms_after_prepare
                      1, 10, GenType::kRandom);    // inc_per_time
  // cfg3
  SetupTxnExecutorCfg(vec_cfgs[3],
                      100, 100, GenType::kMin,     // with_prepare_rate
                      20, 30, GenType::kRandom,    // to_be_rollbacked_rate
                      0, 0, GenType::kMin,         // delay_ms_after_prepare
                      20, 30, GenType::kRandom);   // inc_per_time
  for (uint32_t i = 0; i < num_threads; ++i) {
    vec_threads.emplace_back(ThreadFuncInsertStoreRdMoWr, txn_store_,
                             &vec_cfgs[i], i, num_keys_in_set, total_increment);
  }
  for (uint32_t j = 0; j < num_threads; ++j) {
    vec_threads[j].join();
  }
}

namespace {
void ThreadFuncInsertStoreWrOnly(
    TransactionStore* txn_store, TransactionExecutorCfg* cfg,
    uint16_t target_key_set, uint32_t num_keys_in_set,
    uint64_t target_increment, std::atomic<bool>* writer_finished) {
  TransactionExecutor executor(txn_store, cfg);
  Status s = executor.InsertStoreWrOnly(target_key_set, num_keys_in_set,
                                        target_increment);
  ASSERT_TRUE(s.IsOK());
  writer_finished->store(true);
}

void ThreadFuncReadStoreRdOnly(
    TransactionStore* txn_store, uint16_t target_key_set,
    uint32_t num_keys_in_set, uint64_t target_increment,
    std::atomic<bool>* writer_finished) {
  TransactionExecutorCfg reader_cfg;
  TransactionExecutor executor(txn_store, &reader_cfg);
  uint64_t actual_increment = 0;
  while (!writer_finished->load()) {
    executor.ReadStoreRdOnly(target_key_set, num_keys_in_set,
                             &actual_increment);
  }

  ASSERT_TRUE(writer_finished->load());
  ASSERT_LE(actual_increment, target_increment);

  // do one more read
  executor.ReadStoreRdOnly(target_key_set, num_keys_in_set, &actual_increment);
  ASSERT_EQ(actual_increment, target_increment);
}

bool IsAllWritersFinished(std::vector<std::atomic<bool>>* vec_writer_finished) {
  for (auto& wf : *vec_writer_finished) {
    if (wf.load() == false) {
      return false;
    }
  }
  return true;
}

void ThreadFuncReadStoreRdOnly1(
    TransactionStore* txn_store, uint16_t num_key_set,
    uint32_t num_keys_in_set, uint64_t target_increment,
    std::vector<std::atomic<bool>>* vec_writer_finished) {
  TransactionExecutorCfg reader_cfg;
  TransactionExecutor executor(txn_store, &reader_cfg);
  std::vector<uint64_t> vec_actual_increment(num_key_set, 0);
  while (IsAllWritersFinished(vec_writer_finished) == false) {
    // not support multi_get currently
    for (uint16_t i = 0; i < num_key_set; ++i) {
      executor.ReadStoreRdOnly(i, num_keys_in_set, &vec_actual_increment[i]);
    }
  }

  ASSERT_TRUE(IsAllWritersFinished(vec_writer_finished));
  for (auto& ai : vec_actual_increment) {
    ASSERT_LE(ai, target_increment);
  }

  // do one more read
  for (uint16_t i = 0; i < num_key_set; ++i) {
    executor.ReadStoreRdOnly(i, num_keys_in_set, &vec_actual_increment[i]);
  }
  for (auto& ai : vec_actual_increment) {
    ASSERT_EQ(ai, target_increment);
  }
}
}   // anonymous namespace

void MultiThreadingPessimisticTxnTests::SingleWriterMultiReaders() {
  const uint32_t num_reader_threads = 4;
  const uint16_t target_key_set = 0;
  const uint32_t num_keys_in_set = 1000;
  const uint64_t total_increment = 4000;
  std::vector<port::Thread> vec_reader_threads;
  vec_reader_threads.reserve(num_reader_threads);
  std::atomic<bool> writer_finished = {false};
  // writer insert cfg
  TransactionExecutorCfg writer_insert_cfg;
  SetupTxnExecutorCfg(writer_insert_cfg,
                      70, 90, GenType::kRandom,    // with_prepare_rate
                      5, 15, GenType::kRandom,     // to_be_rollbacked_rate
                      1, 5, GenType::kRandom,      // delay_ms_after_prepare
                      1, 10, GenType::kRandom);    // inc_per_time
  port::Thread writer_thread(ThreadFuncInsertStoreWrOnly, txn_store_,
                             &writer_insert_cfg, target_key_set,
                             num_keys_in_set, total_increment,
                             &writer_finished);
  for (uint32_t i = 0; i < num_reader_threads; ++i) {
    vec_reader_threads.emplace_back(ThreadFuncReadStoreRdOnly, txn_store_,
                                    target_key_set, num_keys_in_set,
                                    total_increment, &writer_finished);
  }
  writer_thread.join();
  for (uint32_t j = 0; j < num_reader_threads; ++j) {
    vec_reader_threads[j].join();
  }
}

void MultiThreadingPessimisticTxnTests::SingleReaderMultiWriters() {
  const uint32_t num_writer_threads = 4;
  const uint32_t num_key_set = num_writer_threads;
  const uint32_t num_keys_in_set = 1000;
  const uint64_t total_increment = 2000;
  std::vector<TransactionExecutorCfg>
      vec_writer_insert_cfgs(num_writer_threads);
  std::vector<port::Thread> vec_writer_threads;
  vec_writer_threads.reserve(num_writer_threads);
  std::vector<std::atomic<bool>> vec_writer_finished(num_writer_threads);
  for (auto& wf : vec_writer_finished) {
    wf = false;
  }
  assert(vec_writer_finished.size() == num_writer_threads);
  // cfg0: default cfg
  // cfg1
  SetupTxnExecutorCfg(vec_writer_insert_cfgs[1],
                      0, 0, GenType::kMin,         // with_prepare_rate
                      0, 0, GenType::kMin,         // to_be_rollbacked_rate
                      0, 0, GenType::kMin,         // delay_ms_after_prepare
                      1, 1, GenType::kMin);        // inc_per_time
  // cfg2
  SetupTxnExecutorCfg(vec_writer_insert_cfgs[2],
                      70, 90, GenType::kRandom,    // with_prepare_rate
                      5, 15, GenType::kRandom,     // to_be_rollbacked_rate
                      1, 5, GenType::kRandom,      // delay_ms_after_prepare
                      1, 10, GenType::kRandom);    // inc_per_time
  // cfg3
  SetupTxnExecutorCfg(vec_writer_insert_cfgs[3],
                      100, 100, GenType::kMin,     // with_prepare_rate
                      20, 30, GenType::kRandom,    // to_be_rollbacked_rate
                      5, 10, GenType::kRandom,     // delay_ms_after_prepare
                      20, 30, GenType::kRandom);   // inc_per_time

  for (uint32_t i = 0; i < num_writer_threads; ++i) {
    vec_writer_threads.emplace_back(ThreadFuncInsertStoreWrOnly, txn_store_,
                                    &vec_writer_insert_cfgs[i], i,
                                    num_keys_in_set, total_increment,
                                    &vec_writer_finished[i]);
  }
  port::Thread reader_thread(ThreadFuncReadStoreRdOnly1, txn_store_,
                             num_key_set, num_keys_in_set, total_increment,
                             &vec_writer_finished);

  for (uint32_t j = 0; j < num_writer_threads; ++j) {
    vec_writer_threads[j].join();
  }
  reader_thread.join();
}

void MultiThreadingPessimisticTxnTests::MultiWritersMultiReaders() {
  const uint32_t num_threads = 4;
  const uint32_t num_key_set = num_threads;
  const uint32_t num_keys_in_set = 1000;
  const uint64_t total_increment = 2000;
  std::vector<TransactionExecutorCfg> vec_writer_insert_cfgs(num_threads);
  std::vector<port::Thread> vec_writer_threads;
  vec_writer_threads.reserve(num_threads);
  std::vector<port::Thread> vec_reader_threads;
  vec_reader_threads.reserve(num_threads);
  std::vector<std::atomic<bool>> vec_writer_finished(num_threads);
  for (auto& wf : vec_writer_finished) {
    wf = false;
  }
  assert(vec_writer_finished.size() == num_threads);
  // cfg0: default cfg
  // cfg1
  SetupTxnExecutorCfg(vec_writer_insert_cfgs[1],
                      0, 0, GenType::kMin,         // with_prepare_rate
                      0, 0, GenType::kMin,         // to_be_rollbacked_rate
                      0, 0, GenType::kMin,         // delay_ms_after_prepare
                      1, 1, GenType::kMin);        // inc_per_time
  // cfg2
  SetupTxnExecutorCfg(vec_writer_insert_cfgs[2],
                      70, 90, GenType::kRandom,    // with_prepare_rate
                      5, 15, GenType::kRandom,     // to_be_rollbacked_rate
                      1, 5, GenType::kRandom,      // delay_ms_after_prepare
                      1, 10, GenType::kRandom);    // inc_per_time
  // cfg3
  SetupTxnExecutorCfg(vec_writer_insert_cfgs[3],
                      100, 100, GenType::kMin,     // with_prepare_rate
                      20, 30, GenType::kRandom,    // to_be_rollbacked_rate
                      5, 10, GenType::kRandom,     // delay_ms_after_prepare
                      20, 30, GenType::kRandom);   // inc_per_time

  for (uint32_t i = 0; i < num_threads; ++i) {
    vec_writer_threads.emplace_back(ThreadFuncInsertStoreWrOnly, txn_store_,
                                    &vec_writer_insert_cfgs[i], i,
                                    num_keys_in_set, total_increment,
                                    &vec_writer_finished[i]);
  }
  for (uint32_t j = 0; j < num_threads; ++j) {
    vec_reader_threads.emplace_back(ThreadFuncReadStoreRdOnly, txn_store_,
                                    j, num_keys_in_set, total_increment,
                                    &vec_writer_finished[j]);
  }

  for (uint32_t k = 0; k < num_threads; ++k) {
    vec_writer_threads[k].join();
    vec_reader_threads[k].join();
  }
}

void InspectPessimisticTxnTests::VersionIncrement() {
  std::vector<std::vector<SeqIncInfos>> expected_of_write_prepared =
  //  before-txn after-write after-prepare after-commit
      {{{0, 0, 0}, {0, 0, 0}, {1, 0, 1}, {1, 2, 2}},  // prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0}, {1, 1, 1}, {2, 2, 2}},  // prepare() and not 2-WQ
       {{0, 0, 0}, {0, 0, 0},            {1, 2, 2}},  // not prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0},            {1, 1, 1}}}; // not prepare() and
                                                      //   not 2-WQ

  std::vector<std::vector<SeqIncInfos>> expected_of_write_committed =
      {{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {4, 4, 4}},  // prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {4, 4, 4}},  // prepare() and not 2-WQ
       {{0, 0, 0}, {0, 0, 0},            {4, 4, 4}},  // not prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0},            {4, 4, 4}}}; // not prepare() and
                                                      //   not 2-WQ

  std::vector<SeqIncInfos>* expected;
  if (write_policy_ == TxnStoreWritePolicy::kWritePrepared) {
    GetExpectedSeqIncInfos(&expected, expected_of_write_prepared);
  } else {
    GetExpectedSeqIncInfos(&expected, expected_of_write_committed);
  }

  WriteOptions write_options;

  // Before txn
  CheckSeqInfos((*expected)[0]);
  // Start a transaction
  Transaction* txn = txn_store_->BeginTransaction(write_options);
  ASSERT_TRUE(txn != nullptr);

  // Write keys in this transaction
  Status s = txn->Put("abc", "def");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("abc1", "def");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("abc2", "def");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("abc3", "def");
  ASSERT_TRUE(s.IsOK());
  // After write
  CheckSeqInfos((*expected)[1]);

  if (with_prepare_) {
    // Prepare transaction
    s = txn->Prepare();
    ASSERT_TRUE(s.IsOK());
    // After prepare
    CheckSeqInfos((*expected)[2]);

    // Commit transaction after prepare
    s = txn->Commit();
    ASSERT_TRUE(s.IsOK());
    // After commit with prepare
    CheckSeqInfos((*expected)[3]);
  } else {
    // Commit transaction
    s = txn->Commit();
    ASSERT_TRUE(s.IsOK());
    // After commit without prepare
    CheckSeqInfos((*expected)[2]);
  }

  delete txn;
}

void InspectPessimisticTxnTests::
    VersionIncrementForPreparingOfEmptyStagingWrite() {
  std::vector<std::vector<SeqIncInfos>> expected_of_write_prepared =
  //  before-txn  after-prepare
      {{{0, 0, 0}, {1, 0, 1}},   // prepare() and 2-WQ
       {{0, 0, 0}, {1, 1, 1}}};  // prepare() and not 2-WQ

  std::vector<std::vector<SeqIncInfos>> expected_of_write_committed =
      {{{0, 0, 0}, {0, 0, 0}},   // prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0}}};  // prepare() and not 2-WQ
  std::vector<SeqIncInfos>* expected;
  if (write_policy_ == TxnStoreWritePolicy::kWritePrepared) {
    GetExpectedSeqIncInfos(&expected, expected_of_write_prepared);
  } else {
    GetExpectedSeqIncInfos(&expected, expected_of_write_committed);
  }

  WriteOptions write_options;
  Status s;

  // Before txn
  CheckSeqInfos((*expected)[0]);
  // Start a transaction
  Transaction* txn = txn_store_->BeginTransaction(write_options);
  ASSERT_TRUE(txn != nullptr);

  // prepare an empty staging write
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  // After prepare
  CheckSeqInfos((*expected)[1]);

  txn_store_->TEST_Crash();
  delete txn;
}

void InspectPessimisticTxnTests::
    VersionIncrementForCommittingOfEmptyStagingWrite() {
  std::vector<std::vector<SeqIncInfos>> expected_of_write_prepared =
  //  before-txn  after-prepare after-commit
      {{{0, 0, 0}, {1, 0, 1}, {1, 2, 2}},  // prepare() and 2-WQ
       {{0, 0, 0}, {1, 1, 1}, {2, 2, 2}},  // prepare() and not 2-WQ
       {{0, 0, 0},            {1, 2, 2}},  // not prepare() and 2-WQ
       {{0, 0, 0},            {1, 1, 1}}}; // not prepare() and not 2-WQ

  std::vector<std::vector<SeqIncInfos>> expected_of_write_committed =
      {{{0, 0, 0}, {0, 0, 0}, {1, 1, 1}},  // prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0}, {1, 1, 1}},  // prepare() and not 2-WQ
       {{0, 0, 0},            {1, 1, 1}},  // not prepare() and 2-WQ
       {{0, 0, 0},            {1, 1, 1}}}; // not prepare() and not 2-WQ

  std::vector<SeqIncInfos>* expected;
  if (write_policy_ == TxnStoreWritePolicy::kWritePrepared) {
    GetExpectedSeqIncInfos(&expected, expected_of_write_prepared);
  } else {
    GetExpectedSeqIncInfos(&expected, expected_of_write_committed);
  }

  WriteOptions write_options;
  Status s;

  // Before txn
  CheckSeqInfos((*expected)[0]);
  // Start a transaction
  Transaction* txn = txn_store_->BeginTransaction(write_options);
  ASSERT_TRUE(txn != nullptr);

  // commit an empty staging write
  if (with_prepare_) {
    // Prepare transaction
    s = txn->Prepare();
    ASSERT_TRUE(s.IsOK());
    // After prepare
    CheckSeqInfos((*expected)[1]);

    // Commit transaction after prepare
    s = txn->Commit();
    ASSERT_TRUE(s.IsOK());
    // After commit with prepare
    CheckSeqInfos((*expected)[2]);
  } else {
    // Commit transaction
    s = txn->Commit();
    ASSERT_TRUE(s.IsOK());
    // After commit without prepare
    CheckSeqInfos((*expected)[1]);
  }

  delete txn;
}

void InspectPessimisticTxnTests::
    VersionIncrementForRollbackingOfEmptyStagingWrite() {
    std::vector<std::vector<SeqIncInfos>> expected_of_write_prepared =
  //  before-txn  after-prepare after-commit
      {{{0, 0, 0}, {1, 0, 1}, {2, 3, 3}},  // prepare() and 2-WQ
       {{0, 0, 0}, {1, 1, 1}, {2, 2, 2}},  // prepare() and not 2-WQ
       {{0, 0, 0},            {0, 0, 0}},  // not prepare() and 2-WQ
       {{0, 0, 0},            {0, 0, 0}}}; // not prepare() and not 2-WQ

  std::vector<std::vector<SeqIncInfos>> expected_of_write_committed =
      {{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}},  // prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}},  // prepare() and not 2-WQ
       {{0, 0, 0},            {0, 0, 0}},  // not prepare() and 2-WQ
       {{0, 0, 0},            {0, 0, 0}}}; // not prepare() and not 2-WQ

  std::vector<SeqIncInfos>* expected;
  if (write_policy_ == TxnStoreWritePolicy::kWritePrepared) {
    GetExpectedSeqIncInfos(&expected, expected_of_write_prepared);
  } else {
    GetExpectedSeqIncInfos(&expected, expected_of_write_committed);
  }

  WriteOptions write_options;
  Status s;

  // Before txn
  CheckSeqInfos((*expected)[0]);
  // Start a transaction
  Transaction* txn = txn_store_->BeginTransaction(write_options);
  ASSERT_TRUE(txn != nullptr);

  // rollback an empty staging write
  if (with_prepare_) {
    // Prepare transaction
    s = txn->Prepare();
    ASSERT_TRUE(s.IsOK());
    // After prepare
    CheckSeqInfos((*expected)[1]);

    // Rollback transaction after prepare
    s = txn->Rollback();
    ASSERT_TRUE(s.IsOK());
    // After rollback with prepare
    CheckSeqInfos((*expected)[2]);
  } else {
    // Rollback transaction
    s = txn->Rollback();
    ASSERT_TRUE(s.IsOK());
    // After rollback without prepare
    CheckSeqInfos((*expected)[1]);
  }

  delete txn;
}

void InspectPessimisticTxnTests::
    WriteBufferInsertTimingBetweenDifferentWritePolicy() {
  std::string key(100, 'f');
  std::string value(100, 'b');
  uint64_t expected_raw_data_size = key.size() + value.size();

  WriteOptions write_options;
  ReadOptions read_options;

  ASSERT_EQ(txn_store_impl_->RawDataSize(), 0ull);
  // Start a transaction
  Transaction* txn = txn_store_->BeginTransaction(write_options);
  ASSERT_TRUE(txn != nullptr);

  Status s = txn->Put(key, value);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(txn_store_impl_->RawDataSize(), 0ull);

  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  // write prepared txn insert data to store during Prepare()
  if (write_policy_ == TxnStoreWritePolicy::kWritePrepared) {
    ASSERT_EQ(txn_store_impl_->RawDataSize(), expected_raw_data_size);
  } else {
    // write committed txn insert data to store during Commit()
    ASSERT_EQ(txn_store_impl_->RawDataSize(), 0ull);
  }

  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(txn_store_impl_->RawDataSize(), expected_raw_data_size);

  delete txn;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
