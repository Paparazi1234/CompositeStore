#pragma once

#include "MVCC_based_txn.h"
#include "include/txn_lock_manager.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class CommonTxnTests {
 public:
  CommonTxnTests(TxnStoreWritePolicy write_policy,
                 bool enable_two_write_queues,
                 const std::string& encoded_version = std::string(""))
      : write_policy_(write_policy),
        enable_two_write_queues_(enable_two_write_queues) {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    store_options.enable_two_write_queues = enable_two_write_queues_;
    if (write_policy_ == WRITE_COMMITTED) {
      txn_store_impl_ = new WriteCommittedTxnStore(store_options,
                                                   txn_store_options,
                                                   txn_lock_mgr_factory);
    } else if (write_policy_ == WRITE_PREPARED) {
      CommitTableOptions commit_table_options;
      txn_store_impl_ = new WritePreparedTxnStore(store_options,
                                                  txn_store_options,
                                                  commit_table_options,
                                                  txn_lock_mgr_factory);
    } else {
      assert(false);
    }
    assert(txn_store_impl_);
    txn_store_ = txn_store_impl_;
    if (encoded_version == "") {
      started_version_seq_ = 0;
    } else {
      started_version_seq_ = std::stoull(encoded_version);
      MultiVersionsManager* mvm_manager =
          txn_store_impl_->GetMultiVersionsManager();
      Version* orig = mvm_manager->CreateVersion();
      orig->DecodeFrom(encoded_version);
      txn_store_impl_->RecoverMultiVersionsManagerFrom(*orig);
      // RecoverMultiVersionsManagerFrom() use started_version_seq_ + 1 as
      // recovered started seq
      started_version_seq_ += 1;
      delete orig;
    }
  }
  ~CommonTxnTests() {
    delete txn_store_;
  }

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
  void PrepareEmptyWriteBatch();
  void CommitEmptyWriteBatch();
  void RollbackEmptyWriteBatch();
  void ReadUnderSnapshot();
  void ReuseTransaction();
  void SingleTxnExcutionFlowTest();
  void MultiThreadsTxnsExcution();

 private:
  uint64_t started_version_seq_;
  TxnStoreWritePolicy write_policy_;
  bool enable_two_write_queues_;
  TransactionStore* txn_store_;
  SkipListBackedInMemoryTxnStore* txn_store_impl_;
};

void CommonTxnTests::SimpleTransactionalReadWrite() {
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

void CommonTxnTests::SimpleNonTransactionalReadWrite() {
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

void CommonTxnTests::ReadTxnOwnWrites() {
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

  delete txn;
}

void CommonTxnTests::ReadAfterPrepare() {
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

void CommonTxnTests::ReadAfterCommit() {
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

void CommonTxnTests::ReadAfterRollback() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());

  // rollback without prepare
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // read after rollback
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // can reuse txn after rollback without prepare
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());

  // rollback with prepare
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // read after rollback
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

void CommonTxnTests::CommitWithPrepare() {
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

void CommonTxnTests::CommitWithoutPrepare() {
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

void CommonTxnTests::RollbackWithPrepare() {
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

void CommonTxnTests::RollbackWithoutPrepare() {
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

void CommonTxnTests::PrepareEmptyWriteBatch() {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // prepare an empty write batch
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn_store_->TEST_Crash();
  delete txn;
}

void CommonTxnTests::CommitEmptyWriteBatch() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // commit(with prepare) an empty write batch
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

  // commit(without prepare) an empty write batch
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

void CommonTxnTests::RollbackEmptyWriteBatch() {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(write_options);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // rollback(without prepare) an empty write batch
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn = txn_store_->BeginTransaction(write_options, txn_options, txn);
  // nothing in txn's own write and store
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // rollback(with prepare) an empty write batch
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Rollback();
  ASSERT_TRUE(s.IsOK());

  // nothing in txn's own write and store still
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

void CommonTxnTests::ReadUnderSnapshot() {
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

void CommonTxnTests::ReuseTransaction() {
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

void CommonTxnTests::SingleTxnExcutionFlowTest() {
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

void CommonTxnTests::MultiThreadsTxnsExcution() {

}

class InspectTxnTests {
 public:
  InspectTxnTests(TxnStoreWritePolicy write_policy,
                  bool enable_two_write_queues,
                  bool with_prepare,
                  const std::string& encoded_version = std::string(""))
      : write_policy_(write_policy),
        enable_two_write_queues_(enable_two_write_queues),
        with_prepare_(with_prepare) {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    store_options.enable_two_write_queues = enable_two_write_queues_;
    if (write_policy_ == WRITE_COMMITTED) {
      txn_store_impl_ = new WriteCommittedTxnStore(store_options,
                                                   txn_store_options,
                                                   txn_lock_mgr_factory);
    } else if (write_policy_ == WRITE_PREPARED) {
      CommitTableOptions commit_table_options;
      txn_store_impl_ = new WritePreparedTxnStore(store_options,
                                                  txn_store_options,
                                                  commit_table_options,
                                                  txn_lock_mgr_factory);
    } else {
      assert(false);
    }
    assert(txn_store_impl_);
    txn_store_ = txn_store_impl_;
    mvm_impl_ = reinterpret_cast<SeqBasedMultiVersionsManager*>(
        txn_store_impl_->GetMultiVersionsManager());
    assert(mvm_impl_);

    if (encoded_version == "") {
      started_version_seq_ = 0;
    } else {
      started_version_seq_ = std::stoull(encoded_version);
      Version* orig = mvm_impl_->CreateVersion();
      orig->DecodeFrom(encoded_version);
      txn_store_impl_->RecoverMultiVersionsManagerFrom(*orig);
      // RecoverMultiVersionsManagerFrom() use started_version_seq_ + 1 as
      // recovered started seq
      started_version_seq_ += 1;
      delete orig;
    }
  }
  ~InspectTxnTests() {
    delete txn_store_;
  }

  void VersionIncrement();
  void VersionIncrementForPreparingOfEmptyWriteBatch();
  void VersionIncrementForCommittingOfEmptyWriteBatch();
  void VersionIncrementForRollbackingOfEmptyWriteBatch();
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

  uint64_t started_version_seq_;
  TxnStoreWritePolicy write_policy_;
  bool enable_two_write_queues_;
  bool with_prepare_;
  TransactionStore* txn_store_;
  SkipListBackedInMemoryTxnStore* txn_store_impl_;
  SeqBasedMultiVersionsManager* mvm_impl_;
};

void InspectTxnTests::VersionIncrement() {
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
  if (write_policy_ == WRITE_PREPARED) {
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

void InspectTxnTests::VersionIncrementForPreparingOfEmptyWriteBatch() {
  std::vector<std::vector<SeqIncInfos>> expected_of_write_prepared =
  //  before-txn  after-prepare
      {{{0, 0, 0}, {1, 0, 1}},   // prepare() and 2-WQ
       {{0, 0, 0}, {1, 1, 1}}};  // prepare() and not 2-WQ

  std::vector<std::vector<SeqIncInfos>> expected_of_write_committed =
      {{{0, 0, 0}, {0, 0, 0}},   // prepare() and 2-WQ
       {{0, 0, 0}, {0, 0, 0}}};  // prepare() and not 2-WQ
  std::vector<SeqIncInfos>* expected;
  if (write_policy_ == WRITE_PREPARED) {
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

  // prepare an empty write batch
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  // After prepare
  CheckSeqInfos((*expected)[1]);

  txn_store_->TEST_Crash();
  delete txn;
}

void InspectTxnTests::VersionIncrementForCommittingOfEmptyWriteBatch() {
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
  if (write_policy_ == WRITE_PREPARED) {
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

  // commit an empty write batch
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

void InspectTxnTests::VersionIncrementForRollbackingOfEmptyWriteBatch() {
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
  if (write_policy_ == WRITE_PREPARED) {
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

  // rollback an empty write batch
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

void InspectTxnTests::WriteBufferInsertTimingBetweenDifferentWritePolicy() {
  std::string key("f", 100);
  std::string value("b", 100);
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
  if (write_policy_ == WRITE_PREPARED) {
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

}   // namespace MULTI_VERSIONS_NAMESPACE
