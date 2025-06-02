#include "MVCC_based_txn.h"
#include "include/txn_lock_manager.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCTxnTest : public testing::Test {
 public:
  MVCCTxnTest() {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    txn_store_ = new WriteCommittedTxnStore(store_options,
                                            txn_store_options,
                                            txn_lock_mgr_factory);
  }

  ~MVCCTxnTest() {
    delete txn_store_;
  }
 protected:
  TransactionStore* txn_store_;
};

TEST_F(MVCCTxnTest, SimpleTxnReadWrite) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  // begin transaction
  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);

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

TEST_F(MVCCTxnTest, NonTransactionalReadWrite) {
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

TEST_F(MVCCTxnTest, ReadTxnOwnWrites) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);
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

TEST_F(MVCCTxnTest, ReadAfterPrepare) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // read after prepare
  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());

  // transactional read can see the prepared writes
  s = txn->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");
  s = txn->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // non-transactional read can't see the prepared writes
  s = txn_store_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_store_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

TEST_F(MVCCTxnTest, ReadAfterCommit) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo1", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());

  // read after commit(2pc)
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

TEST_F(MVCCTxnTest, ReadUnderSnapshot) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;
  const Snapshot* snapshot1;
  const Snapshot* snapshot2;

  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();  // commit without prepare
  ASSERT_TRUE(s.IsOK());
  snapshot1 = txn_store_->TakeSnapshot();   // take a snapshot

  txn = txn_store_->BeginTransaction(txn_options, write_options, txn);
  s = txn->Put("foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();  // commit without prepare
  ASSERT_TRUE(s.IsOK());
  snapshot2 = txn_store_->TakeSnapshot();   // take another snapshot

  txn = txn_store_->BeginTransaction(txn_options, write_options, txn);
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

TEST_F(MVCCTxnTest, ReuseTransaction) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);
  s = txn->Put("foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = txn->Delete("foo1");
  ASSERT_TRUE(s.IsOK());

  s = txn->Prepare();
  ASSERT_TRUE(s.IsOK());
  s = txn->Commit();
  ASSERT_TRUE(s.IsOK());

  // reuse transaction
  txn = txn_store_->BeginTransaction(txn_options, write_options, txn);
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

TEST_F(MVCCTxnTest, SingleTxnExcutionFlowTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = txn_store_->BeginTransaction(txn_options, write_options);
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

  txn = txn_store_->BeginTransaction(txn_options, write_options, txn);
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

  txn = txn_store_->BeginTransaction(txn_options, write_options, txn);
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

TEST_F(MVCCTxnTest, MultiThreadsTxnsExcution) {

}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
