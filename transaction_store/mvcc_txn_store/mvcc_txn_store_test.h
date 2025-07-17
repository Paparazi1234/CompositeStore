#pragma once

#include "pessimistic_txn_store/write_committed_txn_store.h"
#include "pessimistic_txn_store/write_prepared_txn_store.h"
#include "txn_lock_manager/empty_txn_lock_manager.h"
#include "test_util/test_util.h"
#include "third-party/gtest/gtest.h"

namespace COMPOSITE_STORE_NAMESPACE {

class MVCCTxnStoreTests {
 public:
  MVCCTxnStoreTests(const TxnTestsSetups& setups)
      : write_policy_(setups.write_policy),
        enable_two_write_queues_(setups.enable_two_write_queues) {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    SkipListBackedMVCCWriteBufferFactory mvcc_write_buffer_factory;
    store_options.enable_two_write_queues = enable_two_write_queues_;
    if (write_policy_ == TxnStoreWritePolicy::kWriteCommitted) {
      store_ptr_ =
          new WriteCommittedTxnStore(store_options,
                                     txn_store_options,
                                     WriteCommittedMultiVersionsManagerFactory(
                                        store_options.enable_two_write_queues),
                                     txn_lock_mgr_factory,
                                     new WriteCommittedTransactionFactory(),
                                     new OrderedMapBackedStagingWriteFactory(),
                                     mvcc_write_buffer_factory);
    } else if (write_policy_ == TxnStoreWritePolicy::kWritePrepared) {
      CommitTableOptions commit_table_options;
      store_ptr_ =
          new WritePreparedTxnStore(store_options,
                                    txn_store_options,
                                    WritePreparedMultiVersionsManagerFactory(
                                        commit_table_options,
                                        store_options.enable_two_write_queues),
                                    txn_lock_mgr_factory,
                                    new WritePreparedTransactionFactory(),
                                    new OrderedMapBackedStagingWriteFactory(),
                                    mvcc_write_buffer_factory);
    } else {
      assert(false);
    }
    assert(store_ptr_);

    if (setups.encoded_version != "") {
      MultiVersionsManager* mvm = store_ptr_->GetMultiVersionsManager();
      Version* orig = mvm->CreateVersion();
      orig->DecodeFrom(setups.encoded_version);
      store_ptr_->RecoverMultiVersionsManagerFrom(*orig);
      delete orig;
    }
  }
  ~MVCCTxnStoreTests() {
    delete store_ptr_;
  }

  void SimpleReadWrite();
  void MultiKeysInterleavedManipulation();
  void MultiThreadsQuery();
  void DumpStoreKVPairs();

 protected:
  TxnStoreWritePolicy write_policy_;
  bool enable_two_write_queues_;
  MVCCTxnStore* store_ptr_;
};

void MVCCTxnStoreTests::SimpleReadWrite() {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  Status s;

  // read non-existence
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // read existence
  s = store_ptr_->Put(write_options, "foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // read after overwritten
  s = store_ptr_->Put(write_options, "foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  // read after deletion
  s = store_ptr_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // delete non-existence
  s = store_ptr_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
}

void MVCCTxnStoreTests::MultiKeysInterleavedManipulation() {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  Status s;
  // multi keys interleaved manipulation
  s = store_ptr_->Put(write_options, "foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo1", "bar3");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo", "bar4");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo", "bar5");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Delete(write_options, "foo1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Delete(write_options, "foo1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo1", "bar6");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo1", "bar6");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar5");
  s = store_ptr_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar6");
}

void MVCCTxnStoreTests::MultiThreadsQuery() {

}

void MVCCTxnStoreTests::DumpStoreKVPairs() {
  WriteOptions write_options;
  store_ptr_->Put(write_options, "foo", "bar");
  store_ptr_->Put(write_options, "foo1", "bar");
  store_ptr_->Put(write_options, "foo2", "bar");
  store_ptr_->Delete(write_options, "foo");
  store_ptr_->Delete(write_options, "foo1");
  store_ptr_->Delete(write_options, "foo2");
  store_ptr_->Put(write_options, "foo", "bar1");
  store_ptr_->Put(write_options, "foo1", "bar1");
  store_ptr_->Put(write_options, "foo2", "bar1");

  std::string expected =
  "KV pairs in store:\n"
  "  {key: foo@7,\ttype: Put,\tvalue: bar1}\n"
  "  {key: foo@4,\ttype: Delete}\n"
  "  {key: foo@1,\ttype: Put,\tvalue: bar}\n"
  "  {key: foo1@8,\ttype: Put,\tvalue: bar1}\n"
  "  {key: foo1@5,\ttype: Delete}\n"
  "  {key: foo1@2,\ttype: Put,\tvalue: bar}\n"
  "  {key: foo2@9,\ttype: Put,\tvalue: bar1}\n"
  "  {key: foo2@6,\ttype: Delete}\n"
  "  {key: foo2@3,\ttype: Put,\tvalue: bar}\n"
  "  Total count in store: 9, dump count: 9\n";

  std::stringstream oss;
  oss.str("");
  store_ptr_->DumpKVPairs(&oss);
  ASSERT_STREQ(oss.str().c_str(), expected.c_str());
}

}   // namespace COMPOSITE_STORE_NAMESPACE
