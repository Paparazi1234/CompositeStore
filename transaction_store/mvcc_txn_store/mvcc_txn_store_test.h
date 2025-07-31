#pragma once

#include "mvcc_txn_store.h"
#include "utils/cast_util.h"
#include "test_utils/txn_test_utils.h"
#include "third-party/gtest/gtest.h"

namespace COMPOSITE_STORE_NAMESPACE {

class MVCCTxnStoreTests {
 public:
  MVCCTxnStoreTests(const TxnTestsSetups& setups)
      : write_policy_(setups.write_policy),
        enable_two_write_queues_(setups.enable_two_write_queues) {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    StoreTraits store_traits;
    store_options.enable_two_write_queues = enable_two_write_queues_;
    store_traits.txn_store_impl_type = TxnStoreImplType::kMVCC;
    store_traits.txn_store_write_policy = write_policy_;
    Status s = TransactionStore::Open(store_options, txn_store_options,
                                      store_traits, &store_ptr_);
    assert(s.IsOK() && store_ptr_ != nullptr);
    txn_store_impl_ = static_cast_with_check<MVCCTxnStore>(store_ptr_);
    if (setups.encoded_version != "") {
      MultiVersionsManager* mvm = txn_store_impl_->GetMultiVersionsManager();
      Version* orig = mvm->CreateVersion();
      orig->DecodeFrom(setups.encoded_version);
      txn_store_impl_->RecoverMultiVersionsManagerFrom(*orig);
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
  TransactionStore* store_ptr_;
  MVCCTxnStore* txn_store_impl_;
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
  txn_store_impl_->DumpKVPairs(&oss);
  ASSERT_STREQ(oss.str().c_str(), expected.c_str());
}

}   // namespace COMPOSITE_STORE_NAMESPACE
