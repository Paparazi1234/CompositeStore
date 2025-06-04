#include "include/transaction_store.h"

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <assert.h>

#define AssertTrue(cond)                                       \
  if (!(cond)) {                                               \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #cond); \
    abort();                                                   \
  }
namespace MULTI_VERSIONS_NAMESPACE {

class MyTest {
 public:
  MyTest(TxnStoreWritePolicy wp, bool commit_with_prepare,
      bool enable_two_write_queues)
      : write_policy_(wp), 
        commit_with_prepare_(commit_with_prepare),
        enable_two_write_queues_(enable_two_write_queues) {
    OpenStore();
  }
  
  ~MyTest() {
    delete txn_store_;
  }

  void Run() {
    EchoTestSetups();
    TestImpl();
    TestImpl();
  }

 private:

  void TestImpl() {
    WriteOptions write_options;
    ReadOptions read_options;
    std::string value;
    Status s;

    EchoSeq("Before txn:\t");
    // Start a transaction
    Transaction* txn0 = txn_store_->BeginTransaction(write_options);
    AssertTrue(txn0);

    // Write keys in this transaction
    s = txn0->Put("abc", "def");
    AssertTrue(s.IsOK());
    s = txn0->Put("abc1", "def");
    AssertTrue(s.IsOK());
    s = txn0->Put("abc2", "def");
    AssertTrue(s.IsOK());
    s = txn0->Put("abc3", "def");
    AssertTrue(s.IsOK());
    EchoSeq("After txn write:");

    if (commit_with_prepare_) {
      // Prepare transaction
      s = txn0->Prepare();
      AssertTrue(s.IsOK());
      EchoSeq("After txn prepare:");
    }

    // Commit transaction
    s = txn0->Commit();
    AssertTrue(s.IsOK());
    EchoSeq("After txn commit:");
    delete txn0;
  }

  void OpenStore() {
    StoreOptions store_options;
    StoreTraits store_traits;
    TransactionStoreOptions txn_store_options;
    Status s;

    store_options.enable_two_write_queue = enable_two_write_queues_;
    store_traits.txn_write_policy = write_policy_;
    s = TransactionStore::Open(
        store_options, txn_store_options, store_traits, &txn_store_);
    AssertTrue(s.IsOK());
  }

  void EchoTestSetups() const {
    const char* write_policy =
        write_policy_ == WRITE_COMMITTED ? "WRITE_COMMITTED" : "WRITE_PREPARED";
    const char* commit_with_prepare = commit_with_prepare_ ? "TRUE" : "FALSE";
    const char* enable_two_write_queues =
        enable_two_write_queues_ ? "TRUE" : "FALSE";
    std::cout<<"Test setups: write_policy("<<write_policy
             <<"), commit_with_prepare("<<commit_with_prepare
             <<"), enable_two_write_queues("<<enable_two_write_queues
             <<")"<<std::endl;
  }

  void EchoSeq(const char* tips) const {
    std::cout<<tips<<"\t";
    std::cout<<"  {last_sequence_: "<<versions_->LastSequence()
        <<", last_published_sequence_: "<<versions_->LastPublishedSequence()
        <<", last_allocated_sequence_: "<<versions_->LastAllocatedSequence()
        <<"}"<<std::endl;
  }

  TransactionStore* txn_store_;
  TxnStoreWritePolicy write_policy_;
  bool commit_with_prepare_;
  bool enable_two_write_queues_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE

using MULTI_VERSIONS_NAMESPACE::MyTest;

int main() {
  MyTest my_test0(MULTI_VERSIONS_NAMESPACE::WRITE_PREPARED, true, true);
  MyTest my_test1(MULTI_VERSIONS_NAMESPACE::WRITE_PREPARED, true, false);
  MyTest my_test2(MULTI_VERSIONS_NAMESPACE::WRITE_PREPARED, false, true);
  MyTest my_test3(MULTI_VERSIONS_NAMESPACE::WRITE_PREPARED, false, false);
  my_test0.Run();
  my_test1.Run();
  my_test2.Run();
  my_test3.Run();

  std::cout<<std::endl;
  MyTest my_test4(MULTI_VERSIONS_NAMESPACE::WRITE_COMMITTED, true, true);
  MyTest my_test5(MULTI_VERSIONS_NAMESPACE::WRITE_COMMITTED, true, false);
  MyTest my_test6(MULTI_VERSIONS_NAMESPACE::WRITE_COMMITTED, false, true);
  MyTest my_test7(MULTI_VERSIONS_NAMESPACE::WRITE_COMMITTED, false, false);

  my_test4.Run();
  my_test5.Run();
  my_test6.Run();
  my_test7.Run();

  return 0;
}

