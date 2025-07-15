#include <iostream>
#include <assert.h>

#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"
#include "multi_version/sequence_based/seq_based_multi_versions.h"

#define AssertTrue(cond)                                       \
  if (!(cond)) {                                               \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #cond); \
    abort();                                                   \
  }
namespace MULTI_VERSIONS_NAMESPACE {

class MyTest {
 public:
  MyTest(TxnStoreWritePolicy wp,
         bool commit_with_prepare,
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
    // s = txn0->Put("abc", "def");
    // AssertTrue(s.IsOK());
    // s = txn0->Put("abc1", "def");
    // AssertTrue(s.IsOK());
    // s = txn0->Put("abc2", "def");
    // AssertTrue(s.IsOK());
    // s = txn0->Put("abc3", "def");
    // AssertTrue(s.IsOK());
    // EchoSeq("After txn write:");

    if (commit_with_prepare_) {
      // Prepare transaction
      s = txn0->Prepare();
      AssertTrue(s.IsOK());
      EchoSeq("After txn prepare:");
    }

    // Commit transaction
    s = txn0->Rollback();
    AssertTrue(s.IsOK());
    EchoSeq("After txn rollback:");
    delete txn0;
  }

  void OpenStore() {
    StoreOptions store_options;
    TransactionStoreOptions txn_store_options;
    StoreTraits store_traits;
    Status s;

    store_options.enable_two_write_queues = enable_two_write_queues_;
    store_traits.txn_write_policy = write_policy_;
    s = TransactionStore::Open(
        store_options, txn_store_options, store_traits, &txn_store_);
    AssertTrue(s.IsOK());
    txn_store_impl_ = static_cast_with_check<MVCCTxnStore>(txn_store_);
    mvm_impl_ = static_cast_with_check<SeqBasedMultiVersionsManager>(
                    txn_store_impl_->GetMultiVersionsManager());
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
    std::cout<<"  {max_readable_seq_: "<<mvm_impl_->MaxReadableVersion()
        <<", max_visible_seq_: "<<mvm_impl_->MaxVisibleVersion()
        <<", so_far_allocated_: "<<mvm_impl_->seq_allocator_.SoFarAllocated()
        <<"}"<<std::endl;
  }

  TransactionStore* txn_store_;
  MVCCTxnStore* txn_store_impl_;
  SeqBasedMultiVersionsManager* mvm_impl_;
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

