#include "pessimistic_txn_store_test.h"

#include <iostream>

namespace COMPOSITE_STORE_NAMESPACE {

class CommonPessimisticTxnTest : public testing::Test {
 public:
  CommonPessimisticTxnTest() {}
  ~CommonPessimisticTxnTest() {}

  void TestCommonFunc(void (CommonPessimisticTxnTests::*func)()) {
    TxnTestSetupsGenerator generator({TxnStoreWritePolicy::kWriteCommitted,
                                      TxnStoreWritePolicy::kWritePrepared},
                                     {false, true}, {true}, {"", "1314"});
    TxnTestsSetups setups;                                      
    while (generator.NextTxnTestSetups(&setups)) {           
      CommonPessimisticTxnTests* test = new CommonPessimisticTxnTests(setups);        
      (test->*func)();              
      delete test;
    }
  }
};

TEST_F(CommonPessimisticTxnTest, SimpleTransactionalReadWrite) {
  TestCommonFunc(&CommonPessimisticTxnTests::SimpleTransactionalReadWrite);
}

TEST_F(CommonPessimisticTxnTest, SimpleNonTransactionalReadWrite) {
  TestCommonFunc(&CommonPessimisticTxnTests::SimpleNonTransactionalReadWrite);
}

TEST_F(CommonPessimisticTxnTest, ReadTxnOwnWrites) {
  TestCommonFunc(&CommonPessimisticTxnTests::ReadTxnOwnWrites);
}

TEST_F(CommonPessimisticTxnTest, ReadAfterPrepare) {
  TestCommonFunc(&CommonPessimisticTxnTests::ReadAfterPrepare);
}

TEST_F(CommonPessimisticTxnTest, ReadAfterCommit) {
  TestCommonFunc(&CommonPessimisticTxnTests::ReadAfterCommit);
}

TEST_F(CommonPessimisticTxnTest, ReadAfterRollback) {
  TestCommonFunc(&CommonPessimisticTxnTests::ReadAfterRollback);
}

TEST_F(CommonPessimisticTxnTest, CommitWithPrepare) {
  TestCommonFunc(&CommonPessimisticTxnTests::CommitWithPrepare);
}

TEST_F(CommonPessimisticTxnTest, CommitWithoutPrepare) {
  TestCommonFunc(&CommonPessimisticTxnTests::CommitWithoutPrepare);
}

TEST_F(CommonPessimisticTxnTest, RollbackWithPrepare) {
  TestCommonFunc(&CommonPessimisticTxnTests::RollbackWithPrepare);
}

TEST_F(CommonPessimisticTxnTest, RollbackWithoutPrepare) {
  TestCommonFunc(&CommonPessimisticTxnTests::RollbackWithoutPrepare);
}

TEST_F(CommonPessimisticTxnTest, PrepareEmptyStagingWrite) {
  TestCommonFunc(&CommonPessimisticTxnTests::PrepareEmptyStagingWrite);
}

TEST_F(CommonPessimisticTxnTest, CommitEmptyStagingWrite) {
  TestCommonFunc(&CommonPessimisticTxnTests::CommitEmptyStagingWrite);
}

TEST_F(CommonPessimisticTxnTest, RollbackEmptyStagingWrite) {
  TestCommonFunc(&CommonPessimisticTxnTests::RollbackEmptyStagingWrite);
}

TEST_F(CommonPessimisticTxnTest, InterleavingPrepareCommitBetweenMultiTxns) {
  TestCommonFunc(
      &CommonPessimisticTxnTests::InterleavingPrepareCommitBetweenMultiTxns);
}

TEST_F(CommonPessimisticTxnTest, InterleavingPrepareRollbackBetweenMultiTxns) {
  TestCommonFunc(
      &CommonPessimisticTxnTests::InterleavingPrepareRollbackBetweenMultiTxns);
}

TEST_F(CommonPessimisticTxnTest, ReadUnderSnapshot) {
  TestCommonFunc(&CommonPessimisticTxnTests::ReadUnderSnapshot);
}

TEST_F(CommonPessimisticTxnTest, ReuseTransaction) {
  TestCommonFunc(&CommonPessimisticTxnTests::ReuseTransaction);
}

TEST_F(CommonPessimisticTxnTest, SingleTxnExcutionFlowTest) {
 TestCommonFunc(&CommonPessimisticTxnTests::SingleTxnExcutionFlowTest);
}

TEST_F(CommonPessimisticTxnTest, LockTimeOut) {
 TestCommonFunc(&CommonPessimisticTxnTests::LockTimeOut);
}

TEST_F(CommonPessimisticTxnTest, TxnExpired) {
 TestCommonFunc(&CommonPessimisticTxnTests::TxnExpired);
}

class MultiThreadingPessimisticTxnTest : public testing::Test {
 public:
  MultiThreadingPessimisticTxnTest() {}
  ~MultiThreadingPessimisticTxnTest() {}

  void TestMultiThreadingFunc(
      void (MultiThreadingPessimisticTxnTests::*func)()) {
    TxnTestSetupsGenerator generator({TxnStoreWritePolicy::kWriteCommitted,
                                      TxnStoreWritePolicy::kWritePrepared},
                                     {false, true}, {true}, {"", "1314"});
    TxnTestsSetups setups;                                      
    while (generator.NextTxnTestSetups(&setups)) {
      MultiThreadingPessimisticTxnTests* test =
          new MultiThreadingPessimisticTxnTests(setups);   
      (test->*func)();
      delete test;
    }
  }
};

TEST_F(MultiThreadingPessimisticTxnTest, MultiThreadsTxnsExcution) {
  TestMultiThreadingFunc(
      &MultiThreadingPessimisticTxnTests::MultiThreadsTxnsExcution);
}

TEST_F(MultiThreadingPessimisticTxnTest, SingleWriterMultiReaders) {
  TestMultiThreadingFunc(
      &MultiThreadingPessimisticTxnTests::SingleWriterMultiReaders);
}

TEST_F(MultiThreadingPessimisticTxnTest, SingleReaderMultiWriters) {
  TestMultiThreadingFunc(
      &MultiThreadingPessimisticTxnTests::SingleReaderMultiWriters);
}

TEST_F(MultiThreadingPessimisticTxnTest, MultiWritersMultiReaders) {
  TestMultiThreadingFunc(
      &MultiThreadingPessimisticTxnTests::MultiWritersMultiReaders);
}

class InspectPessimisticTxnTest : public testing::Test {
 public:
  InspectPessimisticTxnTest() {}
  ~InspectPessimisticTxnTest() {}

  void TestInspectFunc(void (InspectPessimisticTxnTests::*func)()) {
    TxnTestSetupsGenerator generator({TxnStoreWritePolicy::kWriteCommitted,
                                      TxnStoreWritePolicy::kWritePrepared},
                                     {false, true}, {false, true},
                                     {"", "1314"});
    TxnTestsSetups setups;                                      
    while (generator.NextTxnTestSetups(&setups)) {           
      InspectPessimisticTxnTests* test = new InspectPessimisticTxnTests(setups);        
      (test->*func)();              
      delete test;
    }
  }
};

TEST_F(InspectPessimisticTxnTest, VersionIncrement) {
  TestInspectFunc(&InspectPessimisticTxnTests::VersionIncrement);
}

TEST_F(InspectPessimisticTxnTest,
    VersionIncrementForPreparingOfEmptyStagingWrite) {
  TxnTestSetupsGenerator generator({TxnStoreWritePolicy::kWriteCommitted,
                                    TxnStoreWritePolicy::kWritePrepared},
                                   {false, true}, {true}, {"", "1314"});
  TxnTestsSetups setups;  
  while (generator.NextTxnTestSetups(&setups)) {
    InspectPessimisticTxnTests* test = new InspectPessimisticTxnTests(setups);
    test->VersionIncrementForPreparingOfEmptyStagingWrite();
    delete test;
  }
}

TEST_F(InspectPessimisticTxnTest,
    VersionIncrementForCommittingOfEmptyStagingWrite) {
  TestInspectFunc(&InspectPessimisticTxnTests::
      VersionIncrementForCommittingOfEmptyStagingWrite);
}

TEST_F(InspectPessimisticTxnTest,
    VersionIncrementForRollbackingOfEmptyStagingWrite) {
  TestInspectFunc(&InspectPessimisticTxnTests::
      VersionIncrementForRollbackingOfEmptyStagingWrite);
}

TEST_F(InspectPessimisticTxnTest,
    WriteBufferInsertTimingBetweenDifferentWritePolicy) {
  TxnTestSetupsGenerator generator({TxnStoreWritePolicy::kWriteCommitted,
                                    TxnStoreWritePolicy::kWritePrepared},
                                   {false, true}, {true}, {"", "1314"});
  TxnTestsSetups setups;  
  while (generator.NextTxnTestSetups(&setups)) {
    InspectPessimisticTxnTests* test = new InspectPessimisticTxnTests(setups);
    test->WriteBufferInsertTimingBetweenDifferentWritePolicy();
    delete test;
  }
}

}   // namespace COMPOSITE_STORE_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
