#include "MVCC_based_txn_test.h"

#include <iostream>

namespace MULTI_VERSIONS_NAMESPACE {

class CommonMVCCTxnTest : public testing::Test {
 public:
  CommonMVCCTxnTest() {}
  ~CommonMVCCTxnTest() {}

  void TestCommonFunc(void (CommonTxnTests::*func)()) {
    TxnTestSetupsGenerator generator({WRITE_COMMITTED, WRITE_PREPARED},
                                     {true, false}, {true}, {"", "1314"});
    TxnTestsSetups setups;                                      
    while (generator.NextTxnTestSetups(&setups)) {           
      CommonTxnTests* test = new CommonTxnTests(setups);        
      (test->*func)();              
      delete test;
    }
  }
};

TEST_F(CommonMVCCTxnTest, SimpleTransactionalReadWrite) {
  TestCommonFunc(&CommonTxnTests::SimpleTransactionalReadWrite);
}

TEST_F(CommonMVCCTxnTest, SimpleNonTransactionalReadWrite) {
  TestCommonFunc(&CommonTxnTests::SimpleNonTransactionalReadWrite);
}

TEST_F(CommonMVCCTxnTest, ReadTxnOwnWrites) {
  TestCommonFunc(&CommonTxnTests::ReadTxnOwnWrites);
}

TEST_F(CommonMVCCTxnTest, ReadAfterPrepare) {
  TestCommonFunc(&CommonTxnTests::ReadAfterPrepare);
}

TEST_F(CommonMVCCTxnTest, ReadAfterCommit) {
  TestCommonFunc(&CommonTxnTests::ReadAfterCommit);
}

TEST_F(CommonMVCCTxnTest, ReadAfterRollback) {
  TestCommonFunc(&CommonTxnTests::ReadAfterRollback);
}

TEST_F(CommonMVCCTxnTest, CommitWithPrepare) {
  TestCommonFunc(&CommonTxnTests::CommitWithPrepare);
}

TEST_F(CommonMVCCTxnTest, CommitWithoutPrepare) {
  TestCommonFunc(&CommonTxnTests::CommitWithoutPrepare);
}

TEST_F(CommonMVCCTxnTest, RollbackWithPrepare) {
  TestCommonFunc(&CommonTxnTests::RollbackWithPrepare);
}

TEST_F(CommonMVCCTxnTest, RollbackWithoutPrepare) {
  TestCommonFunc(&CommonTxnTests::RollbackWithoutPrepare);
}

TEST_F(CommonMVCCTxnTest, PrepareEmptyWriteBatch) {
  TestCommonFunc(&CommonTxnTests::PrepareEmptyWriteBatch);
}

TEST_F(CommonMVCCTxnTest, CommitEmptyWriteBatch) {
  TestCommonFunc(&CommonTxnTests::CommitEmptyWriteBatch);
}

TEST_F(CommonMVCCTxnTest, RollbackEmptyWriteBatch) {
  TestCommonFunc(&CommonTxnTests::RollbackEmptyWriteBatch);
}

TEST_F(CommonMVCCTxnTest, ReadUnderSnapshot) {
  TestCommonFunc(&CommonTxnTests::ReadUnderSnapshot);
}

TEST_F(CommonMVCCTxnTest, ReuseTransaction) {
  TestCommonFunc(&CommonTxnTests::ReuseTransaction);
}

TEST_F(CommonMVCCTxnTest, SingleTxnExcutionFlowTest) {
 TestCommonFunc(&CommonTxnTests::SingleTxnExcutionFlowTest);
}

TEST_F(CommonMVCCTxnTest, MultiThreadsTxnsExcution) {
  TestCommonFunc(&CommonTxnTests::MultiThreadsTxnsExcution);
}

class MultiThreadedMVCCTxnTests : public testing::Test {
 public:
  MultiThreadedMVCCTxnTests() {}
  ~MultiThreadedMVCCTxnTests() {}
};

TEST_F(MultiThreadedMVCCTxnTests, SingleWriterMultiReaders) {
  
}

class InspectMVCCTxnTest : public testing::Test {
 public:
  InspectMVCCTxnTest() {}
  ~InspectMVCCTxnTest() {}

  void TestInspectFunc(void (InspectTxnTests::*func)()) {
    TxnTestSetupsGenerator generator({WRITE_COMMITTED, WRITE_PREPARED},
                                   {true, false}, {true, false}, {"", "1314"});
    TxnTestsSetups setups;                                      
    while (generator.NextTxnTestSetups(&setups)) {           
      InspectTxnTests* test = new InspectTxnTests(setups);        
      (test->*func)();              
      delete test;
    }
  }
};

TEST_F(InspectMVCCTxnTest, VersionIncrement) {
  TestInspectFunc(&InspectTxnTests::VersionIncrement);
}

TEST_F(InspectMVCCTxnTest, VersionIncrementForPreparingOfEmptyWriteBatch) {
  TxnTestSetupsGenerator generator({WRITE_COMMITTED, WRITE_PREPARED},
                                   {true, false}, {true}, {"", "1314"});
  TxnTestsSetups setups;  
  while (generator.NextTxnTestSetups(&setups)) {
    InspectTxnTests* test = new InspectTxnTests(setups);
    test->VersionIncrementForPreparingOfEmptyWriteBatch();
    delete test;
  }
}

TEST_F(InspectMVCCTxnTest, VersionIncrementForCommittingOfEmptyWriteBatch) {
  TestInspectFunc(
      &InspectTxnTests::VersionIncrementForCommittingOfEmptyWriteBatch);
}

TEST_F(InspectMVCCTxnTest, VersionIncrementForRollbackingOfEmptyWriteBatch) {
  TestInspectFunc(
      &InspectTxnTests::VersionIncrementForRollbackingOfEmptyWriteBatch);
}

TEST_F(InspectMVCCTxnTest, WriteBufferInsertTimingBetweenDifferentWritePolicy) {
  TxnTestSetupsGenerator generator({WRITE_COMMITTED, WRITE_PREPARED},
                                   {true, false}, {true}, {"", "1314"});
  TxnTestsSetups setups;  
  while (generator.NextTxnTestSetups(&setups)) {
    InspectTxnTests* test = new InspectTxnTests(setups);
    test->WriteBufferInsertTimingBetweenDifferentWritePolicy();
    delete test;
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
