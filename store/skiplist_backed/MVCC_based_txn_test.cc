#include "MVCC_based_txn_test.h"

#include <iostream>

#include "test_util/test_util.h"

namespace MULTI_VERSIONS_NAMESPACE {

class CommonMVCCTxnTest : public testing::Test {
 public:
  CommonMVCCTxnTest() {}
  ~CommonMVCCTxnTest() {}
};

TEST_F(CommonMVCCTxnTest, SimpleTransactionalReadWrite) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->SimpleTransactionalReadWrite();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, SimpleNonTransactionalReadWrite) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->SimpleNonTransactionalReadWrite();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, ReadTxnOwnWrites) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->ReadTxnOwnWrites();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, ReadAfterPrepare) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->ReadAfterPrepare();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, ReadAfterCommit) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->ReadAfterCommit();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, RollbackDuringWriteStage) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->RollbackDuringWriteStage();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, RollbackAfterPrepare) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->RollbackAfterPrepare();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, ReadUnderSnapshot) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->ReadUnderSnapshot();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, ReuseTransaction) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->ReuseTransaction();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, DISABLED_SingleTxnExcutionFlowTest) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->SingleTxnExcutionFlowTest();
    delete test;
  }
}

TEST_F(CommonMVCCTxnTest, MultiThreadsTxnsExcution) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonTxnTests* test = new CommonTxnTests(write_policy,
                                              enable_two_write_queues);
    test->MultiThreadsTxnsExcution();
    delete test;
  }
}

class InspectMVCCTxnTest : public testing::Test {
 public:
  InspectMVCCTxnTest() {}
  ~InspectMVCCTxnTest() {}
};

TEST_F(InspectMVCCTxnTest, TestSeqInc) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    InspectTxnTest* test = new InspectTxnTest(write_policy,
                                              enable_two_write_queues, true);
    test->TestSeqInc();
    delete test;
  }

  generator.Reset();
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    InspectTxnTest* test = new InspectTxnTest(write_policy,
                                              enable_two_write_queues, false);
    test->TestSeqInc();
    delete test;
  }
}

TEST_F(InspectMVCCTxnTest, TestMemoryIncAfterPrepare) {
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    InspectTxnTest* test = new InspectTxnTest(write_policy,
                                              enable_two_write_queues, false);
    test->TestMemoryIncAfterPrepare();
    delete test;
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
