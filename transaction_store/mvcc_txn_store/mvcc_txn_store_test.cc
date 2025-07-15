#include "mvcc_txn_store_test.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCTxnStoreTest : public testing::Test {
 public:
  MVCCTxnStoreTest() {}
  ~MVCCTxnStoreTest() {}

  void TestMVCCTxnStoreFunc(void (MVCCTxnStoreTests::*func)()) {
    TxnTestSetupsGenerator generator({TxnStoreWritePolicy::kWriteCommitted,
                                      TxnStoreWritePolicy::kWritePrepared},
                                     {false, true}, {true}, {"", "1314"});
    TxnTestsSetups setups;                                      
    while (generator.NextTxnTestSetups(&setups)) {           
      MVCCTxnStoreTests* test = new MVCCTxnStoreTests(setups);        
      (test->*func)();              
      delete test;
    }
  }
};

TEST_F(MVCCTxnStoreTest, SimpleReadWrite) {
  TestMVCCTxnStoreFunc(&MVCCTxnStoreTests::SimpleReadWrite);
}

TEST_F(MVCCTxnStoreTest, MultiKeysInterleavedManipulation) {
  TestMVCCTxnStoreFunc(&MVCCTxnStoreTests::MultiKeysInterleavedManipulation);
}

TEST_F(MVCCTxnStoreTest, MultiThreadsQuery) {
  TestMVCCTxnStoreFunc(&MVCCTxnStoreTests::MultiThreadsQuery);
}

TEST_F(MVCCTxnStoreTest, DISABLED_DumpStoreKVPairs) {
  TestMVCCTxnStoreFunc(&MVCCTxnStoreTests::DumpStoreKVPairs);
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
