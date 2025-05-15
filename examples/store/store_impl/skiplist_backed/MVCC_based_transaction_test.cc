#include "seq_based_multi_versions.h"
#include "../../../third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class BasicMVCCTxnTest : public testing::Test {
 public:
  BasicMVCCTxnTest() {
    i = 0;
  }
  ~BasicMVCCTxnTest() override {}
 private:
  uint64_t i;
};

TEST_F(BasicMVCCTxnTest, basic0) {
  ASSERT_EQ(0, 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}   // namespace MULTI_VERSIONS_NAMESPACE