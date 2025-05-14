#include "seq_based_multi_versions.h"
#include "../third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class BasicTest : public testing::Test {
 public:
  BasicTest() {
    i = 0;
  }
  ~BasicTest() override {}
 private:
  uint64_t i;
};

TEST_F(BasicTest, basic0) {
  ASSERT_EQ(0, 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
