#include "skiplist_backed_mvcc_wb.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedMVCCWBTest : public testing::Test {
 public:
  SkipListBackedMVCCWBTest() {}
  ~SkipListBackedMVCCWBTest() {}
};

TEST_F(SkipListBackedMVCCWBTest, BasicRead) {

}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
