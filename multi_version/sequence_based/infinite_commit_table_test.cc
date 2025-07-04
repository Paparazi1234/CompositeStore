#include "infinite_commit_table.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class InfiniteCommitTableTest : public testing::Test {
 public:
  InfiniteCommitTableTest() {}
  ~InfiniteCommitTableTest() {}
};

TEST_F(InfiniteCommitTableTest, BasicTest) {

}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
