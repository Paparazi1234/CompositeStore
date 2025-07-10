#include "skiplist_rep.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListRepTest : public testing::Test {
 public:
  SkipListRepTest() {}
  ~SkipListRepTest() {}
};

TEST_F(SkipListRepTest, BasicRead) {

}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
