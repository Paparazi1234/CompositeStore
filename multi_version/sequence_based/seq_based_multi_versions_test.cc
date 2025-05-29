#include "seq_based_multi_versions.h"
#include "write_prepared_seq_based_multi_versions.h"
#include "seq_based_snapshot.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedMultiVersionsTest : public testing::Test {
 public:
  SeqBasedMultiVersionsTest() {}
  ~SeqBasedMultiVersionsTest() {}
};

TEST_F(SeqBasedMultiVersionsTest, WCMultiVersionsManager) {
  // brand new version manager
  WriteCommittedSeqBasedMultiVersionsManager wcsbmvm;

  Version* version = wcsbmvm.CreateVersion();
  


}

TEST_F(SeqBasedMultiVersionsTest, WPMultiVersionsManager) {
  WritePreparedSeqBasedMultiVersionsManager wpsbmvm;

  


}

TEST_F(SeqBasedMultiVersionsTest, WCSnapshotManager) {
  WriteCommittedSeqBasedMultiVersionsManager wcsbmvm;
  WriteCommittedSeqBasedSnapshotManager wcsbsm(&wcsbmvm);

  


}

TEST_F(SeqBasedMultiVersionsTest, WPSnapshotManager) {
  WritePreparedSeqBasedMultiVersionsManager wpsbmvm;
  WritePreparedSeqBasedSnapshotManager wpsbsm(&wpsbmvm);
  
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
