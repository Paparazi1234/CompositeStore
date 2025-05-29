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

TEST_F(SeqBasedMultiVersionsTest, SeqBasedVersionTest) {
  WriteCommittedSeqBasedMultiVersionsManager versions_manager;
  SeqBasedVersion* version1 =
      reinterpret_cast<SeqBasedVersion*>(versions_manager.CreateVersion());
  SeqBasedVersion* version2 =
      reinterpret_cast<SeqBasedVersion*>(versions_manager.CreateVersion());

  ASSERT_EQ(version1->Seq(), 0);
  ASSERT_EQ(version2->Seq(), 0);

  version1->IncreaseByOne();
  version2->IncreaseByOne();
  ASSERT_TRUE(version1->CompareWith(*version2) == 0);

  version2->IncreaseBy(2);
  //  descending ordered by version
  ASSERT_TRUE(version1->CompareWith(*version2) > 0);
  ASSERT_TRUE(version2->CompareWith(*version1) < 0);

  ASSERT_EQ(version1->Seq(), 1);
  ASSERT_EQ(version2->Seq(), 3);

  // encode to
  std::string encoded;
  version1->EncodeTo(&encoded);
  ASSERT_STREQ(encoded.c_str(), "1");

  // decode from
  encoded = "10";
  version1->DecodeFrom(encoded);
  ASSERT_EQ(version1->Seq(), 10);

  delete version1;
  delete version2;
}

namespace {
void TestWCMultiVersionsManager(
    WriteCommittedSeqBasedMultiVersionsManager& versions_manager) {
  Version* latest_visible = nullptr;
  Version* latest_visible1 = nullptr;
  Version* mini_uncommitted = nullptr;

  latest_visible = versions_manager.LatestVisibleVersion(nullptr);
  mini_uncommitted = versions_manager.MiniUncommittedVersion(nullptr);

  // latest visible is always < mini uncommitted
  ASSERT_TRUE(latest_visible->CompareWith(*mini_uncommitted) > 0);

  // mini uncommitted - latest visible >= 1
  latest_visible->IncreaseBy(1);
  ASSERT_TRUE(latest_visible->CompareWith(*mini_uncommitted) == 0);
  
  latest_visible->IncreaseBy(10);
  // can't see prepared version
  versions_manager.PrepareVersion(*latest_visible);
  latest_visible1 = versions_manager.LatestVisibleVersion(nullptr);
  ASSERT_TRUE(latest_visible->CompareWith(*latest_visible1) < 0);
  mini_uncommitted = versions_manager.MiniUncommittedVersion(mini_uncommitted);
  ASSERT_TRUE(latest_visible->CompareWith(*mini_uncommitted) < 0);

  // can see committed version
  versions_manager.CommitVersion(*latest_visible);
  latest_visible1 = versions_manager.LatestVisibleVersion(latest_visible1);
  ASSERT_TRUE(latest_visible->CompareWith(*latest_visible1) == 0);
  mini_uncommitted = versions_manager.MiniUncommittedVersion(mini_uncommitted);
  ASSERT_TRUE(latest_visible->CompareWith(*mini_uncommitted) > 0);

  latest_visible->IncreaseBy(10);
  // can't see rollbacked version
  versions_manager.RollbackVersion(*latest_visible);
  latest_visible1 = versions_manager.LatestVisibleVersion(latest_visible1);
  ASSERT_TRUE(latest_visible->CompareWith(*latest_visible1) < 0);
  mini_uncommitted = versions_manager.MiniUncommittedVersion(mini_uncommitted);
  ASSERT_TRUE(latest_visible->CompareWith(*mini_uncommitted) < 0);

  delete latest_visible;
  delete latest_visible1;
  delete mini_uncommitted;
}
};  // anonymous namespace

TEST_F(SeqBasedMultiVersionsTest, WCMultiVersionsManager) {
  // brand new version manager
  WriteCommittedSeqBasedMultiVersionsManager versions_manager;
  TestWCMultiVersionsManager(versions_manager);
  
  // version manager initializes from an existed version
  std::string encoded = "1314";
  WriteCommittedSeqBasedMultiVersionsManager versions_manager1;
  Version* orig = versions_manager1.CreateVersion();
  orig->DecodeFrom(encoded);
  versions_manager1.Initialize(*orig);
  TestWCMultiVersionsManager(versions_manager1);
  delete orig;
}

namespace {
void EchoVersionSeq(int num, Version* version, const Snapshot* snapshot) {
  SeqBasedVersion* version_impl = reinterpret_cast<SeqBasedVersion*>(version);
  const SeqBasedSnapshot* snapshot_impl = reinterpret_cast<const SeqBasedSnapshot*>(snapshot);
  std::cout<<num<<": version seq: "<<version_impl->Seq()
      <<" snapshot seq: "<<snapshot_impl->Seq()<<std::endl;
}
void TestWCSnapshotManagerReadView(
    WriteCommittedSeqBasedMultiVersionsManager& versions_manager,
    WriteCommittedSeqBasedSnapshotManager& snapshot_manager) {
  Version* latest_visible = nullptr;
  Version* latest_visible1 = nullptr;
  const Snapshot* read_view1 = nullptr;
  const Snapshot* read_view2 = nullptr;
  const Snapshot* read_view3 = nullptr;

  latest_visible = versions_manager.LatestVisibleVersion(nullptr);
  latest_visible1 = versions_manager.LatestVisibleVersion(nullptr);

  latest_visible->IncreaseBy(1);
  versions_manager.CommitVersion(*latest_visible);
  read_view1 = snapshot_manager.LatestReadView();

  latest_visible->IncreaseBy(10);
  versions_manager.CommitVersion(*latest_visible);
  read_view2 = snapshot_manager.LatestReadView();

  latest_visible->IncreaseBy(10);
  versions_manager.CommitVersion(*latest_visible);
  read_view3 = snapshot_manager.LatestReadView();

  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1));
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2));
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3));

  latest_visible1->IncreaseBy(5);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1));
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2));
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3));

  latest_visible1->IncreaseBy(10);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1));
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2));
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3));

  latest_visible1->IncreaseBy(10);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1));
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2));
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3));
  
  delete latest_visible;
  delete latest_visible1;
  delete read_view1;
  delete read_view2;
  delete read_view3;
}
};  // anonymous namespace

TEST_F(SeqBasedMultiVersionsTest, WCSnapshotManagerReadView) {
  // brand new version manager
  WriteCommittedSeqBasedMultiVersionsManager versions_manager;
  WriteCommittedSeqBasedSnapshotManager snapshot_manager(&versions_manager);
  TestWCSnapshotManagerReadView(versions_manager, snapshot_manager);

  // version manager initializes from an existed version
  std::string encoded = "1314";
  WriteCommittedSeqBasedMultiVersionsManager versions_manager1;
  Version* orig = versions_manager1.CreateVersion();
  orig->DecodeFrom(encoded);
  versions_manager1.Initialize(*orig);
  WriteCommittedSeqBasedSnapshotManager snapshot_manager1(&versions_manager1);
  TestWCSnapshotManagerReadView(versions_manager1, snapshot_manager1);
  delete orig;
}

TEST_F(SeqBasedMultiVersionsTest, WPMultiVersionsManager) {
  WritePreparedSeqBasedMultiVersionsManager wpsbmvm;

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
