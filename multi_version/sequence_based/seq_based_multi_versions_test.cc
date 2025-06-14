#include "seq_based_multi_versions.h"
#include "seq_based_snapshot.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedMultiVersionsTest : public testing::Test {
 public:
  SeqBasedMultiVersionsTest() {}
  ~SeqBasedMultiVersionsTest() {}
};

TEST_F(SeqBasedMultiVersionsTest, SeqBasedVersionTest) {
  WriteCommittedMultiVersionsManager versions_manager;
  SeqBasedVersion* version1 =
      reinterpret_cast<SeqBasedVersion*>(versions_manager.CreateVersion());
  SeqBasedVersion* version2 =
      reinterpret_cast<SeqBasedVersion*>(versions_manager.CreateVersion());

  ASSERT_EQ(version1->Seq(), uint64_t(0));
  ASSERT_EQ(version2->Seq(), uint64_t(0));

  version1->IncreaseByOne();
  version2->IncreaseByOne();
  ASSERT_TRUE(version1->CompareWith(*version2) == 0);

  version2->IncreaseBy(2);
  //  descending ordered by version
  ASSERT_TRUE(version1->CompareWith(*version2) > 0);
  ASSERT_TRUE(version2->CompareWith(*version1) < 0);

  ASSERT_EQ(version1->Seq(), uint64_t(1));
  ASSERT_EQ(version2->Seq(), uint64_t(3));

  // encode to
  std::string encoded;
  version1->EncodeTo(&encoded);
  ASSERT_STREQ(encoded.c_str(), "1");

  // decode from
  encoded = "10";
  version1->DecodeFrom(encoded);
  ASSERT_EQ(version1->Seq(), uint64_t(10));

  delete version1;
  delete version2;
}

namespace {
void TestWCSnapshotManagerReadView(
    WriteCommittedMultiVersionsManager& versions_manager,
    WriteCommittedSnapshotManager& snapshot_manager) {
  Version* latest_visible = nullptr;
  Version* latest_visible1 = nullptr;
  const Snapshot* read_view1 = nullptr;
  const Snapshot* read_view2 = nullptr;
  const Snapshot* read_view3 = nullptr;
  const Version& dummy_version = versions_manager.VersionLimitsMax();
  bool snap_exists;

  latest_visible = versions_manager.LatestVisibleVersion(nullptr);
  latest_visible1 = versions_manager.LatestVisibleVersion(nullptr);

  latest_visible->IncreaseBy(1);
  versions_manager.BeginCommitVersions(dummy_version, *latest_visible, 1);
  versions_manager.EndCommitVersions(dummy_version, *latest_visible, 1);
  read_view1 = snapshot_manager.LatestReadView();

  latest_visible->IncreaseBy(10);
  versions_manager.BeginCommitVersions(dummy_version, *latest_visible, 1);
  versions_manager.EndCommitVersions(dummy_version, *latest_visible, 1);
  read_view2 = snapshot_manager.LatestReadView();

  latest_visible->IncreaseBy(10);
  versions_manager.BeginCommitVersions(dummy_version, *latest_visible, 1);
  versions_manager.EndCommitVersions(dummy_version, *latest_visible, 1);
  read_view3 = snapshot_manager.LatestReadView();

  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);

  latest_visible1->IncreaseBy(5);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);

  latest_visible1->IncreaseBy(10);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);

  latest_visible1->IncreaseBy(10);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_FALSE(versions_manager.IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);
  
  delete latest_visible;
  delete latest_visible1;
  delete read_view1;
  delete read_view2;
  delete read_view3;
}
}  // anonymous namespace

TEST_F(SeqBasedMultiVersionsTest, WCSnapshotManagerReadView) {
  // brand new version manager
  WriteCommittedMultiVersionsManager versions_manager;
  WriteCommittedSnapshotManager snapshot_manager(&versions_manager);
  TestWCSnapshotManagerReadView(versions_manager, snapshot_manager);

  // version manager initializes from an existed version
  std::string encoded = "1314";
  WriteCommittedMultiVersionsManager versions_manager1;
  Version* orig = versions_manager1.CreateVersion();
  orig->DecodeFrom(encoded);
  versions_manager1.Initialize(*orig);
  WriteCommittedSnapshotManager snapshot_manager1(&versions_manager1);
  TestWCSnapshotManagerReadView(versions_manager1, snapshot_manager1);
  delete orig;
}

namespace {
void TestWCSnapshotManagerTakeSnapshot(
    WriteCommittedMultiVersionsManager& versions_manager,
    WriteCommittedSnapshotManager& snapshot_manager) {
  uint32_t latest_visible_seq;
  Version* latest_visible = nullptr;
  const Snapshot* snapshot1 = nullptr;
  const Snapshot* snapshot2 = nullptr;
  const Snapshot* snapshot3 = nullptr;
  const Snapshot* snapshot4 = nullptr;
  const Version& dummy_version = versions_manager.VersionLimitsMax();

  ASSERT_TRUE(snapshot_manager.IsEmpty());

  latest_visible = versions_manager.LatestVisibleVersion(nullptr);
  latest_visible_seq =
      reinterpret_cast<SeqBasedVersion*>(latest_visible)->Seq();
  snapshot1 = snapshot_manager.TakeSnapshot();
  ASSERT_FALSE(snapshot_manager.IsEmpty());
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(1));

  latest_visible->IncreaseBy(5);
  versions_manager.BeginCommitVersions(dummy_version, *latest_visible, 1);
  versions_manager.EndCommitVersions(dummy_version, *latest_visible, 1);
  snapshot2 = snapshot_manager.TakeSnapshot();
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(2));
  // no new version committed, so the same snapshot is returned
  snapshot3 = snapshot_manager.TakeSnapshot();
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(2));
  ASSERT_EQ(snapshot2, snapshot3);

  latest_visible->IncreaseBy(5);
  versions_manager.BeginCommitVersions(dummy_version, *latest_visible, 1);
  versions_manager.EndCommitVersions(dummy_version, *latest_visible, 1);
  snapshot4 = snapshot_manager.TakeSnapshot();
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(3));

  std::vector<const Snapshot*> snapshots;
  snapshot_manager.GetAllLivingSnapshots(snapshots);
  ASSERT_EQ(snapshots.size(), 3ull);
  uint32_t expected_step[3] = {0, 5, 10};
  for (size_t i = 0; i < snapshots.size(); ++i) {
    const SeqBasedSnapshot* snapshot_impl =
        reinterpret_cast<const SeqBasedSnapshot*>(snapshots[i]);
    ASSERT_EQ(snapshot_impl->Seq(), latest_visible_seq + expected_step[i]);
  }

  snapshot_manager.ReleaseSnapshot(snapshot1);
  snapshot1 = nullptr;
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(2));

  snapshot_manager.ReleaseSnapshot(snapshot2);
  snapshot2 = nullptr;
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(2));

  snapshot_manager.ReleaseSnapshot(snapshot3);
  snapshot3 = nullptr;
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(1));

  snapshot_manager.ReleaseSnapshot(snapshot4);
  snapshot4 = nullptr;
  ASSERT_EQ(snapshot_manager.NumLivingSnapshot(), uint32_t(0));
  ASSERT_TRUE(snapshot_manager.IsEmpty());

  delete latest_visible;
}
}  // anonymous namespace

TEST_F(SeqBasedMultiVersionsTest, WCSnapshotManagerTakeSnapshot) {
  // brand new version manager
  WriteCommittedMultiVersionsManager versions_manager;
  WriteCommittedSnapshotManager snapshot_manager(&versions_manager);
  TestWCSnapshotManagerTakeSnapshot(versions_manager, snapshot_manager);

  // version manager initializes from an existed version
  std::string encoded = "1314";
  WriteCommittedMultiVersionsManager versions_manager1;
  Version* orig = versions_manager1.CreateVersion();
  orig->DecodeFrom(encoded);
  versions_manager1.Initialize(*orig);
  WriteCommittedSnapshotManager snapshot_manager1(&versions_manager1);
  TestWCSnapshotManagerTakeSnapshot(versions_manager1, snapshot_manager1);
  delete orig;
}

TEST_F(SeqBasedMultiVersionsTest, WPMultiVersionsManager) {
  CommitTableOptions options;
  WritePreparedMultiVersionsManager wpmvm(options);

}

TEST_F(SeqBasedMultiVersionsTest, WPSnapshotManager) {
  CommitTableOptions options;
  WritePreparedMultiVersionsManager wpmvm(options);
  WritePreparedSnapshotManager wpsm(&wpmvm);
  
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
