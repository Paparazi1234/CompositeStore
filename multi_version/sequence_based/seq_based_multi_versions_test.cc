#include "seq_based_multi_versions.h"
#include "seq_based_snapshot.h"
#include "test_util/test_util.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class CommonSeqBasedMultiVersionsTests {
 public:
  CommonSeqBasedMultiVersionsTests(TxnStoreWritePolicy write_policy,
      bool enable_two_write_queues)
      : write_policy_(write_policy),
        enable_two_write_queues_(enable_two_write_queues) {
    if (write_policy_ == WRITE_COMMITTED) {
      multi_versions_manager_ =
          new WriteCommittedMultiVersionsManager(enable_two_write_queues_);
      snapshot_manager_ =
          new WriteCommittedSnapshotManager(multi_versions_manager_);
    } else if (write_policy_ == WRITE_PREPARED) {
      CommitTableOptions options;
      multi_versions_manager_ =
          new WritePreparedMultiVersionsManager(
              options, enable_two_write_queues_);
      snapshot_manager_ =
          new WritePreparedSnapshotManager(multi_versions_manager_);
    } else {
      assert(false);
    }
    assert(multi_versions_manager_);
    assert(snapshot_manager_);
  }

  ~CommonSeqBasedMultiVersionsTests() {
    delete multi_versions_manager_;
    delete snapshot_manager_;
  }

  void InitializeMultiVersionManager(const std::string& orig_vresion_seq) {
    Version* orig = multi_versions_manager_->CreateVersion();
    orig->DecodeFrom(orig_vresion_seq);
    multi_versions_manager_->Initialize(*orig);
    delete orig;
  }

  // Common test functions
  void SnapshotManagerReadView();
  void SnapshotManagerTakeSnapshot();
 private:
  TxnStoreWritePolicy write_policy_;
  bool enable_two_write_queues_;
  SeqBasedMultiVersionsManager* multi_versions_manager_;
  SeqBasedSnapshotManager* snapshot_manager_;
};

void CommonSeqBasedMultiVersionsTests::SnapshotManagerReadView() {
  Version* latest_visible = nullptr;
  Version* latest_visible1 = nullptr;
  const Snapshot* read_view1 = nullptr;
  const Snapshot* read_view2 = nullptr;
  const Snapshot* read_view3 = nullptr;
  const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();
  bool snap_exists;

  latest_visible = multi_versions_manager_->LatestVisibleVersion(nullptr);
  latest_visible1 = multi_versions_manager_->LatestVisibleVersion(nullptr);

  latest_visible->IncreaseBy(1);
  multi_versions_manager_->BeginCommitVersions(dummy_version,
                                               *latest_visible, 1);
  multi_versions_manager_->EndCommitVersions(dummy_version, *latest_visible, 1);
  read_view1 = snapshot_manager_->LatestReadView(nullptr);

  latest_visible->IncreaseBy(10);
  multi_versions_manager_->BeginCommitVersions(dummy_version,
                                               *latest_visible, 1);
  multi_versions_manager_->EndCommitVersions(dummy_version, *latest_visible, 1);
  read_view2 = snapshot_manager_->LatestReadView(nullptr);

  latest_visible->IncreaseBy(10);
  multi_versions_manager_->BeginCommitVersions(dummy_version,
                                               *latest_visible, 1);
  multi_versions_manager_->EndCommitVersions(dummy_version, *latest_visible, 1);
  read_view3 = snapshot_manager_->LatestReadView(nullptr);

  ASSERT_TRUE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);

  latest_visible1->IncreaseBy(5);
  ASSERT_FALSE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);

  latest_visible1->IncreaseBy(10);
  ASSERT_FALSE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_FALSE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_TRUE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);

  latest_visible1->IncreaseBy(10);
  ASSERT_FALSE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view1, &snap_exists) && snap_exists == true);
  ASSERT_FALSE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view2, &snap_exists) && snap_exists == true);
  ASSERT_FALSE(multi_versions_manager_->IsVersionVisibleToSnapshot(
      *latest_visible1, *read_view3, &snap_exists) && snap_exists == true);
  
  delete latest_visible;
  delete latest_visible1;
  delete read_view1;
  delete read_view2;
  delete read_view3;
}

void CommonSeqBasedMultiVersionsTests::SnapshotManagerTakeSnapshot() {
    uint32_t latest_visible_seq;
  Version* latest_visible = nullptr;
  const Snapshot* snapshot1 = nullptr;
  const Snapshot* snapshot2 = nullptr;
  const Snapshot* snapshot3 = nullptr;
  const Snapshot* snapshot4 = nullptr;
  const Version& dummy_version = multi_versions_manager_->VersionLimitsMax();

  ASSERT_TRUE(snapshot_manager_->IsEmpty());

  latest_visible = multi_versions_manager_->LatestVisibleVersion(nullptr);
  latest_visible_seq =
      reinterpret_cast<SeqBasedVersion*>(latest_visible)->Seq();
  snapshot1 = snapshot_manager_->TakeSnapshot();
  ASSERT_FALSE(snapshot_manager_->IsEmpty());
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(1));

  latest_visible->IncreaseBy(5);
  multi_versions_manager_->BeginCommitVersions(dummy_version,
                                               *latest_visible, 1);
  multi_versions_manager_->EndCommitVersions(dummy_version, *latest_visible, 1);
  snapshot2 = snapshot_manager_->TakeSnapshot();
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(2));
  // no new version committed, so the same snapshot is returned
  snapshot3 = snapshot_manager_->TakeSnapshot();
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(2));
  ASSERT_EQ(snapshot2, snapshot3);

  latest_visible->IncreaseBy(5);
  multi_versions_manager_->BeginCommitVersions(dummy_version,
                                               *latest_visible, 1);
  multi_versions_manager_->EndCommitVersions(dummy_version, *latest_visible, 1);
  snapshot4 = snapshot_manager_->TakeSnapshot();
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(3));

  std::vector<const Snapshot*> snapshots;
  snapshot_manager_->GetAllLivingSnapshots(snapshots);
  ASSERT_EQ(snapshots.size(), 3ull);
  uint32_t expected_step[3] = {0, 5, 10};
  for (size_t i = 0; i < snapshots.size(); ++i) {
    const SeqBasedSnapshot* snapshot_impl =
        reinterpret_cast<const SeqBasedSnapshot*>(snapshots[i]);
    ASSERT_EQ(snapshot_impl->Seq(), latest_visible_seq + expected_step[i]);
  }

  snapshot_manager_->ReleaseSnapshot(snapshot1);
  snapshot1 = nullptr;
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(2));

  snapshot_manager_->ReleaseSnapshot(snapshot2);
  snapshot2 = nullptr;
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(2));

  snapshot_manager_->ReleaseSnapshot(snapshot3);
  snapshot3 = nullptr;
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(1));

  snapshot_manager_->ReleaseSnapshot(snapshot4);
  snapshot4 = nullptr;
  ASSERT_EQ(snapshot_manager_->NumLivingSnapshot(), uint32_t(0));
  ASSERT_TRUE(snapshot_manager_->IsEmpty());

  delete latest_visible;
}

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

TEST_F(SeqBasedMultiVersionsTest, DISABLED_TestSnapshotManagerReadView) {
  // brand new version manager
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonSeqBasedMultiVersionsTests* test =
        new CommonSeqBasedMultiVersionsTests(write_policy,
                                             enable_two_write_queues);
    test->SnapshotManagerReadView();
    delete test;
  }

  // version manager initializes from an existed version
  generator.Reset();
  std::string encoded_version = "1314";
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonSeqBasedMultiVersionsTests* test =
        new CommonSeqBasedMultiVersionsTests(write_policy,
                                             enable_two_write_queues);
    test->InitializeMultiVersionManager(encoded_version);
    test->SnapshotManagerReadView();
    delete test;
  }
}

TEST_F(SeqBasedMultiVersionsTest, DISABLED_TestSnapshotManagerTakeSnapshot) {
  // brand new version manager
  TestSetupsGenerator generator;
  TxnStoreWritePolicy write_policy;
  bool enable_two_write_queues;
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonSeqBasedMultiVersionsTests* test =
        new CommonSeqBasedMultiVersionsTests(write_policy,
                                             enable_two_write_queues);
    test->SnapshotManagerTakeSnapshot();
    delete test;
  }

  // version manager initializes from an existed version
  generator.Reset();
  std::string encoded_version = "1314";
  while (generator.GenerateTestSetups(&write_policy,
                                      &enable_two_write_queues)) {
    CommonSeqBasedMultiVersionsTests* test =
        new CommonSeqBasedMultiVersionsTests(write_policy,
                                             enable_two_write_queues);
    test->InitializeMultiVersionManager(encoded_version);
    test->SnapshotManagerTakeSnapshot();
    delete test;
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
