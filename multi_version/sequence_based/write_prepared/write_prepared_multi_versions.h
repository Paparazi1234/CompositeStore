#pragma once

#include "multi_version/sequence_based/seq_based_multi_versions.h"
#include "infinite_commit_table.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WritePreparedMultiVersionsManager : public SeqBasedMultiVersionsManager {
 public:
  // No copying allowed
  WritePreparedMultiVersionsManager(
      const WritePreparedMultiVersionsManager&) = delete;
  WritePreparedMultiVersionsManager& operator=(
      const WritePreparedMultiVersionsManager&) = delete;

  WritePreparedMultiVersionsManager(const CommitTableOptions& options,
                                    bool enable_two_write_queues = false)
      : SeqBasedMultiVersionsManager(
          MaxReadableVersionRep(enable_two_write_queues),
          MaxVisibleVersionRep(enable_two_write_queues)),
        commit_table_(options, max_readable_seq_, max_visible_seq_) {
    if (enable_two_write_queues) {
      assert(std::addressof(max_readable_seq_) !=
             std::addressof(max_visible_seq_));
      assert(std::addressof(max_readable_seq_) == &max_readable_rep_);
      assert(std::addressof(max_visible_seq_) == &max_visible_rep_);
    } else {
      assert(std::addressof(max_readable_seq_) ==
             std::addressof(max_visible_seq_));
      assert(std::addressof(max_readable_seq_) == &max_readable_rep_);
      assert(std::addressof(max_visible_seq_) == &max_readable_rep_);
    }
  }
  ~WritePreparedMultiVersionsManager() {}

  virtual void BeginPrepareVersions(
    const Version& prepared_uncommitted_started,
    uint32_t num_prepared_uncommitteds) override;
  virtual void EndPrepareVersions(const Version& end_uncommitted) override;

  virtual void BeginCommitVersions(
      const Version& prepared_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds) override;
  virtual void EndCommitVersions(
      const Version& prepared_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds) override;

  virtual void BeginRollbackVersions(
      const Version& prepared_uncommitted_started,
      const Version& rollbacked_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds,
      uint32_t num_rollbacked_uncommitteds) override;
  virtual void EndRollbackVersions(
      const Version& prepared_uncommitted_started,
      const Version& rollbacked_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds,
      uint32_t num_rollbacked_uncommitteds) override;
  virtual bool IsVersionVisibleToSnapshot(const Version& version,
                                          const Snapshot& snapshot,
                                          bool* snap_exists) const override;
  
  void TEST_Crash() override {
    commit_table_.TEST_Crash();
  }

  void SetSnapshotsRetrieveCallback(
      const GetSnapshotsCallback* get_snapshots_cb) {
    commit_table_.SetSnapshotsRetrieveCallback(get_snapshots_cb);
  }

  void SetAdvanceMaxCommittedByOneCallback(
      AdvanceMaxCommittedByOneCallback* callback) {
    commit_table_.SetAdvanceMaxCommittedByOneCallback(callback);
  }

  TakeSnapshotCallback* GetSnapshotCreationCallback() {
    return commit_table_.GetSnapshotCreationCallback();
  }

 private:
  virtual std::atomic<uint64_t>& MaxReadableVersionRep(
      bool enable_two_write_queues) override {
    return max_readable_rep_;
  }

  virtual std::atomic<uint64_t>& MaxVisibleVersionRep(
      bool enable_two_write_queues) override {
    if (enable_two_write_queues) {
      return max_visible_rep_;
    } else {
      // when enable_two_write_queues == false, max_visible is same as
      // max_readable
      return max_readable_rep_;
    }
  }

  std::atomic<uint64_t> max_readable_rep_ = {0};
  // max_visible_rep_ only takes effect when enable_two_write_queues == true,
  // when enable_two_write_queues == false, max_visible is same as max_readable
  std::atomic<uint64_t> max_visible_rep_ = {0};
  InfiniteCommitTable commit_table_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
