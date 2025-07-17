#pragma once

#include "multi_versions/sequence_based/seq_based_snapshot.h"
#include "multi_versions/sequence_based/seq_limits.h"
#include "infinite_commit_table.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WritePreparedSeqBasedSnapshot : public SeqBasedSnapshot {
 public:
  explicit WritePreparedSeqBasedSnapshot(
      uint64_t seq,
      uint64_t min_uncommitted = kUnCommittedLimitsMin)
          : SeqBasedSnapshot(seq),
            min_uncommitted_(min_uncommitted) {}
  ~WritePreparedSeqBasedSnapshot() {}

  uint64_t MiniUnCommitted() const {
    return min_uncommitted_;
  }

  void SetMiniUnCommitted(uint64_t min_uncommitted) {
    min_uncommitted_ = min_uncommitted;
  }

 private:
  uint64_t min_uncommitted_;
};

class  WritePreparedSnapshotManager : public SeqBasedSnapshotManager {
 public:
  // No copying allowed
  WritePreparedSnapshotManager(const WritePreparedSnapshotManager&) = delete;
  WritePreparedSnapshotManager& operator=(
      const WritePreparedSnapshotManager&) = delete;
  
  WritePreparedSnapshotManager(
      SeqBasedMultiVersionsManager* multi_versions_manager)
      : SeqBasedSnapshotManager(multi_versions_manager) {}
  ~WritePreparedSnapshotManager() {}

  virtual Snapshot* CreateSnapshot() const override;

  virtual const Snapshot& SnapshotLimitsMin() const override {
    return wp_snapshot_limits_min_;
  }

  virtual const Snapshot& SnapshotLimitsMax() const override {
    return wp_snapshot_limits_max_;
  }

  void GetSnapshots(uint64_t max, std::vector<uint64_t>& snapshots) const;

  class WPGetSnapshotsCallback : public GetSnapshotsCallback {
   public:
    ~WPGetSnapshotsCallback() {}
    WPGetSnapshotsCallback(const WritePreparedSnapshotManager* mgr)
        : mgr_(mgr) {}

    virtual void GetSnapshots(
        uint64_t max, std::vector<uint64_t>& snapshots) const override {
      mgr_->GetSnapshots(max, snapshots);
    }
   private:
    const WritePreparedSnapshotManager* const mgr_;
  };

  void SetSnapshotCreationCallback(TakeSnapshotCallback* take_snapshot_cb) {
    take_snapshot_callback_.reset(take_snapshot_cb);
  }
 private:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal(
      Snapshot* reused) override;

  std::unique_ptr<TakeSnapshotCallback> take_snapshot_callback_;

  static const WritePreparedSeqBasedSnapshot wp_snapshot_limits_min_;
  static const WritePreparedSeqBasedSnapshot wp_snapshot_limits_max_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
