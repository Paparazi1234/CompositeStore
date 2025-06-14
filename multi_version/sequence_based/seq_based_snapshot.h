#pragma once

#include <memory>
#include <mutex>
#include <map>
#include <assert.h>

#include "seq_based_multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedSnapshot : public Snapshot {
 public:
  SeqBasedSnapshot(uint64_t seq) : rep_(seq), refs_(0) {}
  ~SeqBasedSnapshot() {}
  uint64_t Seq() const {
    return rep_;
  }

  virtual const Version* MaxVersionInSnapshot() const override {
    return new SeqBasedVersion(Seq());
  }
 protected:
  friend class SeqBasedSnapshotManager;

  void Ref() const { refs_++; }

  bool Unref() const {
    assert(refs_ > 0);
    refs_--;
    return refs_ == 0;
  }

  uint64_t rep_;
  mutable uint64_t refs_;  // reference counted
};

class WPSeqBasedSnapshot : public SeqBasedSnapshot {
 public:
  WPSeqBasedSnapshot(uint64_t seq,
                     uint64_t min_uncommitted = kUnCommittedLimitsMin)
                     : SeqBasedSnapshot(seq),
                       min_uncommitted_(min_uncommitted) {}
  ~WPSeqBasedSnapshot() {}

  uint64_t MiniUnCommitted() const {
    return min_uncommitted_;
  }

 private:
  uint64_t min_uncommitted_;
};


class SeqBasedSnapshotManager : public SnapshotManager {
 public:
  // No copying allowed
  SeqBasedSnapshotManager(const SeqBasedSnapshotManager&) = delete;
  SeqBasedSnapshotManager& operator=(const SeqBasedSnapshotManager&) = delete;

  SeqBasedSnapshotManager(SeqBasedMultiVersionsManager* multi_versions_manager)
      : multi_versions_manager_(multi_versions_manager) {}
  ~SeqBasedSnapshotManager() {}
 
  virtual const Snapshot* LatestReadView() override;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;
  virtual bool IsEmpty() const override;
  virtual uint32_t NumLivingSnapshot() const override;
  virtual void GetAllLivingSnapshots(
      std::vector<const Snapshot*>& snapshots) const override;

 protected:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() = 0;

  const SeqBasedMultiVersionsManager* multi_versions_manager_;
  mutable std::mutex map_mutex_;
  using SnapshotsMap =
    std::map<uint64_t, std::unique_ptr<const SeqBasedSnapshot>>;
  SnapshotsMap snapshots_map_;
};

class  WriteCommittedSnapshotManager : public SeqBasedSnapshotManager {
 public:
  // No copying allowed
  WriteCommittedSnapshotManager(const WriteCommittedSnapshotManager&) = delete;
  WriteCommittedSnapshotManager& operator=(
      const WriteCommittedSnapshotManager&) = delete;
  
  WriteCommittedSnapshotManager(
      SeqBasedMultiVersionsManager* multi_versions_manager)
      : SeqBasedSnapshotManager(multi_versions_manager) {}
  ~WriteCommittedSnapshotManager() {}

 private:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() override;
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
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() override;

  std::unique_ptr<TakeSnapshotCallback> take_snapshot_callback_;
};

// just a wrapper of WriteCommittedSnapshotManager
class EmptySnapshotManager : public WriteCommittedSnapshotManager {
 public:
  // No copying allowed
  EmptySnapshotManager(const EmptySnapshotManager&) = delete;
  EmptySnapshotManager& operator=(const EmptySnapshotManager&) = delete;
  
  EmptySnapshotManager(SeqBasedMultiVersionsManager* multi_versions_manager)
      : WriteCommittedSnapshotManager(multi_versions_manager) {}
  ~EmptySnapshotManager() {}
};

}   // namespace MULTI_VERSIONS_NAMESPACE
