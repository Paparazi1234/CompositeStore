#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <assert.h>

#include "../../include/snapshot.h"
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
 private:
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
  virtual void GetAllLivingSnapshot() override;

 protected:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() = 0;

  const SeqBasedMultiVersionsManager* multi_versions_manager_;
  std::mutex map_mutex_;
  using SnapshotsMap =
    std::unordered_map<uint64_t, std::unique_ptr<const SeqBasedSnapshot>>;
  SnapshotsMap snapshots_map_;
};

class  WriteCommittedSeqBasedSnapshotManager : public SeqBasedSnapshotManager {
 public:
  // No copying allowed
  WriteCommittedSeqBasedSnapshotManager(
      const WriteCommittedSeqBasedSnapshotManager&) = delete;
  WriteCommittedSeqBasedSnapshotManager&
      operator=(const WriteCommittedSeqBasedSnapshotManager&) = delete;
  
  WriteCommittedSeqBasedSnapshotManager(
      SeqBasedMultiVersionsManager* multi_versions_manager)
          : SeqBasedSnapshotManager(multi_versions_manager) {}
  ~WriteCommittedSeqBasedSnapshotManager() {}

 private:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() override;
};

class  WritePreparedSeqBasedSnapshotManager : public SeqBasedSnapshotManager {
 public:
  // No copying allowed
  WritePreparedSeqBasedSnapshotManager(
      const WritePreparedSeqBasedSnapshotManager&) = delete;
  WritePreparedSeqBasedSnapshotManager&
      operator=(const WritePreparedSeqBasedSnapshotManager&) = delete;
  
  WritePreparedSeqBasedSnapshotManager(
      SeqBasedMultiVersionsManager* multi_versions_manager)
          : SeqBasedSnapshotManager(multi_versions_manager) {}
  ~WritePreparedSeqBasedSnapshotManager() {}

 private:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
