#pragma once

#include <map>
#include <assert.h>

#include "seq_based_multi_versions.h"
#include "port/port.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedSnapshot : public Snapshot {
 public:
  explicit SeqBasedSnapshot(uint64_t seq) : rep_(seq), refs_(0) {}
  ~SeqBasedSnapshot() {}
  uint64_t Seq() const {
    return rep_;
  }

  void SetSeq(uint64_t seq) {
    rep_ = seq;
  }

  const Version* MaxVersionInSnapshot(Version* reused) const override {
    if (reused) {
      SeqBasedVersion* version_impl =
          static_cast_with_check<SeqBasedVersion>(reused);
      version_impl->SetSeq(Seq());
      return version_impl;
    } else {
      return new SeqBasedVersion(Seq());
    }
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

class SeqBasedSnapshotManager : public SnapshotManager {
 public:
  // No copying allowed
  SeqBasedSnapshotManager(const SeqBasedSnapshotManager&) = delete;
  SeqBasedSnapshotManager& operator=(const SeqBasedSnapshotManager&) = delete;

  SeqBasedSnapshotManager(SeqBasedMultiVersionsManager* multi_versions_manager)
      : multi_versions_manager_(multi_versions_manager) {}
  ~SeqBasedSnapshotManager() {}
 
  virtual Snapshot* CreateSnapshot() const override;
  virtual const Snapshot* LatestReadView(Snapshot* reused) override;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;
  virtual bool IsEmpty() const override;
  virtual uint32_t NumLivingSnapshot() const override;
  virtual void GetAllLivingSnapshots(
      std::vector<const Snapshot*>& snapshots) const override;
  
  virtual const Snapshot& SnapshotLimitsMin() const override {
    return snapshot_limits_min_;
  }

  virtual const Snapshot& SnapshotLimitsMax() const override {
    return snapshot_limits_max_;
  }

 protected:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal(
      Snapshot* reused = nullptr) = 0;

  const SeqBasedMultiVersionsManager* multi_versions_manager_;
  mutable port::Mutex map_mutex_;
  using SnapshotsMap =
    std::map<uint64_t, std::unique_ptr<const SeqBasedSnapshot>>;
  SnapshotsMap snapshots_map_;

 private:
  static const SeqBasedSnapshot snapshot_limits_min_;
  static const SeqBasedSnapshot snapshot_limits_max_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
