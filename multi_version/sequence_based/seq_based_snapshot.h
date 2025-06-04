#pragma once

#include <memory>
#include <mutex>
#include <map>
#include <assert.h>

#include "write_prepared_multi_versions.h"

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
      : SeqBasedSnapshotManager(multi_versions_manager) {
    WritePreparedMultiVersionsManager* mgr_impl =
        reinterpret_cast<WritePreparedMultiVersionsManager*>(
            multi_versions_manager);
    GetSnapshotsFunctor functor(this);
    mgr_impl->SetSnapshotsRetrieveFunc(functor);
  }
  ~WritePreparedSnapshotManager() {}
  void GetSnapshots(uint64_t max, std::vector<uint64_t>& snapshots) const;

  class GetSnapshotsFunctor {
   public:
    GetSnapshotsFunctor(const WritePreparedSnapshotManager* mgr) : mgr_(mgr) {}
    void operator()(uint64_t max, std::vector<uint64_t>& snapshots) {
      mgr_->GetSnapshots(max, snapshots);
    }
   private:
    const WritePreparedSnapshotManager* const mgr_;
  };
 private:
  virtual const SeqBasedSnapshot* TakeSnapshotInternal() override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
