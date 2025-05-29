#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

const Snapshot* SeqBasedSnapshotManager::LatestReadView() {
  return TakeSnapshotInternal();
}

const Snapshot* SeqBasedSnapshotManager::TakeSnapshot() {
  std::unique_ptr<const SeqBasedSnapshot> snapshot(TakeSnapshotInternal());
  assert(snapshot.get() != nullptr);
  std::lock_guard<std::mutex> lock(map_mutex_);
  SnapshotsMap::iterator iter = snapshots_map_.find(snapshot->Seq());
  if (iter == snapshots_map_.end()) {
    iter = snapshots_map_.insert({snapshot->Seq(), std::move(snapshot)}).first;
  }
  iter->second->Ref();
  return iter->second.get();
}

void SeqBasedSnapshotManager::ReleaseSnapshot(const Snapshot* snapshot) {
  if (snapshot == nullptr) {
    return;
  }
  const SeqBasedSnapshot* snapshot_impl =
      reinterpret_cast<const SeqBasedSnapshot*>(snapshot);
  std::lock_guard<std::mutex> lock(map_mutex_);
  if (snapshots_map_.find(snapshot_impl->Seq()) != snapshots_map_.end()) {
    if (snapshot_impl->Unref()) {
      snapshots_map_.erase(snapshot_impl->Seq());
    }
  }
}

bool SeqBasedSnapshotManager::IsEmpty() const {
  std::lock_guard<std::mutex> lock(map_mutex_);
  return snapshots_map_.empty();
}

uint32_t SeqBasedSnapshotManager::NumLivingSnapshot() const {
  std::lock_guard<std::mutex> lock(map_mutex_);
  return snapshots_map_.size();
}

void SeqBasedSnapshotManager::GetAllLivingSnapshot(
    std::vector<const Snapshot*>& snapshots) const {
  snapshots.clear();
  std::lock_guard<std::mutex> lock(map_mutex_);
  for (auto& snapshot : snapshots_map_) {
    snapshots.push_back(snapshot.second.get());
  }
}

const SeqBasedSnapshot* WriteCommittedSeqBasedSnapshotManager::
    TakeSnapshotInternal() {
  const WriteCommittedSeqBasedMultiVersionsManager* WC_mvm =
      reinterpret_cast<const WriteCommittedSeqBasedMultiVersionsManager*>
      (multi_versions_manager_);
  SeqBasedVersion* latest_version = reinterpret_cast<SeqBasedVersion*>(
      WC_mvm->LatestVisibleVersion(nullptr));
  SeqBasedSnapshot* snapshot = new SeqBasedSnapshot(latest_version->Seq());
  delete latest_version;
  return snapshot;
}

const SeqBasedSnapshot* WritePreparedSeqBasedSnapshotManager::
    TakeSnapshotInternal() {
  return new SeqBasedSnapshot(0);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
