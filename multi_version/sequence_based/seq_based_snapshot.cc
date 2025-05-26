#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

const Snapshot* SeqBasedSnapshotManager::LatestReadView() {
  return TakeSnapshotInternal();
}

const Snapshot* SeqBasedSnapshotManager::TakeSnapshot() {
  std::unique_ptr<const SeqBasedSnapshot> snapshot(TakeSnapshotInternal());
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
  const SeqBasedSnapshot* s =
      reinterpret_cast<const SeqBasedSnapshot*>(snapshot);
  {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (snapshots_map_.find(s->Seq()) != snapshots_map_.end()) {
      if (s->Unref()) {
        snapshots_map_.erase(s->Seq());
      }
    }
  }
}

void SeqBasedSnapshotManager::GetAllLivingSnapshot() {

}

const SeqBasedSnapshot* WriteCommittedSeqBasedSnapshotManager::
    TakeSnapshotInternal() {
  const WriteCommittedSeqBasedMultiVersionsManager* WC_mvm =
      reinterpret_cast<const WriteCommittedSeqBasedMultiVersionsManager*>
      (multi_versions_manager_);
  SeqBasedVersion* latest_version = reinterpret_cast<SeqBasedVersion*>(
      WC_mvm->LatestVisibleVersion());
  return new SeqBasedSnapshot(latest_version->Seq());
}

const SeqBasedSnapshot* WritePreparedSeqBasedSnapshotManager::
    TakeSnapshotInternal() {
  return new SeqBasedSnapshot(0);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
