#include "seq_based_snapshot.h"

#include "seq_based_multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

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
  WriteCommittedSeqBasedMultiVersionsManager* version_manager =
      reinterpret_cast<WriteCommittedSeqBasedMultiVersionsManager*>
      (multi_versions_manager_.get());
  const Version& latest_version = version_manager->LatestVisibleVersion();
  const SeqBasedVersion* latest_version_ptr =
    reinterpret_cast<const SeqBasedVersion*>(&latest_version);
  return new SeqBasedSnapshot(latest_version_ptr->Seq());
}

const SeqBasedSnapshot* WritePreparedSeqBasedSnapshotManager::
    TakeSnapshotInternal() {

}

}   // namespace MULTI_VERSIONS_NAMESPACE
