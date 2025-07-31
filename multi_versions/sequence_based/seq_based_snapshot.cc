#include "seq_based_snapshot.h"

#include "seq_limits.h"
#include "utils/mutex_lock.h"

namespace COMPOSITE_STORE_NAMESPACE {

const SeqBasedSnapshot SeqBasedSnapshotManager::snapshot_limits_min_ =
    SeqBasedSnapshot(kSeqNumberLimitsMin);
const SeqBasedSnapshot SeqBasedSnapshotManager::snapshot_limits_max_ =
    SeqBasedSnapshot(kSeqNumberLimitsMax);

Snapshot* SeqBasedSnapshotManager::CreateSnapshot() const {
  return new SeqBasedSnapshot(0);
}

const Snapshot* SeqBasedSnapshotManager::LatestReadView(Snapshot* reused) {
  return TakeSnapshotInternal(reused);
}

const Snapshot* SeqBasedSnapshotManager::TakeSnapshot() {
  std::unique_ptr<const SeqBasedSnapshot> snapshot(TakeSnapshotInternal());
  assert(snapshot.get() != nullptr);
  MutexLock lock(&map_mutex_);
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
      static_cast_with_check<const SeqBasedSnapshot>(snapshot);
  MutexLock lock(&map_mutex_);
  if (snapshots_map_.find(snapshot_impl->Seq()) != snapshots_map_.end()) {
    if (snapshot_impl->Unref()) {
      snapshots_map_.erase(snapshot_impl->Seq());
    }
  }
}

bool SeqBasedSnapshotManager::IsEmpty() const {
  MutexLock lock(&map_mutex_);
  return snapshots_map_.empty();
}

uint32_t SeqBasedSnapshotManager::NumLivingSnapshot() const {
  MutexLock lock(&map_mutex_);
  return snapshots_map_.size();
}

void SeqBasedSnapshotManager::GetAllLivingSnapshots(
    std::vector<const Snapshot*>& snapshots) const {
  snapshots.clear();
  MutexLock lock(&map_mutex_);
  for (auto& snapshot : snapshots_map_) {
    snapshots.push_back(snapshot.second.get());
  }
}

}   // namespace COMPOSITE_STORE_NAMESPACE
