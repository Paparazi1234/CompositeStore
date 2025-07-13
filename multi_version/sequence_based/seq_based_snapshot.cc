#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

const SeqBasedSnapshot SeqBasedSnapshotManager::snapshot_limits_min_ =
    SeqBasedSnapshot(kSeqNumberLimitsMin);
const SeqBasedSnapshot SeqBasedSnapshotManager::snapshot_limits_max_ =
    SeqBasedSnapshot(kSeqNumberLimitsMax);

const WritePreparedSeqBasedSnapshot
    WritePreparedSnapshotManager::wp_snapshot_limits_min_ =
      WritePreparedSeqBasedSnapshot(kSeqNumberLimitsMin, kUnCommittedLimitsMin);
const WritePreparedSeqBasedSnapshot
    WritePreparedSnapshotManager::wp_snapshot_limits_max_ =
      WritePreparedSeqBasedSnapshot(kSeqNumberLimitsMax, kUnCommittedLimitsMin);

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
      reinterpret_cast<const SeqBasedSnapshot*>(snapshot);
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

const SeqBasedSnapshot* WriteCommittedSnapshotManager::TakeSnapshotInternal(
    Snapshot* reused) {
  const WriteCommittedMultiVersionsManager* WC_mvm =
      reinterpret_cast<const WriteCommittedMultiVersionsManager*>
      (multi_versions_manager_);
  SeqBasedVersion tmp;
  SeqBasedVersion* latest_visible = reinterpret_cast<SeqBasedVersion*>(
      WC_mvm->LatestVisibleVersion(&tmp));
  if (reused != nullptr) {
    SeqBasedSnapshot* reused_impl = reinterpret_cast<SeqBasedSnapshot*>(reused);
    reused_impl->SetSeq(latest_visible->Seq());
    return reused_impl;
  } else {
    return new SeqBasedSnapshot(latest_visible->Seq());
  }
}

Snapshot* WritePreparedSnapshotManager::CreateSnapshot() const {
  return new WritePreparedSeqBasedSnapshot(0);
}

void WritePreparedSnapshotManager::GetSnapshots(
    uint64_t max, std::vector<uint64_t>& snapshots) const {
  snapshots.clear();
  MutexLock lock(&map_mutex_);
  for (auto it = snapshots_map_.cbegin(); it != snapshots_map_.end(); ++it) {
    uint64_t snapshot_seq = it->first;
    if (snapshot_seq > max) {
      break;
    }
    snapshots.push_back(snapshot_seq);
  }
}

const SeqBasedSnapshot* WritePreparedSnapshotManager::TakeSnapshotInternal(
    Snapshot* reused) {
  assert(take_snapshot_callback_.get() != nullptr);
  uint64_t snapshot_seq;
  uint64_t min_uncommitted;
  take_snapshot_callback_->TakeSnapshot(&snapshot_seq, &min_uncommitted);
  if (reused != nullptr) {
    WritePreparedSeqBasedSnapshot* reused_impl =
        reinterpret_cast<WritePreparedSeqBasedSnapshot*>(reused);
    reused_impl->SetSeq(snapshot_seq);
    reused_impl->SetMiniUnCommitted(min_uncommitted);
    return reused_impl;
  } else {
    return new WritePreparedSeqBasedSnapshot(snapshot_seq, min_uncommitted);
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE
