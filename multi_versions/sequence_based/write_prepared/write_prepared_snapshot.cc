#include "write_prepared_snapshot.h"

#include "util/cast_util.h"
#include "util/mutex_lock.h"

namespace COMPOSITE_STORE_NAMESPACE {

const WritePreparedSeqBasedSnapshot
    WritePreparedSnapshotManager::wp_snapshot_limits_min_ =
      WritePreparedSeqBasedSnapshot(kSeqNumberLimitsMin, kUnCommittedLimitsMin);
const WritePreparedSeqBasedSnapshot
    WritePreparedSnapshotManager::wp_snapshot_limits_max_ =
      WritePreparedSeqBasedSnapshot(kSeqNumberLimitsMax, kUnCommittedLimitsMin);

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
        static_cast_with_check<WritePreparedSeqBasedSnapshot>(reused);
    reused_impl->SetSeq(snapshot_seq);
    reused_impl->SetMiniUnCommitted(min_uncommitted);
    return reused_impl;
  } else {
    return new WritePreparedSeqBasedSnapshot(snapshot_seq, min_uncommitted);
  }
}

}   // namespace COMPOSITE_STORE_NAMESPACE
