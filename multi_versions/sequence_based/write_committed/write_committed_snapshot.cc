#include "write_committed_snapshot.h"

namespace COMPOSITE_STORE_NAMESPACE {

const SeqBasedSnapshot* WriteCommittedSnapshotManager::TakeSnapshotInternal(
    Snapshot* reused) {
  SeqBasedVersion tmp;
  SeqBasedVersion* latest_visible = static_cast_with_check<SeqBasedVersion>(
      multi_versions_manager_->LatestVisibleVersion(&tmp));
  if (reused != nullptr) {
    SeqBasedSnapshot* reused_impl =
        static_cast_with_check<SeqBasedSnapshot>(reused);
    reused_impl->SetSeq(latest_visible->Seq());
    return reused_impl;
  } else {
    return new SeqBasedSnapshot(latest_visible->Seq());
  }
}

}   // namespace COMPOSITE_STORE_NAMESPACE
