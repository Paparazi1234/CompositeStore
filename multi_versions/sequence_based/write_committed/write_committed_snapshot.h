#pragma once

#include "multi_versions/sequence_based/seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

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
  virtual const SeqBasedSnapshot* TakeSnapshotInternal(
      Snapshot* reused) override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
