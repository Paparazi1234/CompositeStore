#include "write_prepared_seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

const Version& WritePreparedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion() const {

}

bool WritePreparedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const {
    
}

MultiVersionsManager* WPSeqBasedMultiVersionsManagerFactory::
    CreateMultiVersionsManager() {
  return new WritePreparedSeqBasedMultiVersionsManager();
}

SnapshotManager* WPSeqBasedMultiVersionsManagerFactory::
    CreateSnapshotManager(MultiVersionsManager* multi_versions_manager) {
  SeqBasedMultiVersionsManager* sbmvm =
      reinterpret_cast<SeqBasedMultiVersionsManager*>(multi_versions_manager);
  return new WritePreparedSeqBasedSnapshotManager(sbmvm);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
