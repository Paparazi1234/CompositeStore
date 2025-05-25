#include "sequence_based/seq_based_multi_versions.h"
#include "sequence_based/write_prepared_seq_based_multi_versions.h"
#include "sequence_based/seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

// write committed
MultiVersionsManager* WCSeqBasedMultiVersionsManagerFactory::
    CreateMultiVersionsManager() {
  return new WriteCommittedSeqBasedMultiVersionsManager();
}

SnapshotManager* WCSeqBasedMultiVersionsManagerFactory::
    CreateSnapshotManager(MultiVersionsManager* multi_versions_manager) {
  SeqBasedMultiVersionsManager* sbmvm =
      reinterpret_cast<SeqBasedMultiVersionsManager*>(multi_versions_manager);
  return new WriteCommittedSeqBasedSnapshotManager(sbmvm);
}

// write prepared
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
