#include "sequence_based/seq_based_multi_versions.h"
#include "sequence_based/write_prepared_multi_versions.h"
#include "sequence_based/seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

// write committed
MultiVersionsManager* WriteCommittedMultiVersionsManagerFactory::
    CreateMultiVersionsManager() const {
  return new WriteCommittedMultiVersionsManager();
}

SnapshotManager* WriteCommittedMultiVersionsManagerFactory::
    CreateSnapshotManager(MultiVersionsManager* multi_versions_manager) const {
  SeqBasedMultiVersionsManager* sbmvm =
      reinterpret_cast<SeqBasedMultiVersionsManager*>(multi_versions_manager);
  return new WriteCommittedSnapshotManager(sbmvm);
}

// write prepared
MultiVersionsManager*  WritePreparedMultiVersionsManagerFactory::
    CreateMultiVersionsManager() const {
  return new WritePreparedMultiVersionsManager();
}

SnapshotManager* WritePreparedMultiVersionsManagerFactory::
    CreateSnapshotManager(MultiVersionsManager* multi_versions_manager) const {
  SeqBasedMultiVersionsManager* sbmvm =
      reinterpret_cast<SeqBasedMultiVersionsManager*>(multi_versions_manager);
  return new WritePreparedSnapshotManager(sbmvm);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
