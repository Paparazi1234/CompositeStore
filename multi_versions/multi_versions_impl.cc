#include "sequence_based/write_committed/write_committed_multi_versions.h"
#include "sequence_based/write_committed/write_committed_snapshot.h"
#include "sequence_based/write_prepared/write_prepared_multi_versions.h"
#include "sequence_based/write_prepared/write_prepared_snapshot.h"

namespace COMPOSITE_STORE_NAMESPACE {

// write committed
MultiVersionsManager* WriteCommittedMultiVersionsManagerFactory::
    CreateMultiVersionsManager() const {
  return new WriteCommittedMultiVersionsManager(enable_two_write_queues_);
}

SnapshotManager* WriteCommittedMultiVersionsManagerFactory::
    CreateSnapshotManager(MultiVersionsManager* multi_versions_manager) const {
  SeqBasedMultiVersionsManager* sbmvm =
      static_cast_with_check<SeqBasedMultiVersionsManager>(
          multi_versions_manager);
  return new WriteCommittedSnapshotManager(sbmvm);
}

// write prepared
MultiVersionsManager* WritePreparedMultiVersionsManagerFactory::
    CreateMultiVersionsManager() const {
  return new WritePreparedMultiVersionsManager(commit_table_options_,
                                               enable_two_write_queues_);
}

SnapshotManager* WritePreparedMultiVersionsManagerFactory::
    CreateSnapshotManager(MultiVersionsManager* multi_versions_manager) const {
  SeqBasedMultiVersionsManager* sbmvm =
      static_cast_with_check<SeqBasedMultiVersionsManager>(
          multi_versions_manager);
  return new WritePreparedSnapshotManager(sbmvm);
}

}   // namespace COMPOSITE_STORE_NAMESPACE
