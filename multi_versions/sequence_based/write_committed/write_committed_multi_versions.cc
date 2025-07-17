#include "write_committed_multi_versions.h"

#include "write_committed_snapshot.h"

namespace COMPOSITE_STORE_NAMESPACE {

void WriteCommittedMultiVersionsManager::BeginPrepareVersions(
    const Version& /*prepared_uncommitted_started*/,
    uint32_t /*num_prepared_uncommitteds*/) {
}

void WriteCommittedMultiVersionsManager::EndPrepareVersions(
    const Version& /*end_uncommitted*/) {
}

void WriteCommittedMultiVersionsManager::BeginCommitVersions(
    const Version& /*prepared_uncommitted_started*/,
    const Version& /*committed*/,
    uint32_t /*num_prepared_uncommitteds*/) {
}

void WriteCommittedMultiVersionsManager::EndCommitVersions(
    const Version& /*prepared_uncommitted_started*/,
    const Version& committed,
    uint32_t /*num_prepared_uncommitteds*/) {
  AdvanceMaxVisibleVersion(committed);
}

void WriteCommittedMultiVersionsManager::BeginRollbackVersions(
    const Version& /*prepared_uncommitted_started*/,
    const Version& /*rollbacked_uncommitted_started*/,
    const Version& /*committed*/,
    uint32_t /*num_prepared_uncommitteds*/,
    uint32_t /*num_rollbacked_uncommitteds*/) {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
}

void WriteCommittedMultiVersionsManager::EndRollbackVersions(
    const Version& /*prepared_uncommitted_started*/,
    const Version& /*rollbacked_uncommitted_started*/,
    const Version& /*committed*/,
    uint32_t /*num_prepared_uncommitteds*/,
    uint32_t /*num_rollbacked_uncommitteds*/) {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
}

bool WriteCommittedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot, bool* snap_exists) const {
  *snap_exists = true;
  const SeqBasedVersion* version_impl =
      static_cast_with_check<const SeqBasedVersion>(&version);
  const SeqBasedSnapshot* snapshot_impl =
      static_cast_with_check<const SeqBasedSnapshot>(&snapshot);
  return version_impl->Seq() <= snapshot_impl->Seq();
}

}   // namespace COMPOSITE_STORE_NAMESPACE
