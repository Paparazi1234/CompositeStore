#include "write_prepared_seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void WritePreparedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& version) {

}

void WritePreparedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& base, uint32_t count) {

}

void WritePreparedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& version) {

}

void WritePreparedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& base, uint32_t count) {

}

void WritePreparedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& version) {

}
void WritePreparedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& base, uint32_t count) {

}

Version* WritePreparedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion() const {
  return new SeqBasedVersion(0);
}

bool WritePreparedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot) const {
  return true;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
