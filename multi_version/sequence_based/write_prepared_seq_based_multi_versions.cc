#include "write_prepared_seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void WritePreparedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& v) {

}

void WritePreparedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& base, size_t count) {

}

void WritePreparedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& v) {

}

void WritePreparedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& base, size_t count) {

}

void WritePreparedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& v) {

}
void WritePreparedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& base, size_t count) {

}

Version* WritePreparedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion() const {
  return new SeqBasedVersion(0);
}

bool WritePreparedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const {
  return true;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
