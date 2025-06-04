#include "write_prepared_multi_versions.h"

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void WritePreparedMultiVersionsManager::PrepareVersion(const Version& version) {

}

void WritePreparedMultiVersionsManager::PrepareVersion(const Version& base, 
                                                       uint32_t count) {

}

void WritePreparedMultiVersionsManager::CommitVersion(const Version& version) {

}

void WritePreparedMultiVersionsManager::CommitVersion(const Version& base, 
                                                      uint32_t count) {

}

void WritePreparedMultiVersionsManager::RollbackVersion(
    const Version& version) {

}
void WritePreparedMultiVersionsManager::RollbackVersion(const Version& base,
                                                        uint32_t count) {

}

Version* WritePreparedMultiVersionsManager::MiniUncommittedVersion(
    Version* reused) const {
  return new SeqBasedVersion(0);
}

bool WritePreparedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot) const {
  return true;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
