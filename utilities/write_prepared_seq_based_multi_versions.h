#pragma once

#include "seq_based_multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WritePreparedSeqBasedMultiVersionsManager :
  public SeqBasedMultiVersionsManager {

 public:
  // No copying allowed
  WritePreparedSeqBasedMultiVersionsManager(
      const WritePreparedSeqBasedMultiVersionsManager&) = delete;
  WritePreparedSeqBasedMultiVersionsManager& operator=(
      const WritePreparedSeqBasedMultiVersionsManager&) = delete;

  ~WritePreparedSeqBasedMultiVersionsManager() {}

  void PrepareVersion(const Version& v) override;
  void PrepareVersion(const Version& base, size_t count) override;
  void CommitVersion(const Version& v) override;
  void CommitVersion(const Version& base, size_t count) override;
  void RollbackVersion(const Version& v) override;
  void RollbackVersion(const Version& base, size_t count) override;
  const Version& MiniUncommittedVersion() const override;
  bool IsVersionVisibleToSnapshot(
      const Version& v, const Snapshot& s) const override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
