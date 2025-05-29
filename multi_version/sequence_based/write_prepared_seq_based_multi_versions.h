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

  WritePreparedSeqBasedMultiVersionsManager() {}
  ~WritePreparedSeqBasedMultiVersionsManager() {}

  virtual void PrepareVersion(const Version& version) override;
  virtual void PrepareVersion(const Version& base, uint32_t count) override;
  virtual void CommitVersion(const Version& version) override;
  virtual void CommitVersion(const Version& base, uint32_t count) override;
  virtual void RollbackVersion(const Version& version) override;
  virtual void RollbackVersion(const Version& base, uint32_t count) override;
  virtual Version* MiniUncommittedVersion() const override;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& version, const Snapshot& snapshot) const override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
