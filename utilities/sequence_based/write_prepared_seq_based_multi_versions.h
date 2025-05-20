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

  virtual void PrepareVersion(const Version& v) override;
  virtual void PrepareVersion(const Version& base, size_t count) override;
  virtual void CommitVersion(const Version& v) override;
  virtual void CommitVersion(const Version& base, size_t count) override;
  virtual void RollbackVersion(const Version& v) override;
  virtual void RollbackVersion(const Version& base, size_t count) override;
  virtual Version* MiniUncommittedVersion() const override;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& v, const Snapshot& s) const override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
