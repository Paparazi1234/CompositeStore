#pragma once

#include "infinite_commit_table.h"
#include "seq_based_multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WritePreparedMultiVersionsManager : public SeqBasedMultiVersionsManager {
 public:
  // No copying allowed
  WritePreparedMultiVersionsManager(
      const WritePreparedMultiVersionsManager&) = delete;
  WritePreparedMultiVersionsManager& operator=(
      const WritePreparedMultiVersionsManager&) = delete;

  WritePreparedMultiVersionsManager(const CommitTableOptions& options)
      : commit_table_(options) {}
  ~WritePreparedMultiVersionsManager() {}

  virtual void PrepareVersion(const Version& version) override;
  virtual void PrepareVersion(const Version& base, uint32_t count) override;
  virtual void CommitVersion(const Version& version) override;
  virtual void CommitVersion(const Version& base, uint32_t count) override;
  virtual void RollbackVersion(const Version& version) override;
  virtual void RollbackVersion(const Version& base, uint32_t count) override;
  virtual Version* MiniUncommittedVersion(Version* reused) const override;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& version, const Snapshot& snapshot) const override;

  void SetSnapshotsRetrieveFunc(const GetSnapshotsFunc& get_snapshots_func) {
    commit_table_.SetSnapshotsRetrieveFunc(get_snapshots_func);
  }
 private:
  InfiniteCommitTable commit_table_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
