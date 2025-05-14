#pragma once

#include <atomic>

#include "multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedVersion : public Version {
 public:
  SeqBasedVersion(uint64_t seq) : rep_(seq) {}
  ~SeqBasedVersion() {}
  uint64_t Seq() const {
    return rep_;
  }

 private:
  uint64_t rep_;
};

class SeqBasedMultiVersionsManager : public MultiVersionsManager {
 public:
  // No copying allowed
  SeqBasedMultiVersionsManager(const SeqBasedMultiVersionsManager&) = delete;
  SeqBasedMultiVersionsManager&
      operator=(const SeqBasedMultiVersionsManager&) = delete;

  ~SeqBasedMultiVersionsManager() {}

  void Initialize(const Version& orig) override;
  const Version& AllocateVersion() override;
  void AdvanceVersionBy(size_t count) override;
  const Version& LatestVisibleVersion() const override;

 protected:
  std::atomic<uint64_t> seq_ = {};
};

class WriteCommittedSeqBasedMultiVersionsManager :
  public SeqBasedMultiVersionsManager {

 public:
  // No copying allowed
  WriteCommittedSeqBasedMultiVersionsManager(
      const WriteCommittedSeqBasedMultiVersionsManager&) = delete;
  WriteCommittedSeqBasedMultiVersionsManager& operator=(
      const WriteCommittedSeqBasedMultiVersionsManager&) = delete;

  ~WriteCommittedSeqBasedMultiVersionsManager() {}

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
