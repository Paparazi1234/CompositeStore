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

  void EncodeTo(std::string& dest) override;
  void DecodeFrom(const std::string& input) override;
  
  int CompareWith(const Version& rhs) override;

 private:
  // friend class SeqBasedMultiVersionsManager;

  uint64_t rep_;
};

class SeqBasedMultiVersionsManager : public MultiVersionsManager {
 public:
  // No copying allowed
  SeqBasedMultiVersionsManager(const SeqBasedMultiVersionsManager&) = delete;
  SeqBasedMultiVersionsManager&
      operator=(const SeqBasedMultiVersionsManager&) = delete;

  SeqBasedMultiVersionsManager() {}
  ~SeqBasedMultiVersionsManager() {}

  virtual void Initialize(const Version& orig) override;
  virtual Version* ConstructVersion(const Version& base, size_t i) override;
  virtual void AdvanceVersionBy(size_t count) override;
  virtual const Version& LatestVisibleVersion() const override;

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

  WriteCommittedSeqBasedMultiVersionsManager() {}
  ~WriteCommittedSeqBasedMultiVersionsManager() {}

  virtual void PrepareVersion(const Version& v) override;
  virtual void PrepareVersion(const Version& base, size_t count) override;
  virtual void CommitVersion(const Version& v) override;
  virtual void CommitVersion(const Version& base, size_t count) override;
  virtual void RollbackVersion(const Version& v) override;
  virtual void RollbackVersion(const Version& base, size_t count) override;
  virtual const Version& MiniUncommittedVersion() const override;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& v, const Snapshot& s) const override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
