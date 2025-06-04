#pragma once

#include <atomic>

#include "include/multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedVersion : public Version {
 public:
  SeqBasedVersion() : rep_(0) {}
  SeqBasedVersion(uint64_t seq) : rep_(seq) {}
  ~SeqBasedVersion() {}

  uint64_t Seq() const {
    return rep_;
  }

  void SetSeq(uint64_t seq) {
    rep_ = seq;
  }

  virtual void IncreaseBy(uint32_t count) override {
    if (count != 0) {
      rep_ += count;
    }
  }

  virtual void IncreaseByOne() override {
    rep_++;
  }

  // descending ordered by version
  virtual int CompareWith(const Version& rhs) const override {
    const SeqBasedVersion* version_rhs =
      reinterpret_cast<const SeqBasedVersion*>(&rhs);
    if (rep_ != version_rhs->Seq()) {
      if (rep_ < version_rhs->Seq()) {
        return +1;
      }
      return -1;
    }
    return 0;
  }

  virtual void EncodeTo(std::string* dest) const override {
    *dest = std::to_string(rep_);
  }

  virtual void DecodeFrom(const std::string& input) override {
    rep_ = std::stoull(input);
  }

 private:
  uint64_t rep_;
};

class SeqBasedMultiVersionsManager : public MultiVersionsManager {
 public:
  // No copying allowed
  SeqBasedMultiVersionsManager(const SeqBasedMultiVersionsManager&) = delete;
  SeqBasedMultiVersionsManager& operator=(
      const SeqBasedMultiVersionsManager&) = delete;

  SeqBasedMultiVersionsManager() {}
  ~SeqBasedMultiVersionsManager() {}

  virtual void Initialize(const Version& orig) override;
  virtual Version* CreateVersion() const override;
  virtual Version* LatestVisibleVersion(Version* reused) const override;

 protected:
  std::atomic<uint64_t> seq_ = {};
};

class WriteCommittedMultiVersionsManager : public SeqBasedMultiVersionsManager {

 public:
  // No copying allowed
  WriteCommittedMultiVersionsManager(
      const WriteCommittedMultiVersionsManager&) = delete;
  WriteCommittedMultiVersionsManager& operator=(
      const WriteCommittedMultiVersionsManager&) = delete;

  WriteCommittedMultiVersionsManager() {}
  ~WriteCommittedMultiVersionsManager() {}

  virtual void PrepareVersion(const Version& version) override;
  virtual void PrepareVersion(const Version& base, uint32_t count) override;
  virtual void CommitVersion(const Version& version) override;
  virtual void CommitVersion(const Version& base, uint32_t count) override;
  virtual void RollbackVersion(const Version& version) override;
  virtual void RollbackVersion(const Version& base, uint32_t count) override;
  virtual Version* MiniUncommittedVersion(Version* reused) const override;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& version, const Snapshot& snapshot) const override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
