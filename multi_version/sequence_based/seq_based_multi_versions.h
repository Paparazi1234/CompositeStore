#pragma once

#include <atomic>
#include <assert.h>

#include "include/multi_versions.h"
#include "util/cast_util.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedVersion : public Version {
 public:
  SeqBasedVersion() : rep_(0) {}
  explicit SeqBasedVersion(uint64_t seq) : rep_(seq) {}
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

  virtual void DuplicateFrom(const Version& src) override {
    const SeqBasedVersion* src_impl =
        static_cast_with_check<const SeqBasedVersion>(&src);
    rep_ = src_impl->Seq();
  }

  // descending ordered by version
  virtual int CompareWith(const Version& rhs) const override {
    const SeqBasedVersion* rhs_impl =
        static_cast_with_check<const SeqBasedVersion>(&rhs);
    if (rep_ != rhs_impl->Seq()) {
      if (rep_ < rhs_impl->Seq()) {
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

  SeqBasedMultiVersionsManager(std::atomic<uint64_t>& max_readable,
                               std::atomic<uint64_t>& max_visible)
                               : max_readable_seq_(max_readable),
                                 max_visible_seq_(max_visible) {}
  ~SeqBasedMultiVersionsManager() {}

  virtual void Initialize(const Version& orig) override;
  virtual Version* AllocateVersion(uint32_t count, Version* reused) override;
  virtual Version* LatestVisibleVersion(Version* reused) const override;
  virtual Version* CreateVersion() const override {
    return new SeqBasedVersion();
  }

  virtual const Version& VersionLimitsMin() const override {
    return version_limits_min_;
  }

  virtual const Version& VersionLimitsMax() const override {
    return version_limits_max_;
  }

 protected:
  friend class MyTest;
  friend class InspectPessimisticTxnTests;
  void AdvanceMaxReadableVersion(const Version& version) {
    const SeqBasedVersion* version_impl =
        static_cast_with_check<const SeqBasedVersion>(&version);
    uint64_t new_max_readable = version_impl->Seq();
    assert(new_max_readable >= max_readable_seq_);
    max_readable_seq_.store(new_max_readable, std::memory_order_seq_cst);
  }

  void AdvanceMaxVisibleVersion(const Version& version) {
    const SeqBasedVersion* version_impl =
        static_cast_with_check<const SeqBasedVersion>(&version);
    uint64_t new_max_visible = version_impl->Seq();
    assert(new_max_visible >= max_visible_seq_);
    max_visible_seq_.store(new_max_visible, std::memory_order_seq_cst);
  }

  uint64_t MaxReadableVersion() const {
    return max_readable_seq_.load(std::memory_order_acquire);
  }

  uint64_t MaxVisibleVersion() const {
    return max_visible_seq_.load(std::memory_order_acquire);
  }

  virtual std::atomic<uint64_t>& MaxReadableVersionRep(
      bool enable_two_write_queues) = 0;
  virtual std::atomic<uint64_t>& MaxVisibleVersionRep(
      bool enable_two_write_queues) = 0;

  class SeqAllocator {
   public:
    SeqAllocator() : so_far_allocated_(0) {}

    uint64_t Allocate(uint32_t count) {
      uint64_t last =
          so_far_allocated_.fetch_add(count, std::memory_order_seq_cst);
      // +1 means the returned seq is the first seq that is variable to use
      return last + 1;
    }

    void Initialize(const uint64_t orig) {
      so_far_allocated_.store(orig, std::memory_order_seq_cst);
    }

    uint64_t SoFarAllocated() const {
      return so_far_allocated_.load(std::memory_order_seq_cst);
    }
   private:
    std::atomic<uint64_t> so_far_allocated_;  // monotonic increase
  };
  // the max seq that can be seen during Get internally, that is to say it's the
  // max Seq that appears in write buffer(seqs appear in WAL aren't always
  // readable if they haven't been written to write buffer yet and
  // max_readable_seq_ maybe not visible to user in write prepared at a certain
  // point in time)
  std::atomic<uint64_t>& max_readable_seq_;
  // the max seq that is visible to user at a certain point in time:
  // 1 in write committed: it's equivalent to max_readable_;
  // 2 in write prepared: it means the max committed seq, but doesn't mean that
  //   all seqs <= max_visible_seq_ are committed;
  //    2.1 
  std::atomic<uint64_t>& max_visible_seq_;

  SeqAllocator seq_allocator_;

 private:
  static const SeqBasedVersion version_limits_min_;
  static const SeqBasedVersion version_limits_max_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
