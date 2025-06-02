#pragma once

#include <assert.h>
#include <stdexcept>
#include <memory>

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
  virtual Version* MiniUncommittedVersion(Version* reused) const override;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& version, const Snapshot& snapshot) const override;

};

// a cache that keep track of recently committed versions
class CommitCache {
 public:
  CommitCache(uint32_t const commit_cache_size_bits)
      : COMMIT_CACHE_SIZE_BITS(commit_cache_size_bits),
        COMMIT_CACHE_SIZE(1<<COMMIT_CACHE_SIZE_BITS),
        FORMAT(commit_cache_size_bits) {
    commit_cache_ = std::unique_ptr<std::atomic<CommitEntry64b>[]>(
        new std::atomic<CommitEntry64b>[COMMIT_CACHE_SIZE] {});
  }

  struct CommitEntry {
    uint64_t prep_seq;
    uint64_t commit_seq;
    CommitEntry() : prep_seq(0), commit_seq(0) {}
    CommitEntry(uint64_t ps, uint64_t cs) : prep_seq(ps), commit_seq(cs) {}
    bool operator==(const CommitEntry& rhs) const {
      return prep_seq == rhs.prep_seq && commit_seq == rhs.commit_seq;
    }
  };

  struct CommitEntry64bFormat {
    explicit CommitEntry64bFormat(size_t index_bits)                              // INDEX_BITS = 23
        : INDEX_BITS(index_bits),                                                 // PREP_BITS = 64 - 8 - 23 = 33
          PREP_BITS(static_cast<size_t>(64 - PAD_BITS - INDEX_BITS)),             // COMMIT_BITS = 64 - 33 = 31
          COMMIT_BITS(static_cast<size_t>(64 - PREP_BITS)),                       // COMMIT_FILTER = 1 << 31 - 1 （即31个bit 1）
          COMMIT_FILTER(static_cast<uint64_t>((1ull << COMMIT_BITS) - 1)),        // DELTA_UPPERBOUND = 1 << 31
          DELTA_UPPERBOUND(static_cast<uint64_t>((1ull << COMMIT_BITS))) {}
    // Number of higher bits of a sequence number that is not used. They are
    // used to encode the value type, ...
    const size_t PAD_BITS = static_cast<size_t>(0);
    // Number of lower bits from prepare seq that can be skipped as they are
    // implied by the index of the entry in the array
    const size_t INDEX_BITS;
    // Number of bits we use to encode the prepare seq
    const size_t PREP_BITS;
    // Number of bits we use to encode the commit seq.
    const size_t COMMIT_BITS;     // 即delta bit，这些bit用于保存commit_seq - prepare_seq的差值
    // Filter to encode/decode commit seq
    const uint64_t COMMIT_FILTER;
    // The value of commit_seq - prepare_seq + 1 must be less than this bound
    const uint64_t DELTA_UPPERBOUND;
  };

  struct CommitEntry64b {
    constexpr CommitEntry64b() noexcept : rep_(0) {}

    CommitEntry64b(const CommitEntry& entry, const CommitEntry64bFormat& format)
        : CommitEntry64b(entry.prep_seq, entry.commit_seq, format) {}

    CommitEntry64b(const uint64_t ps, const uint64_t cs,
                  const CommitEntry64bFormat& format) {
      assert(ps < static_cast<uint64_t>(
                      (1ull << (format.PREP_BITS + format.INDEX_BITS))));
      assert(ps <= cs);
      uint64_t delta = cs - ps + 1;  // make initialized delta always >= 1
      // zero is reserved for uninitialized entries
      assert(0 < delta);
      assert(delta < format.DELTA_UPPERBOUND);
      if (delta >= format.DELTA_UPPERBOUND) {
        throw std::runtime_error(
            "commit_seq >> prepare_seq. The allowed distance is " +
            std::to_string(format.DELTA_UPPERBOUND) + " commit_seq is " +
            std::to_string(cs) + " prepare_seq is " + std::to_string(ps));
      }
      rep_ = (ps << format.PAD_BITS) & ~format.COMMIT_FILTER;
      rep_ = rep_ | delta;   // |...prepare_seq(33bits)...|...delta(31bits)...|
    }

    // Return false if the entry is empty
    // 由CommitEntry64b转换为对应的CommitEntry结果
    // 本函数返回false：表示本CommitEntry64b在commit cache中为未初始化的状态；
    bool Parse(const uint64_t indexed_seq, CommitEntry* entry,
              const CommitEntry64bFormat& format) {
      uint64_t delta = rep_ & format.COMMIT_FILTER;   // 获取commit seq - prepare seq + 1 
      // zero is reserved for uninitialized entries
      assert(delta < static_cast<uint64_t>((1ull << format.COMMIT_BITS)));
      if (delta == 0) {   // delta为0表示本CommitEntry64b在commit cache中为未初始化的状态
        return false;  // initialized entry would have non-zero delta
      }

      assert(indexed_seq < static_cast<uint64_t>((1ull << format.INDEX_BITS)));
      uint64_t prep_up = rep_ & ~format.COMMIT_FILTER;    // 取prepare的高位部分
      prep_up >>= format.PAD_BITS;
      const uint64_t& prep_low = indexed_seq;   // indexed_seq即为prepare的低位部分
      entry->prep_seq = prep_up | prep_low;   // 高位部分和低位部分按位与即为prepare seq

      entry->commit_seq = entry->prep_seq + delta - 1;    // 根据prepare seq和delta计算commit seq
      return true;
    }

   private:
    uint64_t rep_;    // 在一个64bits内编码prepare seq和commit seq
  };

  bool GetCommitEntry(const uint64_t indexed_seq, CommitEntry64b* entry_64b,
                      CommitEntry* entry) const;

  bool ExchangeCommitEntry(const uint64_t indexed_seq,
                           CommitEntry64b& expected_entry,
                           const CommitEntry& new_entry);

 private:
  const uint32_t COMMIT_CACHE_SIZE_BITS;
  const uint32_t COMMIT_CACHE_SIZE;
  const CommitEntry64bFormat FORMAT;
  std::unique_ptr<std::atomic<CommitEntry64b>[]> commit_cache_;
};

// a heap that keep track of recently prepared versions
class PreparedHeap {

};

class DelayedPrepared {

};

}   // namespace MULTI_VERSIONS_NAMESPACE
