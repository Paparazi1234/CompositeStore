#pragma once

#include <assert.h>
#include <stdexcept>
#include <atomic>
#include <memory>
#include <deque>
#include <queue>
#include <set>
#include <map>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <algorithm>
#include <functional>

#include "seq_limits.h"
#include "include/store_traits.h"
#include "util/mutex_lock.h"
#include "port/likely.h"

namespace MULTI_VERSIONS_NAMESPACE {

using SequenceNumber = uint64_t;

class GetSnapshotsCallback {
 public:
  virtual ~GetSnapshotsCallback() {}
  virtual void GetSnapshots(uint64_t max,
                            std::vector<uint64_t>& snapshots) const = 0;
};

class TakeSnapshotCallback {
 public:
  virtual ~TakeSnapshotCallback() {}
  virtual void TakeSnapshot(uint64_t* snapshot_seq,
                            uint64_t* min_uncommitted) = 0;
};

class AdvanceMaxCommittedByOneCallback {
 public:
  virtual ~AdvanceMaxCommittedByOneCallback() {}
  virtual void AdvanceLatestVisibleByOne() = 0;
};

// a heap that keep track of recently prepared versions
class UnCommittedsHeap {
  // The mutex is required for push and pop from UnCommittedsHeap. ::erase will
  // use external synchronization via prepared_mutex_.
  port::Mutex push_pop_mutex_;    // 对本PreparedHeap的push和pop操作都需要持有push_pop_mutex_锁
  std::deque<uint64_t> heap_;     // main heap：使用push_pop_mutex_同步读写，用于保存所有已经执行prepare阶段的事务（对应这些事务的prepare seq），其中有的事务已经提交有的还没有提交，所有prepare seq升序排序
  std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>    // 小堆，使用prepared_mutex_同步读写，用于保存main heap中那些执行prepare阶段比较晚但是执行commit比较早的
      erased_heap_;                                                               // 事务（即erased_heap_中的事务都已经提交）；为什么需要该变量？这是因为PreparedHeap属于堆，因此具有堆的特性，为了实现
                                                                                  // 其O(1)的entry erase性能，因此将那些晚执行prepare但是早执行commit的事务保存在额外的erased_heap_中，以避免每次删除这样的事务时
                                                                                  // 都需要维护main heap（使用std::deque作为存储，升序排序），对这些事务的从main heap中的实际删除处理分摊到后续对main heap的pop操作中（即
                                                                                  // 后续需要对main heap执行pop时，检查erased_heap_的堆顶和main heap的堆顶是否相等，如果是，那么同时将二者的堆顶删除直到满足erased_heap_
                                                                                  // 的堆顶比main heap的堆顶大为止（if any），由此也可以知道，每次对PreparedHeap执行完pop操作之后，如果main heap中还有entry，那么堆顶
                                                                                  // 就是当前除开delayed_prepared_之外的最早执行prepare阶段且还没有commit的事务。
  std::atomic<uint64_t> heap_top_ = {kSeqNumberLimitsMax};   // 记录PreparedHeap当前的堆顶，如果heap_top_=kMaxSequenceNumber，表示PreparedHeap当前为空；每次对PreparedHeap执行pop时都需要维护heap_top_
  bool TEST_CRASH_ = false;
  public:
  ~UnCommittedsHeap() {
    if (!TEST_CRASH_) {
      assert(heap_.empty());
      assert(erased_heap_.empty());
    }
  }
  port::Mutex* push_pop_mutex() { return &push_pop_mutex_; }

  // 判断PreparedHeap是否为空，无锁操作
  // 1、当本函数返回true时，PreparedHeap肯定为空
  // 2、当本函数返回false时，返回的这一时刻，PreparedHeap不一定不为空，在加锁后对PreparedHeap继续访问时，需要double check（即empty()==false存在false positive）
  inline bool empty() const {
    return top() == kSeqNumberLimitsMax;
  }
  // Returns kSeqNumberLimitsMax if empty() and the smallest otherwise.
  // 获取PreparedHeap当前的堆顶，无锁操作
  // 1、如果PreparedHeap当前不为空，那么返回的是当前除开delayed_prepared_之外的最早执行prepare阶段且还没有commit的事务
  // 2、如果PreparedHeap当前为空，那么返回kMaxSequenceNumber
  inline uint64_t top() const {
    return heap_top_.load(std::memory_order_acquire);
  }

  // 本函数执行之前需要先持有push_pop_mutex_
  // 本函数当事务执行prepare阶段时调用：将该事务的prepare seq添加到PreparedHeap中
  inline void push(uint64_t v) {
    if (heap_.empty()) {    // 如果添加之前PreparedHeap为空，那么更新heap_top_，这里注意更新顺序：先更新heap_top_然后再插入heap_，这是因为对heap_top_的并发读是无锁的，如果先插入heap_然后再更新heap_top_，而恰好
      heap_top_.store(v, std::memory_order_release);    // 在这两步操作之间，存在并发的线程调用empty()判断PreparedHeap是否为空，此时empty()返回true，这就会导致该线程miss掉插入heap_的entry，这种情况不应该发生
    } else {                                            // 而如果先更新heap_top_然后再插入heap_，即使在这两步操作之间存在并发的线程调用empty()判断PreparedHeap是否为空，此时empty()返回false，当该并发线程需要继续
      assert(heap_top_.load() < v);                     // 处理本PreparedHeap时，需要持有prepared_mutex_，然后再执行double check(即在加锁之后再检查一遍PreparedHeap是否为空，此时可能会为空，因为在加锁的过程中
    }                                                   // PreparedHeap可能被其他并发的线程清空了，但这样不会发生出错，也就是说当调用empty()判断PreparedHeap是否为空时，如果返回false，并不代表当我们实际处理PreparedHeap时，其任然不为空)
    heap_.push_back(v);   // 添加到main heap中
  }

  // 从PreparedHeap中删除最老的prepare seq，同时维护erased_heap_
  // 1、本函数当事务执行commit或者rollback时调用；
  // 或者2、本函数当max_evicted_seq_需要更新时调用；
  void pop(bool locked = false) {
    if (!locked) {
      push_pop_mutex()->lock();
    }
    heap_.pop_front();    // 从main heap中删除最老的prepare seq
    while (!heap_.empty() && !erased_heap_.empty() &&                               // 如果main heap和erased_heap_都不为空，且main heap中当前的最老prepare seq大于等于erased_heap_.top()，
            // heap_.top() > erased_heap_.top() could happen if we have erased       // 那么一直删除erased_heap_中的prepare seq直到满足erased_heap_.top() > heap_.front()
            // a non-existent entry. Ideally the user should not do that but we
            // should be resilient against it.
            heap_.front() >= erased_heap_.top()) {
      if (heap_.front() == erased_heap_.top()) {    // 如果erased_heap_的top和main heap中的最老prepare seq相等，那么继续删除main heap中的最老prepare seq（因为erased_heap_中记录的都是已经提交的事务的prepa seq）
        heap_.pop_front();
      }
      uint64_t erased __attribute__((__unused__));
      erased = erased_heap_.top();    // 一直删除erased_heap_中的prepare seq直到满足erased_heap_.top() > heap_.front()
      erased_heap_.pop();
      // No duplicate prepare sequence numbers
      assert(erased_heap_.empty() || erased_heap_.top() != erased);
    }
    while (heap_.empty() && !erased_heap_.empty()) {    // 如果main heap为空，那么erased_heap_也需要为空
      erased_heap_.pop();
    }
    heap_top_.store(!heap_.empty() ? heap_.front() : kSeqNumberLimitsMax,
                    std::memory_order_release);
    if (!locked) {
      push_pop_mutex()->unlock();
    }
  }
  // Concurrrent calls needs external synchronization. It is safe to be called
  // concurrent to push and pop though.
  // 本函数当事务执行commit或者rollback时调用，将该事务从PreparedHeap中删除：
  // 由于本函数是commit或者rollback路径的热点函数，因此将其实现为O(1)复杂度
  // 1、如果该事务已经不在PreparedHeap中，很可能是在max_evicted_seq_更新时，将其移到了delayed_prepared_中，那么不需要处理；
  // 2、如果该事务为PreparedHeap的堆顶，那么直接将其从PreparedHeap中删除
  // 3、如果该事务不为PreparedHeap的堆顶，那么本次执行earse并没有实际将其从main heap中删除，而是将其记录erased_heap_中，对其从main heap的实际删除处理分摊到后续对main heap的pop处理中
  void erase(uint64_t seq) {
    if (!empty()) {
      auto top_seq = top();
      if (seq < top_seq) {    // 1、该事务已经不在PreparedHeap中，很可能是在max_evicted_seq_更新时，将其移到了delayed_prepared_中，那么不需要处理
        // Already popped, ignore it.
      } else if (top_seq == seq) {    // 2、如果该事务为PreparedHeap的堆顶，那么直接将其从PreparedHeap中删除
        pop();
#ifndef NDEBUG
        MutexLock lock(push_pop_mutex());
        assert(heap_.empty() || heap_.front() != seq);
#endif
      } else {  // 3、如果该事务不为PreparedHeap的堆顶，那么本次执行earse并没有实际将其从main heap中删除，而是将其记录erased_heap_中，对其从main heap的实际删除处理分摊到后续对main heap的pop处理中
        // Down the heap, remember to pop it later
        erased_heap_.push(seq);
      }
    }
  }

  void TEST_Crash() {
    TEST_CRASH_ = true;
  }
};

class RecentUnCommitteds {
 public:
  ~RecentUnCommitteds() {}

  bool GetMiniUnCommittedIfNotEmpty(uint64_t* min_uncommitted) {
    assert(min_uncommitted);
    const uint64_t top = uncommitteds_.top();
    const bool empty = (top == kSeqNumberLimitsMax);
    if (!empty) {
      *min_uncommitted = top;
    }
    return empty;
  }

  bool IsEmpty() const {
    return uncommitteds_.empty();
  }

  uint64_t Top() const {
    return uncommitteds_.top();
  }

  void Push(uint64_t uncommitted) {
    uncommitteds_.push(uncommitted);
  }

  void Pop(bool locked = false) {
    uncommitteds_.pop(locked);
  }

  void Earse(uint64_t uncommitted) {
    uncommitteds_.erase(uncommitted);
  }

  uint64_t MiniUnCommitted() const {
    return uncommitteds_.top();
  }

  void EnterExclusive() {
    uncommitteds_.push_pop_mutex()->lock();
  }

  void ExitExclusive() {
    uncommitteds_.push_pop_mutex()->unlock();
  }

  void TEST_Crash() {
    uncommitteds_.TEST_Crash();
  }

 private:
  UnCommittedsHeap uncommitteds_;
};

// keep track of long-live uncommitted versions
class LongLiveUnCommitteds {
 public:
  ~LongLiveUnCommitteds() {}

  void Add(uint64_t uncommitted) {
    longlive_uncommitteds_.insert(uncommitted);
  }

  void Earse(uint64_t uncommitted) {
    if (!longlive_uncommitteds_.empty()) {
      longlive_uncommitteds_.erase(uncommitted);
      auto it = committed_longlive_uncommitteds_.find(uncommitted);
      if (it != committed_longlive_uncommitteds_.end()) {
        committed_longlive_uncommitteds_.erase(it);
      }
      if (longlive_uncommitteds_.empty()) {
        SetEmpty(true);
      }
    }
  }

  void Commit(uint64_t uncommitted, uint64_t committed) {
    auto iter = longlive_uncommitteds_.find(uncommitted);
    if (iter != longlive_uncommitteds_.end()) {
      committed_longlive_uncommitteds_[uncommitted] = committed;
    }
  }

  uint64_t MiniUnCommitted() const {
    return *longlive_uncommitteds_.begin();
  }

  bool GetCommittedOfVersion(uint64_t version, uint64_t* committed) const {
    assert(committed);
    *committed = kSeqNumberLimitsMax;
    bool exists = false;
    if (longlive_uncommitteds_.find(version) != longlive_uncommitteds_.end()) {
      exists = true;
      auto iter = committed_longlive_uncommitteds_.find(version);
      if (iter != committed_longlive_uncommitteds_.end()) {
        *committed = iter->second;
      }
    }
    assert((exists && *committed == kSeqNumberLimitsMax) ||
           (exists && *committed != kSeqNumberLimitsMax) ||
           (!exists && *committed == kSeqNumberLimitsMax));
    return exists;
  }

  inline bool IsEmpty() const {
    return longlive_uncommitteds_empty_.load(std::memory_order_acquire);
  }

  inline void SetEmpty(bool empty) {
    longlive_uncommitteds_empty_.store(empty, std::memory_order_release);
  }

 private:
  std::atomic<bool> longlive_uncommitteds_empty_ = {true};
  std::set<uint64_t> longlive_uncommitteds_;
  std::unordered_map<uint64_t, uint64_t> committed_longlive_uncommitteds_;
};

// a class that keep track of uncommitted versions
class UncommittedsTracker {
 public:
  UncommittedsTracker(const std::atomic<uint64_t>* future_history_boundary)
      : future_history_boundary_(future_history_boundary) {}
  ~UncommittedsTracker() {}

  void AddUnCommitted(uint64_t uncommitted, uint32_t count) {
    assert(count > 0);
    recent_uncommitteds_.EnterExclusive();
    for (uint32_t i = 0; i < count; ++i) {
      uint64_t uncommitted_version = uncommitted + i;
      recent_uncommitteds_.Push(uncommitted_version);   // 将目标事务添加到prepared_txns_中
      uint64_t future_history_boundary = future_history_boundary_->load();
      if (UNLIKELY(uncommitted_version <= future_history_boundary)) {   // 这种场景基本不会发生，如果出现这种场景，那么执行一次CheckPreparedAgainstMax，将prepared_txns_中小于等于future_max_evicted_seq_的未提交seq移动到delayed_prepared_中
        // This should not happen in normal case
        AdvanceHistoryBoundary(future_history_boundary, true /*locked*/);
      }
    }
    recent_uncommitteds_.ExitExclusive();
  }


  void EraseUnCommitted(uint64_t started, uint32_t count) {
    assert(count > 0);
    WriteLock write_lock(&uncommitteds_mutex_);   // 对prepared_mutex_加写锁，此时只有本线程能够访问prepared_txns_、delayed_prepared_和delayed_prepared_commits_
    for (uint32_t i = 0; i < count; ++i) {
      recent_uncommitteds_.Earse(started + i);
      longlive_uncommitteds_.Earse(started + i);
    }
  }

  void AdvanceHistoryBoundary(uint64_t new_history_boundary, bool locked) {
    // When max_evicted_seq_ advances, move older entries from prepared_txns_
    // to delayed_prepared_. This guarantees that if a seq is lower than max,
    // then it is not in prepared_txns_ and save an expensive, synchronized
    // lookup from a shared set. delayed_prepared_ is expected to be empty in
    // normal cases.
    uint64_t mini_recent_uncommitted;
    bool empty = recent_uncommitteds_.GetMiniUnCommittedIfNotEmpty(
        &mini_recent_uncommitted);

    // Preliminary check to avoid the synchronization cost
    if (!empty && mini_recent_uncommitted <= new_history_boundary) {
      if (locked) {
        // Needed to avoid double locking in pop().
        recent_uncommitteds_.ExitExclusive();
      }
      WriteLock write_lock(&uncommitteds_mutex_);
      // Need to fetch fresh values of ::top after mutex is acquired
      empty = recent_uncommitteds_.GetMiniUnCommittedIfNotEmpty(
          &mini_recent_uncommitted);
      while (!empty && mini_recent_uncommitted <= new_history_boundary) {
        longlive_uncommitteds_.Add(mini_recent_uncommitted);
        longlive_uncommitteds_.SetEmpty(false);
        // Update prepared_txns_ after updating delayed_prepared_empty_ otherwise
        // there will be a point in time that the entry is neither in
        // prepared_txns_ nor in delayed_prepared_, which will not be checked if
        // delayed_prepared_empty_ is false.
        recent_uncommitteds_.Pop();
        empty = recent_uncommitteds_.GetMiniUnCommittedIfNotEmpty(
            &mini_recent_uncommitted);
      }
      if (locked) {
        recent_uncommitteds_.EnterExclusive();
      }
    }
  }

  void CommitLongLiveUnCommitted(uint64_t uncommitted, uint64_t committed) {
    if (UNLIKELY(!longlive_uncommitteds_.IsEmpty())) {
      WriteLock write_lock(&uncommitteds_mutex_);
      longlive_uncommitteds_.Commit(uncommitted, committed);
    }
  }

  uint64_t MiniUnCommitted() const {
    uint64_t min_uncommitted = recent_uncommitteds_.MiniUnCommitted();
    if (!longlive_uncommitteds_.IsEmpty()) {
      ReadLock read_lock(&uncommitteds_mutex_);
      min_uncommitted = longlive_uncommitteds_.MiniUnCommitted();
    }
    return min_uncommitted;
  }

  bool GetCommittedOfVersion(uint64_t version, uint64_t* committed) const {
    ReadLock read_lock(&uncommitteds_mutex_);
    return longlive_uncommitteds_.GetCommittedOfVersion(version, committed);
  }

  bool IsLongLiveUnCommittedsEmpty() const {
    return longlive_uncommitteds_.IsEmpty();
  }

  void TEST_Crash() {
    recent_uncommitteds_.TEST_Crash();
  }

 private:
  const std::atomic<uint64_t>* const future_history_boundary_;
  mutable port::SharedMutex uncommitteds_mutex_;
  RecentUnCommitteds recent_uncommitteds_;
  LongLiveUnCommitteds longlive_uncommitteds_;
};

// a class that keeps track of recently committed versions
class RecentCommitteds {
 public:
  RecentCommitteds(const uint32_t commit_cache_size_bits)
      : commit_cache_(commit_cache_size_bits) {}
  // a lock-free cache that keep track of recently committed versions
  class CommitCache {
   public:
    CommitCache(const uint32_t commit_cache_size_bits)
        : COMMIT_CACHE_SIZE_BITS(commit_cache_size_bits),
          COMMIT_CACHE_SIZE(0x1<<COMMIT_CACHE_SIZE_BITS),
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
      const size_t PAD_BITS = static_cast<size_t>(8);
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

      CommitEntry64b(const CommitEntry& entry,
          const CommitEntry64bFormat& format)
          : CommitEntry64b(entry.prep_seq, entry.commit_seq, format) {}

      CommitEntry64b(const uint64_t ps, const uint64_t cs,
                    const CommitEntry64bFormat& format) {
        assert(ps < static_cast<uint64_t>(
                        (1ull << (format.PREP_BITS + format.INDEX_BITS))));
        // in write withot prepare and not enable two write queues case, we
        // have ps == cs
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

    bool GetCommitEntry(const uint64_t indexed_seq,
                        CommitEntry64b* entry_64b,
                        CommitEntry* entry) const {
      *entry_64b = commit_cache_[static_cast<size_t>(indexed_seq)].load(
          std::memory_order_acquire);
      bool valid = entry_64b->Parse(indexed_seq, entry, FORMAT);
      return valid;
    }

    bool ExchangeCommitEntry(const uint64_t indexed_seq,
                             CommitEntry64b& expected_entry_64b,
                             const CommitEntry& new_entry) {
      auto& atomic_entry = commit_cache_[static_cast<size_t>(indexed_seq)];
      CommitEntry64b new_entry_64b(new_entry, FORMAT);
      bool succ = atomic_entry.compare_exchange_strong(
          expected_entry_64b, new_entry_64b,
          std::memory_order_acq_rel, std::memory_order_acquire);
      return succ;
    }

    bool GetCommittedOfVersion(const uint64_t version,
                               uint64_t* committed) const {
      assert(committed);
      *committed = kSeqNumberLimitsMax;
      uint64_t index = version % COMMIT_CACHE_SIZE;
      CommitEntry64b entry_64b;
      CommitEntry entry;
      entry_64b = commit_cache_[static_cast<size_t>(index)].load(
          std::memory_order_acquire);
      bool exists = entry_64b.Parse(index, &entry, FORMAT);
      if (exists && entry.prep_seq == version) {
        *committed = entry.commit_seq;
      } else {  // not exists or exists but entry.prep_seq != version
        exists = false;
      }
      assert((exists && *committed != kSeqNumberLimitsMax) ||
             (!exists && *committed == kSeqNumberLimitsMax));
      return exists;
    }

    inline uint64_t GetLocation(const uint64_t seq) const {
      return seq % COMMIT_CACHE_SIZE;
    }

   private:
    const uint32_t COMMIT_CACHE_SIZE_BITS;
    const uint32_t COMMIT_CACHE_SIZE;
    const CommitEntry64bFormat FORMAT;
    std::unique_ptr<std::atomic<CommitEntry64b>[]> commit_cache_;
  };

  bool GetCommitEntryAt(const uint64_t index,
                        CommitCache::CommitEntry64b* entry_64b,
                        CommitCache::CommitEntry* entry) const {
    return commit_cache_.GetCommitEntry(index, entry_64b, entry);
  }

  bool UpdateCommitEntryAt(const uint64_t index,
                           CommitCache::CommitEntry64b& expected_entry_64b,
                           const CommitCache::CommitEntry& new_entry) {
    return commit_cache_.ExchangeCommitEntry(
        index, expected_entry_64b, new_entry);
  }

  bool GetCommittedOfVersion(const uint64_t version,
                             uint64_t* committed) const {
    return commit_cache_.GetCommittedOfVersion(version, committed);
  } 

  inline uint64_t GetLocation(const uint64_t version) const {
    return commit_cache_.GetLocation(version);
  }

 private:
  CommitCache commit_cache_;
};

// a class that keeps track of old committed versions
class HistoryCommitteds {
 public:
  HistoryCommitteds(const uint32_t snapshot_cache_size_bits,
                    const std::atomic<uint64_t>* history_boundary)
      : SNAPSHOT_CACHE_SIZE_BITS(snapshot_cache_size_bits),
        SNAPSHOT_CACHE_SIZE(0x1<<SNAPSHOT_CACHE_SIZE_BITS),
        history_boundary_(history_boundary) {
    long_live_snapshots_ = std::unique_ptr<std::atomic<SequenceNumber>[]>(
        new std::atomic<SequenceNumber>[SNAPSHOT_CACHE_SIZE] {});
  }
  ~HistoryCommitteds() {}

  void AddHistoryCommitted(const SequenceNumber& prepared_version,
                           const SequenceNumber& committed_version);

  void AdvanceHistoryBoundary(const SequenceNumber& new_boundary);

  bool IsVersionVisibleToSnapshot(const SequenceNumber& version,
                                  const SequenceNumber& snapshot,
                                  bool* snap_exists) const {
    return snap_invisible_versions_lower_bounds_.IsVersionVisibleToSnapshot(
        version, snapshot, snap_exists);
  }

  void SetSnapshotsRetrieveCallback(
      const GetSnapshotsCallback* get_snapshots_cb) {
    assert(get_snapshots_cb);
    get_snapshots_callback_.reset(get_snapshots_cb);
  }

 private:
  class SnapshotInvisibleVersionsLowerBounds {
   public:
    class SnapshotInvisibleVersionsLowerBound {
     public:
      inline void Add(const SequenceNumber& version) {
        invisible_lower_bound_versions.insert(
            std::upper_bound(invisible_lower_bound_versions.begin(),
                invisible_lower_bound_versions.end(), version), version);
      }
      inline bool IsVisible(const SequenceNumber& version) const {
        bool is_one_of_lower_bounds =
            std::binary_search(invisible_lower_bound_versions.begin(),
                              invisible_lower_bound_versions.end(), version);
        // if the target version isn't one of the invisible lower bounds, then it
        // is visible to the snapshot
        return !is_one_of_lower_bounds;
      }
     private:
      std::vector<SequenceNumber> invisible_lower_bound_versions;
    };

    void AddLowerBound(const SequenceNumber& snapshot,
                       const SequenceNumber& prepared_version,
                       const SequenceNumber& committed_version) {
      assert(prepared_version < committed_version);
      assert(prepared_version <= snapshot && snapshot < committed_version);
      WriteLock write_lock(&lower_bound_map_mutex_);
      lower_bound_map_empty_.store(false, std::memory_order_release);
      lower_bound_map_[snapshot].Add(prepared_version);
    }

    void EarseSnapshot(const SequenceNumber& snapshot) {
      bool need_gc = false;
      {
        ReadLock read_lock(&lower_bound_map_mutex_);
        need_gc = (lower_bound_map_.find(snapshot) != lower_bound_map_.end());
      }
      if (need_gc) {
        WriteLock write_lock(&lower_bound_map_mutex_);
        lower_bound_map_.erase(snapshot);
        lower_bound_map_empty_.store(lower_bound_map_.empty(),
                                    std::memory_order_release);
      }
    }

    // we need to insert the newly added snapshots in the map even though there
    // are no lower bound versions for them at the time such that the Read path 
    // can reach the newly added snapshots  
    void InitializeNewlyAdded(const std::vector<SequenceNumber>& snapshots) {
      assert(!snapshots.empty());
      WriteLock write_lock(&lower_bound_map_mutex_);
      for (auto snapshot : snapshots) {
        lower_bound_map_[snapshot];
      }
      lower_bound_map_empty_.store(false, std::memory_order_release);
    }
    
    bool IsVersionVisibleToSnapshot(const SequenceNumber& version,
                                    const SequenceNumber& snapshot,
                                    bool* snap_exists) const {
      assert(version <= snapshot);
      assert(snap_exists);
      *snap_exists = true;
      bool visible = false;
      if (IsEmpty()) {
        *snap_exists = false;
      } else {
        ReadLock read_lock(&lower_bound_map_mutex_);
        auto iter = lower_bound_map_.find(snapshot);
        if (iter == lower_bound_map_.end()) {
          *snap_exists = false;
        } else {
          visible = iter->second.IsVisible(version);
        }
      }
      assert((visible && *snap_exists == true) ||
             (!visible && *snap_exists == true) ||
             (!visible && *snap_exists == false));
      return visible;
    }

    bool IsEmpty() const {
      return lower_bound_map_empty_.load(std::memory_order_acquire);
    }
   private:
    mutable port::SharedMutex lower_bound_map_mutex_;
    std::map<SequenceNumber, SnapshotInvisibleVersionsLowerBound>
        lower_bound_map_;
    std::atomic<bool> lower_bound_map_empty_ = {true};
  };

  void UpdateLongLiveSnapshots(const std::vector<uint64_t>& snapshots,
                               const uint64_t version);
  void CleanupReleasedLongLiveSnapshots(
      const std::vector<uint64_t>& old_snapshots,
      const std::vector<uint64_t>& new_snapshots);
  bool UpdateSnapshotInvisibleVersionsLowerBounds(
      const uint64_t& prepared_version, const uint64_t& committed_version,
      const uint64_t& snapshot, const bool next_is_larger = true);

  const uint32_t SNAPSHOT_CACHE_SIZE_BITS;
  const uint32_t SNAPSHOT_CACHE_SIZE;
  const std::atomic<uint64_t>* const history_boundary_;
  std::unique_ptr<const GetSnapshotsCallback> get_snapshots_callback_;
  port::SharedMutex snapshots_mutex_;
  SequenceNumber snapshots_version_ = 0;
  std::unique_ptr<std::atomic<SequenceNumber>[]> long_live_snapshots_;
  std::vector<SequenceNumber> long_live_snapshots_extra_;
  std::vector<SequenceNumber> long_live_snapshots_all_;
  std::atomic<uint32_t> num_long_live_snapshots_ = {};

  SnapshotInvisibleVersionsLowerBounds snap_invisible_versions_lower_bounds_;
};

// a class that pretend to have infinite capacity to keep track of all committed
// versions, it's main purpose is used to determine whether a version 
// encountered during store reading is visible to a specific snapshot
class InfiniteCommitTable {
 public:
  InfiniteCommitTable(const CommitTableOptions& options,
                      const std::atomic<uint64_t>& max_readable_version,
                      const std::atomic<uint64_t>& max_committed_version)
      : MAX_CAS_RETRIES(options.max_CAS_retries),
        recent_committeds_(options.commit_cache_size_bits),
        history_committeds_(options.snapshot_cache_size_bits,
                            &(this->history_boundary_)),
        uncommitteds_tracker_(&(this->future_history_boundary_)),
        max_readable_version_(max_readable_version),
        max_committed_version_(max_committed_version) {
    HISTORY_BOUNDARY_INC_STEP =
        std::max((0x1u<<options.commit_cache_size_bits) / 100,
                 static_cast<uint32_t>(1));
  }
  
  ~InfiniteCommitTable() {}

  void AddCommittedVersion(uint64_t prepared_version,
                           uint64_t committed_version, uint32_t loop_cnt = 0);

  void AddUnCommittedVersion(uint64_t version, uint32_t count) {
    uncommitteds_tracker_.AddUnCommitted(version, count);
  }

  void EraseUnCommittedVersion(uint64_t started, uint32_t count) {
    uncommitteds_tracker_.EraseUnCommitted(started, count);
  }

  bool IsVersionVisibleToSnapshot(uint64_t version, uint64_t snapshot,
                                  uint64_t min_uncommitted,
                                  bool* snap_exists) const;

  void SetSnapshotsRetrieveCallback(
      const GetSnapshotsCallback* get_snapshots_cb) {
    history_committeds_.SetSnapshotsRetrieveCallback(get_snapshots_cb);
  }

  void SetAdvanceMaxCommittedByOneCallback(
      AdvanceMaxCommittedByOneCallback* callback) {
    advance_max_committed_by_one_cb.reset(callback);
  }

  class TakeSnapshotCallbackImpl : public TakeSnapshotCallback {
   public:
    TakeSnapshotCallbackImpl(InfiniteCommitTable* commit_table)
        :commit_table_(commit_table) {}
    ~TakeSnapshotCallbackImpl() {}
    void TakeSnapshot(uint64_t* snapshot_seq,
                      uint64_t* min_uncommitted) override {
      commit_table_->TakeSnapshot(snapshot_seq, min_uncommitted);
    }

   private:
    InfiniteCommitTable* commit_table_;
  };
  TakeSnapshotCallback* GetSnapshotCreationCallback() {
    return new TakeSnapshotCallbackImpl(this);
  }

  void TEST_Crash() {
    uncommitteds_tracker_.TEST_Crash();
  }

 private:
  void TakeSnapshot(uint64_t* snapshot_seq, uint64_t* min_uncommitted);
  uint64_t MiniUnCommittedVersion() const;
  uint64_t CalculateNewHistoryBoundry(uint64_t based) const;
  void AdvanceHistoryBoundary(const SequenceNumber& prev_boundary,
                              const SequenceNumber& new_boundary);

  inline uint64_t GetHistoryBoundry() const {
    return history_boundary_.load(std::memory_order_acquire);
  }

  uint64_t MaxCommittedVersion() const {
    return max_committed_version_.load(std::memory_order_seq_cst);
  }

  uint64_t MaxReadableVersion() const {
    return max_readable_version_.load(std::memory_order_acquire);
  }

  const uint32_t MAX_CAS_RETRIES;                                 
  uint32_t HISTORY_BOUNDARY_INC_STEP = 1;
  std::atomic<uint64_t> future_history_boundary_ = {};
  std::atomic<uint64_t> history_boundary_ = {};
  RecentCommitteds recent_committeds_;
  HistoryCommitteds history_committeds_;
  UncommittedsTracker uncommitteds_tracker_;

  const std::atomic<uint64_t>& max_readable_version_;
  const std::atomic<uint64_t>& max_committed_version_;
  std::unique_ptr<AdvanceMaxCommittedByOneCallback>
      advance_max_committed_by_one_cb;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
