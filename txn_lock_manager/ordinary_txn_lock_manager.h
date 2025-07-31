#pragma once

#include "composite_store/txn_lock_manager.h"

#include <vector>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <assert.h>
#include <functional>

#include "utils/system_clock.h"
#include "port/port.h"

namespace COMPOSITE_STORE_NAMESPACE {

struct LockMeta {
  LockMeta(const uint64_t _txn_id, bool _exclusive)
      : exclusive(_exclusive) {
    helding_txn_ids.push_back(_txn_id);
  }

  LockMeta(const LockMeta& other)
      : helding_txn_ids(other.helding_txn_ids),
        exclusive(other.exclusive) {}

  LockMeta& operator=(const LockMeta& other) {
    helding_txn_ids = other.helding_txn_ids;
    exclusive = other.exclusive;
    return *this;
  }

  std::vector<uint64_t> helding_txn_ids;
  bool exclusive;
};

struct LockTableShard {
  LockTableShard() : shard_condvar(&shard_mutex) {}

  port::Mutex shard_mutex;
  port::CondVar shard_condvar;

  std::unordered_map<std::string, LockMeta> key_locks;
};

class LockTable {
 public:
  LockTable(uint32_t num_shards, int64_t num_locks_limit)
      : num_shards_(num_shards),
        num_locks_limit_(num_locks_limit) {
    lock_table_shards_.reserve(num_shards_);
    for (uint32_t i = 0; i < num_shards_; ++i) {
      lock_table_shards_.push_back(new LockTableShard());
    }
  }

  ~LockTable() {
    for (auto shard_ptr : lock_table_shards_) {
      delete shard_ptr;
    }
  } 

  uint32_t GetShardIndex(const std::string& key) const {
    std::hash<std::string> hash_func;     // Todo：修改为更加好的算法
    uint64_t hash_val = hash_func(key);
    return hash_val % num_shards_;
  }

  LockTableShard* GetShard(uint32_t shard_index) const {
    assert(shard_index < num_shards_);
    return lock_table_shards_[shard_index];
  }

  uint32_t NumShards() const {
    return num_shards_;
  }

  uint64_t NumLocks() const {
    return num_locks_.load();
  }

  Status EnterShard(LockTableShard* shard, int64_t timeout_time_ms);
  void ExitShard(LockTableShard* shard);

  Status WaitUnlockEventOnShardUntil(LockTableShard* shard, uint64_t end_ts_us);
  void NotifyUnlockFinishedOnShard(LockTableShard* shard);

  Status TryLockKeyOnShard(LockTableShard* shard, uint64_t txn_id,
                           const std::string& key, bool exclusive);
  void UnlockKeyOnShard(LockTableShard* shard, uint64_t txn_id,
                        const std::string& key);

 private:
  const uint32_t num_shards_;
  std::vector<LockTableShard*> lock_table_shards_;

  const int64_t num_locks_limit_;
  std::atomic<int64_t> num_locks_ = {0};
};

class OrdinaryTxnLockManager : public TxnLockManager {
 public:
  // No copying allowed
  OrdinaryTxnLockManager(const OrdinaryTxnLockManager&) = delete;
  OrdinaryTxnLockManager& operator=(const OrdinaryTxnLockManager&) = delete;

  OrdinaryTxnLockManager(SystemClock* system_clock, uint32_t num_shards,
                         int64_t num_locks_limit = -1)
      : system_clock_(system_clock),
        lock_table_(num_shards, num_locks_limit) {}
  ~OrdinaryTxnLockManager() {}

  Status TryLock(uint64_t txn_id, const std::string& key, bool exclusive,
                 int64_t timeout_time_ms) override;
  void UnLock(uint64_t txn_id, const std::string& key) override;
  void UnLock(uint64_t txn_id, const TxnLockTracker& tracker) override;

  uint64_t NumLocks() const override;

 private:
  SystemClock* system_clock_;

  LockTable lock_table_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
