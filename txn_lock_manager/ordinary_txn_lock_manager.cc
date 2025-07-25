#include "ordinary_txn_lock_manager.h"

#include <algorithm>

namespace COMPOSITE_STORE_NAMESPACE {

Status LockTable::EnterShard(LockTableShard* shard, int64_t timeout_time_ms){
  Status s;
  bool locked = false;
  if (timeout_time_ms < 0) {
    shard->shard_mutex.Lock();
    locked = true;
  } else if (timeout_time_ms == 0) {
    locked = shard->shard_mutex.TryLock();
    if (!locked) {
      s = Status::Busy();
    }
  } else {
    locked = shard->shard_mutex.TryLockFor(timeout_time_ms * 1000);
    if (!locked) {
      s = Status::TimedOut();
    }
  }
  assert((locked == true && s.IsOK()) ||
         (locked == false && s.IsBusy()) ||
         (locked == false && s.IsTimedOut()));
  return s;
}
  
void LockTable::ExitShard(LockTableShard* shard){
  shard->shard_mutex.AssertHeld();
  shard->shard_mutex.Unlock();
}

Status LockTable::WaitUnlockEventOnShardUntil(LockTableShard* shard,
                                              uint64_t end_ts_us){
  shard->shard_mutex.AssertHeld();

  Status s;
  bool timeout = false;
  if (end_ts_us == 0) {     // won't timeout
    shard->shard_condvar.Wait();
  } else {
    timeout = shard->shard_condvar.TimedWait(end_ts_us);
    if (timeout) {
      s = Status::TimedOut();
    }
  }
  assert((timeout == false && s.IsOK()) ||
         (timeout == true && s.IsTimedOut()));
  return s;
}

void LockTable::NotifyUnlockFinishedOnShard(LockTableShard* shard) {
  shard->shard_condvar.SignalAll();
}

Status LockTable::TryLockKeyOnShard(LockTableShard* shard, uint64_t txn_id,
                                    const std::string& key, bool exclusive) {
  shard->shard_mutex.AssertHeld();
  
  Status s;
  auto lock_iter = shard->key_locks.find(key);
  if (lock_iter != shard->key_locks.end()) {  // the key is already locked
    LockMeta& lock_meta = lock_iter->second;
    assert(lock_meta.helding_txn_ids.size() == 1 || !lock_meta.exclusive);

    if (lock_meta.exclusive || exclusive) {
      if (lock_meta.helding_txn_ids.size() == 1 &&
          lock_meta.helding_txn_ids[0] == txn_id) { // the lock is and only
        lock_meta.exclusive = exclusive;            // locked by txn_id already
      } else {  // can't lock the key in this attempt
        s = Status::TimedOut();
      }
    } else {  // requesting shared access to a shared lock, so just grant it
      assert(!lock_meta.exclusive && !exclusive);
      auto txn_iter = std::find(lock_meta.helding_txn_ids.begin(),
                                lock_meta.helding_txn_ids.end(), txn_id);
      if (txn_iter == lock_meta.helding_txn_ids.end()) {
        lock_meta.helding_txn_ids.push_back(txn_id);
      }
    }
  } else {  // the key has not been locked yet
    if (num_locks_limit_ > 0 &&
        num_locks_.load(std::memory_order_acquire) >= num_locks_limit_) {
      s = Status::Busy();
    } else {  // no lock limits or under lock limits yet, so just grant it
      shard->key_locks.emplace(key, LockMeta(txn_id, exclusive));
      num_locks_.fetch_add(1, std::memory_order_seq_cst);
    }
  }
  return s;
}

void LockTable::UnlockKeyOnShard(LockTableShard* shard, uint64_t txn_id,
                                 const std::string& key) {
  shard->shard_mutex.AssertHeld();

  auto lock_iter = shard->key_locks.find(key);
  assert(lock_iter != shard->key_locks.end());

  std::vector<uint64_t>& helding_txn_ids = lock_iter->second.helding_txn_ids;
  if (helding_txn_ids.size() == 1) {
    assert(helding_txn_ids[0] == txn_id);
    // the key is only locked by txn_id, so just erase the lock
    shard->key_locks.erase(lock_iter);
    assert(num_locks_.load(std::memory_order_relaxed) > 0);
    num_locks_.fetch_sub(1, std::memory_order_seq_cst);
  } else {  // the key is not only locked by txn_id, so remove txn_id from lock
            // meta
    auto txn_iter = std::find(helding_txn_ids.begin(),
                              helding_txn_ids.end(), txn_id);
    assert(txn_iter != helding_txn_ids.end());
    auto last_iter = helding_txn_ids.end() - 1;
    if (txn_iter != last_iter) {
      *txn_iter = *last_iter;
    }
    helding_txn_ids.pop_back();
  }
}

Status OrdinaryTxnLockManager::TryLock(uint64_t txn_id,
                                       const std::string& key,
                                       bool exclusive,
                                       int64_t timeout_time_ms) {
  const uint64_t WONT_END = 0;
  uint64_t end_ts_us = WONT_END;
  if (timeout_time_ms > 0) {  // timeout_time_ms < 0, means lock until succeed
    end_ts_us = system_clock_->NowMicros() + timeout_time_ms * 1000;
  }

  uint32_t shard_index = lock_table_.GetShardIndex(key);
  LockTableShard* shard = lock_table_.GetShard(shard_index);
  Status s = lock_table_.EnterShard(shard, timeout_time_ms);
  if (!s.IsOK()) {
    return s;
  }

  // first attempt
  s = lock_table_.TryLockKeyOnShard(shard, txn_id, key, exclusive);
  if (!s.IsOK() && timeout_time_ms != 0) {
    bool timeout = (end_ts_us != WONT_END &&
                    system_clock_->NowMicros() >= end_ts_us);
    while (!s.IsOK() && !timeout) {
      s = lock_table_.WaitUnlockEventOnShardUntil(shard, end_ts_us);
      if (!s.IsOK()) {
        assert(s.IsTimedOut());
        // when timeout during WaitUnlockEventOnShard, we will do one more
        // attempt follow
        timeout = true;
      }

      s = lock_table_.TryLockKeyOnShard(shard, txn_id, key, exclusive);
      // if timeout during WaitUnlockEventOnShard, no need to re-calculate
      // and will break the loop no mater TryLockKeyOnShard succeed or not
      if (!timeout) {
        timeout = (end_ts_us != WONT_END &&
                   system_clock_->NowMicros() >= end_ts_us);
      }
    }
    assert((s.IsOK() && !timeout) ||
           (s.IsOK() && timeout) ||   // timeout when WaitUnlockEventOnShard
           (!s.IsOK() && timeout));   // but succeed after one more attempt
  } else {
    // first attempt succeed or first attempt failed but can only do one attempt
    assert(s.IsOK() || (!s.IsOK() && timeout_time_ms == 0));
  }

  lock_table_.ExitShard(shard);
  return s;
}

void OrdinaryTxnLockManager::UnLock(uint64_t txn_id, const std::string& key) {
  uint32_t shard_index = lock_table_.GetShardIndex(key);
  LockTableShard* shard = lock_table_.GetShard(shard_index);
  const int64_t WONT_TIMEOUT = -1;
  Status s = lock_table_.EnterShard(shard, WONT_TIMEOUT);
  assert(s.IsOK());
  lock_table_.UnlockKeyOnShard(shard, txn_id, key);
  lock_table_.ExitShard(shard);

  lock_table_.NotifyUnlockFinishedOnShard(shard);
}

void OrdinaryTxnLockManager::UnLock(uint64_t txn_id,
                                    const TxnLockTracker& tracker) {
  std::unordered_map<uint32_t, std::vector<const std::string*>> keys_by_shard(
      lock_table_.NumShards());
  std::unique_ptr<TxnLockTracker::TrackedKeyIterator> key_iter(
      tracker.GetTrackedKeyIterator());
  while (key_iter->HasNext()) {
    const std::string& key = key_iter->Next();
    uint32_t shard_index = lock_table_.GetShardIndex(key);
    keys_by_shard[shard_index].push_back(&key);
  }

  for (auto& shard_iter : keys_by_shard) {
    uint32_t shard_index = shard_iter.first;
    auto& shard_keys = shard_iter.second;

    LockTableShard* shard = lock_table_.GetShard(shard_index);
    const int64_t WONT_TIMEOUT = -1;
    Status s = lock_table_.EnterShard(shard, WONT_TIMEOUT);
    assert(s.IsOK());
    for (auto key : shard_keys) {
      lock_table_.UnlockKeyOnShard(shard, txn_id, *key);
    }
    lock_table_.ExitShard(shard);

    lock_table_.NotifyUnlockFinishedOnShard(shard);
  }
}

uint64_t OrdinaryTxnLockManager::NumLocks() const {
  return lock_table_.NumLocks();
}

}   // namespace COMPOSITE_STORE_NAMESPACE
