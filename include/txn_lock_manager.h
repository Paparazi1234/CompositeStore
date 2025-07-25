#pragma once

#include <string>

#include "status.h"
#include "util/system_clock.h"

namespace COMPOSITE_STORE_NAMESPACE {

class TxnLockTracker {
 public:
  // No copying allowed
  TxnLockTracker(const TxnLockTracker&) = delete;
  TxnLockTracker& operator=(const TxnLockTracker&) = delete;

  TxnLockTracker() {}
  virtual ~TxnLockTracker() {}

  virtual void TrackKey(const std::string& key, bool exclusive) = 0;
  virtual void Clear() = 0;

  virtual uint64_t NumTrackedKeys() const = 0;
  virtual bool IsKeyAlreadyTracked(const std::string& key,
                                   bool* exclusive) const = 0;

  class TrackedKeyIterator {
   public:
    virtual ~TrackedKeyIterator() {}

    virtual bool HasNext() const = 0;
    virtual const std::string& Next() = 0;
  };
  virtual TrackedKeyIterator* GetTrackedKeyIterator() const = 0;
};

class TxnLockManager {
 public:
  // No copying allowed
  TxnLockManager(const TxnLockManager&) = delete;
  TxnLockManager& operator=(const TxnLockManager&) = delete;

  TxnLockManager() {}
  virtual ~TxnLockManager() {}

  virtual Status TryLock(uint64_t txn_id, const std::string& key,
                         bool exclusive, int64_t timeout_time_ms) = 0;
  virtual void UnLock(uint64_t txn_id, const std::string& key) = 0;
  virtual void UnLock(uint64_t txn_id, const TxnLockTracker& tracker) = 0;

  virtual uint64_t NumLocks() const = 0;
};

// Factory
class TxnLockTrackerFactory {
 public:
  virtual ~TxnLockTrackerFactory() {}

  virtual TxnLockTracker* CreateTxnLockTracker() const = 0;
};

class TxnLockManagerFactory {
 public:
  virtual ~TxnLockManagerFactory() {}

  virtual TxnLockManager* CreateTxnLockManager(
      SystemClock* system_clock, int64_t num_locks_limit = -1)const = 0;
};

class OrdinaryTxnLockTrackerFactory : public TxnLockTrackerFactory {
 public:
  OrdinaryTxnLockTrackerFactory() {}
  ~OrdinaryTxnLockTrackerFactory() {}

  TxnLockTracker* CreateTxnLockTracker() const override;
};

class OrdinaryTxnLockManagerFactory : public TxnLockManagerFactory {
 public:
  explicit OrdinaryTxnLockManagerFactory(uint32_t num_shards = 16)
      : num_shards_(num_shards) {}
  ~OrdinaryTxnLockManagerFactory() {}

  TxnLockManager* CreateTxnLockManager(
      SystemClock* system_clock, int64_t num_locks_limit) const override;

 private:
  uint32_t num_shards_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
