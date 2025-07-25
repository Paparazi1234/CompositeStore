#pragma once

#include "include/txn_lock_manager.h"

#include <unordered_map>

namespace COMPOSITE_STORE_NAMESPACE {

struct TrackedKeyInfo {
  explicit TrackedKeyInfo(bool ex) : exclusive(ex) {}

  bool exclusive;
};

using TrackedKeyInfos = std::unordered_map<std::string, TrackedKeyInfo>;

class OrdinaryTxnLockTracker : public TxnLockTracker {
 public:
  OrdinaryTxnLockTracker() {}
  ~OrdinaryTxnLockTracker() {}

  void TrackKey(const std::string& key, bool exclusive) override;
  void Clear() override;

  uint64_t NumTrackedKeys() const override;
  bool IsKeyAlreadyTracked(const std::string& key,
                           bool* exclusive) const override;

  TrackedKeyIterator* GetTrackedKeyIterator() const override {
    return new TrackedKeyInfosIterator(tracked_key_infos_);
  }

 private:
  class TrackedKeyInfosIterator : public TrackedKeyIterator {
   public:
    TrackedKeyInfosIterator(const TrackedKeyInfos& infos)
        : infos_(infos), iter_(infos_.begin()) {}
    ~TrackedKeyInfosIterator() {}

    bool HasNext() const override {
      return (iter_ != infos_.end());
    }

    const std::string& Next() override {
      return (iter_++)->first;
    }
  
   private:
    const TrackedKeyInfos& infos_;
    TrackedKeyInfos::const_iterator iter_;
  };

  TrackedKeyInfos tracked_key_infos_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
