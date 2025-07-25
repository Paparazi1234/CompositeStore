#include "ordinary_txn_lock_tracker.h"

namespace COMPOSITE_STORE_NAMESPACE {

void OrdinaryTxnLockTracker::TrackKey(const std::string& key, bool exclusive) {
  auto res = tracked_key_infos_.try_emplace(key, exclusive);
  if (!res.second) {    // not first time tracked，maybe update exclusive
    auto iter = res.first;
    iter->second.exclusive = iter->second.exclusive | exclusive;
  }
}

void OrdinaryTxnLockTracker::Clear() {
  tracked_key_infos_.clear();
}

uint64_t OrdinaryTxnLockTracker::NumTrackedKeys() const {
  return tracked_key_infos_.size();
}

bool OrdinaryTxnLockTracker::IsKeyAlreadyTracked(const std::string& key,
                                                 bool* exclusive) const {
  *exclusive = false;
  bool already_tracked = false;
  auto iter = tracked_key_infos_.find(key);
  if (iter != tracked_key_infos_.end()) {
    *exclusive = iter->second.exclusive;
    already_tracked = true;
  }
  return already_tracked;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
