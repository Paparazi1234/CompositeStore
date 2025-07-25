#include "ordinary_txn_lock_tracker.h"
#include "ordinary_txn_lock_manager.h"

namespace COMPOSITE_STORE_NAMESPACE {

TxnLockTracker* OrdinaryTxnLockTrackerFactory::CreateTxnLockTracker() const {
  return new OrdinaryTxnLockTracker();
}

TxnLockManager* OrdinaryTxnLockManagerFactory::CreateTxnLockManager(
    SystemClock* system_clock, int64_t num_locks_limit) const {
  return new OrdinaryTxnLockManager(system_clock,
                                    num_shards_, num_locks_limit);
}

}   // namespace COMPOSITE_STORE_NAMESPACE
