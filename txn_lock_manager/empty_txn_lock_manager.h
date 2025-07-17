#pragma once

#include "include/txn_lock_manager.h"

namespace COMPOSITE_STORE_NAMESPACE {

class EmptyTxnLockManager : public TxnLockManager {
 public:
  // No copying allowed
  EmptyTxnLockManager(const EmptyTxnLockManager&) = delete;
  EmptyTxnLockManager& operator=(const EmptyTxnLockManager&) = delete;

  EmptyTxnLockManager() {}
  ~EmptyTxnLockManager() {}

  virtual Status TryLock(const std::string& key) override;
  virtual void UnLock(const std::string& key) override;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
