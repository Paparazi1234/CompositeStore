#include "empty_txn_lock_manager.h"

namespace COMPOSITE_STORE_NAMESPACE {

Status EmptyTxnLockManager::TryLock(const std::string& key) {
  return Status::OK();
}

void EmptyTxnLockManager::UnLock(const std::string& key) {
  return;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
