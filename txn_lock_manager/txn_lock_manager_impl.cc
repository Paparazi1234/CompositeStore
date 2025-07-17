#include "empty_txn_lock_manager.h"

namespace COMPOSITE_STORE_NAMESPACE {

TxnLockManager* EmptyTxnLockManagerFactory::CreateTxnLockManager() const {
  return new EmptyTxnLockManager();
}

}   // namespace COMPOSITE_STORE_NAMESPACE
