#include "empty_txn_lock_manager.h"

namespace MULTI_VERSIONS_NAMESPACE {

TxnLockManager* EmptyTxnLockManagerFactory::CreateTxnLockManager() {
  return new EmptyTxnLockManager();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
