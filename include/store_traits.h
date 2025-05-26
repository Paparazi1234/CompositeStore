#pragma once

#include "multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

enum StoreBackedType : unsigned char {
  kSkipListBacked = 0x0,
  kMaxStoreBackedType
};

enum TxnStoreWritePolicy : unsigned char {
  WRITE_COMMITTED = 0x0,
  WRITE_PREPARED = 0x1
};

enum TxnLockManagerType : unsigned char {
  kEmptyTxnLoxkManager = 0x0
};

struct StoreTraits {
  StoreBackedType backed_type = kSkipListBacked;
  TxnStoreWritePolicy txn_write_policy = WRITE_COMMITTED;
  TxnLockManagerType txn_lock_manager_type = kEmptyTxnLoxkManager;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
