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

struct CommitTableOptions {
  uint32_t max_CAS_retries = 100;
  uint32_t commit_cache_size_bits = 23;   // default: 8M entries
  uint32_t snapshot_cache_size_bits = 7;  // default: 128 entries
};

struct StoreTraits {
  StoreBackedType backed_type = kSkipListBacked;
  TxnStoreWritePolicy txn_write_policy = WRITE_COMMITTED;
  TxnLockManagerType txn_lock_manager_type = kEmptyTxnLoxkManager;
  CommitTableOptions commit_table_options;  // used for WRITE_PREPARED policy
};

}   // namespace MULTI_VERSIONS_NAMESPACE
