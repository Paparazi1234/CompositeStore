#pragma once

#include "multi_versions.h"

namespace COMPOSITE_STORE_NAMESPACE {

enum class TxnStoreImplType : unsigned char {
  kDefault = 0x0,
  kMVCC = 0x1,
  kMaxStoreImplType
};

enum class MVCCWriteBufferBackedType : unsigned char {
  kSkipListBacked = 0x0,
  kMaxBackedType
};

enum class StagingWriteType : unsigned char {
  kOrderedMapBacked = 0x0,
  kMaxStagingWriteType
};

enum class ConcurrencyControlPolicy : unsigned char {
  kPessimisticConcurrencyControl = 0x0,
  kOptimisticConcurrencyControl = 0x1,
  kMaxConcurrencyControlPolicyType
};

enum class OptimisticTxnValidatePolicy : unsigned char {
  kValidateSerially = 0x0,
  kMaxValidatePolicyType
};

enum class TxnStoreWritePolicy : unsigned char {
  kWriteCommitted = 0x0,
  kWritePrepared = 0x1,
  kMaxWritePolicyType
};

enum class TxnLockManagerType : unsigned char {
  kOrdinaryTxnLockManager = 0x0,
  kMaxLockManagerType
};

enum class TxnLockTrackerType : unsigned char {
  kOrdinaryTxnLockTracker = 0x0,
  kMaxLockTrackerType
};

struct StoreTraits {
  TxnStoreImplType txn_store_impl_type = TxnStoreImplType::kDefault;
  MVCCWriteBufferBackedType write_buffer_backed_type =
      MVCCWriteBufferBackedType::kSkipListBacked;
  StagingWriteType staging_write_type = StagingWriteType::kOrderedMapBacked;
  ConcurrencyControlPolicy concurrency_control_Policy =
      ConcurrencyControlPolicy::kPessimisticConcurrencyControl;
  OptimisticTxnValidatePolicy optimistic_txn_validate_policy = 
      OptimisticTxnValidatePolicy::kValidateSerially;
  TxnStoreWritePolicy txn_store_write_policy =
      TxnStoreWritePolicy::kWriteCommitted;
  TxnLockManagerType txn_lock_manager_type =
      TxnLockManagerType::kOrdinaryTxnLockManager;
  TxnLockTrackerType txn_lock_tracker_type =
      TxnLockTrackerType::kOrdinaryTxnLockTracker;
  CommitTableOptions commit_table_options;  // used for kWritePrepared policy
};

}   // namespace COMPOSITE_STORE_NAMESPACE
