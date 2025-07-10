#pragma once

#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class OptimisticTxnStore : public MVCCTxnStore {
 public:
  // No copying allowed
  OptimisticTxnStore(const OptimisticTxnStore&) = delete;
  OptimisticTxnStore& operator=(const OptimisticTxnStore&) = delete;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
