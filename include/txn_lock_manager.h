#pragma once

#include <string>

#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TxnLockManager {
 public:
  // No copying allowed
  TxnLockManager(const TxnLockManager&) = delete;
  TxnLockManager& operator=(const TxnLockManager&) = delete;

  TxnLockManager() {}
  virtual ~TxnLockManager() {}

  virtual Status TryLock(const std::string& key) = 0;
  virtual void UnLock(const std::string& key) = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
