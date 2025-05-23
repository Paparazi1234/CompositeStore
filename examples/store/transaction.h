#pragma once

#include <string>

#include "options.h"
#include "../../include/status.h"

namespace MULTI_VERSIONS_NAMESPACE {

struct TransactionOptions {
  
};

class Transaction {
 public:
  // No copying allowed
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  Transaction() {}
  virtual ~Transaction() {}

  virtual Status Put(const std::string& key, const std::string& value) = 0; 
  virtual Status Delete(const std::string& key) = 0;

  virtual Status Get(const ReadOptions& read_options, const std::string& key,
                     std::string* value) = 0;

  virtual Status Prepare() = 0;
  virtual Status Commit() = 0;
  virtual Status Rollback() = 0;
  virtual void SetSnapshot() = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
