#pragma once

#include <string>

namespace MULTI_VERSIONS_NAMESPACE {

class TransactionOptions {
  
};

class Transaction {
 public:
  // No copying allowed
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  virtual ~Transaction() {}

  virtual int Put(const std::string& key, const std::string& value) = 0; 
  virtual int Delete(const std::string& key) = 0;
  virtual int Get(const std::string& key, std::string* value) = 0;

  virtual int Prepare() = 0;
  virtual int Commit() = 0;
  virtual int Rollback() = 0;
  virtual void SetSnapshot() = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
