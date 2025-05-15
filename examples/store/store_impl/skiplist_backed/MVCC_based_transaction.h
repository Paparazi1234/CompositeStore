#pragma once

#include "transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCBasedTransaction : public Transaction {
 public:
  // No copying allowed
  MVCCBasedTransaction(const MVCCBasedTransaction&) = delete;
  MVCCBasedTransaction& operator=(const MVCCBasedTransaction&) = delete;

  MVCCBasedTransaction();
  ~MVCCBasedTransaction() {}

  virtual Status Put(const std::string& key, const std::string& value) override; 
  virtual Status Delete(const std::string& key) override;

  virtual Status Get(const std::string& key, std::string* value) override;

  virtual Status Prepare() override;
  virtual Status Commit() override;
  virtual Status Rollback() override;
  virtual void SetSnapshot() override;
 private:
  
};

}   // namespace MULTI_VERSIONS_NAMESPACE
