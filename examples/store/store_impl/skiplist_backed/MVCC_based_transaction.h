#pragma once

#include "write_batch.h"
#include "../../transaction_store.h"
#include "../../transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCBasedTransaction : public Transaction {
 public:
  // No copying allowed
  MVCCBasedTransaction(const MVCCBasedTransaction&) = delete;
  MVCCBasedTransaction& operator=(const MVCCBasedTransaction&) = delete;

  MVCCBasedTransaction(TransactionStore* transaction_store)
      : transaction_store_(transaction_store) {}
  ~MVCCBasedTransaction() {}

  virtual Status Put(const std::string& key, const std::string& value) override; 
  virtual Status Delete(const std::string& key) override;

  virtual Status Get(const ReadOptions& read_options, const std::string& key,
                     std::string* value) override;

  virtual void SetSnapshot() override;

 private:
  Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

  WriteBatch write_batch_;
  TransactionStore* transaction_store_;
};

class WriteCommittedTransaction : public MVCCBasedTransaction {
 public:
  // No copying allowed
  WriteCommittedTransaction(const WriteCommittedTransaction&) = delete;
  WriteCommittedTransaction& operator=(
      const WriteCommittedTransaction&) = delete;

  WriteCommittedTransaction(TransactionStore* transaction_store)
      : MVCCBasedTransaction(transaction_store) {}
  ~WriteCommittedTransaction() {}

  virtual Status Prepare() override;
  virtual Status Commit() override;
  virtual Status Rollback() override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
