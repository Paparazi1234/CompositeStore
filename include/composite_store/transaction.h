#pragma once

#include <string>

#include "options.h"
#include "status.h"

namespace COMPOSITE_STORE_NAMESPACE {

class TransactionStore;

struct TransactionOptions {
  // txn_duration_ms <= 0, means txn won't expired
  int64_t txn_duration_ms = -1;
  // lock_timeout_ms < 0: won't timeout; lock_timeout_ms == 0: just one locking
  // attempt; lock_timeout_ms > 0: try until lock_timeout_ms
  int64_t lock_timeout_ms = -1;
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

class TransactionFactory {
 public:
  virtual ~TransactionFactory() {}

  virtual Transaction* CreateTransaction(const WriteOptions& write_options, 
                                         const TransactionOptions& txn_options,
                                         TransactionStore* txn_store,
                                         Transaction* reused = nullptr) = 0;
};

class WriteCommittedTransactionFactory : public TransactionFactory {
 public:
  WriteCommittedTransactionFactory() {}
  ~WriteCommittedTransactionFactory() {}

  Transaction* CreateTransaction(const WriteOptions& write_options, 
                                 const TransactionOptions& txn_options,
                                 TransactionStore* txn_store,
                                 Transaction* reused = nullptr) override;
};

class WritePreparedTransactionFactory : public TransactionFactory {
 public:
  WritePreparedTransactionFactory() {}
  ~WritePreparedTransactionFactory() {}

  Transaction* CreateTransaction(const WriteOptions& write_options, 
                                 const TransactionOptions& txn_options,
                                 TransactionStore* txn_store,
                                 Transaction* reused = nullptr) override;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
