#pragma once

#include "include/transaction_store.h"
#include "include/txn_lock_manager.h"
#include "skliplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryTxnStore : public TransactionStore {
 public:
  // No copying allowed
  SkipListBackedInMemoryTxnStore(
      const SkipListBackedInMemoryTxnStore&) = delete;
  SkipListBackedInMemoryTxnStore& operator=(
      const SkipListBackedInMemoryTxnStore&) = delete;

  SkipListBackedInMemoryTxnStore();
  ~SkipListBackedInMemoryTxnStore() {}

  // non-transactional write
  virtual Status Put(const WriteOptions& write_options,
                     const std::string& key, const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                        const std::string& key) override;

  // non-transactional read
  virtual Status Get(const ReadOptions& read_options,
                  const std::string& key, std::string* value) override;
  
  virtual MultiVersionsManager* GetMultiVersionsManager() const override {
    return base_store_->GetMultiVersionsManager();
  }

  virtual Transaction* BeginTransaction(
      const TransactionOptions& txn_options,
      const WriteOptions& write_options) override;
  virtual const Snapshot* TakeSnapshot() override;

  virtual Status TryLock(const std::string& key) override;
  virtual void UnLock(const std::string& key) override;

 private:
  SkipListBackedInMemoryStore* base_store_;
  std::shared_ptr<TxnLockManager> txn_lock_manager_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
