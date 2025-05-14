#pragma once

#include "skliplist_backed_in_memory_store.h"
#include "transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryTxnStore : public SkipListBackedInMemoryStore {
 public:
  // No copying allowed
  SkipListBackedInMemoryTxnStore(
      const SkipListBackedInMemoryTxnStore&) = delete;
  SkipListBackedInMemoryTxnStore& operator=(
      const SkipListBackedInMemoryTxnStore&) = delete;

  SkipListBackedInMemoryTxnStore();
  ~SkipListBackedInMemoryTxnStore() {}

  Status Put(const WriteOptions& write_options,
             const std::string& key, const std::string& value) override;
  Status Delete(const WriteOptions& write_options,
                const std::string& key) override;
  Status Get(const ReadOptions& read_options,
             const std::string& key, std::string* value) override;

  Transaction* BeginTransaction(const TransactionOptions& txnOptions,
                                const WriteOptions& wOptions);
  const Snapshot* TakeSnapshot(); 

 private:

};

}   // namespace MULTI_VERSIONS_NAMESPACE
