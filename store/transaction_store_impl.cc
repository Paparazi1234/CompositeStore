#include "skiplist_backed/skliplist_backed_in_memory_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TransactionStoreFactory {
 public:
  virtual ~TransactionStoreFactory() {}

  virtual TransactionStore* CreateTransactionStore() = 0;
};

class SkipListBackedInMemoryTxnStoreFactory : public TransactionStoreFactory {
 public:
  ~SkipListBackedInMemoryTxnStoreFactory() {}

  virtual TransactionStore* CreateTransactionStore() override {
    return new SkipListBackedInMemoryTxnStore();
  }
};

Status TransactionStore::Open(TransactionStore** store_ptr) {
  SkipListBackedInMemoryTxnStoreFactory slbimtsf;
  *store_ptr = slbimtsf.CreateTransactionStore();
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
